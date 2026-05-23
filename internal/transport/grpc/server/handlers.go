// Package server serves the replication gRPC API.
package server

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/transmogr/transmogr/internal/models"
	transformers "github.com/transmogr/transmogr/internal/replication/transformers"
	grpcprotocol "github.com/transmogr/transmogr/internal/transport/grpc/protocol"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE
type replicationService interface {
	// Handshake builds the initial handshake payload for a newly opened stream.
	Handshake(ctx context.Context) models.HandshakeMessage
	// SendSnapshotToPeer streams snapshot chunks to the remote peer using the provided send callback.
	SendSnapshotToPeer(
		ctx context.Context,
		peerRegion string,
		table string,
		afterPrimaryKey []models.PrimaryKeyPart,
		send func(models.SnapshotChunkMessage) error,
	) error
	// RecordMsgSent records one outbound gRPC message by type.
	RecordMsgSent(msgType int)
	// RecordMsgReceived records one inbound gRPC message by type.
	RecordMsgReceived(msgType int)
}

type ackOutbox interface {
	Subscribe(ctx context.Context, peerRegion string) (<-chan models.OutboxBatch, func(), error)
	ListPending(ctx context.Context, peerRegion string, limit int) ([]models.OutboxBatch, error)
	ListPendingWindow(
		ctx context.Context,
		peerRegion string,
		lowerExclusive, upperInclusive time.Time,
		afterID uint64,
		limit int,
	) ([]models.OutboxBatch, error)
	Ack(ctx context.Context, peerRegion, table, lastEventID string) (bool, error)
	SnapshotWatermark(ctx context.Context) (time.Time, error)
}

// Handler implements the replication gRPC service handlers.
type Handler struct {
	peerv1.UnimplementedReplicationServiceServer

	service            replicationService
	outbox             ackOutbox
	pipeline           transformers.Outbound
	localRegion        string
	allowedPeerRegions map[string]struct{}
}

const replayPendingPageSize = 256

// Consume streams this region's replication feed to one remote consumer.
func (h *Handler) Consume(req *peerv1.ConsumeRequest, stream peerv1.ReplicationService_ConsumeServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	h.service.RecordMsgReceived(grpcprotocol.MessageMetricType(req))

	// Match the outbox subscriber capacity so this handoff does not become the
	// tighter in-memory bottleneck between publishLive and stream.Send.
	sendQueue := make(chan *peerv1.ConsumeResponse, models.LiveReplicationBufferSize)
	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	localHandshake := h.service.Handshake(ctx)
	remoteRegion, err := h.handleConsumeRequest(ctx, req, localHandshake)
	if err != nil {
		return err
	}
	var unsubscribe func()
	defer func() {
		cancel()
		if unsubscribe != nil {
			unsubscribe()
		}
		close(sendQueue)
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- h.sendLoop(stream, sendQueue)
	}()

	sendQueue <- grpcprotocol.NewHandshakeMessage(localHandshake)

	snapshotRequests := grpcprotocol.SnapshotRequestsFromConsume(req)
	snapshotWatermark := time.Time{}
	if len(snapshotRequests) > 0 {
		snapshotWatermark, err = h.outbox.SnapshotWatermark(ctx)
		if err != nil {
			return status.Errorf(codes.Internal, "snapshot watermark: %v", err)
		}
		for _, request := range snapshotRequests {
			if err := h.sendSnapshot(ctx, remoteRegion, request, snapshotWatermark, sendQueue); err != nil {
				return err
			}
		}
	}

	liveBatches, unsubscribe, err := h.outbox.Subscribe(ctx, remoteRegion)
	if err != nil {
		return status.Errorf(codes.Internal, "subscribe served outbox: %v", err)
	}

	liveWatermark, err := h.outbox.SnapshotWatermark(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "live watermark: %v", err)
	}

	if len(snapshotRequests) > 0 {
		if err := h.replayPendingBatches(ctx, remoteRegion, time.Time{}, snapshotWatermark, sendQueue); err != nil {
			return err
		}
		if err := h.replayPendingBatches(ctx, remoteRegion, snapshotWatermark, liveWatermark, sendQueue); err != nil {
			return err
		}
		for _, request := range snapshotRequests {
			select {
			case <-ctx.Done():
				return nil
			case sendQueue <- grpcprotocol.ToProtoMessage(models.StreamMessage{
				PeerRegion: remoteRegion,
				SnapshotComplete: &models.SnapshotCompleteMessage{
					Table: request.Table,
				},
			}):
			}
		}
	} else {
		if err := h.replayPendingBatches(ctx, remoteRegion, time.Time{}, liveWatermark, sendQueue); err != nil {
			return err
		}
	}

	if err := h.forwardLiveBatchesStart(ctx, liveBatches, liveWatermark, sendQueue, errCh, &wg); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err == nil {
			return nil
		}
		return status.Errorf(codes.Unavailable, "send loop: %v", err)
	}
}

// Ack advances the sender's outbox cursor after the peer confirms delivery.
func (h *Handler) Ack(
	ctx context.Context,
	req *peerv1.AckRequest,
) (*peerv1.AckResponse, error) {
	h.service.RecordMsgReceived(grpcprotocol.MessageMetricType(req))
	if err := h.validateRemoteRegion(ctx, req.GetRegion()); err != nil {
		return nil, err
	}
	if h.outbox == nil {
		return nil, status.Error(codes.FailedPrecondition, "outbox ack handler is not configured")
	}
	// Acks advance delivery state for batches previously served to this consumer region.
	if _, err := h.outbox.Ack(ctx, req.GetRegion(), req.GetTable(), req.GetLastEventId()); err != nil {
		return nil, status.Errorf(codes.Internal, "ack outbox batch: %v", err)
	}
	return &peerv1.AckResponse{}, nil
}

func (h *Handler) replayPendingBatches(
	ctx context.Context,
	remoteRegion string,
	lowerExclusive time.Time,
	upperInclusive time.Time,
	sendQueue chan<- *peerv1.ConsumeResponse,
) error {
	var afterID uint64
	for {
		pending, err := h.outbox.ListPendingWindow(
			ctx,
			remoteRegion,
			lowerExclusive,
			upperInclusive,
			afterID,
			replayPendingPageSize,
		)
		if err != nil {
			return status.Errorf(codes.Internal, "list pending outbox for %s: %v", remoteRegion, err)
		}
		if len(pending) == 0 {
			return nil
		}

		for _, batch := range pending {
			message, err := h.batchToProto(ctx, batch)
			if err != nil {
				return status.Errorf(codes.Internal, "encode pending outbox batch: %v", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case sendQueue <- message:
			}
			afterID = batch.ID
		}
	}
}

func (h *Handler) forwardLiveBatches(
	ctx context.Context,
	batches <-chan models.OutboxBatch,
	lowerExclusive time.Time,
	sendQueue chan<- *peerv1.ConsumeResponse,
) error {
	for batch := range batches {
		if !batch.CreatedAt.IsZero() && !batch.CreatedAt.After(lowerExclusive) {
			continue
		}
		message, err := h.batchToProto(ctx, batch)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case sendQueue <- message:
		}
	}
	return nil
}

func (h *Handler) forwardLiveBatchesStart(
	ctx context.Context,
	liveBatches <-chan models.OutboxBatch,
	lowerExclusive time.Time,
	sendQueue chan<- *peerv1.ConsumeResponse,
	errCh chan<- error,
	wg *sync.WaitGroup,
) error {
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- h.forwardLiveBatches(ctx, liveBatches, lowerExclusive, sendQueue)
	}()
	return nil
}

func (h *Handler) batchToProto(ctx context.Context, batch models.OutboxBatch) (*peerv1.ConsumeResponse, error) {
	batch, err := h.pipeline.PrepareBatch(ctx, batch)
	if err != nil {
		return nil, err
	}

	if batch.TxID != "" {
		return grpcprotocol.ToProtoMessage(models.StreamMessage{
			PeerRegion: batch.PeerRegion,
			TransactionBatch: &models.TransactionBatchMessage{
				TxID:     batch.TxID,
				Sequence: batch.Sequence,
				Events:   batch.Events,
			},
		}), nil
	}

	return grpcprotocol.ToProtoMessage(models.StreamMessage{
		PeerRegion: batch.PeerRegion,
		EventBatch: &models.EventBatchMessage{
			Sequence: batch.Sequence,
			Events:   batch.Events,
		},
	}), nil
}

func (h *Handler) sendSnapshot(
	ctx context.Context,
	remoteRegion string,
	request models.SnapshotRequest,
	watermark time.Time,
	sendQueue chan<- *peerv1.ConsumeResponse,
) error {
	err := h.service.SendSnapshotToPeer(
		ctx,
		remoteRegion,
		request.Table,
		request.AfterPrimaryKey,
		func(chunk models.SnapshotChunkMessage) error {
			chunk.Watermark = watermark
			rows, err := h.pipeline.PrepareSnapshotRows(ctx, request.Table, chunk.Rows)
			if err != nil {
				return err
			}
			chunk.Rows = rows
			msg := grpcprotocol.ToProtoMessage(models.StreamMessage{
				PeerRegion:    remoteRegion,
				SnapshotChunk: &chunk,
			})
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sendQueue <- msg:
				return nil
			}
		},
	)
	if err != nil {
		if errors.Is(err, models.ErrLeaseNotAcquired) {
			return status.Errorf(codes.Unavailable, "send snapshot: %v", err)
		}
		slog.Error("failed to send snapshot",
			"peer_region", remoteRegion,
			"table", request.Table,
			"error", err,
		)
		return status.Errorf(codes.Internal, "send snapshot: %v", err)
	}
	return nil
}

func withinWindow(value, lowerExclusive, upperInclusive time.Time) bool {
	if value.IsZero() {
		return false
	}
	if !lowerExclusive.IsZero() && !value.After(lowerExclusive) {
		return false
	}
	if !upperInclusive.IsZero() && value.After(upperInclusive) {
		return false
	}
	return true
}
