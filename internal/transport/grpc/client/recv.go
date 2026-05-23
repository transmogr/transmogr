package client

import (
	"context"
	"log/slog"
	"time"

	"github.com/transmogr/transmogr/internal/models"
	grpcprotocol "github.com/transmogr/transmogr/internal/transport/grpc/protocol"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

// recvLoop reads the server-emitted replication feed for one peer region and
// dispatches each message until the stream is closed or an error occurs.
func (m *Manager) recvLoop(
	ctx context.Context,
	peerRegion string,
	localHandshake models.HandshakeMessage,
	client peerv1.ReplicationServiceClient,
	stream peerv1.ReplicationService_ConsumeClient,
) error {
	handshakeReceived := false

	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		m.replication.RecordMsgReceived(grpcprotocol.MessageMetricType(message))

		// Discard any non-handshake messages that arrive before the server handshake.
		// The server may pipeline feed data before we process its identity message.
		if !handshakeReceived {
			if _, ok := message.GetMsg().(*peerv1.ConsumeResponse_Handshake); !ok {
				slog.Warn("dropping message received before server handshake",
					"peer_region", peerRegion,
					"msg_type", grpcprotocol.MessageMetricType(message),
				)
				continue
			}
		}

		switch payload := message.GetMsg().(type) {
		case *peerv1.ConsumeResponse_Handshake:
			if err := m.handleHandshake(peerRegion, localHandshake, payload.Handshake); err != nil {
				return err
			}
			handshakeReceived = true

		case *peerv1.ConsumeResponse_SnapshotChunk:
			rows, err := grpcprotocol.ToSnapshotRows(payload.SnapshotChunk.GetRows())
			if err != nil {
				return err
			}
			rows, err = m.pipeline.PrepareSnapshotRows(ctx, payload.SnapshotChunk.GetTable(), rows)
			if err != nil {
				return err
			}
			watermark := time.Time{}
			if ts := payload.SnapshotChunk.GetWatermark(); ts != nil {
				watermark = ts.AsTime()
			}
			if err := m.replication.ApplySnapshotChunk(
				ctx,
				peerRegion,
				payload.SnapshotChunk.GetTable(),
				rows,
				payload.SnapshotChunk.GetLastChunk(),
				watermark,
			); err != nil {
				return err
			}

		case *peerv1.ConsumeResponse_SnapshotComplete:
			if err := m.replication.CompleteSnapshot(ctx, peerRegion, payload.SnapshotComplete.GetTable()); err != nil {
				return err
			}

		case *peerv1.ConsumeResponse_EventBatch:
			if err := m.handleEventBatch(ctx, client, payload.EventBatch); err != nil {
				return err
			}

		case *peerv1.ConsumeResponse_TransactionBatch:
			if err := m.handleTransactionBatch(ctx, client, payload.TransactionBatch); err != nil {
				return err
			}

		case *peerv1.ConsumeResponse_Heartbeat:
			_ = payload
		}
	}
}

// handleHandshake validates the server's handshake against the local configuration.
func (m *Manager) handleHandshake(
	_ string,
	localHandshake models.HandshakeMessage,
	hs *peerv1.Handshake,
) error {
	return grpcprotocol.CheckHandshakeCompatibility(localHandshake, hs)
}

// handleEventBatch decodes, applies, and acknowledges one event batch from the peer feed.
func (m *Manager) handleEventBatch(
	ctx context.Context,
	client peerv1.ReplicationServiceClient,
	batch *peerv1.EventBatch,
) error {
	events, err := grpcprotocol.ToReplicationEvents(batch.GetEvents())
	if err != nil {
		return err
	}
	events, err = m.pipeline.PrepareEvents(ctx, events)
	if err != nil {
		return err
	}
	if err := m.replication.ApplyEventBatch(ctx, events); err != nil {
		return err
	}

	ackTable := ""
	if len(events) > 0 {
		ackTable = events[0].Table
	}
	return m.sendAck(ctx, client, ackTable, grpcprotocol.LastProtoEventID(batch.GetEvents()))
}

// handleTransactionBatch decodes, applies, and acknowledges one transaction batch from the peer feed.
func (m *Manager) handleTransactionBatch(
	ctx context.Context,
	client peerv1.ReplicationServiceClient,
	batch *peerv1.TransactionBatch,
) error {
	events, err := grpcprotocol.ToReplicationEvents(batch.GetEvents())
	if err != nil {
		return err
	}
	events, err = m.pipeline.PrepareEvents(ctx, events)
	if err != nil {
		return err
	}
	if err := m.replication.ApplyTransactionBatch(ctx, events); err != nil {
		return err
	}
	return m.sendAck(ctx, client, "", grpcprotocol.LastProtoEventID(batch.GetEvents()))
}

func (m *Manager) sendAck(
	ctx context.Context,
	client peerv1.ReplicationServiceClient,
	table, lastEventID string,
) error {
	request := grpcprotocol.NewAckRequest(m.localRegion, table, lastEventID)
	m.replication.RecordMsgSent(grpcprotocol.MessageMetricType(request))
	_, err := client.Ack(ctx, request)
	return err
}
