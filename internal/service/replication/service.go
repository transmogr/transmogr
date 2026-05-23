package replication

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/transmogr/transmogr/internal/models"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

const streamBusPublishTimeout = time.Second

type (
	snapshotService interface {
		// SendToPeer streams local snapshot chunks to a remote peer.
		SendToPeer(
			ctx context.Context,
			peerRegion string,
			table string,
			afterPrimaryKey []models.PrimaryKeyPart,
			send func(models.SnapshotChunk) error,
		) error
		// ApplyIncomingChunk applies one inbound snapshot chunk to the local store.
		ApplyIncomingChunk(ctx context.Context, peerRegion, table string, chunk models.SnapshotChunk) error
		// CompleteIncomingSnapshot marks one inbound snapshot bootstrap as finished.
		CompleteIncomingSnapshot(ctx context.Context, peerRegion, table string) error
	}

	replicationState interface {
		// CommitStateAppliedTransaction atomically applies all events in a CDC transaction and advances replication state.
		CommitStateAppliedTransaction(
			ctx context.Context,
			events []models.ReplicationEvent,
			mode models.ReplicationApplyMode,
		) error
		// GetStateCursor loads the persisted replication cursor for a peer and table.
		GetStateCursor(ctx context.Context, peerRegion, table string) (models.StateCursor, error)
		// GetStateSnapshotJob loads persisted snapshot state for a peer and table.
		GetStateSnapshotJob(ctx context.Context, peerRegion, table string) (models.StateSnapshotJob, error)
	}

	streamBus interface {
		// Subscribe registers an outbound stream consumer for one peer region.
		Subscribe(ctx context.Context, peerRegion string) (<-chan models.StreamMessage, func(), error)
		// Publish fan-outs one outbound stream message.
		Publish(ctx context.Context, peerRegion string, message models.StreamMessage) error
	}
)

// Metrics records runtime metrics for replication operations.
type Metrics interface {
	RecordReconnectAttempt(delay time.Duration)
	RecordAckLatency(latency time.Duration)
	RecordApplyApplied()
	RecordApplyStale()
	RecordApplyError()
	RecordSnapshotChunkSent(rows int)
	RecordSnapshotChunkReceived(rows int, finished bool)
	RecordSnapshotSendError()
	RecordSnapshotApplyError()
	RecordMsgSent(msgType int)
	RecordMsgReceived(msgType int)
}

//revive:enable:var-naming

// Service coordinates replication, snapshot transfer, and stream-level acks.
type Service struct {
	region        string
	tables        []string
	version       string
	configuration string
	applyMode     models.ReplicationApplyMode
	snapshot      snapshotService
	state         replicationState
	metrics       Metrics
	bus           streamBus
}

// NewService creates the replication service with an externally provided stream bus.
func NewService(
	region string,
	tables []string,
	version string,
	configuration string,
	applyMode models.ReplicationApplyMode,
	snapshotService snapshotService,
	stateRepo replicationState,
	bus streamBus,
	m Metrics,
) *Service {
	if m == nil {
		m = NewCollector()
	}
	if applyMode == "" {
		applyMode = models.ReplicationApplyModeUpsert
	}

	return &Service{
		region:        region,
		tables:        normalizeTables(tables),
		version:       version,
		configuration: configuration,
		applyMode:     applyMode,
		snapshot:      snapshotService,
		state:         stateRepo,
		metrics:       m,
		bus:           bus,
	}
}

// Handshake builds the local stream handshake payload.
func (s *Service) Handshake(_ context.Context) models.HandshakeMessage {
	return models.HandshakeMessage{
		Region:        s.region,
		Version:       s.version,
		Configuration: s.configuration,
	}
}

// Tables returns the sorted list of configured replicated table names.
func (s *Service) Tables() []string {
	return cloneStrings(s.tables)
}

// Subscribe registers an outbound stream subscriber for a peer.
func (s *Service) Subscribe(ctx context.Context, peerRegion string) (<-chan models.StreamMessage, func(), error) {
	return s.bus.Subscribe(ctx, peerRegion)
}

// QueuePing enqueues a ping message for a peer stream.
func (s *Service) QueuePing(peerRegion string, now time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), streamBusPublishTimeout)
	defer cancel()

	return s.bus.Publish(ctx, peerRegion, models.StreamMessage{
		PeerRegion: peerRegion,
		Ping: &models.PingMessage{
			SentAt: now,
		},
	})
}

// QueueAck enqueues an acknowledgement for a peer batch.
func (s *Service) QueueAck(peerRegion, table, lastEventID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), streamBusPublishTimeout)
	defer cancel()

	return s.bus.Publish(ctx, peerRegion, models.StreamMessage{
		PeerRegion: peerRegion,
		Ack: &models.AckMessage{
			Table:       table,
			LastEventID: lastEventID,
		},
	})
}

// RecordReconnectAttempt records the last reconnect delay chosen by the client.
func (s *Service) RecordReconnectAttempt(delay time.Duration) {
	s.metrics.RecordReconnectAttempt(delay)
}

// RecordMsgSent records one outbound gRPC message by type.
func (s *Service) RecordMsgSent(msgType int) {
	s.metrics.RecordMsgSent(msgType)
}

// RecordMsgReceived records one inbound gRPC message by type.
func (s *Service) RecordMsgReceived(msgType int) {
	s.metrics.RecordMsgReceived(msgType)
}

// QueueSnapshotRequests calls send for each table that has no replication cursor and no
// completed snapshot from the given peer. The caller is responsible for routing the request
// to the specific stream that should serve the snapshot.
func (s *Service) QueueSnapshotRequests(
	ctx context.Context,
	peerRegion string,
	send func(models.SnapshotRequest) error,
) error {
	for _, table := range s.tables {
		cursor, err := s.state.GetStateCursor(ctx, peerRegion, table)
		if err != nil {
			return fmt.Errorf("check cursor for snapshot bootstrap (%s/%s): %w", peerRegion, table, err)
		}
		if cursor.LastEventID != "" {
			continue
		}

		job, err := s.state.GetStateSnapshotJob(ctx, peerRegion, table)
		if err != nil {
			return fmt.Errorf("check snapshot job for snapshot bootstrap (%s/%s): %w", peerRegion, table, err)
		}
		if job.Finished {
			continue
		}

		if err := send(models.SnapshotRequest{
			Table:           table,
			AfterPrimaryKey: models.ClonePrimaryKeyParts(job.LastPrimaryKey),
		}); err != nil {
			return fmt.Errorf("queue snapshot request (%s/%s): %w", peerRegion, table, err)
		}

		slog.Info("snapshot requested", "peer_region", peerRegion, "table", table)
	}

	return nil
}

// SendSnapshotToPeer streams local snapshot chunks to a remote peer via the provided send callback.
// The caller is responsible for routing chunks to the specific stream that requested the snapshot.
func (s *Service) SendSnapshotToPeer(
	ctx context.Context,
	peerRegion, table string,
	afterPrimaryKey []models.PrimaryKeyPart,
	send func(models.SnapshotChunkMessage) error,
) error {
	err := s.snapshot.SendToPeer(
		ctx,
		peerRegion,
		table,
		models.ClonePrimaryKeyParts(afterPrimaryKey),
		func(chunk models.SnapshotChunk) error {
			if err := send(models.SnapshotChunkMessage{
				Table:     table,
				Rows:      cloneSnapshotRows(chunk.Rows),
				Last:      chunk.Last,
				Watermark: chunk.Watermark,
			}); err != nil {
				return err
			}
			s.metrics.RecordSnapshotChunkSent(len(chunk.Rows))
			return nil
		},
	)
	if err != nil {
		s.metrics.RecordSnapshotSendError()
	}
	return err
}

// ApplySnapshotChunk applies an inbound snapshot chunk.
func (s *Service) ApplySnapshotChunk(
	ctx context.Context,
	peerRegion string,
	table string,
	rows []models.SnapshotRow,
	last bool,
	watermark time.Time,
) error {
	if err := s.snapshot.ApplyIncomingChunk(ctx, peerRegion, table, models.SnapshotChunk{
		Rows:      cloneSnapshotRows(rows),
		Last:      last,
		Watermark: watermark,
	}); err != nil {
		s.metrics.RecordSnapshotApplyError()
		return err
	}
	s.metrics.RecordSnapshotChunkReceived(len(rows), last)
	return nil
}

// CompleteSnapshot marks an inbound snapshot as fully caught up and complete.
func (s *Service) CompleteSnapshot(ctx context.Context, peerRegion, table string) error {
	if err := s.snapshot.CompleteIncomingSnapshot(ctx, peerRegion, table); err != nil {
		s.metrics.RecordSnapshotApplyError()
		return err
	}
	return nil
}

// ApplyEventBatch applies a batch of inbound polling events atomically in a single DB transaction.
func (s *Service) ApplyEventBatch(ctx context.Context, events []models.ReplicationEvent) error {
	return s.applyBatch(ctx, events, true)
}

// ApplyTransactionBatch applies all events from a CDC transaction atomically in a single DB transaction.
func (s *Service) ApplyTransactionBatch(ctx context.Context, events []models.ReplicationEvent) error {
	return s.applyBatch(ctx, events, false)
}

func (s *Service) applyBatch(
	ctx context.Context,
	events []models.ReplicationEvent,
	sortForCursor bool,
) error {
	if len(events) == 0 {
		return nil
	}

	prepared := make([]models.ReplicationEvent, 0, len(events))
	for _, event := range events {
		if event.PeerRegion == "" {
			return errors.New("event peer region is required")
		}
		if event.Table == "" {
			return errors.New("event table is required")
		}
		if event.EventID == "" {
			return errors.New("event id is required")
		}
		if len(event.PrimaryKey) == 0 {
			return errors.New("event primary key is required")
		}
		if event.Operation == "" {
			event.Operation = models.ReplicationOperationUpsert
		}
		if !applyAllowed(s.applyMode, event.Operation) {
			continue
		}
		prepared = append(prepared, event)
	}

	if len(prepared) == 0 {
		return nil
	}

	if sortForCursor {
		slices.SortStableFunc(prepared, compareCursorOrderedEvents)
	}

	if err := s.state.CommitStateAppliedTransaction(ctx, prepared, s.applyMode); err != nil {
		s.metrics.RecordApplyError()
		return err
	}
	s.metrics.RecordApplyApplied()
	return nil
}

func compareCursorOrderedEvents(left, right models.ReplicationEvent) int {
	if diff := cmp.Compare(left.PeerRegion, right.PeerRegion); diff != 0 {
		return diff
	}
	if diff := cmp.Compare(left.Table, right.Table); diff != 0 {
		return diff
	}
	if diff := cmp.Compare(left.Version, right.Version); diff != 0 {
		return diff
	}
	return models.ComparePrimaryKeys(left.PrimaryKey, right.PrimaryKey)
}

func cloneSnapshotRows(src []models.SnapshotRow) []models.SnapshotRow {
	if src == nil {
		return nil
	}

	dst := make([]models.SnapshotRow, 0, len(src))
	for _, row := range src {
		dst = append(dst, models.SnapshotRow{
			PrimaryKey: models.ClonePrimaryKeyParts(row.PrimaryKey),
			Payload:    cloneBytes(row.Payload),
			Metadata:   models.CloneFields(row.Metadata),
			Version:    row.Version,
		})
	}

	return dst
}

func cloneStrings(src []string) []string {
	if src == nil {
		return nil
	}

	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}

	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func normalizeTables(src []string) []string {
	dst := cloneStrings(src)
	slices.Sort(dst)
	return dst
}

// applyAllowed reports whether an inbound event with the given operation should
// be applied under the configured apply mode.
func applyAllowed(mode models.ReplicationApplyMode, op models.ReplicationOperation) bool {
	switch mode {
	case models.ReplicationApplyModeInsertOnly:
		return op == models.ReplicationOperationInsert
	case models.ReplicationApplyModeUpsert:
		return op != models.ReplicationOperationDelete
	default: // FullSync
		return true
	}
}
