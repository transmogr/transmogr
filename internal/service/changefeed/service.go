// Package changefeed reads database changes and converts them into replication events.
package changefeed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/transmogr/transmogr/internal/models"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

// Metrics records runtime metrics for changefeed operations.
type Metrics interface {
	RecordChangefeedEvent()
	RecordChangefeedBatch()
}

type (

	// Outbox stages outbound replication events for later delivery.
	Outbox interface {
		// NextSequence allocates the next outbound sequence for a peer and table.
		NextSequence(ctx context.Context, peerRegion, table string) (uint64, error)
		// Enqueue persists or buffers one outbound batch.
		Enqueue(ctx context.Context, batch models.OutboxBatch) error
	}

	// LeaseService runs work under distributed lease ownership.
	LeaseService interface {
		// RunWithLease executes fn while the caller owns the configured lease.
		RunWithLease(ctx context.Context, fn func(ctx context.Context) error) error
	}

	// PeersService lists outbound peers eligible for replication.
	PeersService interface {
		// Peers returns the peers currently eligible for outbound replication.
		Peers() []models.Peer
	}

	// Adapter provides database-specific CDC capture for the producer.
	Adapter interface {
		// Run starts database-specific change capture and pushes normalized batches into the handler.
		// CDC adapters set ChangefeedBatch.TxID; polling adapters leave it empty.
		Run(ctx context.Context, onBatch func(context.Context, models.ChangefeedBatch) error) error
		// CursorTables lists tables that may have persisted changefeed cursors.
		CursorTables() []string
		// RestoreCursors initializes adapter-specific cursors from persisted state.
		RestoreCursors(ctx context.Context, load func(table string) (models.ChangefeedCursor, error)) error
	}

	// StateStore persists replication cursors.
	StateStore interface {
		GetChangefeedCursor(ctx context.Context, table string) (models.ChangefeedCursor, error)
		SaveChangefeedCursor(ctx context.Context, cursor models.ChangefeedCursor) error
	}
)

// ServiceConfig controls logical replication capture from PostgreSQL WAL.
type ServiceConfig struct {
	Source         string
	LocalRegion    string
	InstanceID     string
	LeaseTTL       time.Duration
	RenewInterval  time.Duration
	BroadcastMode  models.ReplicationBroadcastMode
	MaxBatchEvents int
}

// Service streams local changes from a database-specific CDC adapter and publishes them to peers.
type Service struct {
	cfg     ServiceConfig
	peers   PeersService
	adapter Adapter
	leases  LeaseService
	outbox  Outbox
	state   StateStore
	metrics Metrics
}

// NewService creates a CDC producer.
func NewService(
	cfg ServiceConfig,
	peers PeersService,
	adapter Adapter,
	leaseService LeaseService,
	outbox Outbox,
	state StateStore,
	m Metrics,
) (*Service, error) {
	if m == nil {
		m = noopChangefeedMetrics{}
	}
	srv := &Service{
		cfg:     cfg,
		peers:   peers,
		adapter: adapter,
		leases:  leaseService,
		outbox:  outbox,
		state:   state,
		metrics: m,
	}

	if srv.peers == nil {
		return nil, fmt.Errorf("peers is required")
	}
	if srv.adapter == nil {
		return nil, fmt.Errorf("adapter is required")
	}
	if srv.leases == nil {
		return nil, fmt.Errorf("leaseService is required")
	}
	if srv.outbox == nil {
		return nil, fmt.Errorf("outbox is required")
	}
	if srv.state == nil {
		return nil, fmt.Errorf("state is required")
	}

	return srv, nil
}

// Run starts the changefeed service until shutdown.
func (p *Service) Run(ctx context.Context) error {
	return p.leases.RunWithLease(ctx, p.runSession)
}

func (p *Service) runSession(ctx context.Context) error {
	if err := p.restoreCursors(ctx); err != nil {
		return err
	}
	slog.Info(
		"changefeed session started",
		"region", p.cfg.LocalRegion,
		"instance_id", p.cfg.InstanceID,
	)
	defer slog.Info(
		"changefeed session stopped",
		"region", p.cfg.LocalRegion,
		"instance_id", p.cfg.InstanceID,
	)
	return p.adapter.Run(ctx, p.HandleBatch)
}

// HandleBatch publishes a normalized batch of changes to all peers.
// Batches with a non-empty TxID are CDC transactions and are sent as TransactionBatch.
// Batches with an empty TxID are polling batches and are sent as EventBatch.
func (p *Service) HandleBatch(ctx context.Context, batch models.ChangefeedBatch) error {
	events := make([]models.ReplicationEvent, 0, len(batch.Changes))
	for _, change := range batch.Changes {
		if !broadcastAllowed(p.cfg.BroadcastMode, change.Operation) {
			continue
		}

		p.metrics.RecordChangefeedEvent()

		fields, err := models.PayloadToFields(change.Payload)
		if err != nil {
			return fmt.Errorf("decode change payload for %s: %w", change.Table, err)
		}

		events = append(events, models.ReplicationEvent{
			PeerRegion: p.cfg.LocalRegion,
			Table:      change.Table,
			EventID:    eventIDFromChange(change),
			PrimaryKey: change.PrimaryKey,
			Operation:  change.Operation,
			Version:    change.Version,
			Fields:     fields,
		})
	}

	if len(events) == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, peerConfig := range p.peers.Peers() {
		peerRegion := peerConfig.Region
		g.Go(func() error {
			if err := p.enqueueBatch(gctx, peerRegion, batch.TxID, events); err != nil {
				return fmt.Errorf("enqueue batch to %s: %w", peerRegion, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	if err := p.saveCursor(ctx, batch); err != nil {
		return err
	}

	return nil
}

func eventIDFromChange(change models.ChangefeedChange) string {
	hasher := sha256.New()
	writeLengthPrefixedString(hasher, change.Table)
	writePrimaryKeyParts(hasher, change.PrimaryKey)
	writeLengthPrefixedString(hasher, strconv.FormatInt(change.Version, 10))
	writeLengthPrefixedString(hasher, string(change.Operation))

	sum := hasher.Sum(nil)
	encoded := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(encoded, sum)
	return string(encoded)
}

func writeLengthPrefixedString(hasher hash.Hash, value string) {
	writeLengthPrefixedBytes(hasher, []byte(value))
}

func writeLengthPrefixedBytes(hasher hash.Hash, value []byte) {
	var lenBuf [20]byte
	lenEncoded := strconv.AppendInt(lenBuf[:0], int64(len(value)), 10)
	_, _ = hasher.Write(lenEncoded)
	_, _ = hasher.Write([]byte{':'})
	_, _ = hasher.Write(value)
}

func writePrimaryKeyParts(hasher hash.Hash, parts []models.PrimaryKeyPart) {
	var countBuf [20]byte
	countEncoded := strconv.AppendInt(countBuf[:0], int64(len(parts)), 10)
	_, _ = hasher.Write(countEncoded)
	_, _ = hasher.Write([]byte{':'})

	for _, part := range parts {
		writeLengthPrefixedString(hasher, part.Column)
		writeLengthPrefixedString(hasher, string(part.Type))
		writeLengthPrefixedBytes(hasher, part.Value)
	}
}

func (p *Service) restoreCursors(ctx context.Context) error {
	cursorTables := p.adapter.CursorTables()
	cursors := make(map[string]models.ChangefeedCursor, len(cursorTables))
	for _, table := range cursorTables {
		cursor, err := p.state.GetChangefeedCursor(ctx, table)
		if err != nil {
			return fmt.Errorf("load changefeed cursor for %s: %w", table, err)
		}
		if cursor.Source != "" && cursor.Source != p.cfg.Source {
			return errors.Join(models.ErrChangefeedCursorSourceMismatch, fmt.Errorf(
				"for %s: stored=%s current=%s",
				table,
				cursor.Source,
				p.cfg.Source,
			))
		}
		if cursor.Source == "" {
			cursor = models.ChangefeedCursor{
				Table:          table,
				Source:         p.cfg.Source,
				LastVersion:    cursor.LastVersion,
				LastPrimaryKey: models.ClonePrimaryKeyParts(cursor.LastPrimaryKey),
			}
			if err := p.state.SaveChangefeedCursor(ctx, cursor); err != nil {
				return fmt.Errorf("initialize changefeed cursor for %s: %w", table, err)
			}
		}
		cursors[table] = cursor
	}

	return p.adapter.RestoreCursors(ctx, func(table string) (models.ChangefeedCursor, error) {
		cursor, ok := cursors[table]
		if !ok {
			return models.ChangefeedCursor{Table: table}, nil
		}
		return cursor, nil
	})
}

func (p *Service) saveCursor(ctx context.Context, batch models.ChangefeedBatch) error {
	if len(batch.Changes) == 0 {
		return nil
	}

	latestByTable := make(map[string]models.ChangefeedCursor, len(batch.Changes))
	for _, change := range batch.Changes {
		latestByTable[change.Table] = models.ChangefeedCursor{
			Table:          change.Table,
			Source:         p.cfg.Source,
			LastVersion:    change.Version,
			LastPrimaryKey: models.ClonePrimaryKeyParts(change.PrimaryKey),
		}
	}

	for table, cursor := range latestByTable {
		if err := p.state.SaveChangefeedCursor(ctx, cursor); err != nil {
			return fmt.Errorf("save changefeed cursor for %s: %w", table, err)
		}
	}

	return nil
}

func (p *Service) enqueueBatch(
	ctx context.Context,
	peerRegion string,
	txID string,
	events []models.ReplicationEvent,
) error {
	// Use the first event's table as the sequence key for polling batches.
	// Transaction batches use an empty table key so they share a cross-table sequence.
	tableKey := ""
	if txID == "" && len(events) > 0 {
		tableKey = events[0].Table
	}

	for len(events) > 0 {
		chunk := events
		if len(chunk) > p.cfg.MaxBatchEvents {
			chunk = events[:p.cfg.MaxBatchEvents]
		}
		events = events[len(chunk):]

		sequence, err := p.outbox.NextSequence(ctx, peerRegion, tableKey)
		if err != nil {
			return err
		}

		batch := models.OutboxBatch{
			PeerRegion: peerRegion,
			Table:      tableKey,
			Sequence:   sequence,
			Events:     chunk,
			TxID:       txID,
		}

		if err := p.outbox.Enqueue(ctx, batch); err != nil {
			return err
		}
		p.metrics.RecordChangefeedBatch()
	}

	return nil
}

type noopChangefeedMetrics struct{}

func (noopChangefeedMetrics) RecordChangefeedEvent() {}
func (noopChangefeedMetrics) RecordChangefeedBatch() {}

// broadcastAllowed reports whether a change with the given operation should be
// published to peers under the configured broadcast mode.
func broadcastAllowed(mode models.ReplicationBroadcastMode, op models.ReplicationOperation) bool {
	switch mode {
	case models.ReplicationBroadcastModeInsertOnly:
		return op == models.ReplicationOperationInsert
	case models.ReplicationBroadcastModeUpsert:
		return op != models.ReplicationOperationDelete
	default: // FullSync
		return true
	}
}
