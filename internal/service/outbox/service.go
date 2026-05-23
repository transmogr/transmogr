// Package outbox defines the outbound event publishing contract.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/transmogr/transmogr/internal/models"
	repositorypostgres "github.com/transmogr/transmogr/internal/repository/postgres"
)

const (
	// DefaultMetricsRefreshInterval is the default cadence for refreshing cached outbox metrics.
	DefaultMetricsRefreshInterval = 5 * time.Second
	defaultMetricsRefreshTimeout  = 5 * time.Second
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE
type repository interface {
	OutboxNextSequence(ctx context.Context, peerRegion, table string) (uint64, error)
	OutboxEnqueue(ctx context.Context, batch models.OutboxBatch) (models.OutboxBatch, bool, error)
	OutboxListPending(ctx context.Context, peerRegion string, limit int) ([]models.OutboxBatch, error)
	OutboxListPendingWindow(
		ctx context.Context,
		peerRegion string,
		lowerExclusive, upperInclusive time.Time,
		afterID uint64,
		limit int,
	) ([]models.OutboxBatch, error)
	OutboxAck(ctx context.Context, peerRegion, table, lastEventID string) (bool, error)
	OutboxSnapshotWatermark(ctx context.Context) (time.Time, error)
	OutboxReady(ctx context.Context) error
	OutboxCleanup(ctx context.Context, maxBatchAge time.Duration, maxPendingPerPeer int) (int64, int64, error)
	OutboxLoadMetricsSnapshot(ctx context.Context) (models.OutboxMetricsSnapshot, error)
}

// Service stages outbound batches, replays pending work, and tracks acks.
type Service struct {
	repo repository

	mu            sync.Mutex
	nextID        uint64
	subs          map[string]map[uint64]chan models.OutboxBatch
	cachedMetrics models.OutboxMetricsSnapshot

	cleanupDeleted        atomic.Uint64
	cleanupDeletedByAge   atomic.Uint64
	cleanupDeletedByCount atomic.Uint64
}

// NewService creates the outbox service backed by PostgreSQL state tables.
func NewService(pool *pgxpool.Pool, stateSchema string) (*Service, error) {
	return NewWithRepository(repositorypostgres.New(pool, nil, stateSchema))
}

// NewWithRepository creates the outbox service on top of a repository.
func NewWithRepository(repo repository) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("outbox repository is required")
	}

	return &Service{
		repo: repo,
		subs: make(map[string]map[uint64]chan models.OutboxBatch),
	}, nil
}

// NextSequence allocates the next durable sequence for one peer and table.
func (s *Service) NextSequence(ctx context.Context, peerRegion, table string) (uint64, error) {
	return s.repo.OutboxNextSequence(ctx, peerRegion, table)
}

// Enqueue persists a pending batch and notifies live subscribers best-effort.
func (s *Service) Enqueue(ctx context.Context, batch models.OutboxBatch) error {
	storedBatch, inserted, err := s.repo.OutboxEnqueue(ctx, batch)
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}

	s.publishLive(storedBatch)
	return nil
}

// Subscribe registers a live peer-specific consumer.
func (s *Service) Subscribe(_ context.Context, peerRegion string) (<-chan models.OutboxBatch, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID++
	id := s.nextID
	ch := make(chan models.OutboxBatch, models.LiveReplicationBufferSize)
	if s.subs[peerRegion] == nil {
		s.subs[peerRegion] = make(map[uint64]chan models.OutboxBatch)
	}
	s.subs[peerRegion][id] = ch

	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		if subs := s.subs[peerRegion]; subs != nil {
			if _, ok := subs[id]; ok {
				delete(subs, id)
				close(ch)
			}
			if len(subs) == 0 {
				delete(s.subs, peerRegion)
			}
		}
	}, nil
}

// ListPending returns the oldest pending batches for one peer.
func (s *Service) ListPending(ctx context.Context, peerRegion string, limit int) ([]models.OutboxBatch, error) {
	return s.repo.OutboxListPending(ctx, peerRegion, limit)
}

// ListPendingWindow returns a page of pending batches within one replay window.
func (s *Service) ListPendingWindow(
	ctx context.Context,
	peerRegion string,
	lowerExclusive, upperInclusive time.Time,
	afterID uint64,
	limit int,
) ([]models.OutboxBatch, error) {
	return s.repo.OutboxListPendingWindow(ctx, peerRegion, lowerExclusive, upperInclusive, afterID, limit)
}

// Ack deletes one persisted pending batch.
func (s *Service) Ack(ctx context.Context, peerRegion, table, lastEventID string) (bool, error) {
	return s.repo.OutboxAck(ctx, peerRegion, table, lastEventID)
}

// SnapshotWatermark returns the database timestamp used as the sender-side snapshot boundary.
func (s *Service) SnapshotWatermark(ctx context.Context) (time.Time, error) {
	return s.repo.OutboxSnapshotWatermark(ctx)
}

// Ready reports whether the PostgreSQL-backed outbox can reach its database.
func (s *Service) Ready(ctx context.Context) error {
	return s.repo.OutboxReady(ctx)
}

// RunMetricsRefresh periodically refreshes the cached outbox metrics snapshot.
func (s *Service) RunMetricsRefresh(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = DefaultMetricsRefreshInterval
	}

	if err := s.refreshMetricsSnapshot(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("outbox metrics refresh failed", "error", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.refreshMetricsSnapshot(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("outbox metrics refresh failed", "error", err)
			}
		}
	}
}

// RunCleanup periodically prunes stale or excessive pending batches.
func (s *Service) RunCleanup(
	ctx context.Context,
	interval, maxBatchAge time.Duration,
	maxPendingPerPeer int,
) error {
	if interval <= 0 {
		return fmt.Errorf("outbox cleanup interval must be positive")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := s.Cleanup(ctx, maxBatchAge, maxPendingPerPeer); err != nil && ctx.Err() == nil {
				slog.Warn("outbox cleanup failed", "error", err)
			}
		}
	}
}

// Cleanup prunes stale or excessive pending batches once.
func (s *Service) Cleanup(ctx context.Context, maxBatchAge time.Duration, maxPendingPerPeer int) (int64, error) {
	deletedByAge, deletedByCount, err := s.repo.OutboxCleanup(ctx, maxBatchAge, maxPendingPerPeer)
	if err != nil {
		return 0, err
	}

	totalDeleted := deletedByAge + deletedByCount
	if deletedByAge > 0 {
		s.cleanupDeletedByAge.Add(uint64(deletedByAge))
		slog.Warn(
			"outbox cleanup deleted unacknowledged batches by age",
			"deleted_batches", deletedByAge,
			"max_batch_age", maxBatchAge,
		)
	}
	if deletedByCount > 0 {
		s.cleanupDeletedByCount.Add(uint64(deletedByCount))
		slog.Warn(
			"outbox cleanup deleted unacknowledged batches by count",
			"deleted_batches", deletedByCount,
			"max_pending_per_peer", maxPendingPerPeer,
		)
	}
	if totalDeleted > 0 {
		s.cleanupDeleted.Add(uint64(totalDeleted))
	}

	return totalDeleted, nil
}

// OutboxMetricsSnapshot returns a point-in-time backlog snapshot for the PostgreSQL outbox.
func (s *Service) OutboxMetricsSnapshot() models.OutboxMetricsSnapshot {
	s.mu.Lock()
	snapshot := s.cachedMetrics
	s.mu.Unlock()
	snapshot.CleanupDeleted = s.cleanupDeleted.Load()
	snapshot.CleanupDeletedByAge = s.cleanupDeletedByAge.Load()
	snapshot.CleanupDeletedByCount = s.cleanupDeletedByCount.Load()
	return snapshot
}

func (s *Service) refreshMetricsSnapshot(ctx context.Context) error {
	refreshCtx, cancel := context.WithTimeout(ctx, defaultMetricsRefreshTimeout)
	defer cancel()

	snapshot, err := s.repo.OutboxLoadMetricsSnapshot(refreshCtx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.cachedMetrics = snapshot
	s.mu.Unlock()

	return nil
}

func (s *Service) publishLive(batch models.OutboxBatch) {
	s.mu.Lock()
	targets := make([]chan models.OutboxBatch, 0, len(s.subs[batch.PeerRegion]))
	for _, ch := range s.subs[batch.PeerRegion] {
		targets = append(targets, ch)
	}
	s.mu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- batch:
			// Live subscribers must treat batches as immutable.
		default:
			// Drop instead of allocating a deep clone for a full channel.
		}
	}
}
