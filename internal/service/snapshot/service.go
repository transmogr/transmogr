// Package snapshot coordinates snapshot bootstrap and catch-up state.
package snapshot

import (
	"context"
	"errors"
	"time"

	"github.com/transmogr/transmogr/internal/models"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

const defaultChunkSize = 1000

type snapshotState interface {
	// UpsertStateSnapshotJob creates or updates persisted snapshot job state.
	UpsertStateSnapshotJob(ctx context.Context, job models.StateSnapshotJob) error
	// CommitStateSnapshotCheckpoint atomically advances snapshot progress for one job.
	CommitStateSnapshotCheckpoint(
		ctx context.Context,
		job models.StateSnapshotJob,
	) (models.CommitSnapshotCheckpointResult, error)
	// MarkStateSnapshotFinished marks a persisted snapshot job as completed.
	MarkStateSnapshotFinished(ctx context.Context, peerRegion, table string) error
	// GetStateSnapshotJob loads persisted snapshot state for one peer and table.
	GetStateSnapshotJob(ctx context.Context, peerRegion, table string) (models.StateSnapshotJob, error)
	// TryStateAcquireLease attempts to acquire snapshot worker ownership.
	TryStateAcquireLease(
		ctx context.Context,
		kind string,
		region string,
		peerRegion string,
		table string,
		ownerID string,
		ttl time.Duration,
	) (models.Lease, bool, error)
	// RenewStateLease extends snapshot worker ownership while still held by this instance.
	RenewStateLease(
		ctx context.Context,
		kind string,
		region string,
		peerRegion string,
		table string,
		ownerID string,
		generation int64,
		ttl time.Duration,
	) (models.Lease, bool, error)
	// ReleaseStateLease releases snapshot worker ownership held by this instance.
	ReleaseStateLease(
		ctx context.Context,
		kind string,
		region string,
		peerRegion string,
		table string,
		ownerID string,
		generation int64,
	) (bool, error)
}

// Service coordinates persisted snapshot replication progress.
type Service interface {
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
	// CompleteIncomingSnapshot marks the inbound snapshot as fully caught up and complete.
	CompleteIncomingSnapshot(ctx context.Context, peerRegion, table string) error
}

// Source reads snapshot chunks from the local application repository.
type Source interface {
	// ReadSnapshotChunk reads one snapshot chunk from the local application store.
	ReadSnapshotChunk(ctx context.Context, req models.SnapshotReadRequest) (models.SnapshotChunk, error)
}

// Applier writes incoming snapshot rows to the local application repository.
type Applier interface {
	// ApplySnapshotChunk writes one inbound snapshot chunk to the local application store.
	ApplySnapshotChunk(ctx context.Context, peerRegion, table string, rows []models.SnapshotRow) error
}

// StateService persists snapshot progress and optionally runs the chunk pipeline.
type StateService struct {
	state     snapshotState
	source    Source
	applier   Applier
	chunkSize int
	lease     *LeaseConfig
}

// LeaseConfig controls distributed snapshot worker ownership.
type LeaseConfig struct {
	LocalRegion   string
	InstanceID    string
	TTL           time.Duration
	RenewInterval time.Duration
}

// NewServiceWithPipelineAndLeases creates a snapshot service with optional worker lease ownership.
func NewServiceWithPipelineAndLeases(
	stateRepo snapshotState,
	source Source,
	applier Applier,
	chunkSize int,
	leaseCfg *LeaseConfig,
) *StateService {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}

	return &StateService{
		state:     stateRepo,
		source:    source,
		applier:   applier,
		chunkSize: chunkSize,
		lease:     normalizedLeaseConfig(leaseCfg),
	}
}

// ensureRequested starts or resumes snapshot state for a peer and table.
func (s *StateService) ensureRequested(ctx context.Context, peerRegion, table string) error {
	if peerRegion == "" {
		return errors.New("snapshot peer region is required")
	}

	if table == "" {
		return errors.New("snapshot table is required")
	}

	return s.state.UpsertStateSnapshotJob(ctx, models.StateSnapshotJob{
		PeerRegion: peerRegion,
		Table:      table,
		Status:     models.SnapshotJobStatusPending,
	})
}

// saveCheckpoint atomically persists snapshot progress for a peer and table.
func (s *StateService) saveCheckpoint(
	ctx context.Context,
	peerRegion string,
	table string,
	lastPrimaryKey []models.PrimaryKeyPart,
	watermark time.Time,
) (models.CommitSnapshotCheckpointResult, error) {
	if peerRegion == "" {
		return models.CommitSnapshotCheckpointResult{}, errors.New("snapshot peer region is required")
	}

	if table == "" {
		return models.CommitSnapshotCheckpointResult{}, errors.New("snapshot table is required")
	}

	return s.state.CommitStateSnapshotCheckpoint(ctx, models.StateSnapshotJob{
		PeerRegion:     peerRegion,
		Table:          table,
		Status:         models.SnapshotJobStatusSnapshotting,
		LastPrimaryKey: lastPrimaryKey,
		WatermarkAt:    watermark,
	})
}

// finish marks a snapshot job as completed.
func (s *StateService) finish(ctx context.Context, peerRegion, table string) error {
	if peerRegion == "" {
		return errors.New("snapshot peer region is required")
	}

	if table == "" {
		return errors.New("snapshot table is required")
	}

	return s.state.MarkStateSnapshotFinished(ctx, peerRegion, table)
}

// SendToPeer streams local snapshot chunks to a remote peer under snapshot lease ownership.
func (s *StateService) SendToPeer(
	ctx context.Context,
	peerRegion string,
	table string,
	afterPrimaryKey []models.PrimaryKeyPart,
	send func(models.SnapshotChunk) error,
) error {
	if send == nil {
		return errors.New("snapshot send callback is required")
	}

	return s.withLease(ctx, peerRegion, table, func(ctx context.Context) error {
		afterPrimaryKey = models.ClonePrimaryKeyParts(afterPrimaryKey)
		for {
			chunk, err := s.readLocalChunk(ctx, table, afterPrimaryKey, s.chunkSize)
			if err != nil {
				return err
			}

			if err := send(chunk); err != nil {
				return err
			}

			if chunk.Last || len(chunk.Rows) == 0 {
				return nil
			}

			afterPrimaryKey = models.ClonePrimaryKeyParts(chunk.Rows[len(chunk.Rows)-1].PrimaryKey)
		}
	})
}

// readLocalChunk loads the next snapshot chunk from the local source.
func (s *StateService) readLocalChunk(
	ctx context.Context,
	table string,
	afterPrimaryKey []models.PrimaryKeyPart,
	limit int,
) (models.SnapshotChunk, error) {
	req := models.SnapshotReadRequest{
		Table:           table,
		AfterPrimaryKey: afterPrimaryKey,
		Limit:           limit,
	}
	if s.lease != nil {
		req.LocalRegion = s.lease.LocalRegion
	}
	return s.source.ReadSnapshotChunk(ctx, req)
}

// ApplyIncomingChunk applies a snapshot chunk and persists its checkpoint.
func (s *StateService) ApplyIncomingChunk(
	ctx context.Context,
	peerRegion string,
	table string,
	chunk models.SnapshotChunk,
) error {
	if len(chunk.Rows) > 0 {
		if err := s.applier.ApplySnapshotChunk(ctx, peerRegion, table, chunk.Rows); err != nil {
			return err
		}

		lastPrimaryKey := models.ClonePrimaryKeyParts(chunk.Rows[len(chunk.Rows)-1].PrimaryKey)
		result, err := s.saveCheckpoint(ctx, peerRegion, table, lastPrimaryKey, chunk.Watermark)
		if err != nil {
			return err
		}
		if !result.Updated && result.Reason == models.CommitSkipReasonSnapshotDone {
			// Previous snapshot was already finished; reset job so this one can proceed.
			if err := s.ensureRequested(ctx, peerRegion, table); err != nil {
				return err
			}
			if _, err := s.saveCheckpoint(ctx, peerRegion, table, lastPrimaryKey, chunk.Watermark); err != nil {
				return err
			}
		}
	}

	if chunk.Last {
		// Ensure the job record exists before marking catching_up (covers empty-table edge case
		// where saveCheckpoint was never called for this snapshot).
		if len(chunk.Rows) == 0 {
			existingJob, err := s.state.GetStateSnapshotJob(ctx, peerRegion, table)
			if err != nil {
				return err
			}
			if err := s.state.UpsertStateSnapshotJob(ctx, models.StateSnapshotJob{
				PeerRegion:     peerRegion,
				Table:          table,
				Status:         models.SnapshotJobStatusCatchingUp,
				LastPrimaryKey: models.ClonePrimaryKeyParts(existingJob.LastPrimaryKey),
				WatermarkAt:    chunk.Watermark,
			}); err != nil {
				return err
			}
		} else {
			if _, err := s.state.CommitStateSnapshotCheckpoint(ctx, models.StateSnapshotJob{
				PeerRegion:     peerRegion,
				Table:          table,
				Status:         models.SnapshotJobStatusCatchingUp,
				LastPrimaryKey: models.ClonePrimaryKeyParts(chunk.Rows[len(chunk.Rows)-1].PrimaryKey),
				WatermarkAt:    chunk.Watermark,
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

// CompleteIncomingSnapshot marks a snapshot job as fully caught up and finished.
func (s *StateService) CompleteIncomingSnapshot(ctx context.Context, peerRegion, table string) error {
	return s.finish(ctx, peerRegion, table)
}

func (s *StateService) withLease(
	ctx context.Context,
	peerRegion string,
	table string,
	fn func(context.Context) error,
) error {
	if fn == nil {
		return nil
	}

	if s.lease == nil {
		return fn(ctx)
	}

	lease, acquired, err := s.state.TryStateAcquireLease(
		ctx,
		"snapshot",
		s.lease.LocalRegion,
		peerRegion,
		table,
		s.lease.InstanceID,
		s.lease.TTL,
	)
	if err != nil {
		return err
	}
	if !acquired {
		return models.ErrLeaseNotAcquired
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		s.renewLeaseLoop(leaseCtx, lease, cancel)
	}()

	err = fn(leaseCtx)
	cancel()
	<-renewDone
	_, _ = s.state.ReleaseStateLease(
		context.Background(),
		lease.Kind,
		lease.Region,
		lease.PeerRegion,
		lease.Table,
		s.lease.InstanceID,
		lease.Generation,
	)

	return err
}

func (s *StateService) renewLeaseLoop(
	ctx context.Context,
	lease models.Lease,
	cancel context.CancelFunc,
) {
	ticker := time.NewTicker(s.lease.RenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, renewed, err := s.state.RenewStateLease(
				ctx,
				lease.Kind,
				lease.Region,
				lease.PeerRegion,
				lease.Table,
				s.lease.InstanceID,
				lease.Generation,
				s.lease.TTL,
			)
			if err != nil || !renewed {
				cancel()
				return
			}
		}
	}
}

func normalizedLeaseConfig(cfg *LeaseConfig) *LeaseConfig {
	if cfg == nil {
		return nil
	}

	if cfg.LocalRegion == "" || cfg.InstanceID == "" || cfg.TTL <= 0 || cfg.RenewInterval <= 0 {
		return nil
	}

	return cfg
}
