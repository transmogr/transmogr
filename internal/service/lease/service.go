// Package lease manages distributed snapshot and replication leases.
package lease

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/transmogr/transmogr/internal/models"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

// Store persists distributed lease ownership.
type Store interface {
	// TryStateAcquireLease attempts to acquire the requested lease.
	TryStateAcquireLease(
		ctx context.Context,
		kind string,
		region string,
		peerRegion string,
		table string,
		ownerID string,
		ttl time.Duration,
	) (models.Lease, bool, error)
	// RenewStateLease extends a lease still owned by this instance.
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
	// ReleaseStateLease releases a lease still owned by this instance.
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

// Config controls shared distributed lease ownership settings.
type Config struct {
	Kind          string
	Region        string
	PeerRegion    string
	Table         string
	OwnerID       string
	TTL           time.Duration
	RenewInterval time.Duration
}

// Metrics records runtime metrics for lease operations.
type Metrics interface {
	RecordLeaseAcquireOK()
	RecordLeaseAcquireMiss()
	RecordLeaseAcquireError()
	RecordLeaseRenewOK()
	RecordLeaseRenewLost()
	RecordLeaseRenewError()
}

//revive:enable:var-naming

// Service coordinates distributed lease acquire, renew, and release.
type Service struct {
	store   Store
	cfg     Config
	metrics Metrics
}

// NewService creates a lease service backed by a persistent store.
func NewService(store Store, cfg Config, metrics Metrics) *Service {
	return &Service{store: store, cfg: cfg, metrics: metrics}
}

// RunWithLease runs fn only while the caller owns the configured lease.
// Transient errors returned by fn trigger a delayed retry until ctx is canceled.
func (s *Service) RunWithLease(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		lease, acquired, err := s.store.TryStateAcquireLease(
			ctx,
			s.cfg.Kind,
			s.cfg.Region,
			s.cfg.PeerRegion,
			s.cfg.Table,
			s.cfg.OwnerID,
			s.cfg.TTL,
		)
		if err != nil {
			if s.metrics != nil {
				s.metrics.RecordLeaseAcquireError()
			}
			return err
		}
		if !acquired {
			if s.metrics != nil {
				s.metrics.RecordLeaseAcquireMiss()
			}
			if !sleepWithContext(ctx, s.cfg.RenewInterval) {
				return nil
			}
			continue
		}
		if s.metrics != nil {
			s.metrics.RecordLeaseAcquireOK()
		}
		slog.Info(
			"lease acquired",
			"kind", lease.Kind,
			"region", lease.Region,
			"peer_region", lease.PeerRegion,
			"table", lease.Table,
			"generation", lease.Generation,
		)

		sessionCtx, cancel := context.WithCancel(ctx)
		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)
			s.renewLeaseLoop(sessionCtx, lease, cancel)
		}()

		err = fn(sessionCtx)
		cancel()
		<-renewDone
		_, _ = s.store.ReleaseStateLease(
			context.Background(),
			lease.Kind,
			lease.Region,
			lease.PeerRegion,
			lease.Table,
			s.cfg.OwnerID,
			lease.Generation,
		)

		if err == nil || errors.Is(err, context.Canceled) {
			slog.Info(
				"lease session stopped",
				"kind", lease.Kind,
				"region", lease.Region,
				"peer_region", lease.PeerRegion,
				"table", lease.Table,
			)
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		if errors.Is(err, models.ErrChangefeedCursorSourceMismatch) {
			return err
		}
		slog.Warn(
			"lease session failed; retrying",
			"kind", lease.Kind,
			"region", lease.Region,
			"peer_region", lease.PeerRegion,
			"table", lease.Table,
			"error", err,
		)

		if !sleepWithContext(ctx, s.cfg.RenewInterval) {
			return nil
		}
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) renewLeaseLoop(
	ctx context.Context,
	lease models.Lease,
	cancel context.CancelFunc,
) {
	ticker := time.NewTicker(s.cfg.RenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, renewed, err := s.store.RenewStateLease(
				ctx,
				lease.Kind,
				lease.Region,
				lease.PeerRegion,
				lease.Table,
				s.cfg.OwnerID,
				lease.Generation,
				s.cfg.TTL,
			)
			if err != nil {
				if s.metrics != nil {
					s.metrics.RecordLeaseRenewError()
				}
				slog.Warn(
					"lease renew failed",
					"kind", lease.Kind,
					"region", lease.Region,
					"peer_region", lease.PeerRegion,
					"table", lease.Table,
					"generation", lease.Generation,
					"error", err,
				)
				cancel()
				return
			}
			if !renewed {
				if s.metrics != nil {
					s.metrics.RecordLeaseRenewLost()
				}
				slog.Warn(
					"lease lost during renew",
					"kind", lease.Kind,
					"region", lease.Region,
					"peer_region", lease.PeerRegion,
					"table", lease.Table,
					"generation", lease.Generation,
				)
				cancel()
				return
			}
			if s.metrics != nil {
				s.metrics.RecordLeaseRenewOK()
			}
		}
	}
}
