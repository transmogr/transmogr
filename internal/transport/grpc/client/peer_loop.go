package client

import (
	"context"
	"log/slog"
	"time"

	"github.com/transmogr/transmogr/internal/models"
)

// runPeerLoop maintains a persistent, reconnecting consume session to a single peer.
// It acquires a distributed lease before opening a session so that only one
// instance in the cluster consumes that peer feed at a time.
func (m *Manager) runPeerLoop(ctx context.Context, peerConfig models.Peer) {
	attempt := 0

	for {
		lease, acquired, err := m.state.TryStateAcquireLease(
			ctx,
			"stream",
			m.localRegion,
			peerConfig.Region,
			"",
			m.instanceID,
			m.leaseTTL,
		)
		if err != nil && ctx.Err() == nil {
			delay := m.backoff.duration(attempt)
			m.replication.RecordReconnectAttempt(delay)
			slog.Warn("consume lease acquire failed",
				"local_region", m.localRegion,
				"peer_region", peerConfig.Region,
				"error", err,
				"reconnect_delay", delay,
			)
			attempt++
			if !sleepWithContext(ctx, delay) {
				return
			}
			continue
		}
		if !acquired {
			if !sleepWithContext(ctx, m.leaseRenewInterval) {
				return
			}
			continue
		}
		slog.Info("peer consume lease acquired",
			"local_region", m.localRegion,
			"peer_region", peerConfig.Region,
			"generation", lease.Generation,
		)

		sessionCtx, cancel := context.WithCancel(ctx)
		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)
			m.renewLeaseLoop(sessionCtx, lease, cancel)
		}()

		err = m.runPeerSession(sessionCtx, peerConfig)
		cancel()
		<-renewDone
		m.releaseLease(lease)

		if err != nil && ctx.Err() == nil {
			delay := m.backoff.duration(attempt)
			m.replication.RecordReconnectAttempt(delay)
			slog.Warn("peer consume session failed",
				"local_region", m.localRegion,
				"peer_region", peerConfig.Region,
				"error", err,
				"reconnect_delay", delay,
			)
			attempt++
			if !sleepWithContext(ctx, delay) {
				return
			}
			continue
		}
		slog.Info("peer consume session stopped",
			"local_region", m.localRegion,
			"peer_region", peerConfig.Region,
		)
		attempt = 0
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

// renewLeaseLoop periodically renews the session lease until the context is
// cancelled or the lease is lost, at which point it cancels the session.
func (m *Manager) renewLeaseLoop(
	ctx context.Context,
	lease models.Lease,
	cancel context.CancelFunc,
) {
	ticker := time.NewTicker(m.leaseRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, renewed, err := m.state.RenewStateLease(
				ctx,
				lease.Kind,
				lease.Region,
				lease.PeerRegion,
				lease.Table,
				m.instanceID,
				lease.Generation,
				m.leaseTTL,
			)
			if err != nil {
				slog.Warn("peer consume lease renew failed",
					"local_region", m.localRegion,
					"peer_region", lease.PeerRegion,
					"generation", lease.Generation,
					"error", err,
				)
				cancel()
				return
			}
			if !renewed {
				slog.Warn("peer consume lease lost during renew",
					"local_region", m.localRegion,
					"peer_region", lease.PeerRegion,
					"generation", lease.Generation,
				)
				cancel()
				return
			}
		}
	}
}

// releaseLease releases a session lease on a best-effort basis.
func (m *Manager) releaseLease(lease models.Lease) {
	_, _ = m.state.ReleaseStateLease(
		context.Background(),
		lease.Kind,
		lease.Region,
		lease.PeerRegion,
		lease.Table,
		m.instanceID,
		lease.Generation,
	)
}
