package app

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/transmogr/transmogr/internal/metrics"
	"github.com/transmogr/transmogr/internal/models"
	repositorypostgres "github.com/transmogr/transmogr/internal/repository/postgres"
	changefeedservice "github.com/transmogr/transmogr/internal/service/changefeed"
	changefeedadapter "github.com/transmogr/transmogr/internal/service/changefeed/adapter"
	leaseservice "github.com/transmogr/transmogr/internal/service/lease"
	"github.com/transmogr/transmogr/internal/service/outbox"
	"github.com/transmogr/transmogr/internal/service/peers"
	"github.com/transmogr/transmogr/internal/service/replication"
	"github.com/transmogr/transmogr/internal/service/snapshot"
	"github.com/transmogr/transmogr/pkg/fanout"
)

func (a *App) initReplication(
	ctx context.Context,
	instanceID string,
	pool *pgxpool.Pool,
	repo *repositorypostgres.Repository,
) (*outbox.Service, *replication.Service, *peers.Service, error) {
	snapshotService := snapshot.NewServiceWithPipelineAndLeases(
		repo,
		repo,
		repo,
		a.cfg.Replication.Snapshot.ChunkSize,
		&snapshot.LeaseConfig{
			LocalRegion:   a.cfg.Region,
			InstanceID:    instanceID,
			TTL:           a.cfg.Lease.TTL,
			RenewInterval: a.cfg.Lease.RenewInterval,
		},
	)

	a.reg = prometheus.NewRegistry()
	metrics.RegisterRuntimeCollectors(a.reg)
	replicationMetrics := metrics.NewPrometheusReplicationMetrics(a.cfg.Region, a.reg)
	leaseMetrics := metrics.NewPrometheusLeaseMetrics(a.cfg.Region, a.reg)
	changefeedMetrics := metrics.NewPrometheusChangefeedMetrics(a.cfg.Region, a.reg)

	tableColumns, err := repo.LoadTableColumns(ctx, a.cfg.TableNames())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load table columns: %w", err)
	}
	handshakeCfg := models.BuildHandshakeConfiguration(
		a.cfg.Replication.Source.Type,
		a.cfg.Tables,
		tableColumns,
	)

	bus := fanout.NewMemory[models.StreamMessage]()
	repSvc := replication.NewService(
		a.cfg.Region,
		a.cfg.TableNames(),
		Version,
		handshakeCfg,
		a.cfg.Replication.ApplyMode,
		snapshotService,
		repo,
		bus,
		replicationMetrics,
	)

	peerMgr := peers.NewService(a.cfg.Region, a.cfg.Peers)

	ob, err := outbox.NewService(pool, a.cfg.DB.Postgres.StateSchema)
	if err != nil {
		return nil, nil, nil, err
	}
	metrics.RegisterOutboxCollector(a.cfg.Region, ob, a.reg)
	a.runners = append(a.runners,
		runnerFunc(func(ctx context.Context) error {
			return ob.RunMetricsRefresh(ctx, outbox.DefaultMetricsRefreshInterval)
		}),
		runnerFunc(func(ctx context.Context) error {
			return ob.RunCleanup(ctx, a.cfg.Outbox.CleanupInterval, a.cfg.Outbox.MaxBatchAge, a.cfg.Outbox.MaxPendingPerPeer)
		}),
	)

	leaseService := leaseservice.NewService(repo, leaseservice.Config{
		Kind:          "producer",
		Region:        a.cfg.Region,
		OwnerID:       instanceID,
		TTL:           a.cfg.Lease.TTL,
		RenewInterval: a.cfg.Lease.RenewInterval,
	}, leaseMetrics)

	changeAdapter := newChangefeedAdapter(a.cfg, pool, repo)

	changefeed, err := changefeedservice.NewService(
		changefeedservice.ServiceConfig{
			Source:         a.cfg.Replication.Source.Type,
			LocalRegion:    a.cfg.Region,
			InstanceID:     instanceID,
			LeaseTTL:       a.cfg.Lease.TTL,
			RenewInterval:  a.cfg.Lease.RenewInterval,
			BroadcastMode:  a.cfg.Replication.BroadcastMode,
			MaxBatchEvents: a.cfg.Replication.MaxBatchEvents,
		},
		peerMgr,
		changeAdapter,
		leaseService,
		ob,
		repo,
		changefeedMetrics,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	a.runners = append(a.runners, changefeed)
	return ob, repSvc, peerMgr, nil
}

// newChangefeedAdapter constructs the changefeed backend selected by configuration.
func newChangefeedAdapter(
	cfg Config,
	pool *pgxpool.Pool,
	repo *repositorypostgres.Repository,
) changefeedservice.Adapter {
	switch cfg.Replication.Source.Type {
	case "polling":
		return changefeedadapter.NewPolling(
			cfg.Region,
			repo,
			cfg.Tables,
			cfg.Replication.Source.Polling.Interval,
		)
	default:
		return changefeedadapter.NewChangeDataCapture(
			cfg.Region,
			cfg.DB.Postgres.DSN,
			pool,
			repo,
			cfg.Tables,
			cfg.Replication.Source.CDC.Publication,
			cfg.Replication.Source.CDC.Slot,
			cfg.Replication.Source.CDC.StatusInterval,
			cfg.Replication.Source.CDC.UpdateDiff,
		)
	}
}
