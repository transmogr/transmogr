// Package e2e_test contains end-to-end tests that boot multiple real Transmogr
// nodes against PostgreSQL containers and verify system behavior across process,
// transport, state, lease, snapshot, and replay boundaries.
package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tc "github.com/testcontainers/testcontainers-go"
	tcp "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/transmogr/transmogr/internal/app"
	"github.com/transmogr/transmogr/internal/models"
	"github.com/transmogr/transmogr/pkg/migrations"
)

const e2eStateSchema = "transmogr"

type postgresInstance struct {
	DSN  string
	Pool *pgxpool.Pool
}

type testNode struct {
	region string
	cancel context.CancelFunc
	done   chan error

	stopOnce sync.Once
	stopErr  error
}

type leaseRecord struct {
	OwnerID    string
	Generation int64
}

type snapshotJobRecord struct {
	Status         string
	LastPrimaryKey []models.PrimaryKeyPart
	Finished       bool
}

// twoRegionFixture is a compact test fixture for the common "us <-> eu" setup
// used by most e2e scenarios. It owns both databases and both gRPC listen
// addresses, while tests remain free to customize the returned node configs.
type twoRegionFixture struct {
	usDB   *postgresInstance
	euDB   *postgresInstance
	usAddr string
	euAddr string
}

// startPostgres starts a standard PostgreSQL container for scenarios that only need
// normal SQL traffic, such as polling replication and lease coordination tests.
func startPostgres(t *testing.T) *postgresInstance {
	t.Helper()
	return startPostgresContainer(t, nil, false)
}

func startPostgresWithStats(t *testing.T) *postgresInstance {
	t.Helper()
	return startPostgresContainer(t, nil, true)
}

// setupPollingFixture creates the standard two-region fixture for polling-based tests.
func setupPollingFixture(t *testing.T) twoRegionFixture {
	t.Helper()

	usDB := startPostgres(t)
	euDB := startPostgres(t)
	createUsersTable(t, usDB)
	createUsersTable(t, euDB)

	return twoRegionFixture{
		usDB:   usDB,
		euDB:   euDB,
		usAddr: reserveListenAddr(t),
		euAddr: reserveListenAddr(t),
	}
}

func setupPollingFixtureWithStats(t *testing.T) twoRegionFixture {
	t.Helper()

	usDB := startPostgresWithStats(t)
	euDB := startPostgresWithStats(t)
	createUsersTable(t, usDB)
	createUsersTable(t, euDB)

	return twoRegionFixture{
		usDB:   usDB,
		euDB:   euDB,
		usAddr: reserveListenAddr(t),
		euAddr: reserveListenAddr(t),
	}
}

// setupCDCFixture creates the standard two-region fixture for CDC-based tests.
func setupCDCFixture(t *testing.T) twoRegionFixture {
	t.Helper()

	usDB := startLogicalPostgres(t)
	euDB := startLogicalPostgres(t)
	createUsersTable(t, usDB)
	createUsersTable(t, euDB)
	enableCDCOnUsers(t, usDB)
	enableCDCOnUsers(t, euDB)

	return twoRegionFixture{
		usDB:   usDB,
		euDB:   euDB,
		usAddr: reserveListenAddr(t),
		euAddr: reserveListenAddr(t),
	}
}

func setupCDCFixtureWithStats(t *testing.T) twoRegionFixture {
	t.Helper()

	usDB := startLogicalPostgresWithStats(t)
	euDB := startLogicalPostgresWithStats(t)
	createUsersTable(t, usDB)
	createUsersTable(t, euDB)
	enableCDCOnUsers(t, usDB)
	enableCDCOnUsers(t, euDB)

	return twoRegionFixture{
		usDB:   usDB,
		euDB:   euDB,
		usAddr: reserveListenAddr(t),
		euAddr: reserveListenAddr(t),
	}
}

// cloneConfig returns a copy of app.Config with slice fields detached so tests can
// mutate one node config without accidentally sharing slice backing arrays.
func cloneConfig(cfg app.Config) app.Config {
	cloned := cfg
	cloned.Peers = append([]models.Endpoint(nil), cfg.Peers...)
	cloned.Tables = append([]models.TableSpec(nil), cfg.Tables...)
	return cloned
}

// startLogicalPostgres starts a PostgreSQL container with logical replication enabled
// so CDC tests can exercise publications, replication slots, and WAL-based delivery.
func startLogicalPostgres(t *testing.T) *postgresInstance {
	t.Helper()
	return startPostgresContainer(t, []string{
		"-c", "wal_level=logical",
		"-c", "max_replication_slots=10",
		"-c", "max_wal_senders=10",
	}, false)
}

func startLogicalPostgresWithStats(t *testing.T) *postgresInstance {
	t.Helper()
	return startPostgresContainer(t, []string{
		"-c", "wal_level=logical",
		"-c", "max_replication_slots=10",
		"-c", "max_wal_senders=10",
	}, true)
}

func startPostgresContainer(t *testing.T, cmd []string, enableStats bool) *postgresInstance {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	opts := []tc.ContainerCustomizer{
		tcp.WithDatabase("transmogr"),
		tcp.WithUsername("postgres"),
		tcp.WithPassword("postgres"),
		tc.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(2 * time.Minute),
		),
	}
	if enableStats {
		cmd = append(cmd,
			"-c", "shared_preload_libraries=pg_stat_statements",
			"-c", "pg_stat_statements.track=all",
		)
	}
	if len(cmd) > 0 {
		opts = append(opts, tc.WithCmd(cmd...))
	}

	container, err := tcp.Run(ctx, "postgres:16-alpine", opts...)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	t.Cleanup(func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer termCancel()
		_ = container.Terminate(termCtx)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("build postgres dsn: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create postgres pool: %v", err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer pingCancel()
	if err := waitForPostgres(pingCtx, pool); err != nil {
		pool.Close()
		t.Fatalf("ping postgres pool: %v", err)
	}

	t.Cleanup(pool.Close)

	if err := migrations.ApplyUp(ctx, pool, e2eStateSchema); err != nil {
		t.Fatalf("apply state migrations: %v", err)
	}
	if enableStats {
		enablePGStatStatements(t, pool)
	}

	return &postgresInstance{
		DSN:  dsn,
		Pool: pool,
	}
}

func enablePGStatStatements(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		t.Fatalf("create pg_stat_statements extension: %v", err)
	}
}

func waitForPostgres(ctx context.Context, pool *pgxpool.Pool) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := pool.Ping(ctx); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func resetPGStatStatements(t *testing.T, db *postgresInstance) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := db.Pool.Exec(ctx, `SELECT pg_stat_statements_reset()`); err != nil {
		t.Fatalf("reset pg_stat_statements: %v", err)
	}
}

func totalStatementCalls(t *testing.T, db *postgresInstance) uint64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var calls uint64
	err := db.Pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(calls), 0)::bigint
		FROM pg_stat_statements
		WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
		  AND query NOT ILIKE '%pg_stat_statements%'
	`).Scan(&calls)
	if err != nil {
		t.Fatalf("sum pg_stat_statements calls: %v", err)
	}

	return calls
}

// createUsersTable creates the shared application table used by the e2e scenarios.
// The schema intentionally includes `updated_at` and `_owner_region` so the same
// table can be exercised by both polling and CDC flows.
func createUsersTable(t *testing.T, db *postgresInstance) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := db.Pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			_owner_region TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("create users table: %v", err)
	}
}

// enableCDCOnUsers enables REPLICA IDENTITY FULL so CDC tests can observe old row
// images for deletes and region-filtered updates.
func enableCDCOnUsers(t *testing.T, db *postgresInstance) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := db.Pool.Exec(ctx, `ALTER TABLE users REPLICA IDENTITY FULL`)
	if err != nil {
		t.Fatalf("enable replica identity full: %v", err)
	}
}

// startNode creates and runs one Transmogr app instance that behaves like a single
// regional pod. Tests compose multiple such nodes to model cross-region clusters
// and same-region multi-pod deployments.
func startNode(t *testing.T, cfg app.Config) *testNode {
	t.Helper()

	ctx := context.Background()
	instance, err := app.New(ctx, cfg)
	if err != nil {
		t.Fatalf("create app for region %s: %v", cfg.Region, err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- instance.Run(runCtx)
	}()

	node := &testNode{
		region: cfg.Region,
		cancel: cancel,
		done:   done,
	}

	return node
}

// stopNodes stops a group of nodes together so peer streams, leases, and cleanup
// loops unwind in a coordinated way during teardown or mid-test restarts.
func stopNodes(t *testing.T, nodes ...*testNode) {
	t.Helper()

	for _, node := range nodes {
		if node == nil {
			continue
		}
		node.cancel()
	}

	for _, node := range nodes {
		if node == nil {
			continue
		}
		if err := node.stop(30 * time.Second); !isExpectedShutdownError(err) {
			t.Fatalf("node %s stopped with error: %v", node.region, err)
		}
	}
}

func (n *testNode) stop(timeout time.Duration) error {
	n.stopOnce.Do(func() {
		select {
		case err := <-n.done:
			n.stopErr = err
		case <-time.After(timeout):
			n.stopErr = fmt.Errorf("timeout stopping node %s", n.region)
		}
	})

	return n.stopErr
}

// pendingOutboxCount returns the number of still-unacked outbound batches for one
// remote peer, which is the main observable for replay/backlog scenarios.
func pendingOutboxCount(t *testing.T, db *postgresInstance, peerRegion string) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := db.Pool.QueryRow(
		ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s.outbound_batches WHERE peer_region = $1`, e2eStateSchema),
		peerRegion,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count pending outbox batches: %v", err)
	}

	return count
}

// currentLease loads the current lease owner and generation for one lease scope
// from the Transmogr state schema.
func currentLease(
	t *testing.T,
	db *postgresInstance,
	kind, region, peerRegion, table string,
) (leaseRecord, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var lease leaseRecord
	err := db.Pool.QueryRow(
		ctx,
		fmt.Sprintf(`
			SELECT owner_id, generation
			FROM %s.leases
			WHERE lease_kind = $1
			  AND region = $2
			  AND peer_region = $3
			  AND table_name IS NOT DISTINCT FROM $4
		`, e2eStateSchema),
		kind,
		region,
		peerRegion,
		nullableText(table),
	).Scan(&lease.OwnerID, &lease.Generation)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return leaseRecord{}, err
		}
		return leaseRecord{}, fmt.Errorf("load lease %s/%s/%s: %w", kind, region, peerRegion, err)
	}

	return lease, nil
}

// leaseCount returns the number of persisted lease rows for one scope so tests can
// assert single-owner invariants even when the specific owner ID is not important.
func leaseCount(
	t *testing.T,
	db *postgresInstance,
	kind, region, peerRegion, table string,
) (int, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := db.Pool.QueryRow(
		ctx,
		fmt.Sprintf(`
			SELECT COUNT(*)
			FROM %s.leases
			WHERE lease_kind = $1
			  AND region = $2
			  AND peer_region = $3
			  AND table_name IS NOT DISTINCT FROM $4
		`, e2eStateSchema),
		kind,
		region,
		peerRegion,
		nullableText(table),
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count lease %s/%s/%s: %w", kind, region, peerRegion, err)
	}

	return count, nil
}

// snapshotJob loads persisted snapshot progress for one peer/table pair from the
// receiver state schema. Tests use this to assert mid-snapshot progress and resume.
func snapshotJob(
	t *testing.T,
	db *postgresInstance,
	peerRegion, table string,
) (snapshotJobRecord, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		job            snapshotJobRecord
		lastPrimaryKey []byte
	)
	err := db.Pool.QueryRow(
		ctx,
		fmt.Sprintf(`
			SELECT status, last_primary_key, finished_at IS NOT NULL
			FROM %s.snapshot_jobs
			WHERE peer_region = $1
			  AND table_name = $2
		`, e2eStateSchema),
		peerRegion,
		table,
	).Scan(&job.Status, &lastPrimaryKey, &job.Finished)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return snapshotJobRecord{}, err
		}
		return snapshotJobRecord{}, fmt.Errorf("load snapshot job %s/%s: %w", peerRegion, table, err)
	}

	pk, err := models.DecodePrimaryKey(lastPrimaryKey)
	if err != nil {
		return snapshotJobRecord{}, fmt.Errorf("decode snapshot job pk %s/%s: %w", peerRegion, table, err)
	}
	job.LastPrimaryKey = pk
	return job, nil
}

// replicationSlotActive reports whether the named logical replication slot exists
// and is currently active on the source database.
func replicationSlotActive(t *testing.T, db *postgresInstance, slot string) (bool, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists bool
	var active bool
	err := db.Pool.QueryRow(
		ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1),
		        COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = $1), false)`,
		slot,
	).Scan(&exists, &active)
	if err != nil {
		return false, fmt.Errorf("check replication slot %s: %w", slot, err)
	}

	return exists && active, nil
}

// seedUsersCopy seeds many `users` rows in one COPY operation so large snapshot
// scenarios do not spend most of their time on per-row round trips.
func seedUsersCopy(t *testing.T, db *postgresInstance, rows [][]any) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := db.Pool.CopyFrom(
		ctx,
		pgx.Identifier{"users"},
		[]string{"id", "name", "updated_at", "_owner_region"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		t.Fatalf("copy users seed rows: %v", err)
	}
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}

	return value
}

// pollingConfig builds a compact in-process config for one polling-based node.
func pollingConfig(region, grpcAddr, dsn string, peers []models.Endpoint) app.Config {
	return testConfig(region, grpcAddr, dsn, peers, "polling")
}

// cdcConfig builds a compact in-process config for one CDC-based node.
func cdcConfig(region, grpcAddr, dsn string, peers []models.Endpoint) app.Config {
	cfg := testConfig(region, grpcAddr, dsn, peers, "cdc")
	cfg.Replication.Source.CDC.Publication = fmt.Sprintf("transmogr_pub_%s", region)
	cfg.Replication.Source.CDC.Slot = fmt.Sprintf("transmogr_slot_%s", region)
	cfg.Replication.Source.CDC.StatusInterval = 200 * time.Millisecond
	return cfg
}

// pollingConfigs returns a ready-to-use polling config pair for the fixture.
func (f twoRegionFixture) pollingConfigs() (app.Config, app.Config) {
	return pollingConfig("us", f.usAddr, f.usDB.DSN, []models.Endpoint{
			{Region: "eu", Endpoint: f.euAddr},
		}),
		pollingConfig("eu", f.euAddr, f.euDB.DSN, []models.Endpoint{
			{Region: "us", Endpoint: f.usAddr},
		})
}

// cdcConfigs returns a ready-to-use CDC config pair for the fixture.
func (f twoRegionFixture) cdcConfigs() (app.Config, app.Config) {
	return cdcConfig("us", f.usAddr, f.usDB.DSN, []models.Endpoint{
			{Region: "eu", Endpoint: f.euAddr},
		}),
		cdcConfig("eu", f.euAddr, f.euDB.DSN, []models.Endpoint{
			{Region: "us", Endpoint: f.usAddr},
		})
}

func (f twoRegionFixture) oneWayPollingConfigs() (app.Config, app.Config) {
	return pollingConfig("us", f.usAddr, f.usDB.DSN, nil),
		pollingConfig("eu", f.euAddr, f.euDB.DSN, []models.Endpoint{
			{Region: "us", Endpoint: f.usAddr},
		})
}

func (f twoRegionFixture) oneWayCDCConfigs() (app.Config, app.Config) {
	return cdcConfig("us", f.usAddr, f.usDB.DSN, nil),
		cdcConfig("eu", f.euAddr, f.euDB.DSN, []models.Endpoint{
			{Region: "us", Endpoint: f.usAddr},
		})
}

func awaitReplicatedPrefixCount(
	ctx context.Context,
	t *testing.T,
	db *postgresInstance,
	prefix string,
	expected int,
	timeout time.Duration,
) {
	t.Helper()

	eventually(t, timeout, 200*time.Millisecond, func() error {
		var count int
		if err := db.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id LIKE $1`,
			prefix+"-%",
		).Scan(&count); err != nil {
			return fmt.Errorf("count replicated rows for prefix %s: %w", prefix, err)
		}
		if count != expected {
			return fmt.Errorf(
				"replicated rows for prefix %s not complete yet: count=%d expected=%d",
				prefix,
				count,
				expected,
			)
		}
		return nil
	})
}

func awaitOutboxDrained(t *testing.T, db *postgresInstance, peerRegion string, timeout time.Duration) {
	t.Helper()

	eventually(t, timeout, 200*time.Millisecond, func() error {
		if pending := pendingOutboxCount(t, db, peerRegion); pending != 0 {
			return fmt.Errorf("expected outbox for %s to be drained, got %d pending batches", peerRegion, pending)
		}
		return nil
	})
}

func testConfig(region, grpcAddr, dsn string, peers []models.Endpoint, source string) app.Config {
	cfg := app.Config{
		Region:    region,
		LogLevel:  "info",
		LogFormat: "text",
		Transport: app.TransportConfig{
			ReconnectDelay: 200 * time.Millisecond,
			PingInterval:   500 * time.Millisecond,
			GRPC: app.GRPCTransportConfig{
				ListenAddr: grpcAddr,
				Insecure:   true,
			},
		},
		Crypto: app.CryptoConfig{
			RefreshInterval: time.Minute,
			RetryAttempts:   3,
			RetryDelay:      time.Second,
		},
		Lease: app.LeaseConfig{
			TTL:             2 * time.Second,
			RenewInterval:   200 * time.Millisecond,
			CleanupInterval: time.Minute,
		},
		Outbox: app.OutboxConfig{
			CleanupInterval:   time.Minute,
			MaxBatchAge:       time.Hour,
			MaxPendingPerPeer: 1000,
		},
		Replication: app.ReplicationConfig{
			Source: app.SourceConfig{
				Type: source,
				CDC: app.CDCSourceConfig{
					StatusInterval: time.Second,
				},
				Polling: app.PollingSourceConfig{
					Interval: 200 * time.Millisecond,
				},
			},
			Snapshot: app.SnapshotConfig{
				ChunkSize: 100,
			},
			ApplyMode:      models.ReplicationApplyModeFullSync,
			BroadcastMode:  models.ReplicationBroadcastModeFullSync,
			MaxBatchEvents: 1000,
		},
		DB: app.DatabaseConfig{
			Postgres: app.PostgresConfig{
				DSN:         dsn,
				StateSchema: e2eStateSchema,
			},
		},
		Peers: peers,
		Tables: []models.TableSpec{
			{
				Name:           "users",
				PrimaryKey:     []string{"id"},
				UpdatedAtField: "updated_at",
				RegionField:    "_owner_region",
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return cfg
}

// reserveListenAddr returns a free loopback address that a test node can bind to.
// The address is reserved only briefly, so tests should start the node soon after.
func reserveListenAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve listen addr: %v", err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

// eventually retries fn until it succeeds or the timeout expires. It is the main
// synchronization primitive in the e2e suite because system behavior is async.
func eventually(t *testing.T, timeout, interval time.Duration, fn func() error) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		err := fn()
		if err == nil {
			return
		}
		lastErr = err

		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-t.Context().Done():
			if !timer.Stop() {
				<-timer.C
			}
			t.Fatal(t.Context().Err())
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("condition not met before timeout")
	}
	t.Fatal(lastErr)
}

// isExpectedShutdownError filters noisy network errors that can happen during
// coordinated teardown once listeners and peer connections are closing together.
func isExpectedShutdownError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return true
	}

	return strings.Contains(err.Error(), "use of closed network connection")
}
