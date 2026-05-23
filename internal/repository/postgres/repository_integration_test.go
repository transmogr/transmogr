package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tc "github.com/testcontainers/testcontainers-go"
	tcp "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/transmogr/transmogr/internal/models"
	"github.com/transmogr/transmogr/pkg/migrations"
)

func TestStateRepositorySuite(t *testing.T) {
	suite.Run(t, new(stateRepositorySuite))
}

type stateRepositorySuite struct {
	suite.Suite
	baseDSN   string
	container tc.Container
	repo      *Repository
	ctx       context.Context
}

func (s *stateRepositorySuite) SetupSuite() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := tcp.Run(
		ctx,
		"postgres:16-alpine",
		tcp.WithDatabase("transmogr"),
		tcp.WithUsername("postgres"),
		tcp.WithPassword("postgres"),
		tc.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	s.Require().NoError(err)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(context.Background())
	}
	s.Require().NoError(err)

	s.container = container
	s.baseDSN = dsn
}

func (s *stateRepositorySuite) TearDownSuite() {
	if s.container == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.Require().NoError(s.container.Terminate(ctx))
}

func (s *stateRepositorySuite) SetupTest() {
	s.ctx = context.Background()

	pool, err := pgxpool.New(s.ctx, s.baseDSN)
	s.Require().NoError(err)
	s.T().Cleanup(func() { pool.Close() })

	applyStateMigrations(s.T(), pool)
	truncateStateTables(s.T(), pool, "transmogr")
	createApplicationTables(s.T(), pool)

	s.repo = New(pool, []models.TableSpec{
		{
			Name:        "users",
			PrimaryKey:  []string{"id"},
			RegionField: "_owner_region",
		},
		{
			Name:        "payments",
			PrimaryKey:  []string{"id"},
			RegionField: "_owner_region",
		},
	}, "transmogr")
}

func (s *stateRepositorySuite) TestCommitAppliedTransaction() {
	events := make([]models.ReplicationEvent, 0, 3)
	for i := range 3 {
		events = append(events, models.ReplicationEvent{
			PeerRegion: "eu",
			Table:      "users",
			EventID:    fmt.Sprintf("evt-%d", i+1),
			PrimaryKey: models.PrimaryKeyString("id", fmt.Sprintf("user-%d", i+1)),
			Version:    int64(i + 1),
			Operation:  models.ReplicationOperationUpsert,
			Fields: map[string][]byte{
				"id":   []byte(fmt.Sprintf(`"user-%d"`, i+1)),
				"name": []byte(fmt.Sprintf(`"user %d"`, i+1)),
			},
		})
	}

	err := s.repo.CommitStateAppliedTransaction(s.ctx, events, models.ReplicationApplyModeFullSync)
	s.Require().NoError(err)

	cursor, err := s.repo.GetStateCursor(s.ctx, "eu", "users")
	s.Require().NoError(err)
	s.Equal("evt-3", cursor.LastEventID)
	s.Equal(int64(3), cursor.LastVersion)

	var count int
	err = s.repo.RawPoolForTests().QueryRow(s.ctx, `SELECT COUNT(*) FROM users WHERE _owner_region = 'eu'`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(3, count)

	// Replaying the same batch is a no-op (all events are stale).
	err = s.repo.CommitStateAppliedTransaction(s.ctx, events, models.ReplicationApplyModeFullSync)
	s.Require().NoError(err)

	cursor, err = s.repo.GetStateCursor(s.ctx, "eu", "users")
	s.Require().NoError(err)
	s.Equal("evt-3", cursor.LastEventID)
}

func (s *stateRepositorySuite) TestCommitAppliedTransactionDeduplicatesPK() {
	events := []models.ReplicationEvent{
		{
			PeerRegion: "eu",
			Table:      "users",
			EventID:    "evt-1",
			PrimaryKey: models.PrimaryKeyString("id", "user-1"),
			Version:    1,
			Operation:  models.ReplicationOperationUpsert,
			Fields:     map[string][]byte{"id": []byte(`"user-1"`), "name": []byte(`"first"`)},
		},
		{
			PeerRegion: "eu",
			Table:      "users",
			EventID:    "evt-2",
			PrimaryKey: models.PrimaryKeyString("id", "user-1"),
			Version:    2,
			Operation:  models.ReplicationOperationUpsert,
			Fields:     map[string][]byte{"id": []byte(`"user-1"`), "name": []byte(`"second"`)},
		},
	}

	err := s.repo.CommitStateAppliedTransaction(s.ctx, events, models.ReplicationApplyModeFullSync)
	s.Require().NoError(err)

	var name string
	err = s.repo.RawPoolForTests().QueryRow(s.ctx, `SELECT name FROM users WHERE id = 'user-1'`).Scan(&name)
	s.Require().NoError(err)
	s.Equal("second", name)

	cursor, err := s.repo.GetStateCursor(s.ctx, "eu", "users")
	s.Require().NoError(err)
	s.Equal("evt-2", cursor.LastEventID)
}

func (s *stateRepositorySuite) TestCommitAppliedTransactionDelete() {
	// First upsert a row.
	upsert := models.ReplicationEvent{
		PeerRegion: "eu",
		Table:      "users",
		EventID:    "evt-1",
		PrimaryKey: models.PrimaryKeyString("id", "user-del"),
		Version:    1,
		Operation:  models.ReplicationOperationUpsert,
		Fields:     map[string][]byte{"id": []byte(`"user-del"`), "name": []byte(`"to be deleted"`)},
	}
	err := s.repo.CommitStateAppliedTransaction(
		s.ctx,
		[]models.ReplicationEvent{upsert},
		models.ReplicationApplyModeFullSync,
	)
	s.Require().NoError(err)

	var count int
	err = s.repo.RawPoolForTests().QueryRow(s.ctx, `SELECT COUNT(*) FROM users WHERE id = 'user-del'`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(1, count)

	// Now delete it.
	del := models.ReplicationEvent{
		PeerRegion: "eu",
		Table:      "users",
		EventID:    "evt-2",
		PrimaryKey: models.PrimaryKeyString("id", "user-del"),
		Version:    2,
		Operation:  models.ReplicationOperationDelete,
	}
	err = s.repo.CommitStateAppliedTransaction(s.ctx, []models.ReplicationEvent{del}, models.ReplicationApplyModeFullSync)
	s.Require().NoError(err)

	err = s.repo.RawPoolForTests().QueryRow(s.ctx, `SELECT COUNT(*) FROM users WHERE id = 'user-del'`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(0, count)

	cursor, err := s.repo.GetStateCursor(s.ctx, "eu", "users")
	s.Require().NoError(err)
	s.Equal("evt-2", cursor.LastEventID)
}

func (s *stateRepositorySuite) TestCommitSnapshotCheckpoint() {
	job := models.StateSnapshotJob{
		PeerRegion:     "us",
		Table:          "users",
		Status:         models.SnapshotJobStatusSnapshotting,
		LastPrimaryKey: models.PrimaryKeyString("id", "pk-1"),
		WatermarkAt:    time.Unix(100, 0).UTC(),
	}

	result, err := s.repo.CommitStateSnapshotCheckpoint(s.ctx, job)
	s.Require().NoError(err)
	s.True(result.Updated)

	gotJob, err := s.repo.GetStateSnapshotJob(s.ctx, job.PeerRegion, job.Table)
	s.Require().NoError(err)
	s.Equal(job.Status, gotJob.Status)
	s.Equal(job.LastPrimaryKey, gotJob.LastPrimaryKey)
	s.True(gotJob.WatermarkAt.Equal(job.WatermarkAt))
	s.False(gotJob.Finished)

	s.Require().NoError(s.repo.MarkStateSnapshotFinished(s.ctx, job.PeerRegion, job.Table))

	skippedResult, err := s.repo.CommitStateSnapshotCheckpoint(s.ctx, models.StateSnapshotJob{
		PeerRegion:     job.PeerRegion,
		Table:          job.Table,
		Status:         models.SnapshotJobStatusSnapshotting,
		LastPrimaryKey: models.PrimaryKeyString("id", "pk-2"),
	})
	s.Require().NoError(err)
	s.False(skippedResult.Updated)
	s.Equal(models.CommitSkipReasonSnapshotDone, skippedResult.Reason)

	gotFinishedJob, err := s.repo.GetStateSnapshotJob(s.ctx, job.PeerRegion, job.Table)
	s.Require().NoError(err)
	s.True(gotFinishedJob.Finished)
	s.Equal(models.PrimaryKeyString("id", "pk-1"), gotFinishedJob.LastPrimaryKey)
}

func (s *stateRepositorySuite) TestLeaseLifecycle() {
	leaseA, acquired, err := s.repo.TryStateAcquireLease(s.ctx, "stream", "eu", "us", "", "pod-a", 5*time.Second)
	s.Require().NoError(err)
	s.True(acquired)
	s.Equal(int64(1), leaseA.Generation)

	_, acquired, err = s.repo.TryStateAcquireLease(s.ctx, "stream", "eu", "us", "", "pod-b", 5*time.Second)
	s.Require().NoError(err)
	s.False(acquired)

	renewedLease, renewed, err := s.repo.RenewStateLease(
		s.ctx,
		"stream",
		"eu",
		"us",
		"",
		"pod-a",
		leaseA.Generation,
		5*time.Second,
	)
	s.Require().NoError(err)
	s.True(renewed)
	s.Equal(leaseA.Generation, renewedLease.Generation)

	lease, err := s.repo.GetStateLease(s.ctx, "stream", "eu", "us", "")
	s.Require().NoError(err)
	s.Equal("pod-a", lease.OwnerID)
	s.Equal(leaseA.Generation, lease.Generation)

	released, err := s.repo.ReleaseStateLease(s.ctx, "stream", "eu", "us", "", "pod-a", leaseA.Generation)
	s.Require().NoError(err)
	s.True(released)

	leaseB, acquired, err := s.repo.TryStateAcquireLease(s.ctx, "stream", "eu", "us", "", "pod-b", 5*time.Second)
	s.Require().NoError(err)
	s.True(acquired)
	s.Equal(int64(1), leaseB.Generation)
}

func (s *stateRepositorySuite) TestCleanupExpiredStateLeases() {
	lease, acquired, err := s.repo.TryStateAcquireLease(s.ctx, "stream", "eu", "us", "", "pod-a", 5*time.Second)
	s.Require().NoError(err)
	s.True(acquired)

	_, err = s.repo.RawPoolForTests().Exec(
		s.ctx,
		`UPDATE transmogr.leases
		 SET expires_at = NOW() - INTERVAL '1 minute'
		 WHERE lease_kind = $1 AND region = $2 AND peer_region = $3 AND table_name IS NULL`,
		lease.Kind,
		lease.Region,
		lease.PeerRegion,
	)
	s.Require().NoError(err)

	deleted, err := s.repo.CleanupExpiredStateLeases(s.ctx)
	s.Require().NoError(err)
	s.Equal(int64(1), deleted)

	gotLease, err := s.repo.GetStateLease(s.ctx, lease.Kind, lease.Region, lease.PeerRegion, lease.Table)
	s.Require().NoError(err)
	s.Equal(models.Lease{}, gotLease)
}

func (s *stateRepositorySuite) TestSnapshotLeasesAreScopedByTable() {
	leaseUsers, acquired, err := s.repo.TryStateAcquireLease(
		s.ctx,
		"snapshot",
		"eu",
		"us",
		"users",
		"pod-a",
		5*time.Second,
	)
	s.Require().NoError(err)
	s.True(acquired)

	leasePayments, acquired, err := s.repo.TryStateAcquireLease(
		s.ctx,
		"snapshot",
		"eu",
		"us",
		"payments",
		"pod-b",
		5*time.Second,
	)
	s.Require().NoError(err)
	s.True(acquired)

	s.Equal("users", leaseUsers.Table)
	s.Equal("payments", leasePayments.Table)
}

func (s *stateRepositorySuite) TestOutboxLifecycle() {
	sequence, err := s.repo.OutboxNextSequence(s.ctx, "eu", "users")
	s.Require().NoError(err)
	s.Equal(uint64(1), sequence)

	batch := models.OutboxBatch{
		PeerRegion: "eu",
		Table:      "users",
		Sequence:   sequence,
		Events: []models.ReplicationEvent{
			{
				EventID:    "evt-1",
				Table:      "users",
				PeerRegion: "local",
				PrimaryKey: models.PrimaryKeyString("id", "u1"),
				Operation:  models.ReplicationOperationUpsert,
				Version:    1,
				Fields:     map[string][]byte{"id": []byte(`"u1"`)},
			},
		},
	}

	stored, inserted, err := s.repo.OutboxEnqueue(s.ctx, batch)
	s.Require().NoError(err)
	s.True(inserted)
	s.NotZero(stored.ID)
	s.False(stored.CreatedAt.IsZero())
	s.Equal(batch.LastEventID(), stored.LastEventID())

	pending, err := s.repo.OutboxListPending(s.ctx, "eu", 10)
	s.Require().NoError(err)
	s.Require().Len(pending, 1)
	s.Equal("evt-1", pending[0].LastEventID())

	acked, err := s.repo.OutboxAck(s.ctx, "eu", "users", "evt-1")
	s.Require().NoError(err)
	s.True(acked)

	pending, err = s.repo.OutboxListPending(s.ctx, "eu", 10)
	s.Require().NoError(err)
	s.Empty(pending)
}

func (s *stateRepositorySuite) TestOutboxCleanupRemovesOldBatches() {
	_, inserted, err := s.repo.OutboxEnqueue(s.ctx, models.OutboxBatch{
		PeerRegion: "eu",
		Table:      "users",
		Sequence:   1,
		Events: []models.ReplicationEvent{
			{
				EventID:    "evt-old",
				Table:      "users",
				PeerRegion: "local",
				PrimaryKey: models.PrimaryKeyString("id", "u1"),
				Operation:  models.ReplicationOperationUpsert,
				Version:    1,
				Fields:     map[string][]byte{"id": []byte(`"u1"`)},
			},
		},
	})
	s.Require().NoError(err)
	s.True(inserted)

	_, err = s.repo.RawPoolForTests().Exec(s.ctx, `
		UPDATE transmogr.outbound_batches
		SET created_at = NOW() - INTERVAL '10 days'
		WHERE peer_region = 'eu' AND table_name = 'users' AND sequence = 1
	`)
	s.Require().NoError(err)

	deletedByAge, deletedByCount, err := s.repo.OutboxCleanup(s.ctx, 24*time.Hour, 0)
	s.Require().NoError(err)
	s.Equal(int64(1), deletedByAge)
	s.Equal(int64(0), deletedByCount)

	pending, err := s.repo.OutboxListPending(s.ctx, "eu", 10)
	s.Require().NoError(err)
	s.Empty(pending)
}

func (s *stateRepositorySuite) TestOutboxCleanupCapsPendingPerPeer() {
	for sequence := uint64(1); sequence <= 3; sequence++ {
		_, inserted, err := s.repo.OutboxEnqueue(s.ctx, models.OutboxBatch{
			PeerRegion: "eu",
			Table:      "users",
			Sequence:   sequence,
			Events: []models.ReplicationEvent{
				{
					EventID:    fmt.Sprintf("evt-%d", sequence),
					Table:      "users",
					PeerRegion: "local",
					PrimaryKey: models.PrimaryKeyString("id", fmt.Sprintf("u%d", sequence)),
					Operation:  models.ReplicationOperationUpsert,
					Version:    int64(sequence),
					Fields:     map[string][]byte{"id": []byte(fmt.Sprintf("\"u%d\"", sequence))},
				},
			},
		})
		s.Require().NoError(err)
		s.True(inserted)
	}

	deletedByAge, deletedByCount, err := s.repo.OutboxCleanup(s.ctx, 0, 2)
	s.Require().NoError(err)
	s.Equal(int64(0), deletedByAge)
	s.Equal(int64(1), deletedByCount)

	pending, err := s.repo.OutboxListPending(s.ctx, "eu", 10)
	s.Require().NoError(err)
	s.Require().Len(pending, 2)
	s.Equal(uint64(2), pending[0].Sequence)
	s.Equal(uint64(3), pending[1].Sequence)
}

func (s *stateRepositorySuite) TestOutboxListPendingZeroLimitReturnsFullBacklog() {
	total := 1100
	for i := 1; i <= total; i++ {
		sequence := uint64(i)
		_, inserted, err := s.repo.OutboxEnqueue(s.ctx, models.OutboxBatch{
			PeerRegion: "eu",
			Table:      "users",
			Sequence:   sequence,
			Events: []models.ReplicationEvent{
				{
					EventID:    fmt.Sprintf("evt-%d", i),
					Table:      "users",
					PeerRegion: "local",
					PrimaryKey: models.PrimaryKeyString("id", fmt.Sprintf("u%d", i)),
					Operation:  models.ReplicationOperationUpsert,
					Version:    int64(i),
					Fields:     map[string][]byte{"id": []byte(fmt.Sprintf("\"u%d\"", i))},
				},
			},
		})
		s.Require().NoError(err)
		s.True(inserted)
	}

	pending, err := s.repo.OutboxListPending(s.ctx, "eu", 0)
	s.Require().NoError(err)
	s.Require().Len(pending, total)
	s.Equal(uint64(1), pending[0].Sequence)
	s.Equal(uint64(total), pending[len(pending)-1].Sequence)
}

func (s *stateRepositorySuite) TestOutboxListPendingWindowPagesByID() {
	tables := []string{"users", "orders", "products"}
	expectedEventIDs := make([]string, 0, len(tables))
	for i, table := range tables {
		eventID := fmt.Sprintf("evt-%s", table)
		_, inserted, err := s.repo.OutboxEnqueue(s.ctx, models.OutboxBatch{
			PeerRegion: "eu",
			Table:      table,
			Sequence:   1,
			Events: []models.ReplicationEvent{
				{
					EventID:    eventID,
					Table:      table,
					PeerRegion: "local",
					PrimaryKey: models.PrimaryKeyString("id", fmt.Sprintf("%d", i+1)),
					Operation:  models.ReplicationOperationUpsert,
					Version:    1,
					Fields:     map[string][]byte{"id": []byte(fmt.Sprintf("\"%d\"", i+1))},
				},
			},
		})
		s.Require().NoError(err)
		s.True(inserted)
		expectedEventIDs = append(expectedEventIDs, eventID)
	}

	var (
		afterID     uint64
		gotEventIDs []string
	)
	for {
		pending, err := s.repo.OutboxListPendingWindow(s.ctx, "eu", time.Time{}, time.Now().Add(time.Hour), afterID, 2)
		s.Require().NoError(err)
		if len(pending) == 0 {
			break
		}
		for _, batch := range pending {
			gotEventIDs = append(gotEventIDs, batch.LastEventID())
			afterID = batch.ID
		}
	}

	s.Equal(expectedEventIDs, gotEventIDs)
}

func TestOutboxBatchValidate(t *testing.T) {
	t.Parallel()

	err := models.OutboxBatch{}.Validate()
	require.Error(t, err)
}

func applyStateMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	if err := migrations.ApplyUp(context.Background(), pool, "transmogr"); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}

func truncateStateTables(t *testing.T, pool *pgxpool.Pool, stateSchema string) {
	t.Helper()

	const query = `
		TRUNCATE TABLE
			%s,
			%s,
			%s,
			%s,
			%s
	`

	formattedQuery := fmt.Sprintf(
		query,
		pgx.Identifier{stateSchema, "peer_cursors"}.Sanitize(),
		pgx.Identifier{stateSchema, "snapshot_jobs"}.Sanitize(),
		pgx.Identifier{stateSchema, "leases"}.Sanitize(),
		pgx.Identifier{stateSchema, "outbound_sequences"}.Sanitize(),
		pgx.Identifier{stateSchema, "outbound_batches"}.Sanitize(),
	)

	if _, err := pool.Exec(context.Background(), formattedQuery); err != nil {
		t.Fatalf("truncate state tables: %v", err)
	}
}

func createApplicationTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		DROP TABLE IF EXISTS payments;
		DROP TABLE IF EXISTS users;
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			_owner_region TEXT
		);
		CREATE TABLE payments (
			id TEXT PRIMARY KEY,
			amount BIGINT NOT NULL,
			_owner_region TEXT
		);
	`)
	if err != nil {
		t.Fatalf("create application tables: %v", err)
	}
}
