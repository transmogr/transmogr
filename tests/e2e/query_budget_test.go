// This file verifies that one replication wave uses a bounded number of SQL
// statements instead of drifting toward per-row query behavior. The tests enable
// `pg_stat_statements`, warm the system to a steady state, reset statement
// counters, then replicate one bulk wave and assert that both source and
// receiver stay within a fixed query budget for that wave.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

const (
	// Live-wave baselines currently land around source=12-15 and receiver=15-18
	// calls on local runs. Limits are intentionally above that healthy range but
	// still low enough to catch regressions toward row-by-row query behavior.
	pollingSourceQueryBudget   = 50
	pollingReceiverQueryBudget = 50
	cdcSourceQueryBudget       = 60
	cdcReceiverQueryBudget     = 60

	// Snapshot baselines currently land around source=20-21 and receiver=36 on
	// local runs. Receiver limits are a bit wider because bootstrap completion
	// still includes snapshot-job persistence and final catch-up bookkeeping.
	pollingSnapshotSourceQueryBudget   = 35
	pollingSnapshotReceiverQueryBudget = 45
	cdcSnapshotSourceQueryBudget       = 35
	cdcSnapshotReceiverQueryBudget     = 45
)

func TestSmokePollingReplicationUsesBoundedQueryCountPerWave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupPollingFixtureWithStats(t)
	usCfg, euCfg := fixture.pollingConfigs()

	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		cancelNodes(euNode, usNode)
	})

	insertUsersWaveSQL(t, fixture.usDB, "polling-budget-ready", 1, "us")
	awaitReplicatedPrefixCount(ctx, t, fixture.euDB, "polling-budget-ready", 1, 30*time.Second)
	awaitOutboxDrained(t, fixture.usDB, "eu", 20*time.Second)

	resetPGStatStatements(t, fixture.usDB)
	resetPGStatStatements(t, fixture.euDB)

	insertUsersWaveSQL(t, fixture.usDB, "polling-budget-wave", 250, "us")
	awaitReplicatedPrefixCount(ctx, t, fixture.euDB, "polling-budget-wave", 250, 30*time.Second)
	awaitOutboxDrained(t, fixture.usDB, "eu", 20*time.Second)

	sourceCalls := totalStatementCalls(t, fixture.usDB)
	receiverCalls := totalStatementCalls(t, fixture.euDB)
	t.Logf("polling query budget: source=%d receiver=%d", sourceCalls, receiverCalls)

	if sourceCalls > pollingSourceQueryBudget {
		t.Fatalf("polling source query budget exceeded: got=%d limit=%d", sourceCalls, pollingSourceQueryBudget)
	}
	if receiverCalls > pollingReceiverQueryBudget {
		t.Fatalf("polling receiver query budget exceeded: got=%d limit=%d", receiverCalls, pollingReceiverQueryBudget)
	}
}

func TestSmokeCDCReplicationUsesBoundedQueryCountPerWave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupCDCFixtureWithStats(t)
	usCfg, euCfg := fixture.cdcConfigs()

	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		cancelNodes(euNode, usNode)
	})

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		active, err := replicationSlotActive(t, fixture.usDB, usCfg.Replication.Source.CDC.Slot)
		if err != nil {
			return err
		}
		if !active {
			return fmt.Errorf("expected cdc query-budget slot %s to be active", usCfg.Replication.Source.CDC.Slot)
		}
		return nil
	})

	insertUsersWaveSQL(t, fixture.usDB, "cdc-budget-ready", 1, "us")
	awaitReplicatedPrefixCount(ctx, t, fixture.euDB, "cdc-budget-ready", 1, 30*time.Second)
	awaitOutboxDrained(t, fixture.usDB, "eu", 20*time.Second)

	resetPGStatStatements(t, fixture.usDB)
	resetPGStatStatements(t, fixture.euDB)

	insertUsersWaveSQL(t, fixture.usDB, "cdc-budget-wave", 250, "us")
	awaitReplicatedPrefixCount(ctx, t, fixture.euDB, "cdc-budget-wave", 250, 30*time.Second)
	awaitOutboxDrained(t, fixture.usDB, "eu", 20*time.Second)

	sourceCalls := totalStatementCalls(t, fixture.usDB)
	receiverCalls := totalStatementCalls(t, fixture.euDB)
	t.Logf("cdc query budget: source=%d receiver=%d", sourceCalls, receiverCalls)

	if sourceCalls > cdcSourceQueryBudget {
		t.Fatalf("cdc source query budget exceeded: got=%d limit=%d", sourceCalls, cdcSourceQueryBudget)
	}
	if receiverCalls > cdcReceiverQueryBudget {
		t.Fatalf("cdc receiver query budget exceeded: got=%d limit=%d", receiverCalls, cdcReceiverQueryBudget)
	}
}

func TestSmokePollingSnapshotUsesBoundedQueryCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupPollingFixtureWithStats(t)
	usCfg, euCfg := fixture.oneWayPollingConfigs()

	insertUsersWaveSQL(t, fixture.usDB, "polling-snapshot-budget", 250, "us")

	usNode := startNode(t, usCfg)
	t.Cleanup(func() {
		cancelNodes(usNode)
	})

	resetPGStatStatements(t, fixture.usDB)
	resetPGStatStatements(t, fixture.euDB)

	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		cancelNodes(euNode)
	})

	awaitReplicatedPrefixCount(ctx, t, fixture.euDB, "polling-snapshot-budget", 250, 30*time.Second)
	awaitSnapshotFinished(t, fixture.euDB, "us", "users", 20*time.Second)
	cancelNodes(euNode, usNode)
	awaitQueryStatsStable(t, fixture.usDB, fixture.euDB, 10*time.Second)

	sourceCalls := totalStatementCalls(t, fixture.usDB)
	receiverCalls := totalStatementCalls(t, fixture.euDB)
	t.Logf("polling snapshot query budget: source=%d receiver=%d", sourceCalls, receiverCalls)

	if sourceCalls > pollingSnapshotSourceQueryBudget {
		t.Fatalf(
			"polling snapshot source query budget exceeded: got=%d limit=%d",
			sourceCalls,
			pollingSnapshotSourceQueryBudget,
		)
	}
	if receiverCalls > pollingSnapshotReceiverQueryBudget {
		t.Fatalf(
			"polling snapshot receiver query budget exceeded: got=%d limit=%d",
			receiverCalls,
			pollingSnapshotReceiverQueryBudget,
		)
	}
}

func TestSmokeCDCSnapshotUsesBoundedQueryCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupCDCFixtureWithStats(t)
	usCfg, euCfg := fixture.oneWayCDCConfigs()

	insertUsersWaveSQL(t, fixture.usDB, "cdc-snapshot-budget", 250, "us")

	usNode := startNode(t, usCfg)
	t.Cleanup(func() {
		cancelNodes(usNode)
	})

	resetPGStatStatements(t, fixture.usDB)
	resetPGStatStatements(t, fixture.euDB)

	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		cancelNodes(euNode)
	})

	awaitReplicatedPrefixCount(ctx, t, fixture.euDB, "cdc-snapshot-budget", 250, 30*time.Second)
	awaitSnapshotFinished(t, fixture.euDB, "us", "users", 20*time.Second)
	cancelNodes(euNode, usNode)
	awaitQueryStatsStable(t, fixture.usDB, fixture.euDB, 10*time.Second)

	sourceCalls := totalStatementCalls(t, fixture.usDB)
	receiverCalls := totalStatementCalls(t, fixture.euDB)
	t.Logf("cdc snapshot query budget: source=%d receiver=%d", sourceCalls, receiverCalls)

	if sourceCalls > cdcSnapshotSourceQueryBudget {
		t.Fatalf("cdc snapshot source query budget exceeded: got=%d limit=%d", sourceCalls, cdcSnapshotSourceQueryBudget)
	}
	if receiverCalls > cdcSnapshotReceiverQueryBudget {
		t.Fatalf(
			"cdc snapshot receiver query budget exceeded: got=%d limit=%d",
			receiverCalls,
			cdcSnapshotReceiverQueryBudget,
		)
	}
}

func insertUsersWaveSQL(t *testing.T, db *postgresInstance, prefix string, count int, owner string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := db.Pool.Exec(ctx, `
		INSERT INTO users (id, name, updated_at, _owner_region)
		SELECT
			$1 || '-' || lpad(gs::text, 4, '0'),
			repeat('q', 1024),
			timezone('utc', now()) + (gs::text || ' microseconds')::interval,
			$2
		FROM generate_series(0, $3 - 1) AS gs
	`, prefix, owner, count)
	if err != nil {
		t.Fatalf("insert users wave sql: %v", err)
	}
}

func awaitSnapshotFinished(
	t *testing.T,
	receiverDB *postgresInstance,
	peerRegion string,
	table string,
	timeout time.Duration,
) {
	t.Helper()

	eventually(t, timeout, 500*time.Millisecond, func() error {
		job, err := snapshotJob(t, receiverDB, peerRegion, table)
		if err != nil {
			return err
		}
		if !job.Finished {
			return fmt.Errorf("snapshot job %s/%s is not finished yet", peerRegion, table)
		}
		return nil
	})
}

func awaitQueryStatsStable(
	t *testing.T,
	sourceDB *postgresInstance,
	receiverDB *postgresInstance,
	timeout time.Duration,
) {
	t.Helper()

	eventually(t, timeout, 500*time.Millisecond, func() error {
		sourceBefore := totalStatementCalls(t, sourceDB)
		receiverBefore := totalStatementCalls(t, receiverDB)
		select {
		case <-time.After(500 * time.Millisecond):
		case <-t.Context().Done():
			return t.Context().Err()
		}
		sourceAfter := totalStatementCalls(t, sourceDB)
		receiverAfter := totalStatementCalls(t, receiverDB)
		if sourceAfter != sourceBefore || receiverAfter != receiverBefore {
			return fmt.Errorf(
				"query stats still changing: source %d->%d receiver %d->%d",
				sourceBefore, sourceAfter, receiverBefore, receiverAfter,
			)
		}
		return nil
	})
}

func cancelNodes(nodes ...*testNode) {
	for _, node := range nodes {
		if node != nil {
			node.cancel()
		}
	}
}
