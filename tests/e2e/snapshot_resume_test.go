// This file verifies the snapshot-resume contract:
// if nodes stop after snapshot progress has already been checkpointed but before
// the snapshot finishes, the next startup must resume from the persisted cursor
// and complete without restarting from scratch or losing rows.
package e2e_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSlowPollingResumesSnapshotAfterRestartMidTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fixture := setupPollingFixture(t)

	seedRows := make([][]any, 0, 2500)
	for i := 0; i < 2500; i++ {
		seedRows = append(seedRows, []any{
			fmt.Sprintf("u-%04d", i),
			strings.Repeat("x", 512),
			time.Now().UTC().Add(-time.Minute),
			"us",
		})
	}
	seedUsersCopy(t, fixture.usDB, seedRows)

	usCfg, euCfg := fixture.pollingConfigs()
	usCfg.Replication.Snapshot.ChunkSize = 5
	euCfg.Replication.Snapshot.ChunkSize = 5

	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)

	eventually(t, 30*time.Second, 100*time.Millisecond, func() error {
		job, err := snapshotJob(t, fixture.euDB, "us", "users")
		if err != nil {
			return err
		}
		if job.Finished {
			return fmt.Errorf("snapshot already finished before interruption point")
		}
		if len(job.LastPrimaryKey) == 0 {
			return fmt.Errorf("snapshot job exists but has no persisted checkpoint yet")
		}
		return nil
	})

	stopNodes(t, euNode, usNode)
	usNode = nil
	euNode = nil

	jobAfterStop, err := snapshotJob(t, fixture.euDB, "us", "users")
	if err != nil {
		t.Fatalf("load snapshot job after coordinated stop: %v", err)
	}
	if jobAfterStop.Finished {
		t.Fatalf("expected unfinished snapshot job after mid-transfer stop")
	}
	if len(jobAfterStop.LastPrimaryKey) == 0 {
		t.Fatalf("expected persisted snapshot checkpoint after mid-transfer stop")
	}

	usNode = startNode(t, usCfg)
	euNode = startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})

	eventually(t, 60*time.Second, 200*time.Millisecond, func() error {
		var count int
		err := fixture.euDB.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&count)
		if err != nil {
			return fmt.Errorf("count replicated snapshot rows: %w", err)
		}
		if count != 2500 {
			return fmt.Errorf("snapshot not fully resumed yet: count=%d", count)
		}
		return nil
	})

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		job, err := snapshotJob(t, fixture.euDB, "us", "users")
		if err != nil {
			return err
		}
		if !job.Finished {
			return fmt.Errorf("snapshot job has not reached finished state yet")
		}
		return nil
	})
}
