// This file verifies the replay contract around temporary peer absence:
// when the sender starts before the destination is reachable, outbound batches
// must accumulate durably in the sender outbox and then be replayed and acked
// once the destination node eventually comes online.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSmokePollingReplaysOutboxAfterPeerReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupPollingFixture(t)
	usCfg, euCfg := fixture.pollingConfigs()

	usNode := startNode(t, usCfg)
	var euNode *testNode
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})

	eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		count, err := leaseCount(t, fixture.usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected us producer lease to be active before insert, got %d", count)
		}
		return nil
	})

	_, err := fixture.usDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-replay",
		"queued-while-down",
		time.Now().UTC().Add(time.Second),
		"us",
	)
	if err != nil {
		t.Fatalf("insert row while peer down: %v", err)
	}

	eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		if pending := pendingOutboxCount(t, fixture.usDB, "eu"); pending == 0 {
			return fmt.Errorf("expected pending outbox batches for eu while peer is still offline")
		}
		return nil
	})

	euNode = startNode(t, euCfg)

	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var name string
		err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1`,
			"u-replay",
		).Scan(&name)
		if err != nil {
			return fmt.Errorf("replayed row not replicated yet: %w", err)
		}
		if name != "queued-while-down" {
			return fmt.Errorf("unexpected replayed row name: %s", name)
		}
		return nil
	})

	eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		if pending := pendingOutboxCount(t, fixture.usDB, "eu"); pending != 0 {
			return fmt.Errorf("expected replayed outbox to be acked, still pending=%d", pending)
		}
		return nil
	})
}
