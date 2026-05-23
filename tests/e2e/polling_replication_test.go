// This file verifies the basic polling replication contract end to end:
// bootstrap from snapshot state when the destination has no cursor yet, then
// propagation of new writes and later updates over the live polling path.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSmokePollingReplicatesSnapshotBootstrapAndLiveChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupPollingFixture(t)

	_, err := fixture.usDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-snapshot",
		"alice",
		time.Now().UTC().Add(-time.Minute),
		"us",
	)
	if err != nil {
		t.Fatalf("insert snapshot seed row: %v", err)
	}

	usCfg, euCfg := fixture.pollingConfigs()
	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})

	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var name, owner string
		err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT name, _owner_region FROM users WHERE id = $1`,
			"u-snapshot",
		).Scan(&name, &owner)
		if err != nil {
			return fmt.Errorf("snapshot row not replicated yet: %w", err)
		}
		if name != "alice" || owner != "us" {
			return fmt.Errorf("unexpected snapshot row values: name=%s owner=%s", name, owner)
		}
		return nil
	})

	_, err = fixture.usDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-live",
		"bob",
		time.Now().UTC(),
		"us",
	)
	if err != nil {
		t.Fatalf("insert live row: %v", err)
	}

	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var name string
		err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1`,
			"u-live",
		).Scan(&name)
		if err != nil {
			return fmt.Errorf("live row not replicated yet: %w", err)
		}
		if name != "bob" {
			return fmt.Errorf("unexpected live row name: %s", name)
		}
		return nil
	})

	_, err = fixture.usDB.Pool.Exec(
		ctx,
		`UPDATE users SET name = $2, updated_at = $3 WHERE id = $1`,
		"u-live",
		"bob-updated",
		time.Now().UTC().Add(time.Second),
	)
	if err != nil {
		t.Fatalf("update live row: %v", err)
	}

	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var name string
		err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1`,
			"u-live",
		).Scan(&name)
		if err != nil {
			return fmt.Errorf("updated row not replicated yet: %w", err)
		}
		if name != "bob-updated" {
			return fmt.Errorf("unexpected updated row name: %s", name)
		}
		return nil
	})
}
