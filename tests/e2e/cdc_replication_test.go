// This file verifies the basic CDC replication contract end to end:
// bootstrap from snapshot state for pre-existing rows, then WAL-driven insert,
// update, and delete delivery through logical replication and apply.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSmokeCDCReplicatesSnapshotBootstrapAndLiveChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupCDCFixture(t)

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

	usCfg, euCfg := fixture.cdcConfigs()
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
			return fmt.Errorf("live insert not replicated yet: %w", err)
		}
		if name != "bob" {
			return fmt.Errorf("unexpected live insert row name: %s", name)
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
			return fmt.Errorf("live update not replicated yet: %w", err)
		}
		if name != "bob-updated" {
			return fmt.Errorf("unexpected live update row name: %s", name)
		}
		return nil
	})

	_, err = fixture.usDB.Pool.Exec(
		ctx,
		`DELETE FROM users WHERE id = $1`,
		"u-live",
	)
	if err != nil {
		t.Fatalf("delete live row: %v", err)
	}

	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var count int
		err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id = $1`,
			"u-live",
		).Scan(&count)
		if err != nil {
			return fmt.Errorf("query replicated delete state: %w", err)
		}
		if count != 0 {
			return fmt.Errorf("live delete not replicated yet: count=%d", count)
		}
		return nil
	})
}
