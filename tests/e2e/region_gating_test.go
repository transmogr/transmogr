// This file verifies the owner-region gating contract:
// when two regions contain the same primary key but each row is locally owned,
// replicated snapshot and live updates from the remote region must not overwrite
// the local row because `_owner_region` fences writes by ownership.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSmokePollingPreservesLocallyOwnedRowsAgainstRemoteSamePKWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupPollingFixture(t)

	_, err := fixture.usDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-shared",
		"us-original",
		time.Now().UTC().Add(-time.Minute),
		"us",
	)
	if err != nil {
		t.Fatalf("insert us owned seed row: %v", err)
	}

	_, err = fixture.euDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-shared",
		"eu-original",
		time.Now().UTC().Add(-time.Minute),
		"eu",
	)
	if err != nil {
		t.Fatalf("insert eu owned seed row: %v", err)
	}

	usCfg, euCfg := fixture.pollingConfigs()
	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		var usName, euName string
		if err := fixture.usDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1`,
			"u-shared",
		).Scan(&usName); err != nil {
			return fmt.Errorf("load us owned row after snapshot exchange: %w", err)
		}
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1`,
			"u-shared",
		).Scan(&euName); err != nil {
			return fmt.Errorf("load eu owned row after snapshot exchange: %w", err)
		}
		if usName != "us-original" || euName != "eu-original" {
			return fmt.Errorf("locally owned rows were overwritten during snapshot exchange: us=%s eu=%s", usName, euName)
		}
		return nil
	})

	_, err = fixture.usDB.Pool.Exec(
		ctx,
		`UPDATE users SET name = $2, updated_at = $3 WHERE id = $1`,
		"u-shared",
		"us-updated",
		time.Now().UTC().Add(time.Second),
	)
	if err != nil {
		t.Fatalf("update us owned row: %v", err)
	}

	_, err = fixture.euDB.Pool.Exec(
		ctx,
		`UPDATE users SET name = $2, updated_at = $3 WHERE id = $1`,
		"u-shared",
		"eu-updated",
		time.Now().UTC().Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("update eu owned row: %v", err)
	}

	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var usName, euName string
		if err := fixture.usDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1`,
			"u-shared",
		).Scan(&usName); err != nil {
			return fmt.Errorf("load us row after conflicting updates: %w", err)
		}
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1`,
			"u-shared",
		).Scan(&euName); err != nil {
			return fmt.Errorf("load eu row after conflicting updates: %w", err)
		}
		if usName != "us-updated" || euName != "eu-updated" {
			return fmt.Errorf("owner-region gating failed after conflicting updates: us=%s eu=%s", usName, euName)
		}
		return nil
	})
}
