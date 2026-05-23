// This file verifies the bidirectional P2P replication contract:
// both regions may produce local writes at the same time, each side must receive
// the other side's row, and the replicated rows must not start bouncing back in
// a feedback loop after they have been applied remotely.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSmokePollingReplicatesBidirectionallyWithoutFeedbackLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupPollingFixture(t)
	usCfg, euCfg := fixture.pollingConfigs()

	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})

	_, err := fixture.usDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-us",
		"from-us",
		time.Now().UTC(),
		"us",
	)
	if err != nil {
		t.Fatalf("insert us source row: %v", err)
	}

	_, err = fixture.euDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-eu",
		"from-eu",
		time.Now().UTC(),
		"eu",
	)
	if err != nil {
		t.Fatalf("insert eu source row: %v", err)
	}

	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var usCount, euCount int
		if err := fixture.usDB.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&usCount); err != nil {
			return fmt.Errorf("count rows on us: %w", err)
		}
		if err := fixture.euDB.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&euCount); err != nil {
			return fmt.Errorf("count rows on eu: %w", err)
		}
		if usCount != 2 || euCount != 2 {
			return fmt.Errorf("expected both regions to converge to 2 rows, got us=%d eu=%d", usCount, euCount)
		}
		return nil
	})

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		var usLocal, usRemote, euLocal, euRemote string

		if err := fixture.usDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1 AND _owner_region = $2`,
			"u-us",
			"us",
		).Scan(&usLocal); err != nil {
			return fmt.Errorf("load us local row: %w", err)
		}
		if err := fixture.usDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1 AND _owner_region = $2`,
			"u-eu",
			"eu",
		).Scan(&usRemote); err != nil {
			return fmt.Errorf("load us remote row: %w", err)
		}
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1 AND _owner_region = $2`,
			"u-eu",
			"eu",
		).Scan(&euLocal); err != nil {
			return fmt.Errorf("load eu local row: %w", err)
		}
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT name FROM users WHERE id = $1 AND _owner_region = $2`,
			"u-us",
			"us",
		).Scan(&euRemote); err != nil {
			return fmt.Errorf("load eu remote row: %w", err)
		}

		if usLocal != "from-us" || usRemote != "from-eu" || euLocal != "from-eu" || euRemote != "from-us" {
			return fmt.Errorf(
				"unexpected bidirectional values: usLocal=%s usRemote=%s euLocal=%s euRemote=%s",
				usLocal, usRemote, euLocal, euRemote,
			)
		}

		if pendingOutboxCount(t, fixture.usDB, "eu") != 0 || pendingOutboxCount(t, fixture.euDB, "us") != 0 {
			return fmt.Errorf("expected bidirectional replication to settle without pending replay backlog")
		}

		return nil
	})
}
