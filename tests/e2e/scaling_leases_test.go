// This file verifies the steady-state scaling contract inside one region:
// multiple pods may run concurrently, but the state schema must converge on a
// single producer lease and a single outbound stream lease for each peer pair,
// while replication still proceeds exactly once from the region.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSmokeSameRegionPodsShareSingleProducerAndStreamLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fixture := setupPollingFixture(t)
	usCfgA, euCfg := fixture.pollingConfigs()
	usCfgB := cloneConfig(usCfgA)
	usCfgB.Transport.GRPC.ListenAddr = reserveListenAddr(t)

	usNodeA := startNode(t, usCfgA)
	usNodeB := startNode(t, usCfgB)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNodeB, usNodeA)
	})

	eventually(t, 15*time.Second, 200*time.Millisecond, func() error {
		producerCount, err := leaseCount(t, fixture.usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		streamCount, err := leaseCount(t, fixture.usDB, "stream", "us", "eu", "")
		if err != nil {
			return err
		}

		if producerCount != 1 {
			return fmt.Errorf("expected exactly one producer lease row, got %d", producerCount)
		}
		if streamCount != 1 {
			return fmt.Errorf("expected exactly one stream lease row, got %d", streamCount)
		}
		return nil
	})

	_, err := fixture.usDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-scaled",
		"from-scaled-region",
		time.Now().UTC(),
		"us",
	)
	if err != nil {
		t.Fatalf("insert row in scaled region: %v", err)
	}

	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var count int
		err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id = $1`,
			"u-scaled",
		).Scan(&count)
		if err != nil {
			return fmt.Errorf("query scaled replication row: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("expected exactly one replicated row, got count=%d", count)
		}
		return nil
	})

	producerLease, err := currentLease(t, fixture.usDB, "producer", "us", "", "")
	if err != nil {
		t.Fatalf("load final producer lease: %v", err)
	}
	streamLease, err := currentLease(t, fixture.usDB, "stream", "us", "eu", "")
	if err != nil {
		t.Fatalf("load final stream lease: %v", err)
	}
	if producerLease.Generation < 1 {
		t.Fatalf("expected producer lease generation >= 1, got %d", producerLease.Generation)
	}
	if streamLease.Generation < 1 {
		t.Fatalf("expected stream lease generation >= 1, got %d", streamLease.Generation)
	}
}
