// This file verifies the same-region producer failover contract:
// when the original pod holding the producer lease stops, a standby pod in the
// same region must become the new lease owner and continue capturing writes
// into the durable outbound outbox for later delivery.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/transmogr/transmogr/internal/models"
)

func TestSlowSameRegionStandbyTakesOverProducerLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	usDB := startPostgres(t)
	createUsersTable(t, usDB)

	unusedPeerAddr := reserveListenAddr(t)

	usCfgA := pollingConfig("us", reserveListenAddr(t), usDB.DSN, []models.Endpoint{
		{Region: "eu", Endpoint: unusedPeerAddr},
	})
	usCfgB := pollingConfig("us", reserveListenAddr(t), usDB.DSN, []models.Endpoint{
		{Region: "eu", Endpoint: unusedPeerAddr},
	})

	usNodeA := startNode(t, usCfgA)

	eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		count, err := leaseCount(t, usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected one initial producer lease row, got %d", count)
		}
		return nil
	})

	initialProducer, err := currentLease(t, usDB, "producer", "us", "", "")
	if err != nil {
		t.Fatalf("load initial producer lease: %v", err)
	}

	usNodeB := startNode(t, usCfgB)
	t.Cleanup(func() {
		stopNodes(t, usNodeB, usNodeA)
	})

	stopNodes(t, usNodeA)
	usNodeA = nil

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		producer, err := currentLease(t, usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		if producer.OwnerID == initialProducer.OwnerID {
			return fmt.Errorf(
				"expected producer lease owner to change after failover: before=%s after=%s",
				initialProducer.OwnerID,
				producer.OwnerID,
			)
		}
		return nil
	})

	_, err = usDB.Pool.Exec(
		ctx,
		`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
		"u-failover",
		"captured-by-standby",
		time.Now().UTC(),
		"us",
	)
	if err != nil {
		t.Fatalf("insert row after producer failover: %v", err)
	}

	eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		pending := pendingOutboxCount(t, usDB, "eu")
		if pending == 0 {
			return fmt.Errorf("expected standby producer to enqueue pending outbox batches")
		}
		return nil
	})
}
