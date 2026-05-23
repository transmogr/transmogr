package models

import (
	"errors"
	"time"
)

// OutboxBatch is one outbound replication batch staged for a peer.
// TxID is non-empty for CDC transaction batches (TransactionBatch wire type).
// Table is empty for CDC transaction batches and populated for polling event batches.
type OutboxBatch struct {
	ID         uint64
	PeerRegion string
	Table      string
	Sequence   uint64
	Events     []ReplicationEvent
	CreatedAt  time.Time
	TxID       string
}

// OutboxMetricsSnapshot is a read-only outbox backlog snapshot.
type OutboxMetricsSnapshot struct {
	PendingBatches        uint64
	OldestBatchAge        time.Duration
	CleanupDeleted        uint64
	CleanupDeletedByAge   uint64
	CleanupDeletedByCount uint64
}

// LastEventID returns the event id acknowledged for this batch.
func (b OutboxBatch) LastEventID() string {
	if len(b.Events) == 0 {
		return ""
	}

	return b.Events[len(b.Events)-1].EventID
}

// Validate checks that a batch can be staged safely.
func (b OutboxBatch) Validate() error {
	if b.PeerRegion == "" {
		return errors.New("outbox peer region is required")
	}
	if b.Sequence == 0 {
		return errors.New("outbox sequence must be positive")
	}
	if len(b.Events) == 0 {
		return errors.New("outbox batch must contain at least one event")
	}
	if b.LastEventID() == "" {
		return errors.New("outbox batch last event id is required")
	}

	return nil
}
