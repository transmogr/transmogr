package models

import "time"

// StateCursor tracks the last applied event state for a peer and table.
type StateCursor struct {
	PeerRegion     string
	Table          string
	LastEventID    string
	LastVersion    int64
	LastPrimaryKey []PrimaryKeyPart
}

// SnapshotJobStatus describes the receiver-side lifecycle of a snapshot job.
type SnapshotJobStatus string

const (
	// SnapshotJobStatusPending means the snapshot job was created but not started.
	SnapshotJobStatusPending SnapshotJobStatus = "pending"
	// SnapshotJobStatusSnapshotting means snapshot rows are currently streaming.
	SnapshotJobStatusSnapshotting SnapshotJobStatus = "snapshotting"
	// SnapshotJobStatusCatchingUp means the snapshot finished and live replay remains.
	SnapshotJobStatusCatchingUp SnapshotJobStatus = "catching_up"
	// SnapshotJobStatusFinished means snapshot bootstrap and catch-up both completed.
	SnapshotJobStatusFinished SnapshotJobStatus = "finished"
)

// StateSnapshotJob stores persisted snapshot progress for a peer and table.
type StateSnapshotJob struct {
	PeerRegion     string
	Table          string
	Status         SnapshotJobStatus
	LastPrimaryKey []PrimaryKeyPart
	WatermarkAt    time.Time
	Finished       bool
}

// CommitSkipReason explains why a repository state mutation was skipped.
type CommitSkipReason string

const (
	// CommitSkipReasonSnapshotDone means the snapshot job was already marked finished.
	CommitSkipReasonSnapshotDone CommitSkipReason = "snapshot_finished"
)

// CommitSnapshotCheckpointResult reports whether snapshot progress was updated.
type CommitSnapshotCheckpointResult struct {
	Updated bool
	Reason  CommitSkipReason
}
