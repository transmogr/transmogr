package models

import "time"

// SnapshotReadRequest describes a chunk read against the local application store.
type SnapshotReadRequest struct {
	// LocalRegion, when non-empty, restricts reads to rows whose region field matches
	// this value. Only effective for tables that have a RegionField configured.
	LocalRegion     string
	PeerRegion      string
	Table           string
	AfterPrimaryKey []PrimaryKeyPart
	Limit           int
}

// SnapshotRequest describes one snapshot bootstrap request for a table.
type SnapshotRequest struct {
	Table           string
	AfterPrimaryKey []PrimaryKeyPart
}

// SnapshotRow is one row transferred during snapshot replication.
type SnapshotRow struct {
	PrimaryKey []PrimaryKeyPart
	Payload    []byte
	Metadata   map[string][]byte
	Version    int64
}

// SnapshotChunk is a batch of snapshot rows plus the stream completion marker.
type SnapshotChunk struct {
	Rows      []SnapshotRow
	Last      bool
	Watermark time.Time
}
