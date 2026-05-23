// Package models defines the internal data structures shared across services.
package models

// ChangefeedChange is a normalized incremental change independent of the capture backend.
type ChangefeedChange struct {
	Table      string
	PrimaryKey []PrimaryKeyPart
	Operation  ReplicationOperation
	Version    int64
	Payload    []byte
}

// ChangefeedBatch groups one or more changes from a capture backend.
// TxID is non-empty for CDC transactions (e.g. PostgreSQL logical replication).
// TxID is empty for polling-based adapters.
type ChangefeedBatch struct {
	TxID    string
	Changes []ChangefeedChange
}

// ChangefeedCursor tracks the last exported polling position for one table.
type ChangefeedCursor struct {
	Table          string
	Source         string
	LastVersion    int64
	LastPrimaryKey []PrimaryKeyPart
}
