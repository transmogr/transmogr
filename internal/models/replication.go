package models

import "encoding/json"

// ReplicationOperation identifies the row mutation represented by a replication event.
type ReplicationOperation string

const (
	// ReplicationOperationInsert creates a new row; no-op if the row already exists under InsertOnly mode.
	ReplicationOperationInsert ReplicationOperation = "insert"
	// ReplicationOperationUpdate applies a new image to an existing row.
	ReplicationOperationUpdate ReplicationOperation = "update"
	// ReplicationOperationUpsert is emitted by adapters that cannot distinguish insert from update (e.g. polling).
	ReplicationOperationUpsert ReplicationOperation = "upsert"
	// ReplicationOperationDelete removes a row by primary key.
	ReplicationOperationDelete ReplicationOperation = "delete"
)

// ReplicationApplyMode controls how inbound events received from peers are applied locally.
type ReplicationApplyMode string

const (
	// ReplicationApplyModeInsertOnly only inserts new rows; existing rows and deletes are ignored.
	ReplicationApplyModeInsertOnly ReplicationApplyMode = "insert_only"
	// ReplicationApplyModeUpsert inserts and updates rows but does not apply deletes.
	ReplicationApplyModeUpsert ReplicationApplyMode = "upsert"
	// ReplicationApplyModeFullSync applies inserts, updates, and deletes — a complete mirror.
	ReplicationApplyModeFullSync ReplicationApplyMode = "full_sync"
)

// ReplicationBroadcastMode controls which local change events are published to peers.
type ReplicationBroadcastMode string

const (
	// ReplicationBroadcastModeInsertOnly only broadcasts new-row events.
	ReplicationBroadcastModeInsertOnly ReplicationBroadcastMode = "insert_only"
	// ReplicationBroadcastModeUpsert broadcasts insert and update events but not deletes.
	ReplicationBroadcastModeUpsert ReplicationBroadcastMode = "upsert"
	// ReplicationBroadcastModeFullSync broadcasts all events including deletes.
	ReplicationBroadcastModeFullSync ReplicationBroadcastMode = "full_sync"
)

// ReplicationEvent is a single change event delivered between peers.
type ReplicationEvent struct {
	PeerRegion string
	Table      string
	EventID    string
	PrimaryKey []PrimaryKeyPart
	Operation  ReplicationOperation
	Version    int64
	Fields     map[string][]byte
	Metadata   map[string][]byte
}

// replicationEventJSON is the JSON wire shape used by MarshalJSON/UnmarshalJSON.
// Fields are stored as raw JSON values (not base64) so that strings, numbers, and
// timestamps remain human-readable in the outbox JSONB column.
type replicationEventJSON struct {
	PeerRegion string                     `json:"PeerRegion"`
	Table      string                     `json:"Table"`
	EventID    string                     `json:"EventID"`
	PrimaryKey []PrimaryKeyPart           `json:"PrimaryKey"`
	Operation  ReplicationOperation       `json:"Operation"`
	Version    int64                      `json:"Version"`
	Fields     map[string]json.RawMessage `json:"Fields"`
	Metadata   map[string]json.RawMessage `json:"Metadata,omitempty"`
}

// MarshalJSON keeps field and metadata values as raw JSON instead of base64.
func (e ReplicationEvent) MarshalJSON() ([]byte, error) {
	raw := make(map[string]json.RawMessage, len(e.Fields))
	for k, v := range e.Fields {
		raw[k] = json.RawMessage(v)
	}
	metadata := make(map[string]json.RawMessage, len(e.Metadata))
	for k, v := range e.Metadata {
		metadata[k] = json.RawMessage(v)
	}
	return json.Marshal(replicationEventJSON{
		PeerRegion: e.PeerRegion,
		Table:      e.Table,
		EventID:    e.EventID,
		PrimaryKey: e.PrimaryKey,
		Operation:  e.Operation,
		Version:    e.Version,
		Fields:     raw,
		Metadata:   metadata,
	})
}

// UnmarshalJSON restores field and metadata values from raw JSON.
func (e *ReplicationEvent) UnmarshalJSON(data []byte) error {
	var a replicationEventJSON
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	e.PeerRegion = a.PeerRegion
	e.Table = a.Table
	e.EventID = a.EventID
	e.PrimaryKey = a.PrimaryKey
	e.Operation = a.Operation
	e.Version = a.Version
	if a.Fields != nil {
		e.Fields = make(map[string][]byte, len(a.Fields))
		for k, v := range a.Fields {
			e.Fields[k] = []byte(v)
		}
	}
	if a.Metadata != nil {
		e.Metadata = make(map[string][]byte, len(a.Metadata))
		for k, v := range a.Metadata {
			e.Metadata[k] = []byte(v)
		}
	}
	return nil
}
