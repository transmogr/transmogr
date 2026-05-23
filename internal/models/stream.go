package models

import (
	"encoding/json"
	"slices"
	"time"
)

// HandshakeMessage announces region, version, and configuration on stream setup.
type HandshakeMessage struct {
	Region        string
	Version       string
	Configuration string
}

// HandshakeConfig is the structured payload serialised into HandshakeMessage.Configuration.
type HandshakeConfig struct {
	Source string           `json:"source"`
	Tables []HandshakeTable `json:"tables"`
}

// HandshakeTable describes one replicated table within HandshakeConfig.
type HandshakeTable struct {
	Name        string       `json:"name"`
	PrimaryKey  []string     `json:"primary_key"`
	RegionField string       `json:"region_field"`
	Columns     []ColumnInfo `json:"columns"`
}

// BuildHandshakeConfiguration serialises the replication configuration into a
// compact JSON string suitable for HandshakeMessage.Configuration.
// The output is normalised: tables are sorted by name and columns are sorted by
// name within each table, so two peers with the same schema but different
// declaration or query order produce identical strings.
func BuildHandshakeConfiguration(sourceType string, tables []TableSpec, columns map[string][]ColumnInfo) string {
	sorted := slices.Clone(tables)
	slices.SortFunc(sorted, func(a, b TableSpec) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})

	cfg := HandshakeConfig{
		Source: sourceType,
		Tables: make([]HandshakeTable, 0, len(sorted)),
	}
	for _, t := range sorted {
		cols := slices.Clone(columns[t.Name])
		slices.SortFunc(cols, func(a, b ColumnInfo) int {
			if a.Name < b.Name {
				return -1
			}
			if a.Name > b.Name {
				return 1
			}
			return 0
		})
		cfg.Tables = append(cfg.Tables, HandshakeTable{
			Name:        t.Name,
			PrimaryKey:  t.PrimaryKey,
			RegionField: t.RegionField,
			Columns:     cols,
		})
	}
	raw, _ := json.Marshal(cfg)
	return string(raw)
}

// ParseHandshakeConfiguration deserialises a HandshakeMessage.Configuration string.
func ParseHandshakeConfiguration(s string) (HandshakeConfig, error) {
	var cfg HandshakeConfig
	if err := json.Unmarshal([]byte(s), &cfg); err != nil {
		return HandshakeConfig{}, err
	}
	return cfg, nil
}

// SnapshotChunkMessage carries a chunk of snapshot rows over the stream.
type SnapshotChunkMessage struct {
	Table     string
	Rows      []SnapshotRow
	Last      bool
	Watermark time.Time
}

// SnapshotCompleteMessage marks the end of snapshot bootstrap for one table.
type SnapshotCompleteMessage struct {
	Table string
}

// EventBatchMessage carries a sequence of incremental replication events (polling path).
// Table is per-event (ReplicationEvent.Table); there is no batch-level table field.
type EventBatchMessage struct {
	Sequence uint64
	Events   []ReplicationEvent
}

// TransactionBatchMessage carries a sequence of events from a single source transaction (CDC path).
type TransactionBatchMessage struct {
	TxID     string
	Sequence uint64
	Events   []ReplicationEvent
}

// AckMessage confirms receipt of events up to the last event ID.
type AckMessage struct {
	Table       string
	LastEventID string
}

// PingMessage keeps an idle replication stream alive.
type PingMessage struct {
	SentAt time.Time
}

// StreamMessage is the internal stream envelope used by the transport layer.
type StreamMessage struct {
	PeerRegion       string
	Handshake        *HandshakeMessage
	SnapshotChunk    *SnapshotChunkMessage
	SnapshotComplete *SnapshotCompleteMessage
	EventBatch       *EventBatchMessage
	TransactionBatch *TransactionBatchMessage
	Ack              *AckMessage
	Ping             *PingMessage
}
