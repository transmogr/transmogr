// Package replication coordinates inbound and outbound replication workflows.
package replication

import "time"

// gRPC message type indices for RecordMsgSent/RecordMsgReceived.
const (
	MsgTypeConsumeRequest   = 0
	MsgTypeHandshake        = 1
	MsgTypeSnapshotChunk    = 2
	MsgTypeSnapshotComplete = 3
	MsgTypeEventBatch       = 4
	MsgTypeTransactionBatch = 5
	MsgTypeAckRequest       = 6
	MsgTypeHeartbeat        = 7
	MsgTypeCount            = 8
)

// MsgTypeNames maps message type index to its Prometheus label value.
var MsgTypeNames = [MsgTypeCount]string{
	"consume_request",
	"handshake",
	"snapshot_chunk",
	"snapshot_complete",
	"event_batch",
	"transaction_batch",
	"ack_request",
	"heartbeat",
}

// noopMetrics discards all recorded metrics. Used in tests.
type noopMetrics struct{}

func (noopMetrics) RecordReconnectAttempt(time.Duration)  {}
func (noopMetrics) RecordAckLatency(time.Duration)        {}
func (noopMetrics) RecordApplyApplied()                   {}
func (noopMetrics) RecordApplyStale()                     {}
func (noopMetrics) RecordApplyError()                     {}
func (noopMetrics) RecordSnapshotChunkSent(int)           {}
func (noopMetrics) RecordSnapshotChunkReceived(int, bool) {}
func (noopMetrics) RecordSnapshotSendError()              {}
func (noopMetrics) RecordSnapshotApplyError()             {}
func (noopMetrics) RecordMsgSent(int)                     {}
func (noopMetrics) RecordMsgReceived(int)                 {}

// NewCollector returns a no-op Metrics suitable for tests.
func NewCollector() Metrics {
	return noopMetrics{}
}
