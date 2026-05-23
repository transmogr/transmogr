package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/transmogr/transmogr/internal/service/replication"
)

// PrometheusReplicationMetrics implements replication.Metrics using prometheus counters and gauges.
type PrometheusReplicationMetrics struct {
	reconnectAttempts      prometheus.Counter
	lastReconnectDelay     prometheus.Gauge
	ackLatencySamples      prometheus.Counter
	ackLatencyTotalSecs    prometheus.Counter
	applyApplied           prometheus.Counter
	applyStale             prometheus.Counter
	applyErrors            prometheus.Counter
	snapshotRowsSent       prometheus.Counter
	snapshotChunksSent     prometheus.Counter
	snapshotRowsReceived   prometheus.Counter
	snapshotChunksReceived prometheus.Counter
	snapshotFinished       prometheus.Counter
	snapshotSendErrors     prometheus.Counter
	snapshotApplyErrors    prometheus.Counter
	grpcMessages           *prometheus.CounterVec
}

var _ replication.Metrics = (*PrometheusReplicationMetrics)(nil)

// NewPrometheusReplicationMetrics creates and registers replication metrics on reg.
// If region is non-empty it is added as a constant label on every metric.
func NewPrometheusReplicationMetrics(region string, reg prometheus.Registerer) *PrometheusReplicationMetrics {
	var cl prometheus.Labels
	if region != "" {
		cl = prometheus.Labels{"region": region}
	}

	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: cl})
		reg.MustRegister(c)
		return c
	}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: cl})
		reg.MustRegister(g)
		return g
	}

	grpcMessages := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "transmogr_grpc_messages_total",
		Help:        "Total gRPC replication messages by direction and message type.",
		ConstLabels: cl,
	}, []string{"direction", "message_type"})
	reg.MustRegister(grpcMessages)

	// Pre-initialise all label combinations so the series exist from startup.
	for _, msgType := range replication.MsgTypeNames {
		grpcMessages.WithLabelValues("sent", msgType)
		grpcMessages.WithLabelValues("received", msgType)
	}

	return &PrometheusReplicationMetrics{
		reconnectAttempts: counter(
			"transmogr_reconnect_attempts_total",
			"Total peer reconnect attempts.",
		),
		lastReconnectDelay: gauge(
			"transmogr_last_reconnect_delay_seconds",
			"Last reconnect delay in seconds.",
		),
		ackLatencySamples: counter(
			"transmogr_ack_latency_samples_total",
			"Total number of ack latency samples.",
		),
		ackLatencyTotalSecs: counter(
			"transmogr_ack_latency_total_seconds",
			"Total observed ack latency in seconds.",
		),
		applyApplied: counter(
			"transmogr_replication_apply_applied_total",
			"Total inbound replication events applied locally.",
		),
		applyStale: counter(
			"transmogr_replication_apply_stale_total",
			"Total inbound replication events skipped as stale.",
		),
		applyErrors: counter(
			"transmogr_replication_apply_errors_total",
			"Total inbound replication apply errors.",
		),
		snapshotRowsSent: counter(
			"transmogr_snapshot_rows_sent_total",
			"Total snapshot rows sent to peers.",
		),
		snapshotChunksSent: counter(
			"transmogr_snapshot_chunks_sent_total",
			"Total snapshot chunks sent to peers.",
		),
		snapshotRowsReceived: counter(
			"transmogr_snapshot_rows_received_total",
			"Total snapshot rows received from peers.",
		),
		snapshotChunksReceived: counter(
			"transmogr_snapshot_chunks_received_total",
			"Total snapshot chunks received from peers.",
		),
		snapshotFinished: counter(
			"transmogr_snapshot_finished_total",
			"Total snapshot transfers completed (last chunk received).",
		),
		snapshotSendErrors: counter(
			"transmogr_snapshot_send_errors_total",
			"Total snapshot send errors.",
		),
		snapshotApplyErrors: counter(
			"transmogr_snapshot_apply_errors_total",
			"Total snapshot apply errors.",
		),
		grpcMessages: grpcMessages,
	}
}

// RecordReconnectAttempt records one reconnect attempt and its last delay.
func (m *PrometheusReplicationMetrics) RecordReconnectAttempt(delay time.Duration) {
	m.reconnectAttempts.Add(1)
	m.lastReconnectDelay.Set(delay.Seconds())
}

// RecordAckLatency records one observed ack latency sample.
func (m *PrometheusReplicationMetrics) RecordAckLatency(latency time.Duration) {
	if latency < 0 {
		latency = 0
	}
	m.ackLatencySamples.Add(1)
	m.ackLatencyTotalSecs.Add(latency.Seconds())
}

// RecordApplyApplied records one successfully applied inbound event.
func (m *PrometheusReplicationMetrics) RecordApplyApplied() {
	m.applyApplied.Add(1)
}

// RecordApplyStale records one skipped stale inbound event.
func (m *PrometheusReplicationMetrics) RecordApplyStale() {
	m.applyStale.Add(1)
}

// RecordApplyError records one inbound apply error.
func (m *PrometheusReplicationMetrics) RecordApplyError() {
	m.applyErrors.Add(1)
}

// RecordSnapshotChunkSent records one outbound snapshot chunk and its row count.
func (m *PrometheusReplicationMetrics) RecordSnapshotChunkSent(rows int) {
	m.snapshotChunksSent.Add(1)
	m.snapshotRowsSent.Add(float64(rows))
}

// RecordSnapshotChunkReceived records one inbound snapshot chunk and its row count.
func (m *PrometheusReplicationMetrics) RecordSnapshotChunkReceived(rows int, finished bool) {
	m.snapshotChunksReceived.Add(1)
	m.snapshotRowsReceived.Add(float64(rows))
	if finished {
		m.snapshotFinished.Add(1)
	}
}

// RecordSnapshotSendError records one snapshot send failure.
func (m *PrometheusReplicationMetrics) RecordSnapshotSendError() {
	m.snapshotSendErrors.Add(1)
}

// RecordSnapshotApplyError records one snapshot apply failure.
func (m *PrometheusReplicationMetrics) RecordSnapshotApplyError() {
	m.snapshotApplyErrors.Add(1)
}

// RecordMsgSent records one outbound gRPC replication message.
func (m *PrometheusReplicationMetrics) RecordMsgSent(msgType int) {
	if msgType >= 0 && msgType < replication.MsgTypeCount {
		m.grpcMessages.WithLabelValues("sent", replication.MsgTypeNames[msgType]).Add(1)
	}
}

// RecordMsgReceived records one inbound gRPC replication message.
func (m *PrometheusReplicationMetrics) RecordMsgReceived(msgType int) {
	if msgType >= 0 && msgType < replication.MsgTypeCount {
		m.grpcMessages.WithLabelValues("received", replication.MsgTypeNames[msgType]).Add(1)
	}
}
