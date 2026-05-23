package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/transmogr/transmogr/internal/models"
)

func TestReplicationMetricsHandler(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewPrometheusReplicationMetrics("eu-west", reg)

	m.RecordReconnectAttempt(3 * time.Second)
	m.RecordAckLatency(10 * time.Millisecond)
	m.RecordAckLatency(0)
	for range 5 {
		m.RecordApplyApplied()
	}
	for range 2 {
		m.RecordApplyStale()
	}
	m.RecordApplyError()
	m.RecordSnapshotChunkSent(100)
	for range 4 {
		m.RecordSnapshotChunkSent(0)
	}
	m.RecordSnapshotChunkReceived(80, false)
	m.RecordSnapshotChunkReceived(0, false)
	m.RecordSnapshotChunkReceived(0, true)
	m.RecordSnapshotChunkReceived(0, true)
	m.RecordSnapshotSendError()
	m.RecordSnapshotApplyError()
	m.RecordMsgSent(0) // consume_request x2
	m.RecordMsgSent(0)
	m.RecordMsgSent(1) // handshake x3
	m.RecordMsgSent(1)
	m.RecordMsgSent(1)
	m.RecordMsgReceived(1) // handshake x1
	for range 4 {
		m.RecordMsgReceived(6) // ack_request x4
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	body := recorder.Body.String()
	for _, fragment := range []string{
		`transmogr_reconnect_attempts_total{region="eu-west"} 1`,
		`transmogr_last_reconnect_delay_seconds{region="eu-west"} 3`,
		`transmogr_ack_latency_samples_total{region="eu-west"} 2`,
		`transmogr_replication_apply_applied_total{region="eu-west"} 5`,
		`transmogr_replication_apply_stale_total{region="eu-west"} 2`,
		`transmogr_replication_apply_errors_total{region="eu-west"} 1`,
		`transmogr_snapshot_rows_sent_total{region="eu-west"} 100`,
		`transmogr_snapshot_chunks_sent_total{region="eu-west"} 5`,
		`transmogr_snapshot_rows_received_total{region="eu-west"} 80`,
		`transmogr_snapshot_chunks_received_total{region="eu-west"} 4`,
		`transmogr_snapshot_finished_total{region="eu-west"} 2`,
		`transmogr_snapshot_send_errors_total{region="eu-west"} 1`,
		`transmogr_snapshot_apply_errors_total{region="eu-west"} 1`,
		`transmogr_grpc_messages_total{direction="sent",message_type="consume_request",region="eu-west"} 2`,
		`transmogr_grpc_messages_total{direction="sent",message_type="handshake",region="eu-west"} 3`,
		`transmogr_grpc_messages_total{direction="received",message_type="handshake",region="eu-west"} 1`,
		`transmogr_grpc_messages_total{direction="received",message_type="ack_request",region="eu-west"} 4`,
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("metrics body missing %q:\n%s", fragment, body)
		}
	}
}

func TestRuntimeCollectorsHandler(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	RegisterRuntimeCollectors(reg)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	body := recorder.Body.String()
	for _, fragment := range []string{
		"process_cpu_seconds_total",
		"process_resident_memory_bytes",
		"go_memstats_heap_alloc_bytes",
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("metrics body missing %q:\n%s", fragment, body)
		}
	}
}

func TestLeaseMetricsHandler(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewPrometheusLeaseMetrics("eu-west", reg)

	for range 9 {
		m.RecordLeaseAcquireOK()
	}
	for range 3 {
		m.RecordLeaseAcquireMiss()
	}
	m.RecordLeaseAcquireError()
	for range 8 {
		m.RecordLeaseRenewOK()
	}
	m.RecordLeaseRenewLost()
	for range 2 {
		m.RecordLeaseRenewError()
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(recorder, request)

	body := recorder.Body.String()
	for _, fragment := range []string{
		`transmogr_lease_acquire_ok_total{region="eu-west"} 9`,
		`transmogr_lease_acquire_miss_total{region="eu-west"} 3`,
		`transmogr_lease_acquire_errors_total{region="eu-west"} 1`,
		`transmogr_lease_renew_ok_total{region="eu-west"} 8`,
		`transmogr_lease_renew_lost_total{region="eu-west"} 1`,
		`transmogr_lease_renew_errors_total{region="eu-west"} 2`,
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("metrics body missing %q:\n%s", fragment, body)
		}
	}
}

func TestChangefeedMetricsHandler(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewPrometheusChangefeedMetrics("eu-west", reg)

	for range 7 {
		m.RecordChangefeedEvent()
	}
	for range 6 {
		m.RecordChangefeedBatch()
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(recorder, request)

	body := recorder.Body.String()
	for _, fragment := range []string{
		`transmogr_changefeed_events_total{region="eu-west"} 7`,
		`transmogr_changefeed_batches_total{region="eu-west"} 6`,
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("metrics body missing %q:\n%s", fragment, body)
		}
	}
}

func TestOutboxMetricsCollector(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	RegisterOutboxCollector("eu-west", fakeOutboxSource{
		pendingBatches:        12,
		oldestBatchAgeSec:     9,
		cleanupDeleted:        4,
		cleanupDeletedByAge:   3,
		cleanupDeletedByCount: 1,
	}, reg)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(recorder, request)

	body := recorder.Body.String()
	for _, fragment := range []string{
		`transmogr_outbox_pending_batches{region="eu-west"} 12`,
		`transmogr_outbox_oldest_batch_age_seconds{region="eu-west"} 9`,
		`transmogr_outbox_cleanup_deleted_total{region="eu-west"} 4`,
		`transmogr_outbox_cleanup_deleted_by_age_total{region="eu-west"} 3`,
		`transmogr_outbox_cleanup_deleted_by_count_total{region="eu-west"} 1`,
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("metrics body missing %q:\n%s", fragment, body)
		}
	}
}

func TestLivezHandler(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/livez", nil)

	livezHandler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "ok\n" {
		t.Fatalf("body = %q, want %q", body, "ok\n")
	}
}

func TestReadyzHandler(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	readyzHandler(nil).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "ok\n" {
		t.Fatalf("body = %q, want %q", body, "ok\n")
	}
}

func TestReadyzHandlerReturnsUnavailableWhenDependencyFails(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	readyzHandler(fakeReadiness{err: errors.New("db down")}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

type fakeReadiness struct {
	err error
}

func (f fakeReadiness) Ready(context.Context) error {
	return f.err
}

type fakeOutboxSource struct {
	pendingBatches        uint64
	oldestBatchAgeSec     float64
	cleanupDeleted        uint64
	cleanupDeletedByAge   uint64
	cleanupDeletedByCount uint64
}

func (f fakeOutboxSource) OutboxMetricsSnapshot() models.OutboxMetricsSnapshot {
	return models.OutboxMetricsSnapshot{
		PendingBatches:        f.pendingBatches,
		OldestBatchAge:        time.Duration(f.oldestBatchAgeSec * float64(time.Second)),
		CleanupDeleted:        f.cleanupDeleted,
		CleanupDeletedByAge:   f.cleanupDeletedByAge,
		CleanupDeletedByCount: f.cleanupDeletedByCount,
	}
}
