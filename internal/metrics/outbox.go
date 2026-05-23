package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/transmogr/transmogr/internal/models"
)

type outboxSnapshotSource interface {
	OutboxMetricsSnapshot() models.OutboxMetricsSnapshot
}

// outboxCollector adapts an outboxSnapshotSource to the prometheus.Collector interface.
// It reads from the source's cached snapshot on every scrape.
type outboxCollector struct {
	source                outboxSnapshotSource
	pendingBatches        *prometheus.Desc
	oldestBatchAge        *prometheus.Desc
	cleanupDeleted        *prometheus.Desc
	cleanupDeletedByAge   *prometheus.Desc
	cleanupDeletedByCount *prometheus.Desc
}

// RegisterOutboxCollector creates and registers an outbox metrics collector on reg.
func RegisterOutboxCollector(region string, source outboxSnapshotSource, reg prometheus.Registerer) {
	var cl prometheus.Labels
	if region != "" {
		cl = prometheus.Labels{"region": region}
	}

	c := &outboxCollector{
		source: source,
		pendingBatches: prometheus.NewDesc(
			"transmogr_outbox_pending_batches",
			"Current number of pending outbound batches in the outbox.",
			nil, cl,
		),
		oldestBatchAge: prometheus.NewDesc(
			"transmogr_outbox_oldest_batch_age_seconds",
			"Age of the oldest pending outbound batch in seconds.",
			nil, cl,
		),
		cleanupDeleted: prometheus.NewDesc(
			"transmogr_outbox_cleanup_deleted_total",
			"Total outbox batches deleted by background cleanup.",
			nil, cl,
		),
		cleanupDeletedByAge: prometheus.NewDesc(
			"transmogr_outbox_cleanup_deleted_by_age_total",
			"Total pending outbox batches deleted by background cleanup because they exceeded outbox.max_batch_age.",
			nil, cl,
		),
		cleanupDeletedByCount: prometheus.NewDesc(
			"transmogr_outbox_cleanup_deleted_by_count_total",
			"Total pending outbox batches deleted by background cleanup because they exceeded outbox.max_pending_per_peer.",
			nil, cl,
		),
	}
	reg.MustRegister(c)
}

func (c *outboxCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pendingBatches
	ch <- c.oldestBatchAge
	ch <- c.cleanupDeleted
	ch <- c.cleanupDeletedByAge
	ch <- c.cleanupDeletedByCount
}

func (c *outboxCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.source.OutboxMetricsSnapshot()
	ch <- prometheus.MustNewConstMetric(c.pendingBatches, prometheus.GaugeValue, float64(snap.PendingBatches))
	ch <- prometheus.MustNewConstMetric(c.oldestBatchAge, prometheus.GaugeValue, snap.OldestBatchAge.Seconds())
	ch <- prometheus.MustNewConstMetric(c.cleanupDeleted, prometheus.CounterValue, float64(snap.CleanupDeleted))
	ch <- prometheus.MustNewConstMetric(c.cleanupDeletedByAge, prometheus.CounterValue, float64(snap.CleanupDeletedByAge))
	ch <- prometheus.MustNewConstMetric(
		c.cleanupDeletedByCount,
		prometheus.CounterValue,
		float64(snap.CleanupDeletedByCount),
	)
}
