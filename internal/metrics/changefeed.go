// Package metrics exposes Prometheus-backed metrics collectors and servers.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/transmogr/transmogr/internal/service/changefeed"
)

// PrometheusChangefeedMetrics implements changefeed.Metrics using prometheus counters.
type PrometheusChangefeedMetrics struct {
	events  prometheus.Counter
	batches prometheus.Counter
}

var _ changefeed.Metrics = (*PrometheusChangefeedMetrics)(nil)

// NewPrometheusChangefeedMetrics creates and registers changefeed metrics on reg.
// If region is non-empty it is added as a constant label on every metric.
func NewPrometheusChangefeedMetrics(region string, reg prometheus.Registerer) *PrometheusChangefeedMetrics {
	var cl prometheus.Labels
	if region != "" {
		cl = prometheus.Labels{"region": region}
	}

	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: cl})
		reg.MustRegister(c)
		return c
	}

	return &PrometheusChangefeedMetrics{
		events:  counter("transmogr_changefeed_events_total", "Total emitted changefeed events."),
		batches: counter("transmogr_changefeed_batches_total", "Total enqueued outbound changefeed batches."),
	}
}

// RecordChangefeedEvent records one emitted changefeed event.
func (m *PrometheusChangefeedMetrics) RecordChangefeedEvent() { m.events.Add(1) }

// RecordChangefeedBatch records one enqueued outbound batch.
func (m *PrometheusChangefeedMetrics) RecordChangefeedBatch() { m.batches.Add(1) }
