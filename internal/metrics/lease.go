package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/transmogr/transmogr/internal/service/lease"
)

// PrometheusLeaseMetrics implements lease.Metrics using prometheus counters.
type PrometheusLeaseMetrics struct {
	acquireOK     prometheus.Counter
	acquireMiss   prometheus.Counter
	acquireErrors prometheus.Counter
	renewOK       prometheus.Counter
	renewLost     prometheus.Counter
	renewErrors   prometheus.Counter
}

var _ lease.Metrics = (*PrometheusLeaseMetrics)(nil)

// NewPrometheusLeaseMetrics creates and registers lease metrics on reg.
// If region is non-empty it is added as a constant label on every metric.
func NewPrometheusLeaseMetrics(region string, reg prometheus.Registerer) *PrometheusLeaseMetrics {
	var cl prometheus.Labels
	if region != "" {
		cl = prometheus.Labels{"region": region}
	}

	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: cl})
		reg.MustRegister(c)
		return c
	}

	return &PrometheusLeaseMetrics{
		acquireOK:     counter("transmogr_lease_acquire_ok_total", "Total successful lease acquisitions."),
		acquireMiss:   counter("transmogr_lease_acquire_miss_total", "Total contended lease acquisition attempts."),
		acquireErrors: counter("transmogr_lease_acquire_errors_total", "Total lease acquisition errors."),
		renewOK:       counter("transmogr_lease_renew_ok_total", "Total successful lease renewals."),
		renewLost:     counter("transmogr_lease_renew_lost_total", "Total lease renewals that lost ownership."),
		renewErrors:   counter("transmogr_lease_renew_errors_total", "Total lease renewal errors."),
	}
}

// RecordLeaseAcquireOK records a successful lease acquisition.
func (m *PrometheusLeaseMetrics) RecordLeaseAcquireOK() { m.acquireOK.Add(1) }

// RecordLeaseAcquireMiss records a contended lease acquisition.
func (m *PrometheusLeaseMetrics) RecordLeaseAcquireMiss() { m.acquireMiss.Add(1) }

// RecordLeaseAcquireError records a failed lease acquisition attempt.
func (m *PrometheusLeaseMetrics) RecordLeaseAcquireError() { m.acquireErrors.Add(1) }

// RecordLeaseRenewOK records a successful lease renewal.
func (m *PrometheusLeaseMetrics) RecordLeaseRenewOK() { m.renewOK.Add(1) }

// RecordLeaseRenewLost records a renewal that lost ownership.
func (m *PrometheusLeaseMetrics) RecordLeaseRenewLost() { m.renewLost.Add(1) }

// RecordLeaseRenewError records a failed renewal attempt.
func (m *PrometheusLeaseMetrics) RecordLeaseRenewError() { m.renewErrors.Add(1) }
