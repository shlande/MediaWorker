// Package monitor provides Prometheus metrics for the storage layer (L4 data plane).
//
// Metric naming convention: all names use the "storage_" prefix to distinguish
// from distribution-layer metrics ("edge_*").
package monitor

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StorageMetrics holds all Prometheus metrics for the storage layer (§12.1).
//
// Labels:
//   - vendor: the cloud drive vendor (115, baidu, quark, onedrive, aliyundrive)
//   - account_id: the specific account identifier (e.g. "acct_03")
//   - state: health state (healthy, degraded, banned) or circuit breaker state (closed, half_open, open)
type StorageMetrics struct {
	reg *prometheus.Registry

	// AccessBackhaulSuccessTotal counts successful backhaul fetches by vendor.
	AccessBackhaulSuccessTotal *prometheus.CounterVec
	// AccessBackhaulRequestTotal counts all backhaul fetch attempts by vendor.
	AccessBackhaulRequestTotal *prometheus.CounterVec
	// AccessBackhaulDurationSeconds records backhaul fetch latency in seconds, bucketed by vendor.
	AccessBackhaulDurationSeconds *prometheus.HistogramVec

	// AccountHealthState reports the current health state of each account (gauge=1 for matching state).
	AccountHealthState *prometheus.GaugeVec
	// AccountRequestsTotal counts API requests per account (vendor + account_id labels).
	AccountRequestsTotal *prometheus.CounterVec

	// LinkpoolHitTotal counts cache hits in the download link pool.
	LinkpoolHitTotal prometheus.Counter
	// LinkpoolRequestTotal counts all download link pool lookups.
	LinkpoolRequestTotal prometheus.Counter

	// CircuitBreakerState reports the current circuit breaker state (gauge=1 for matching state).
	CircuitBreakerState *prometheus.GaugeVec
}

// NewStorageMetrics creates and registers all storage-layer metrics with a new
// prometheus.Registry.
func NewStorageMetrics() *StorageMetrics {
	reg := prometheus.NewRegistry()
	m := &StorageMetrics{
		AccessBackhaulSuccessTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_access_backhaul_success_total",
				Help: "Total number of successful backhaul fetches, by vendor.",
			},
			[]string{"vendor"},
		),
		AccessBackhaulRequestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_access_backhaul_request_total",
				Help: "Total number of backhaul fetch attempts, by vendor.",
			},
			[]string{"vendor"},
		),
		AccessBackhaulDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "storage_access_backhaul_duration_seconds",
				Help:    "Backhaul fetch latency in seconds, by vendor.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"vendor"},
		),

		AccountHealthState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "storage_account_health_state",
				Help: "Current health state of each account (1 for the matching state).",
			},
			[]string{"vendor", "state"},
		),
		AccountRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_account_requests_total",
				Help: "Total number of API requests per account.",
			},
			[]string{"vendor", "account_id"},
		),

		LinkpoolHitTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "storage_linkpool_hit_total",
				Help: "Total number of download link pool cache hits.",
			},
		),
		LinkpoolRequestTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "storage_linkpool_request_total",
				Help: "Total number of download link pool lookups.",
			},
		),

		CircuitBreakerState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "storage_circuit_breaker_state",
				Help: "Current circuit breaker state (1 for the matching state).",
			},
			[]string{"account_id", "state"},
		),
		reg: reg,
	}
	m.register(reg)
	return m
}

// register registers all metrics with the given prometheus.Registry.
func (m *StorageMetrics) register(r *prometheus.Registry) {
	r.MustRegister(
		m.AccessBackhaulSuccessTotal,
		m.AccessBackhaulRequestTotal,
		m.AccessBackhaulDurationSeconds,
		m.AccountHealthState,
		m.AccountRequestsTotal,
		m.LinkpoolHitTotal,
		m.LinkpoolRequestTotal,
		m.CircuitBreakerState,
	)
}

// HTTPHandler returns an http.Handler that exposes metrics on /metrics.
func (m *StorageMetrics) HTTPHandler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// RecordBackhaulSuccess increments the success counter for the given vendor.
func (m *StorageMetrics) RecordBackhaulSuccess(vendor string) {
	m.AccessBackhaulSuccessTotal.WithLabelValues(vendor).Inc()
}

// RecordBackhaulRequest increments the request counter for the given vendor.
func (m *StorageMetrics) RecordBackhaulRequest(vendor string) {
	m.AccessBackhaulRequestTotal.WithLabelValues(vendor).Inc()
}

// RecordBackhaulDuration observes a backhaul fetch latency for the given vendor.
func (m *StorageMetrics) RecordBackhaulDuration(vendor string, seconds float64) {
	m.AccessBackhaulDurationSeconds.WithLabelValues(vendor).Observe(seconds)
}

// SetAccountHealth sets the health state gauge for an account.
// Only the matching state label gets 1; all others for the same vendor get 0.
func (m *StorageMetrics) SetAccountHealth(vendor, state string) {
	// Reset all states for this vendor.
	for _, s := range []string{"healthy", "degraded", "banned"} {
		m.AccountHealthState.WithLabelValues(vendor, s).Set(0)
	}
	m.AccountHealthState.WithLabelValues(vendor, state).Set(1)
}

// RecordAccountRequest increments the request counter for the given account.
func (m *StorageMetrics) RecordAccountRequest(vendor, accountID string) {
	m.AccountRequestsTotal.WithLabelValues(vendor, accountID).Inc()
}

// RecordLinkpoolHit increments the link pool hit counter.
func (m *StorageMetrics) RecordLinkpoolHit() {
	m.LinkpoolHitTotal.Inc()
}

// RecordLinkpoolRequest increments the link pool request counter.
func (m *StorageMetrics) RecordLinkpoolRequest() {
	m.LinkpoolRequestTotal.Inc()
}

// SetCircuitBreakerState sets the circuit breaker state gauge for an account.
// Only the matching state label gets 1; all others for the same account get 0.
func (m *StorageMetrics) SetCircuitBreakerState(accountID, state string) {
	for _, s := range []string{"closed", "half_open", "open"} {
		m.CircuitBreakerState.WithLabelValues(accountID, s).Set(0)
	}
	m.CircuitBreakerState.WithLabelValues(accountID, state).Set(1)
}