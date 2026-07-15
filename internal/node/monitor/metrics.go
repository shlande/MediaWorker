// Package monitor provides Prometheus metrics registration and alert rules for the distribution layer.
package monitor

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the distribution layer (§13.1).
type Metrics struct {
	CacheHitTotal     *prometheus.CounterVec   // labels: cache_type ("prefix"|"warm"|"cold")
	CacheRequestTotal *prometheus.CounterVec   // labels: cache_type
	TTFBSeconds       *prometheus.HistogramVec // labels: cache_type
	PeerHitTotal      prometheus.Counter       // sibling ICP hit
	PeerRequestTotal  prometheus.Counter       // sibling ICP request
	BackhaulBandwidth prometheus.Gauge         // current backhaul bandwidth bytes
	BackhaulCapacity  prometheus.Gauge         // backhaul capacity bytes
	PeerScore         *prometheus.GaugeVec     // labels: peer_id
	JWTRefreshSuccess prometheus.Counter       // JWT refresh success count
	JWTRefreshLastTS  prometheus.Gauge         // last successful JWT refresh timestamp (unix)
	RelayBytesTotal   prometheus.Counter       // relay forwarded bytes
	BackhaulBytesTotal prometheus.Counter      // backhaul bytes total

	registry *prometheus.Registry
}

// NewMetrics creates and registers all metrics with a new prometheus.Registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		CacheHitTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "edge_cache_hit_total",
				Help: "Total number of cache hits by cache type.",
			},
			[]string{"cache_type"},
		),
		CacheRequestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "edge_cache_request_total",
				Help: "Total number of cache requests by cache type.",
			},
			[]string{"cache_type"},
		),
		TTFBSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "edge_ttfb_seconds",
				Help:    "Time to first byte in seconds by cache type.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"cache_type"},
		),
		PeerHitTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "edge_peer_hit_total",
				Help: "Total number of sibling peer ICP hits.",
			},
		),
		PeerRequestTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "edge_peer_request_total",
				Help: "Total number of sibling peer ICP requests.",
			},
		),
		BackhaulBandwidth: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "edge_backhaul_bandwidth_bytes",
				Help: "Current backhaul bandwidth usage in bytes.",
			},
		),
		BackhaulCapacity: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "edge_backhaul_capacity_bytes",
				Help: "Backhaul capacity in bytes.",
			},
		),
		PeerScore: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "edge_peer_score",
				Help: "Current score of each peer.",
			},
			[]string{"peer_id"},
		),
		JWTRefreshSuccess: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "edge_jwt_refresh_success_total",
				Help: "Total number of successful JWT refresh operations.",
			},
		),
		JWTRefreshLastTS: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "edge_jwt_refresh_last_success_timestamp",
				Help: "Unix timestamp of the last successful JWT refresh.",
			},
		),
		RelayBytesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "edge_relay_bytes_total",
				Help: "Total bytes forwarded through relay.",
			},
		),
		BackhaulBytesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "edge_backhaul_bytes_total",
				Help: "Total bytes fetched through backhaul.",
			},
		),
		registry: reg,
	}
	m.Register(reg)
	return m
}

// Register registers all metrics with the given prometheus.Registry.
// Idempotent — safe to call multiple times (Register silently ignores duplicates).
func (m *Metrics) Register(r *prometheus.Registry) {
	r.MustRegister(
		m.CacheHitTotal,
		m.CacheRequestTotal,
		m.TTFBSeconds,
		m.PeerHitTotal,
		m.PeerRequestTotal,
		m.BackhaulBandwidth,
		m.BackhaulCapacity,
		m.PeerScore,
		m.JWTRefreshSuccess,
		m.JWTRefreshLastTS,
		m.RelayBytesTotal,
		m.BackhaulBytesTotal,
	)
}

// HTTPHandler returns an http.Handler that exposes metrics on the /metrics endpoint.
func (m *Metrics) HTTPHandler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RecordCacheHit increments the cache hit counter for the given cache type.
func (m *Metrics) RecordCacheHit(cacheType string) {
	m.CacheHitTotal.WithLabelValues(cacheType).Inc()
}

// RecordCacheRequest increments the cache request counter for the given cache type.
func (m *Metrics) RecordCacheRequest(cacheType string) {
	m.CacheRequestTotal.WithLabelValues(cacheType).Inc()
}

// RecordTTFB observes a time-to-first-byte value for the given cache type.
func (m *Metrics) RecordTTFB(cacheType string, seconds float64) {
	m.TTFBSeconds.WithLabelValues(cacheType).Observe(seconds)
}

// RecordPeerHit increments the peer hit counter.
func (m *Metrics) RecordPeerHit() {
	m.PeerHitTotal.Inc()
}

// RecordPeerRequest increments the peer request counter.
func (m *Metrics) RecordPeerRequest() {
	m.PeerRequestTotal.Inc()
}

// SetBackhaulBandwidth sets the current backhaul bandwidth gauge.
func (m *Metrics) SetBackhaulBandwidth(bytes int64) {
	m.BackhaulBandwidth.Set(float64(bytes))
}

// SetBackhaulCapacity sets the backhaul capacity gauge.
func (m *Metrics) SetBackhaulCapacity(bytes int64) {
	m.BackhaulCapacity.Set(float64(bytes))
}

// SetPeerScore sets the score gauge for a given peer.
func (m *Metrics) SetPeerScore(peerID string, score float64) {
	m.PeerScore.WithLabelValues(peerID).Set(score)
}

// RecordJWTRefreshSuccess increments the JWT refresh success counter.
func (m *Metrics) RecordJWTRefreshSuccess() {
	m.JWTRefreshSuccess.Inc()
}

// SetJWTRefreshLastTS sets the last successful JWT refresh timestamp.
func (m *Metrics) SetJWTRefreshLastTS(unixTS int64) {
	m.JWTRefreshLastTS.Set(float64(unixTS))
}

// RecordRelayBytes adds forwarded bytes to the relay counter.
func (m *Metrics) RecordRelayBytes(n int64) {
	m.RelayBytesTotal.Add(float64(n))
}

// RecordBackhaulBytes adds bytes to the backhaul counter.
func (m *Metrics) RecordBackhaulBytes(n int64) {
	m.BackhaulBytesTotal.Add(float64(n))
}
