// Package monitor provides Prometheus metrics registration and alert rules for the distribution layer.
package monitor

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the MediaWorker distribution plane.
//
// Originally a 12-instrument edge-only registry (§13.1); extended in T20 to
// also host control-plane and ingest-worker counters so all three binaries
// share one registry and one /metrics endpoint (plan line 275 — no separate
// port). Edge instruments keep the edge_* prefix so existing alert rules are
// untouched; new cross-service counters use cp_* and ingest_* prefixes.
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

	// T20 — edge-node JWT acquisition. JWTRefreshSuccess already covers the
	// refresh success side; the counters below add the failure side and the
	// initial acquisition pair (initial failure enters degraded mode, see
	// cmd/edge-node/main.go:215).
	JWTInitialSuccess prometheus.Counter
	JWTInitialFailure prometheus.Counter
	JWTRefreshFailure prometheus.Counter

	// T20 — control-plane JWT issuance. outcome ∈ {"success","invalid_peerid",
	// "invalid_signature","rate_limited","internal_error"} — matches the
	// error classes returned by JWTService.HandleJWTRequest.
	CPJWTIssuedTotal *prometheus.CounterVec // labels: outcome

	// T20 — control-plane CONTENT_INGESTED event intake (SyncBroadcaster
	// subscribe callback). Counted after successful JSON decode of the
	// event payload, before PinOrchestrator.OnContentIngested is called.
	CPContentIngestedReceived prometheus.Counter

	// T20 — control-plane PinPlan dispatch attempts (fire-and-forget via
	// broadcaster.SendToNode). Counts dispatch attempts, not confirmed
	// delivery — matches the orchestrator's best-effort semantics.
	CPPinPlanDispatched prometheus.Counter

	// T20 — ingest-worker ingest outcomes. outcome ∈ {"success","failure"}.
	// Failure covers ANY error from pipeline.Ingest (transcode/upload/tx).
	IngestTotal *prometheus.CounterVec // labels: content_type, outcome

	// T20 — mirrors internal/ingest/syncpub.publishFailures (atomic.Uint64).
	// Exposed as a gauge, not a counter, because the source is already a
	// monotonic counter — re-exposing as a prometheus.Counter would
	// double-count on scraper retries. A gauge reflects the source value
	// exactly. Refreshed on each /metrics scrape via UpdatePublishFailures.
	IngestPublishFailures prometheus.Gauge

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
		JWTInitialSuccess: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "edge_jwt_initial_success_total",
				Help: "Successful initial JWT acquisitions from the control plane.",
			},
		),
		JWTInitialFailure: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "edge_jwt_initial_failure_total",
				Help: "Failed initial JWT acquisitions. Non-zero count indicates the node is in degraded mode.",
			},
		),
		JWTRefreshFailure: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "edge_jwt_refresh_failure_total",
				Help: "Failed periodic JWT refresh attempts. The refresh loop continues; sustained growth signals CP unreachability.",
			},
		),
		CPJWTIssuedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cp_jwt_issued_total",
				Help: "JWT issuance outcomes on the control plane. outcome=label.",
			},
			[]string{"outcome"},
		),
		CPContentIngestedReceived: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "cp_content_ingested_received_total",
				Help: "CONTENT_INGESTED events decoded by the control-plane SyncBroadcaster subscriber.",
			},
		),
		CPPinPlanDispatched: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "cp_pin_plan_dispatched_total",
				Help: "PinPlan updates dispatched by the control-plane PinOrchestrator.",
			},
		),
		IngestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ingest_total",
				Help: "Ingest request outcomes by content_type and outcome.",
			},
			[]string{"content_type", "outcome"},
		),
		IngestPublishFailures: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "ingest_publish_failures",
				Help: "Current value of syncpub.PublishFailures() (mirrored atomic counter).",
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
		m.JWTInitialSuccess,
		m.JWTInitialFailure,
		m.JWTRefreshFailure,
		m.CPJWTIssuedTotal,
		m.CPContentIngestedReceived,
		m.CPPinPlanDispatched,
		m.IngestTotal,
		m.IngestPublishFailures,
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

// ─── T20 recorder helpers ───

// RecordJWTInitialSuccess increments the edge-node initial JWT acquisition
// success counter.
func (m *Metrics) RecordJWTInitialSuccess() {
	m.JWTInitialSuccess.Inc()
}

// RecordJWTInitialFailure increments the edge-node initial JWT acquisition
// failure counter. The node enters degraded mode on each failure.
func (m *Metrics) RecordJWTInitialFailure() {
	m.JWTInitialFailure.Inc()
}

// RecordJWTRefreshFailure increments the edge-node periodic JWT refresh
// failure counter. The refresh loop continues regardless.
func (m *Metrics) RecordJWTRefreshFailure() {
	m.JWTRefreshFailure.Inc()
}

// RecordCPJWTIssued increments the control-plane JWT issuance counter with
// the given outcome label. Use one of the CPJWTOutcome* constants.
func (m *Metrics) RecordCPJWTIssued(outcome string) {
	m.CPJWTIssuedTotal.WithLabelValues(outcome).Inc()
}

// CPJWT outcome label values — keep in sync with the error classes returned
// by JWTService.HandleJWTRequest (service.go:88+).
const (
	CPJWTOutcomeSuccess        = "success"
	CPJWTOutcomeInvalidPeerID  = "invalid_peerid"
	CPJWTOutcomeInvalidSig     = "invalid_signature"
	CPJWTOutcomeRateLimited    = "rate_limited"
	CPJWTOutcomeInternalError  = "internal_error"
)

// RecordCPContentIngestedReceived increments the control-plane
// CONTENT_INGESTED event intake counter.
func (m *Metrics) RecordCPContentIngestedReceived() {
	m.CPContentIngestedReceived.Inc()
}

// RecordCPPinPlanDispatched increments the control-plane PinPlan dispatch
// counter.
func (m *Metrics) RecordCPPinPlanDispatched() {
	m.CPPinPlanDispatched.Inc()
}

// RecordIngest increments the ingest-worker ingest counter with the given
// content_type and outcome label.
func (m *Metrics) RecordIngest(contentType, outcome string) {
	m.IngestTotal.WithLabelValues(contentType, outcome).Inc()
}

// Ingest outcome label values.
const (
	IngestOutcomeSuccess = "success"
	IngestOutcomeFailure = "failure"
)

// SetIngestPublishFailures mirrors syncpub.PublishFailures() into the gauge.
// Called by the ingest-worker's /metrics handler before scraping.
func (m *Metrics) SetIngestPublishFailures(n uint64) {
	m.IngestPublishFailures.Set(float64(n))
}
