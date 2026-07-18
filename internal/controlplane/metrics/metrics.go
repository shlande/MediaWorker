// Package metrics provides Prometheus metrics for the control-plane (CP).
//
// This package is CP-resident: it intentionally lives under
// internal/controlplane/ so the control-plane binary never links any
// internal/node/* package (enforced by TestIsolation_ControlPlaneBinaryNoNodeCode).
//
// Metric naming: CP-specific counters use the "cp_" prefix, matching the
// names originally defined in internal/node/monitor (T20). Edge/ingest
// instruments stay in internal/node/monitor and are NOT duplicated here.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus metrics that the control-plane binary
// increments and exposes on /metrics. Only CP-owned instruments live here —
// edge/ingest counters remain in internal/node/monitor.
type Metrics struct {
	// CPJWTIssuedTotal counts JWT issuance outcomes on the control plane.
	// outcome ∈ {"success","invalid_peerid","invalid_signature",
	// "rate_limited","internal_error"} — matches the error classes returned
	// by JWTService.HandleJWTRequest.
	CPJWTIssuedTotal *prometheus.CounterVec

	// CPContentIngestedReceived counts CONTENT_INGESTED events decoded by
	// the control-plane SyncBroadcaster subscriber (before PinOrchestrator
	// .OnContentIngested is called).
	CPContentIngestedReceived prometheus.Counter

	// CPPinPlanDispatched counts PinPlan updates dispatched by the
	// control-plane PinOrchestrator (fire-and-forget via broadcaster
	// .SendToNode). Counts dispatch attempts, not confirmed delivery.
	CPPinPlanDispatched prometheus.Counter

	registry *prometheus.Registry
}

// NewMetrics creates and registers all CP metrics with a new
// prometheus.Registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
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
		registry: reg,
	}
	m.register(reg)
	return m
}

// register registers all metrics with the given prometheus.Registry.
func (m *Metrics) register(r *prometheus.Registry) {
	r.MustRegister(
		m.CPJWTIssuedTotal,
		m.CPContentIngestedReceived,
		m.CPPinPlanDispatched,
	)
}

// HTTPHandler returns an http.Handler that exposes metrics on /metrics.
func (m *Metrics) HTTPHandler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RecordCPJWTIssued increments the control-plane JWT issuance counter with
// the given outcome label. Use one of the CPJWTOutcome* constants.
func (m *Metrics) RecordCPJWTIssued(outcome string) {
	m.CPJWTIssuedTotal.WithLabelValues(outcome).Inc()
}

// CPJWT outcome label values — keep in sync with the error classes returned
// by JWTService.HandleJWTRequest (service.go:88+).
const (
	CPJWTOutcomeSuccess       = "success"
	CPJWTOutcomeInvalidPeerID = "invalid_peerid"
	CPJWTOutcomeInvalidSig    = "invalid_signature"
	CPJWTOutcomeRateLimited   = "rate_limited"
	CPJWTOutcomeInternalError = "internal_error"
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
