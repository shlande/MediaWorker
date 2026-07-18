package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shlande/mediaworker/internal/node/monitor"
)

// TestEdgeNode_MetricsHTTPHandler_Returns200AndKeyNames (T20) verifies that
// the edge-node /metrics endpoint returns 200 and exposes the key metric
// names required by plan line 278. This is a unit-level test of the handler
// wiring; the end-to-end backhaul/JWT counter increment is verified in
// internal/node/monitor/metrics_t20_test.go and the cmd/control-plane tests.
func TestEdgeNode_MetricsHTTPHandler_Returns200AndKeyNames(t *testing.T) {
	m := monitor.NewMetrics()
	// Touch the counters the edge-node actually increments so the metric
	// families materialise in /metrics (Prometheus CounterVec families only
	// appear after the first WithLabelValues call).
	m.RecordCacheRequest("warm")
	m.RecordCacheHit("warm")
	m.RecordPeerRequest()
	m.RecordPeerHit()
	m.RecordJWTInitialSuccess()
	m.RecordJWTRefreshSuccess()
	m.SetJWTRefreshLastTS(0)

	srv := httptest.NewServer(m.HTTPHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	want := []string{
		"edge_cache_hit_total",
		"edge_cache_request_total",
		"edge_peer_hit_total",
		"edge_peer_request_total",
		"edge_jwt_initial_success_total",
		"edge_jwt_refresh_success_total",
		"edge_jwt_refresh_failure_total",
	}
	for _, name := range want {
		if !strings.Contains(string(body), name) {
			t.Errorf("metric %q not present in /metrics body", name)
		}
	}
}
