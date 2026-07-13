package monitor

import (
	"os"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"gopkg.in/yaml.v3"
)

// ─── Metrics Registration & Recording ───

func TestMetrics_Register(t *testing.T) {
	// Given: a new Metrics instance
	// When:  Register is called (inside NewMetrics)
	// Then:  no panic occurs
	m := NewMetrics()
	if m.registry == nil {
		t.Fatal("registry is nil")
	}
	if _, err := m.registry.Gather(); err != nil {
		t.Fatalf("gather failed: %v", err)
	}
}

func TestMetrics_RecordCacheHit(t *testing.T) {
	// Given: a fresh Metrics instance
	// When:  RecordCacheHit("warm") is called
	// Then:  the counter with label cache_type="warm" is incremented to 1
	m := NewMetrics()
	m.RecordCacheHit("warm")

	families := mustGather(t, m)
	counter := findCounter(t, families, "edge_cache_hit_total", "warm")
	if counter.GetValue() != 1.0 {
		t.Fatalf("expected 1, got %v", counter.GetValue())
	}
}

func TestMetrics_RecordCacheRequest(t *testing.T) {
	// Given: a fresh Metrics instance
	// When:  RecordCacheRequest("warm") is called twice
	// Then:  counter = 2
	m := NewMetrics()
	m.RecordCacheRequest("warm")
	m.RecordCacheRequest("warm")

	families := mustGather(t, m)
	counter := findCounter(t, families, "edge_cache_request_total", "warm")
	if counter.GetValue() != 2.0 {
		t.Fatalf("expected 2, got %v", counter.GetValue())
	}
}

func TestMetrics_RecordTTFB(t *testing.T) {
	// Given: a fresh Metrics instance
	// When:  RecordTTFB("warm", 0.5) is called
	// Then:  histogram count = 1, sum = 0.5
	m := NewMetrics()
	m.RecordTTFB("warm", 0.5)

	families := mustGather(t, m)
	hist := findHistogram(t, families, "edge_ttfb_seconds", "warm")
	if hist.GetSampleCount() != 1 {
		t.Fatalf("expected sample count 1, got %d", hist.GetSampleCount())
	}
	if hist.GetSampleSum() != 0.5 {
		t.Fatalf("expected sample sum 0.5, got %v", hist.GetSampleSum())
	}
}

func TestMetrics_SetPeerScore(t *testing.T) {
	// Given: a fresh Metrics instance
	// When:  SetPeerScore("peer1", 5.0) is called
	// Then:  gauge value = 5.0
	m := NewMetrics()
	m.SetPeerScore("peer1", 5.0)

	families := mustGather(t, m)
	gauge := findGauge(t, families, "edge_peer_score", "peer1")
	if gauge.GetValue() != 5.0 {
		t.Fatalf("expected 5.0, got %v", gauge.GetValue())
	}
}

func TestMetrics_HTTPHandler(t *testing.T) {
	// Given: a new Metrics instance
	// When:  HTTPHandler() is called
	// Then:  returns a non-nil http.Handler
	m := NewMetrics()
	h := m.HTTPHandler()
	if h == nil {
		t.Fatal("HTTPHandler returned nil")
	}
}

func TestMetrics_AllMetricsExposed(t *testing.T) {
	// Given: a Metrics instance with some data recorded
	// When:  gathered
	// Then:  all 12 expected metric names are present
	m := NewMetrics()
	// Record at least one value so vectors are present
	m.RecordCacheHit("warm")
	m.RecordCacheRequest("warm")
	m.RecordTTFB("warm", 0.1)
	m.RecordPeerHit()
	m.RecordPeerRequest()
	m.SetBackhaulBandwidth(100)
	m.SetBackhaulCapacity(200)
	m.SetPeerScore("peer1", 1.0)
	m.RecordJWTRefreshSuccess()
	m.SetJWTRefreshLastTS(1000)
	m.RecordRelayBytes(500)
	m.RecordBackhaulBytes(600)

	families := mustGather(t, m)

	expected := []string{
		"edge_cache_hit_total",
		"edge_cache_request_total",
		"edge_ttfb_seconds",
		"edge_peer_hit_total",
		"edge_peer_request_total",
		"edge_backhaul_bandwidth_bytes",
		"edge_backhaul_capacity_bytes",
		"edge_peer_score",
		"edge_jwt_refresh_success_total",
		"edge_jwt_refresh_last_success_timestamp",
		"edge_relay_bytes_total",
		"edge_backhaul_bytes_total",
	}

	seen := make(map[string]bool)
	for _, f := range families {
		if f.Name != nil {
			seen[f.GetName()] = true
		}
	}

	for _, name := range expected {
		if !seen[name] {
			t.Errorf("expected metric %q not found", name)
		}
	}

	if len(seen) != len(expected) {
		t.Errorf("expected %d metrics, got %d", len(expected), len(seen))
	}
}

// ─── Alert Rules YAML ───

func TestAlertsYAML_Syntax(t *testing.T) {
	// Given: the prometheus-alerts.yml config file
	// When:  read and parsed
	// Then:  valid YAML with 7 expected alert rules
	data, err := os.ReadFile("../../configs/prometheus-alerts.yml")
	if err != nil {
		t.Fatalf("failed to read alerts YAML: %v", err)
	}

	var doc struct {
		Groups []struct {
			Name  string `yaml:"name"`
			Rules []struct {
				Alert string `yaml:"alert"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid YAML: %v", err)
	}

	if len(doc.Groups) == 0 {
		t.Fatal("no alert groups found")
	}

	group := doc.Groups[0]
	if group.Name != "cloud_dash_critical" {
		t.Errorf("expected group name 'cloud_dash_critical', got %q", group.Name)
	}

	expectedAlerts := []string{
		"HighBackhaulFailureRate",
		"LowCacheHitRate",
		"L4NodeDown",
		"RegionalL4AllDown",
		"SchedulerPartition",
		"PeerScoreGraylist",
		"HighTTFB",
	}

	seen := make(map[string]bool)
	for _, r := range group.Rules {
		seen[r.Alert] = true
	}

	for _, name := range expectedAlerts {
		if !seen[name] {
			t.Errorf("expected alert %q not found", name)
		}
	}

	if len(group.Rules) != len(expectedAlerts) {
		t.Errorf("expected %d alerts, got %d", len(expectedAlerts), len(group.Rules))
	}
}

// ─── Helpers ───

func mustGather(t *testing.T, m *Metrics) []*dto.MetricFamily {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return families
}

func findFamily(t *testing.T, families []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func matchLabel(metric *dto.Metric, labelName, expected string) *dto.Metric {
	for _, lp := range metric.GetLabel() {
		if lp.GetName() == labelName && lp.GetValue() == expected {
			return metric
		}
	}
	return nil
}

func findMetric(t *testing.T, families []*dto.MetricFamily, name, labelValue string) *dto.Metric {
	t.Helper()
	family := findFamily(t, families, name)
	for _, m := range family.GetMetric() {
		// For families without label (peer hit, peer request, etc.), return first.
		if labelValue == "" {
			return m
		}
		if matched := matchLabel(m, "cache_type", labelValue); matched != nil {
			return matched
		}
		if matched := matchLabel(m, "peer_id", labelValue); matched != nil {
			return matched
		}
	}
	// Re-check with contains — some labels are non-cache_type (e.g., peer_id).
	for _, m := range family.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetValue() == labelValue {
				return m
			}
		}
	}
	t.Fatalf("metric %q with label value %q not found", name, labelValue)
	return nil
}

func findCounter(t *testing.T, families []*dto.MetricFamily, name, labelValue string) *dto.Counter {
	t.Helper()
	m := findMetric(t, families, name, labelValue)
	if m.Counter == nil {
		t.Fatalf("metric %q is not a counter", name)
	}
	return m.Counter
}

func findGauge(t *testing.T, families []*dto.MetricFamily, name, labelValue string) *dto.Gauge {
	t.Helper()
	m := findMetric(t, families, name, labelValue)
	if m.Gauge == nil {
		t.Fatalf("metric %q is not a gauge", name)
	}
	return m.Gauge
}

func findHistogram(t *testing.T, families []*dto.MetricFamily, name, labelValue string) *dto.Histogram {
	t.Helper()
	m := findMetric(t, families, name, labelValue)
	if m.Histogram == nil {
		t.Fatalf("metric %q is not a histogram", name)
	}
	return m.Histogram
}


