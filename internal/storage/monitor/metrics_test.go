package monitor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"gopkg.in/yaml.v3"
)

// ─── Metrics Registration & Recording ───

func TestNewStorageMetrics_RegistersAll(t *testing.T) {
	m := NewStorageMetrics()
	if m == nil {
		t.Fatal("NewStorageMetrics returned nil")
	}
}

func TestStorageMetrics_RecordBackhaulSuccess(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordBackhaulSuccess("115")
	val := findCounterValue(t, m, "storage_access_backhaul_success_total", "vendor", "115")
	if val != 1.0 {
		t.Fatalf("expected 1.0, got %v", val)
	}
}

func TestStorageMetrics_RecordBackhaulRequest(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordBackhaulRequest("115")
	m.RecordBackhaulRequest("115")
	val := findCounterValue(t, m, "storage_access_backhaul_request_total", "vendor", "115")
	if val != 2.0 {
		t.Fatalf("expected 2.0, got %v", val)
	}
}

func TestStorageMetrics_RecordBackhaulDuration(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordBackhaulDuration("baidu", 0.5)
	hist := findHistogram(t, m, "storage_access_backhaul_duration_seconds", "vendor", "baidu")
	if hist.GetSampleCount() != 1 {
		t.Fatalf("expected sample count 1, got %d", hist.GetSampleCount())
	}
	if hist.GetSampleSum() < 0.49 || hist.GetSampleSum() > 0.51 {
		t.Fatalf("expected sample sum ~0.5, got %v", hist.GetSampleSum())
	}
}

func TestStorageMetrics_SetAccountHealth(t *testing.T) {
	m := NewStorageMetrics()
	m.SetAccountHealth("115", "banned")
	for _, s := range []string{"healthy", "degraded", "banned"} {
		val := findGaugeValue(t, m, "storage_account_health_state", "vendor", "115", "state", s)
		if s == "banned" && val != 1.0 {
			t.Fatalf("expected banned=1.0, got %v", val)
		}
		if s != "banned" && val != 0.0 {
			t.Fatalf("expected %s=0.0, got %v", s, val)
		}
	}
}

func TestStorageMetrics_RecordAccountRequest(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordAccountRequest("115", "acct_03")
	val := findCounterValueMulti(t, m, "storage_account_requests_total",
		[]string{"vendor", "account_id"}, []string{"115", "acct_03"})
	if val != 1.0 {
		t.Fatalf("expected 1.0, got %v", val)
	}
}

func TestStorageMetrics_LinkpoolHit(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordLinkpoolHit()
	m.RecordLinkpoolHit()
	val := findCounterValueSimple(t, m, "storage_linkpool_hit_total")
	if val != 2.0 {
		t.Fatalf("expected 2.0, got %v", val)
	}
}

func TestStorageMetrics_LinkpoolRequest(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordLinkpoolRequest()
	val := findCounterValueSimple(t, m, "storage_linkpool_request_total")
	if val != 1.0 {
		t.Fatalf("expected 1.0, got %v", val)
	}
}

func TestStorageMetrics_SetCircuitBreakerState(t *testing.T) {
	m := NewStorageMetrics()
	m.SetCircuitBreakerState("acct_03", "open")
	for _, s := range []string{"closed", "half_open", "open"} {
		val := findGaugeValue(t, m, "storage_circuit_breaker_state", "account_id", "acct_03", "state", s)
		if s == "open" && val != 1.0 {
			t.Fatalf("expected open=1.0, got %v", val)
		}
		if s != "open" && val != 0.0 {
			t.Fatalf("expected %s=0.0, got %v", s, val)
		}
	}
}

func TestStorageMetrics_AllExpectedNames(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordBackhaulSuccess("115")
	m.RecordBackhaulRequest("115")
	m.RecordBackhaulDuration("115", 0.1)
	m.SetAccountHealth("115", "healthy")
	m.RecordAccountRequest("115", "acct_03")
	m.RecordLinkpoolHit()
	m.RecordLinkpoolRequest()
	m.SetCircuitBreakerState("acct_03", "closed")

	families := gatherFamilies(t, m)

	expected := []string{
		"storage_access_backhaul_success_total",
		"storage_access_backhaul_request_total",
		"storage_access_backhaul_duration_seconds",
		"storage_account_health_state",
		"storage_account_requests_total",
		"storage_linkpool_hit_total",
		"storage_linkpool_request_total",
		"storage_circuit_breaker_state",
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

// ─── /metrics HTTP Endpoint ───

func TestMetricsEndpoint_Returns200(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordBackhaulSuccess("115")

	handler := m.HTTPHandler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected Content-Type containing text/plain, got %q", ct)
	}
}

func TestMetricsEndpoint_ShowsUpdatedValues(t *testing.T) {
	m := NewStorageMetrics()
	m.RecordBackhaulSuccess("115")

	handler := m.HTTPHandler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `storage_access_backhaul_success_total{vendor="115"} 1`) {
		t.Fatalf("expected updated metric value in /metrics output:\n%s", body)
	}
}

func TestMetricsServer_NewAndAddr(t *testing.T) {
	m := NewStorageMetrics()
	s := NewMetricsServer(":9090", m)
	if s.Addr() != ":9090" {
		t.Fatalf("expected :9090, got %q", s.Addr())
	}
}

func TestMetricsServer_StartStop(t *testing.T) {
	m := NewStorageMetrics()
	s := NewMetricsServer(":0", m)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

// ─── Alert Rules YAML ───

func TestAlertsStorageYAML_Syntax(t *testing.T) {
	data, err := os.ReadFile("../../../configs/prometheus-alerts-storage.yml")
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
		"MassAccountBan",
		"VendorAllDown",
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

// ─── Example: integration with real code ───

func TestStorageMetrics_ExampleUsage(t *testing.T) {
	m := NewStorageMetrics()

	// Simulate a backhaul fetch that fails.
	vendor := "115"
	m.RecordBackhaulRequest(vendor)
	// On error: we do not increment success.
	// Later, a successful fetch:
	m.RecordBackhaulRequest(vendor)
	m.RecordBackhaulSuccess(vendor)

	// Simulate link pool hit.
	m.RecordLinkpoolRequest()
	m.RecordLinkpoolHit()

	families := gatherFamilies(t, m)
	_ = families
}

// ─── Helpers ───

func gatherFamilies(t *testing.T, m *StorageMetrics) []*dto.MetricFamily {
	t.Helper()
	families, err := m.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return families
}

func findFamily(t *testing.T, m *StorageMetrics, name string) *dto.MetricFamily {
	t.Helper()
	families := gatherFamilies(t, m)
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func findMetricWithLabels(t *testing.T, m *StorageMetrics, name string, labels ...string) *dto.Metric {
	t.Helper()
	if len(labels)%2 != 0 {
		t.Fatal("labels must be key,value pairs")
	}
	family := findFamily(t, m, name)
	for _, met := range family.GetMetric() {
		match := true
		for i := 0; i < len(labels); i += 2 {
			key, val := labels[i], labels[i+1]
			found := false
			for _, lp := range met.GetLabel() {
				if lp.GetName() == key && lp.GetValue() == val {
					found = true
					break
				}
			}
			if !found {
				match = false
				break
			}
		}
		if match {
			return met
		}
	}
	t.Fatalf("metric %q with labels %v not found", name, fmt.Sprint(labels))
	return nil
}

func findCounterValue(t *testing.T, m *StorageMetrics, name, labelKey, labelValue string) float64 {
	t.Helper()
	met := findMetricWithLabels(t, m, name, labelKey, labelValue)
	if met.Counter == nil {
		t.Fatalf("metric %q is not a counter", name)
	}
	return met.Counter.GetValue()
}

func findCounterValueSimple(t *testing.T, m *StorageMetrics, name string) float64 {
	t.Helper()
	family := findFamily(t, m, name)
	for _, met := range family.GetMetric() {
		if met.Counter != nil {
			return met.Counter.GetValue()
		}
	}
	t.Fatalf("metric %q has no counter metric", name)
	return 0
}

func findCounterValueMulti(t *testing.T, m *StorageMetrics, name string, keys, vals []string) float64 {
	t.Helper()
	labels := make([]string, 0, len(keys)*2)
	for i := range keys {
		labels = append(labels, keys[i], vals[i])
	}
	met := findMetricWithLabels(t, m, name, labels...)
	if met.Counter == nil {
		t.Fatalf("metric %q is not a counter", name)
	}
	return met.Counter.GetValue()
}

func findGaugeValue(t *testing.T, m *StorageMetrics, name string, labels ...string) float64 {
	t.Helper()
	met := findMetricWithLabels(t, m, name, labels...)
	if met.Gauge == nil {
		t.Fatalf("metric %q is not a gauge", name)
	}
	return met.Gauge.GetValue()
}

func findHistogram(t *testing.T, m *StorageMetrics, name string, labels ...string) *dto.Histogram {
	t.Helper()
	met := findMetricWithLabels(t, m, name, labels...)
	if met.Histogram == nil {
		t.Fatalf("metric %q is not a histogram", name)
	}
	return met.Histogram
}
