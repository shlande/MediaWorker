package backhaul

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/shlande/mediaworker/internal/node/monitor"
)

// gaugeFloat reads the current value of a prometheus gauge/counter without
// pulling in prometheus/testutil (would add a new go.mod dependency).
func gaugeFloat(t *testing.T, m prometheus.Metric) float64 {
	t.Helper()
	var v dto.Metric
	if err := m.Write(&v); err != nil {
		t.Fatalf("metric write: %v", err)
	}
	if v.Gauge != nil {
		return v.Gauge.GetValue()
	}
	return v.Counter.GetValue()
}

// ─── Stats24h ───────────────────────────────────────────────────────────────

func TestStats24h_SuccessRateAndP95(t *testing.T) {
	// Given: 100 observations (95 success), latencies 1..100 ms, all in-window
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	now := time.Now()
	for i := 1; i <= 100; i++ {
		bm.recordObservation(observation{ts: now.Add(-time.Hour), success: i <= 95, latencyMs: int64(i), bytes: 10})
	}

	// When: computing stats
	rate, p95, totalBytes := bm.Stats24h(now)

	// Then: rate ≈ 0.95, p95 = 95 (nearest-rank of 1..100), bytes cumulative
	if math.Abs(rate-0.95) > 1e-9 {
		t.Errorf("successRate = %f, want ~0.95", rate)
	}
	if p95 < 90 || p95 > 100 {
		t.Errorf("p95Ms = %d, want in [90,100] (nearest-rank of 1..100 → 95)", p95)
	}
	if totalBytes != 1000 {
		t.Errorf("totalBytes = %d, want 1000", totalBytes)
	}
}

func TestStats24h_ZeroObservations(t *testing.T) {
	// Given: no observations
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)

	// When: computing stats
	rate, p95, totalBytes := bm.Stats24h(time.Now())

	// Then: successRate is exactly 0, not NaN (division guard)
	if rate != 0 || math.IsNaN(rate) {
		t.Errorf("successRate = %f, want 0 (not NaN)", rate)
	}
	if p95 != 0 {
		t.Errorf("p95Ms = %d, want 0", p95)
	}
	if totalBytes != 0 {
		t.Errorf("totalBytes = %d, want 0", totalBytes)
	}
}

func TestStats24h_ExcludesObservationsOlderThan24h(t *testing.T) {
	// Given: one in-window success and one 25h-old failure
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	now := time.Now()
	bm.recordObservation(observation{ts: now.Add(-25 * time.Hour), success: false, latencyMs: 999, bytes: 5})
	bm.recordObservation(observation{ts: now.Add(-time.Hour), success: true, latencyMs: 42, bytes: 5})

	// When: computing stats
	rate, p95, totalBytes := bm.Stats24h(now)

	// Then: only the in-window observation shapes rate/p95; bytes stay cumulative
	if rate != 1 {
		t.Errorf("successRate = %f, want 1 (25h-old failure excluded)", rate)
	}
	if p95 != 42 {
		t.Errorf("p95Ms = %d, want 42", p95)
	}
	if totalBytes != 10 {
		t.Errorf("totalBytes = %d, want 10 (cumulative, not windowed)", totalBytes)
	}
}

// ─── BackhaulUtilization ────────────────────────────────────────────────────

func TestBackhaulUtilization_AveragesLast60s(t *testing.T) {
	// Given: 600 bytes observed just now, plus 10KB observed 2 minutes ago
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	now := time.Now()
	bm.recordObservation(observation{ts: now.Add(-2 * time.Minute), success: true, latencyMs: 1, bytes: 10_000})
	bm.recordObservation(observation{ts: now, success: true, latencyMs: 1, bytes: 600})

	// When: reading utilization
	util := bm.BackhaulUtilization()

	// Then: only the in-window 600 bytes count → 600/60 = 10 bytes/sec
	if util < 9 || util > 11 {
		t.Errorf("BackhaulUtilization() = %f, want ~10 bytes/sec", util)
	}
}

func TestBackhaulUtilization_ZeroWithoutTraffic(t *testing.T) {
	// Given/When: no observations
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)

	// Then: utilization is 0
	if got := bm.BackhaulUtilization(); got != 0 {
		t.Errorf("BackhaulUtilization() = %f, want 0", got)
	}
}

// ─── Ring eviction ──────────────────────────────────────────────────────────

func TestRecordObservation_RingBoundedAtCapacity(t *testing.T) {
	// Given: more observations than the ring capacity
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	now := time.Now()
	for i := 0; i < backhaulObservationCapacity+2000; i++ {
		bm.recordObservation(observation{ts: now, success: true, latencyMs: 1, bytes: 1})
	}

	// Then: the ring never exceeds capacity (oldest evicted)
	bm.obsMu.Lock()
	n := len(bm.wins)
	oldest := bm.wins[0]
	bm.obsMu.Unlock()
	if n > backhaulObservationCapacity {
		t.Errorf("ring length = %d, want <= %d", n, backhaulObservationCapacity)
	}
	if oldest.ts != now {
		t.Errorf("ring corrupted: oldest surviving ts = %v, want %v", oldest.ts, now)
	}
	rate, _, totalBytes := bm.Stats24h(now.Add(time.Second))
	if rate != 1 {
		t.Errorf("successRate = %f, want 1 after eviction", rate)
	}
	if totalBytes != int64(backhaulObservationCapacity+2000) {
		t.Errorf("totalBytes = %d, want %d", totalBytes, backhaulObservationCapacity+2000)
	}
}

// ─── HandleBlobL4 instrumentation ───────────────────────────────────────────

func TestHandleBlobL4_RecordsSuccessObservation(t *testing.T) {
	// Given: a cache-missing blob served by the data plane (6KB so the
	// bytes/sec gauge clears the 1-byte truncation floor)
	cache := newMockCache()
	payload := bytes.Repeat([]byte("x"), 6000)
	dp := &mockDataPlane{data: payload}
	m := monitor.NewMetrics()
	bm := NewBackhaulManager(cache, dp, nil, nil)
	bm.SetMetrics(m)

	// When: handling the request
	var buf bytes.Buffer
	if err := bm.HandleBlobL4(context.Background(), &buf, testBlobHash); err != nil {
		t.Fatalf("HandleBlobL4: %v", err)
	}

	// Then: one success observation with the dataplane byte count
	rate, _, totalBytes := bm.Stats24h(time.Now())
	if rate != 1 {
		t.Errorf("successRate = %f, want 1", rate)
	}
	if totalBytes != int64(len(payload)) {
		t.Errorf("totalBytes = %d, want %d (dataplane stream length)", totalBytes, len(payload))
	}
	if got := gaugeFloat(t, m.BackhaulBytesTotal); got != float64(len(payload)) {
		t.Errorf("edge_backhaul_bytes_total = %f, want %d", got, len(payload))
	}
	if got := gaugeFloat(t, m.BackhaulBandwidth); got <= 0 {
		t.Errorf("edge_backhaul_bandwidth_bytes = %f, want > 0 after bytes", got)
	}
	if util := bm.BackhaulUtilization(); util <= 0 {
		t.Errorf("BackhaulUtilization() = %f, want > 0 after bytes", util)
	}
}

func TestHandleBlobL4_RecordsFailureObservation(t *testing.T) {
	// Given: a failing data plane
	cache := newMockCache()
	dp := &mockDataPlane{err: errors.New("data plane down")}
	m := monitor.NewMetrics()
	bm := NewBackhaulManager(cache, dp, nil, nil)
	bm.SetMetrics(m)

	// When: handling the request
	var buf bytes.Buffer
	if err := bm.HandleBlobL4(context.Background(), &buf, testBlobHash); err == nil {
		t.Fatal("expected error, got nil")
	}

	// Then: one failure observation, zero bytes
	rate, _, totalBytes := bm.Stats24h(time.Now())
	if rate != 0 {
		t.Errorf("successRate = %f, want 0", rate)
	}
	if totalBytes != 0 {
		t.Errorf("totalBytes = %d, want 0", totalBytes)
	}
	if got := gaugeFloat(t, m.BackhaulBytesTotal); got != 0 {
		t.Errorf("edge_backhaul_bytes_total = %f, want 0", got)
	}
}

func TestHandleBlobL4_CacheHitRecordsNoObservation(t *testing.T) {
	// Given: a cache-hit blob (NOT backhaul)
	cache := newMockCache()
	_ = cache.Put(testBlobHash, []byte(testBlobData), 0)
	bm := NewBackhaulManager(cache, &mockDataPlane{}, nil, nil)

	// When: handling the request
	var buf bytes.Buffer
	if err := bm.HandleBlobL4(context.Background(), &buf, testBlobHash); err != nil {
		t.Fatalf("HandleBlobL4: %v", err)
	}

	// Then: no observation recorded
	rate, _, totalBytes := bm.Stats24h(time.Now())
	if rate != 0 || totalBytes != 0 {
		t.Errorf("cache hit leaked into backhaul stats: rate=%f bytes=%d, want 0/0", rate, totalBytes)
	}
}

// ─── Capacity gauge wiring ──────────────────────────────────────────────────

func TestSetBackhaulCapacityMbps_PublishesGauge(t *testing.T) {
	// Given: a manager with metrics wired
	m := monitor.NewMetrics()
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	bm.SetMetrics(m)

	// When: setting 1000 mbps capacity
	bm.SetBackhaulCapacityMbps(1000)

	// Then: gauge = 1000 * 1e6 / 8 = 125_000_000 bytes/sec
	if got := gaugeFloat(t, m.BackhaulCapacity); got != 125_000_000 {
		t.Errorf("edge_backhaul_capacity_bytes = %f, want 125000000", got)
	}
}

func TestSetBackhaulCapacityMbps_ZeroDoesNotReport(t *testing.T) {
	// Given: a manager with metrics wired
	m := monitor.NewMetrics()
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	bm.SetMetrics(m)

	// When: capacity is 0 (unknown) or negative
	bm.SetBackhaulCapacityMbps(0)
	bm.SetBackhaulCapacityMbps(-5)

	// Then: the gauge stays at its zero default (not reported)
	if got := gaugeFloat(t, m.BackhaulCapacity); got != 0 {
		t.Errorf("edge_backhaul_capacity_bytes = %f, want 0 (unreported)", got)
	}
}
