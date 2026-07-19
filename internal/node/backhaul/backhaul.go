package backhaul

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shlande/mediaworker/internal/node/monitor"
	"golang.org/x/sync/singleflight"
)

// observation is one local-backhaul (data-plane) fetch attempt record.
// Only the L4 local-backhaul path (l4.go) produces observations; sibling
// ICP fetches and cache hits are NOT backhaul and are excluded by design.
// Todo 42 extends tracking with ttfb observations in a separate slice —
// keep this struct focused on the attempt outcome.
type observation struct {
	ts        time.Time
	success   bool
	latencyMs int64
	bytes     int64
}

const (
	// backhaulObservationCapacity bounds the observation ring. Stats24h
	// filters by timestamp, so the ring is purely a memory bound — at 10k
	// entries the p95 sort touches at most ~10k int64s per call.
	backhaulObservationCapacity = 10_000

	// ttfbObservationCapacity bounds the ttfb sample ring (todo 42): same
	// memory-bound reasoning as backhaulObservationCapacity.
	ttfbObservationCapacity = 10_000

	backhaulStatsWindow = 24 * time.Hour

	backhaulUtilizationWindow = time.Minute
)

type BackhaulManager struct {
	cache CacheReader

	sfGroup singleflight.Group

	dataPlane  DataPlane
	icpFetcher ICPFetcher
	l4Fetcher  L4Fetcher

	// metrics is optional (nil = no instrumentation). When set, HandleBlobL4
	// and HandleBlobNoL4 increment edge_cache_request_total + edge_cache_hit_total
	// (cache_type="warm") and edge_peer_request_total + edge_peer_hit_total
	// (sibling ICP).
	metrics *monitor.Metrics

	// obsMu guards wins. wins is a bounded ring of the most recent
	// backhaul observations; when it exceeds backhaulObservationCapacity
	// the oldest half is evicted in one amortized copy (append stays O(1)
	// amortized, backing array bounded at ~capacity).
	obsMu sync.Mutex
	wins  []observation

	// ttfbMs is a bounded ring (same eviction pattern as wins) of ttfb
	// samples in milliseconds from successful L4 data-plane fetches.
	// ttfb = fetch start → stream ready (response headers received); the
	// first payload byte is not separately observable through the DataPlane
	// stream interface, so stream-ready is the v1 approximation.
	ttfbMs []int64

	// bytesTotal mirrors edge_backhaul_bytes_total; atomic so Stats24h
	// reads it without obsMu.
	bytesTotal atomic.Int64

	// cacheReqs/cacheHits are process-lifetime warm-cache request/hit
	// counters for the admin status endpoint (todo 42). The prometheus
	// CounterVec cannot be read back, hence private atomics. The derived
	// rate is CUMULATIVE since process start, not a windowed rate.
	cacheReqs atomic.Int64
	cacheHits atomic.Int64
}

type ICPFetcher interface {
	FetchFromPeer(ctx context.Context, blobHash string) (interface{}, bool, error)
}

type L4Fetcher interface {
	FetchFromL4Node(ctx context.Context, blobHash string) (interface{}, error)
}

type CacheReader interface {
	Get(blobHash string) ([]byte, bool)
}

func NewBackhaulManager(
	cache CacheReader,
	dataPlane DataPlane,
	icpFetcher ICPFetcher,
	l4Fetcher L4Fetcher,
) *BackhaulManager {
	return &BackhaulManager{
		cache:      cache,
		dataPlane:  dataPlane,
		icpFetcher: icpFetcher,
		l4Fetcher:  l4Fetcher,
	}
}

// SetMetrics wires a Metrics instance for backhaul instrumentation. Optional —
// a nil Metrics value (the zero value) disables instrumentation bit-for-bit
// (the recorder calls are no-ops on a nil receiver, but we guard explicitly
// to avoid surprising callers with nil-deref panics). Idempotent.
func (bm *BackhaulManager) SetMetrics(m *monitor.Metrics) {
	bm.metrics = m
}

// recordCacheRequest is a nil-safe helper for the cache hit/miss counters.
// cache_type="warm" matches the existing edge_cache_hit_total alert rule
// (LowCacheHitRate) — keep the label aligned with the alert queries.
// The atomic counters ALWAYS advance (they back WarmCacheHitRate); the
// prometheus increments stay metrics-gated.
func (bm *BackhaulManager) recordCacheRequest(hit bool) {
	bm.cacheReqs.Add(1)
	if hit {
		bm.cacheHits.Add(1)
	}
	if bm.metrics == nil {
		return
	}
	bm.metrics.RecordCacheRequest("warm")
	if hit {
		bm.metrics.RecordCacheHit("warm")
	}
}

// WarmCacheHitRate returns the cumulative warm-cache hit rate since process
// start (hits/requests over the /blob serving path). Zero requests yield
// exactly 0, never NaN. v1 simplification: cumulative, not windowed —
// trend series come from Prometheus directly.
func (bm *BackhaulManager) WarmCacheHitRate() float64 {
	reqs := bm.cacheReqs.Load()
	if reqs == 0 {
		return 0
	}
	return float64(bm.cacheHits.Load()) / float64(reqs)
}

// recordTTFB appends one ttfb sample (ms) to the bounded ring.
func (bm *BackhaulManager) recordTTFB(d time.Duration) {
	ms := d.Milliseconds()
	bm.obsMu.Lock()
	bm.ttfbMs = append(bm.ttfbMs, ms)
	if len(bm.ttfbMs) > ttfbObservationCapacity {
		drop := len(bm.ttfbMs) - ttfbObservationCapacity/2
		copy(bm.ttfbMs, bm.ttfbMs[drop:])
		bm.ttfbMs = bm.ttfbMs[:len(bm.ttfbMs)-drop]
	}
	bm.obsMu.Unlock()
}

// TTFBP95Ms returns the nearest-rank P95 over the ttfb sample ring. Zero
// samples yield 0. Cumulative since process start (the ring is not
// time-windowed) — v1 simplification; trends come from Prometheus.
func (bm *BackhaulManager) TTFBP95Ms() int64 {
	bm.obsMu.Lock()
	if len(bm.ttfbMs) == 0 {
		bm.obsMu.Unlock()
		return 0
	}
	sorted := make([]int64, len(bm.ttfbMs))
	copy(sorted, bm.ttfbMs)
	bm.obsMu.Unlock()

	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := (95*len(sorted) + 99) / 100 // ceil(0.95 * n)
	return sorted[rank-1]
}

// recordICPRequest is a nil-safe helper for the peer ICP hit/miss counters.
func (bm *BackhaulManager) recordICPRequest(hit bool) {
	if bm.metrics == nil {
		return
	}
	bm.metrics.RecordPeerRequest()
	if hit {
		bm.metrics.RecordPeerHit()
	}
}

// recordObservation appends one backhaul attempt to the ring, accumulates
// the byte total, and backfills the edge_backhaul_bytes_total counter and
// edge_backhaul_bandwidth_bytes gauge (on-stats-change update, no ticker).
// Nil-metrics safe.
func (bm *BackhaulManager) recordObservation(o observation) {
	bm.bytesTotal.Add(o.bytes)

	bm.obsMu.Lock()
	bm.wins = append(bm.wins, o)
	if len(bm.wins) > backhaulObservationCapacity {
		drop := len(bm.wins) - backhaulObservationCapacity/2
		copy(bm.wins, bm.wins[drop:])
		bm.wins = bm.wins[:len(bm.wins)-drop]
	}
	util := bm.utilizationLocked(o.ts)
	bm.obsMu.Unlock()

	if bm.metrics == nil {
		return
	}
	if o.bytes > 0 {
		bm.metrics.RecordBackhaulBytes(o.bytes)
	}
	bm.metrics.SetBackhaulBandwidth(int64(util))
}

// Stats24h returns the success rate and p95 latency over the trailing 24h
// window ending at now, plus the cumulative backhaul byte total. p95 is the
// nearest-rank of the sorted window latencies — the sample is bounded by
// backhaulObservationCapacity (10k), so the sort stays cheap. Zero
// observations in the window yield successRate 0 (not NaN).
func (bm *BackhaulManager) Stats24h(now time.Time) (successRate float64, p95Ms int64, totalBytes int64) {
	cutoff := now.Add(-backhaulStatsWindow)

	bm.obsMu.Lock()
	var total, successes int64
	lat := make([]int64, 0, len(bm.wins))
	for _, o := range bm.wins {
		if o.ts.Before(cutoff) {
			continue
		}
		total++
		if o.success {
			successes++
		}
		lat = append(lat, o.latencyMs)
	}
	bm.obsMu.Unlock()

	if total == 0 {
		return 0, 0, bm.bytesTotal.Load()
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	rank := (95*len(lat) + 99) / 100 // ceil(0.95 * n)
	return float64(successes) / float64(total), lat[rank-1], bm.bytesTotal.Load()
}

// BackhaulUtilization returns the local-backhaul throughput in bytes/sec,
// averaged over the trailing 60s window.
func (bm *BackhaulManager) BackhaulUtilization() float64 {
	bm.obsMu.Lock()
	defer bm.obsMu.Unlock()
	return bm.utilizationLocked(time.Now())
}

func (bm *BackhaulManager) utilizationLocked(now time.Time) float64 {
	cutoff := now.Add(-backhaulUtilizationWindow)
	var sum int64
	for _, o := range bm.wins {
		if o.ts.Before(cutoff) {
			continue
		}
		sum += o.bytes
	}
	return float64(sum) / backhaulUtilizationWindow.Seconds()
}

// SetBackhaulCapacityMbps publishes the configured backhaul capacity to the
// edge_backhaul_capacity_bytes gauge (mbps * 1e6 / 8 bytes/sec — the gauge
// is bytes-based; the UI contract's *_bps fields multiply by 8 downstream).
// mbps <= 0 means "capacity unknown": the gauge is left unreported. Wire
// from cfg.Access.DataPlane.BackhaulCapacityMbps at node startup.
func (bm *BackhaulManager) SetBackhaulCapacityMbps(mbps int) {
	if mbps <= 0 || bm.metrics == nil {
		return
	}
	bm.metrics.SetBackhaulCapacity(int64(mbps) * 1_000_000 / 8)
}
