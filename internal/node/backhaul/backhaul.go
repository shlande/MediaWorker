package backhaul

import (
	"context"

	"github.com/shlande/mediaworker/internal/node/monitor"
	"golang.org/x/sync/singleflight"
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
func (bm *BackhaulManager) recordCacheRequest(hit bool) {
	if bm.metrics == nil {
		return
	}
	bm.metrics.RecordCacheRequest("warm")
	if hit {
		bm.metrics.RecordCacheHit("warm")
	}
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

func (bm *BackhaulManager) BackhaulUtilization() float64 {
	return 0
}
