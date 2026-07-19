// Package linkpool implements a download-link LRU cache with pre-expiry background refresh.
//
// LinkPool caches *types.DownloadLink returned by driver.Driver.GetLink() so that hot-path
// callers (SelectForRead in accountpool) do not call GetLink() on every segment fetch.
//
// Design:
//   - LRU eviction with configurable capacity (default 10,000 entries).
//   - Background refresh triggered when a cached link enters the 5-minute-before-expiry
//     window. Only one goroutine per key performs the refresh (atomic.CompareAndSwap on
//     CachedLink.Refreshing).
//   - Synchronous fetch+re-cache on cache miss.
//   - Takes driver.Driver (not *Account) to avoid import cycle with accountpool.
package linkpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

// defaultCapacity is the default LRU cache size. 10,000 entries corresponds to roughly
// 10,000 recently accessed segments; the expected hit rate is > 95 %.
const defaultCapacity = 10000

// CachedLink wraps a *types.DownloadLink with caching metadata.
type CachedLink struct {
	Link *types.DownloadLink

	// CachedAt is when this entry was inserted into the cache.
	CachedAt time.Time
	// ExpireAt is the link's expiration time (from DownloadLink.ExpireAt).
	ExpireAt time.Time

	// Refreshing is CAS-guarded: true when a background refresh goroutine is running.
	Refreshing atomic.Bool
}

// key returns the cache key for a vendor / account-ID / file-ID tuple.
func key(vendor types.Vendor, accountID, fileID string) string {
	return fmt.Sprintf("%s:%s:%s", vendor, accountID, fileID)
}

// LinkPool is a concurrency-safe LRU cache for download links.
//
// Zero value is NOT usable — call NewLinkPool.
type LinkPool struct {
	mu    sync.Mutex
	cache *lru.Cache[string, *CachedLink]

	// requests counts every GetOrFetch call; hits counts calls served from a
	// fresh cached entry. Atomics (not mu-guarded) so HitRate reads never
	// contend with the hot GetOrFetch path. storage/monitor counters are not
	// readable back, so the pool keeps its own counters for the admin API.
	requests atomic.Int64
	hits     atomic.Int64
}

// NewLinkPool creates a LinkPool with the given maximum entries.
// If maxEntries <= 0, defaultCapacity (10,000) is used.
func NewLinkPool(maxEntries int) *LinkPool {
	if maxEntries <= 0 {
		maxEntries = defaultCapacity
	}
	c, err := lru.New[string, *CachedLink](maxEntries)
	if err != nil {
		// lru.New only errors when maxEntries <= 0; we already guard above.
		panic(fmt.Sprintf("linkpool: lru.New: %v", err))
	}
	return &LinkPool{cache: c}
}

// refreshWindowStart is the time before ExpireAt at which we start considering a link
// "close to expiry". When time.Now() is after this point, we attempt a background refresh.
const refreshWindowStart = 5 * time.Minute

// staleHardLimit is the time before ExpireAt at which the cached link is considered
// expired and a synchronous fetch is forced.
const staleHardLimit = 2 * time.Minute

// GetOrFetch returns a cached download link for the given fileID, or fetches one via
// driver.GetLink on cache miss / expiration.
//
// On cache hit with plenty of remaining time (now < ExpireAt - 2min), the cached link is
// returned immediately. If the link is within the 5-minute refresh window and no other
// goroutine is already refreshing, a background fetch is triggered via CAS on
// CachedLink.Refreshing.
//
// On cache miss or expired cache, a synchronous fetch is performed and the result is
// stored in the cache.
func (lp *LinkPool) GetOrFetch(
	ctx context.Context,
	d driver.Driver,
	vendor types.Vendor,
	accountID, fileID string,
) (*types.DownloadLink, error) {
	k := key(vendor, accountID, fileID)

	lp.requests.Add(1)

	lp.mu.Lock()
	cached, ok := lp.cache.Get(k)
	lp.mu.Unlock()

	if ok {
		now := time.Now()
		staleCutoff := cached.ExpireAt.Add(-staleHardLimit)

		if now.Before(staleCutoff) {
			lp.hits.Add(1)
			// Link has plenty of remaining lifetime.
			// If it is within the refresh window, try a background refresh.
			refreshCutoff := cached.ExpireAt.Add(-refreshWindowStart)
			if now.After(refreshCutoff) && cached.Refreshing.CompareAndSwap(false, true) {
				go lp.refreshLink(ctx, k, d, fileID)
			}
			return cached.Link, nil
		}
		// Link is too close to expiry (or already expired) — fall through to
		// synchronous fetch.
	}

	return lp.fetchAndCache(ctx, k, d, fileID)
}

// fetchAndCache calls driver.GetLink, stores the result, and returns it.
func (lp *LinkPool) fetchAndCache(
	ctx context.Context,
	key string,
	d driver.Driver,
	fileID string,
) (*types.DownloadLink, error) {
	link, err := d.GetLink(ctx, fileID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	cl := &CachedLink{
		Link:     link,
		CachedAt: now,
		ExpireAt: link.ExpireAt,
	}

	lp.mu.Lock()
	lp.cache.Add(key, cl)
	lp.mu.Unlock()

	return link, nil
}

// refreshLink is the background refresh goroutine. It must be launched only after a
// successful CAS on CachedLink.Refreshing (true → this goroutine owns the refresh).
func (lp *LinkPool) refreshLink(
	ctx context.Context,
	key string,
	d driver.Driver,
	fileID string,
) {
	// Best-effort: log errors but never panic or crash the caller.
	link, err := d.GetLink(ctx, fileID)
	if err != nil {
		// Refresh failed — clear the Refreshing flag so the next caller can retry.
		lp.mu.Lock()
		if cached, ok := lp.cache.Get(key); ok {
			cached.Refreshing.Store(false)
		}
		lp.mu.Unlock()
		return
	}

	now := time.Now()
	cl := &CachedLink{
		Link:     link,
		CachedAt: now,
		ExpireAt: link.ExpireAt,
	}
	// Refreshing is intentionally NOT copied from the old entry — the new entry starts
	// with Refreshing == false (default), so the next refresh window is uncontested.

	lp.mu.Lock()
	lp.cache.Add(key, cl)
	lp.mu.Unlock()
}

// Len returns the current number of entries in the cache.
func (lp *LinkPool) Len() int {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	return lp.cache.Len()
}

// HitRate returns hits/requests over the pool's lifetime. Zero requests yield
// 0 (never NaN — the value is JSON-marshalled by the admin API).
func (lp *LinkPool) HitRate() float64 {
	reqs := lp.requests.Load()
	if reqs == 0 {
		return 0
	}
	return float64(lp.hits.Load()) / float64(reqs)
}
