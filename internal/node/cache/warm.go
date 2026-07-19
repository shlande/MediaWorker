package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// WarmCache is the L3 cache tier backed by SSD storage.
// It uses content-aware LRU eviction with injected PinChecker and PopSource.
//
// The pinChecker and popSource fields are mutable so they can be wired after
// construction (the PinStore and GossipSub stack are typically built after the
// cache). All reads and writes of these fields are guarded by mu. The Evict
// call path in Put takes an RLock to snapshot the two functions atomically.
//
// evictMu guards evictTimestamps; it is separate from mu so the eviction
// counter can be read without contending on cache operations.
type WarmCache struct {
	mu              sync.RWMutex
	rootPath        string
	maxSize         int64
	usedSize        int64
	index           *MemoryIndex
	pinChecker      PinChecker
	popSource       PopSource
	evictMu         sync.Mutex
	evictTimestamps []time.Time

	// flushing is set true during Flush. When true, Put returns
	// ErrCacheFlushing and the caller should retry or 503 upstream.
	flushing bool
}

// NewWarmCache creates a WarmCache rooted at the given path. The root directory
// is created if it does not exist. pinChecker and popSource may be nil — safe
// defaults are installed; wire the real implementations post-construction via
// SetPinChecker / SetPopSource.
func NewWarmCache(
	rootPath string,
	maxSize int64,
	index *MemoryIndex,
	pinChecker PinChecker,
	popSource PopSource,
) *WarmCache {
	_ = os.MkdirAll(rootPath, 0o755)
	if pinChecker == nil {
		pinChecker = func(blobHash string) bool { return false }
	}
	if popSource == nil {
		popSource = func() []*VideoMeta { return nil }
	}
	return &WarmCache{
		rootPath:   rootPath,
		maxSize:    maxSize,
		index:      index,
		pinChecker: pinChecker,
		popSource:  popSource,
	}
}

// SetPinChecker atomically replaces the cache's PinChecker. Safe for concurrent
// use with Put/Get/Has and with SetPopSource. Passing nil installs the
// always-false default (nothing pinned).
func (wc *WarmCache) SetPinChecker(pc PinChecker) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if pc == nil {
		pc = func(blobHash string) bool { return false }
	}
	wc.pinChecker = pc
}

// SetPopSource atomically replaces the cache's PopSource. Safe for concurrent
// use with Put/Get/Has and with SetPinChecker. Passing nil installs the
// always-empty default (Evict will return ErrCacheFull on the next call).
func (wc *WarmCache) SetPopSource(ps PopSource) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if ps == nil {
		ps = func() []*VideoMeta { return nil }
	}
	wc.popSource = ps
}

// blobPath returns the on-disk path for a blob hash.
func (wc *WarmCache) blobPath(blobHash string) string {
	return filepath.Join(wc.rootPath, blobHash)
}

// UsedSize returns the current used size in bytes.
func (wc *WarmCache) UsedSize() int64 { return wc.usedSize }

// Usage returns the used and total capacity of the warm cache in bytes.
// Reads follow the same unsynchronized convention as UsedSize: usedSize is
// mutated by Put's eviction path outside wc.mu (pre-existing), so this is an
// approximate snapshot — accurate enough for periodic status reporting.
func (wc *WarmCache) Usage() (used, total int64) {
	return wc.usedSize, wc.maxSize
}

// Count returns the number of warm-cache entries in the index (entries whose
// Location is "warm"), matching the Has() semantics.
func (wc *WarmCache) Count() int {
	n := 0
	for _, e := range wc.index.All() {
		if e.Location == "warm" {
			n++
		}
	}
	return n
}

// evictionWindow bounds the sliding counter to 1 hour.
const evictionWindow = 1 * time.Hour

// recordEviction appends a timestamp to the sliding window and lazily prunes
// expired entries. Must be called under evictMu.
func (wc *WarmCache) recordEviction(now time.Time) {
	wc.evictMu.Lock()
	wc.evictTimestamps = append(wc.evictTimestamps, now)
	cutoff := now.Add(-evictionWindow)
	n := 0
	for _, ts := range wc.evictTimestamps {
		if !ts.Before(cutoff) {
			wc.evictTimestamps[n] = ts
			n++
		}
	}
	wc.evictTimestamps = wc.evictTimestamps[:n]
	wc.evictMu.Unlock()
}

// Evictions1h returns the number of warm-cache evictions in the last hour
// (lazy-pruned sliding window). Safe for concurrent use with Put/Get.
func (wc *WarmCache) Evictions1h() int {
	now := time.Now()
	cutoff := now.Add(-evictionWindow)

	wc.evictMu.Lock()
	// Prune expired entries before counting.
	n := 0
	for _, ts := range wc.evictTimestamps {
		if !ts.Before(cutoff) {
			wc.evictTimestamps[n] = ts
			n++
		}
	}
	wc.evictTimestamps = wc.evictTimestamps[:n]
	count := n
	wc.evictMu.Unlock()
	return count
}

// Put writes data to disk and creates an index entry. If the cache is full,
// it triggers Evict first to make room.
func (wc *WarmCache) Put(blobHash string, data []byte, bitrate int) error {
	wc.mu.RLock()
	flushing := wc.flushing
	wc.mu.RUnlock()
	if flushing {
		return ErrCacheFlushing
	}

	size := int64(len(data))

	// Make room via content-aware eviction if needed.
	for wc.usedSize+size > wc.maxSize {
		// Snapshot the injected funcs under RLock so a concurrent
		// SetPinChecker / SetPopSource cannot tear the read.
		wc.mu.RLock()
		pc := wc.pinChecker
		ps := wc.popSource
		wc.mu.RUnlock()

		seg, err := Evict(pc, ps, wc.index)
		if err != nil {
			return ErrCacheFull
		}
		// Remove the evicted segment from disk and index.
		path := wc.blobPath(seg.BlobHash)
		if e, ok := wc.index.Get(seg.BlobHash); ok {
			wc.usedSize -= e.Size
		}
		wc.index.Delete(seg.BlobHash)
		_ = os.Remove(path)
		wc.recordEviction(time.Now())
	}

	path := wc.blobPath(blobHash)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("warm cache: write %s: %w", blobHash, err)
	}

	entry := &IndexEntry{
		Location: "warm",
		Size:     size,
		LRUStamp: time.Now(),
		Bitrate:  bitrate,
	}
	wc.index.Put(blobHash, entry)
	wc.usedSize += size
	return nil
}

// Get reads data from disk and updates the LRU stamp.
func (wc *WarmCache) Get(blobHash string) ([]byte, bool) {
	entry, ok := wc.index.Get(blobHash)
	if !ok || entry.Location != "warm" {
		return nil, false
	}

	path := wc.blobPath(blobHash)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	// Touch the LRU stamp on access.
	entry.LRUStamp = time.Now()
	return data, true
}

// Has reports whether the blob exists in the warm cache.
func (wc *WarmCache) Has(blobHash string) bool {
	entry, ok := wc.index.Get(blobHash)
	if !ok {
		return false
	}
	return entry.Location == "warm"
}

// ErrCacheFlushing is returned by Put when the cache is being flushed.
var ErrCacheFlushing = fmt.Errorf("warm cache: flush in progress")

// Flush stops accepting new Put, removes all warm entries from the index and
// their on-disk files, recomputes usedSize by walking the cache directory, then
// resumes Put acceptance. Errors from individual os.Remove calls are collected
// and returned as a multi-error; the flush continues past individual file
// removal failures.
//
// Get calls during flush will miss (index entries are deleted first) — this is
// acceptable because flush is an admin-triggered maintenance operation.
func (wc *WarmCache) Flush(ctx context.Context) error {
	wc.mu.Lock()
	if wc.flushing {
		wc.mu.Unlock()
		return fmt.Errorf("warm cache: flush already in progress")
	}
	wc.flushing = true
	wc.mu.Unlock()

	defer func() {
		wc.mu.Lock()
		wc.flushing = false
		wc.mu.Unlock()
	}()

	// Collect all warm entries while holding the index lock.
	snapshot := wc.index.All()
	var toDelete []string
	for hash, entry := range snapshot {
		if entry.Location != "warm" {
			continue
		}
		toDelete = append(toDelete, hash)
	}

	// Delete from index first so Get returns false during flush.
	var errs []string
	for _, hash := range toDelete {
		wc.index.Delete(hash)
		path := wc.blobPath(hash)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Sprintf("remove %s: %v", hash, err))
		}
	}

	// Recompute usedSize by walking the cache directory. This is more
	// trustworthy than relying on per-Put/eviction decrements, especially
	// after a bulk delete where accounting drift is possible.
	usedSize := recalcUsedSize(wc.rootPath)
	wc.usedSize = usedSize

	if len(errs) > 0 {
		return fmt.Errorf("warm cache flush: %s", strings.Join(errs, "; "))
	}
	return nil
}

// recalcUsedSize walks rootPath and sums the size of all regular files.
func recalcUsedSize(rootPath string) int64 {
	var total int64
	entries, err := os.ReadDir(rootPath)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}
