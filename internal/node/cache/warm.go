package cache

import (
	"fmt"
	"os"
	"path/filepath"
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
type WarmCache struct {
	mu         sync.RWMutex
	rootPath   string
	maxSize    int64
	usedSize   int64
	index      *MemoryIndex
	pinChecker PinChecker
	popSource  PopSource
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

// Put writes data to disk and creates an index entry. If the cache is full,
// it triggers Evict first to make room.
func (wc *WarmCache) Put(blobHash string, data []byte, bitrate int) error {
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
