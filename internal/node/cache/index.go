package cache

import (
	"errors"
	"sync"
	"time"
)

// IndexEntry represents a blob's cached state in the memory index (L1).
// It tracks location, access pattern, and latency to inform eviction decisions.
type IndexEntry struct {
	Location           string // "prefix" | "warm" | "cold"
	Size               int64
	LRUStamp           time.Time
	PopScore           float64
	LastFetchLatencyMs int64
	Bitrate            int  // bps, used for eviction sorting in WarmCache
	IsPrefix           bool // pinned by PinStore; skip in eviction
}

// ErrNotFound is returned when a blob hash is not in the index.
var ErrNotFound = errors.New("cache index: blob not found")

// MemoryIndex is the L1 in-memory index mapping blob_hash to IndexEntry.
// Safe for concurrent use.
type MemoryIndex struct {
	mu sync.RWMutex
	m  map[string]*IndexEntry
}

// NewMemoryIndex creates a new empty MemoryIndex.
func NewMemoryIndex() *MemoryIndex {
	return &MemoryIndex{
		m: make(map[string]*IndexEntry),
	}
}

// Get retrieves an entry by blob hash.
func (mi *MemoryIndex) Get(blobHash string) (*IndexEntry, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	e, ok := mi.m[blobHash]
	return e, ok
}

// Put inserts or replaces an entry for the given blob hash.
func (mi *MemoryIndex) Put(blobHash string, entry *IndexEntry) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.m[blobHash] = entry
}

// Delete removes an entry from the index.
func (mi *MemoryIndex) Delete(blobHash string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	delete(mi.m, blobHash)
}

// Has reports whether the blob hash exists in the index.
func (mi *MemoryIndex) Has(blobHash string) bool {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	_, ok := mi.m[blobHash]
	return ok
}

// UpdateLatency updates the LastFetchLatencyMs for a blob hash.
// No-op if the blob is not in the index.
func (mi *MemoryIndex) UpdateLatency(blobHash string, latencyMs int64) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	if e, ok := mi.m[blobHash]; ok {
		e.LastFetchLatencyMs = latencyMs
	}
}

// All returns a snapshot of all entries (used by eviction logic).
func (mi *MemoryIndex) All() map[string]*IndexEntry {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	cp := make(map[string]*IndexEntry, len(mi.m))
	for k, v := range mi.m {
		cp[k] = v
	}
	return cp
}
