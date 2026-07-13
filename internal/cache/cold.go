package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ColdCache is the L4 cache tier backed by HDD/MinIO storage.
// It uses simple timestamp-based LRU eviction (no content-aware logic).
type ColdCache struct {
	rootPath string
	maxSize  int64
	usedSize int64
	index    *MemoryIndex
}

// NewColdCache creates a ColdCache rooted at the given path.
func NewColdCache(rootPath string, maxSize int64, index *MemoryIndex) *ColdCache {
	return &ColdCache{
		rootPath: rootPath,
		maxSize:  maxSize,
		index:    index,
	}
}

// blobPath returns the on-disk path for a blob hash.
func (cc *ColdCache) blobPath(blobHash string) string {
	return filepath.Join(cc.rootPath, blobHash)
}

// evictSimpleLRU evicts the least-recently-used non-pinned segment from the cold cache.
func (cc *ColdCache) evictSimpleLRU() (string, error) {
	all := cc.index.All()
	if len(all) == 0 {
		return "", ErrCacheFull
	}

	// Collect cold entries.
	type candidate struct {
		hash  string
		entry *IndexEntry
	}
	var candidates []candidate
	for h, e := range all {
		if e.Location == "cold" && !e.IsPrefix {
			candidates = append(candidates, candidate{hash: h, entry: e})
		}
	}

	if len(candidates) == 0 {
		return "", ErrCacheFull
	}

	// Sort by LRUStamp ascending (oldest first).
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].entry.LRUStamp.Before(candidates[j].entry.LRUStamp)
	})

	return candidates[0].hash, nil
}

// Put writes data to disk and creates an index entry. Evicts via simple LRU if full.
func (cc *ColdCache) Put(blobHash string, data []byte) error {
	size := int64(len(data))

	for cc.usedSize+size > cc.maxSize {
		hash, err := cc.evictSimpleLRU()
		if err != nil {
			return ErrCacheFull
		}
		if e, ok := cc.index.Get(hash); ok {
			cc.usedSize -= e.Size
		}
		cc.index.Delete(hash)
		_ = os.Remove(cc.blobPath(hash))
	}

	path := cc.blobPath(blobHash)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("cold cache: write %s: %w", blobHash, err)
	}

	entry := &IndexEntry{
		Location: "cold",
		Size:     size,
		LRUStamp: time.Now(),
	}
	cc.index.Put(blobHash, entry)
	cc.usedSize += size
	return nil
}

// Get reads data from disk and updates the LRU stamp.
func (cc *ColdCache) Get(blobHash string) ([]byte, bool) {
	entry, ok := cc.index.Get(blobHash)
	if !ok || entry.Location != "cold" {
		return nil, false
	}

	path := cc.blobPath(blobHash)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	entry.LRUStamp = time.Now()
	return data, true
}

// Has reports whether the blob exists in the cold cache.
func (cc *ColdCache) Has(blobHash string) bool {
	entry, ok := cc.index.Get(blobHash)
	if !ok {
		return false
	}
	return entry.Location == "cold"
}
