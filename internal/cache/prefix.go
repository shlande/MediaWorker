package cache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// PrefixCache is the L2 cache tier backed by NVMe storage.
// Blobs stored here are pinned (IsPrefix=true) and exempt from LRU eviction.
type PrefixCache struct {
	rootPath string
	maxSize  int64
	usedSize int64
	index    *MemoryIndex
}

// NewPrefixCache creates a PrefixCache rooted at the given path.
func NewPrefixCache(rootPath string, maxSize int64, index *MemoryIndex) *PrefixCache {
	return &PrefixCache{
		rootPath: rootPath,
		maxSize:  maxSize,
		index:    index,
	}
}

// Available returns the remaining bytes before the cache is full.
func (pc *PrefixCache) Available() int64 {
	return pc.maxSize - pc.usedSize
}

// blobPath returns the on-disk path for a blob hash.
func (pc *PrefixCache) blobPath(blobHash string) string {
	return filepath.Join(pc.rootPath, blobHash)
}

// Put writes data to disk and creates an index entry marked as prefix/pinned.
func (pc *PrefixCache) Put(blobHash string, data []byte) error {
	size := int64(len(data))
	if pc.usedSize+size > pc.maxSize {
		return fmt.Errorf("prefix cache: insufficient space (%d available, need %d)", pc.Available(), size)
	}

	path := pc.blobPath(blobHash)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("prefix cache: write %s: %w", blobHash, err)
	}

	entry := &IndexEntry{
		Location: "prefix",
		Size:     size,
		IsPrefix: true,
	}
	pc.index.Put(blobHash, entry)
	pc.usedSize += size
	return nil
}

// Get reads data from disk. Returns (nil, false) if not found.
func (pc *PrefixCache) Get(blobHash string) ([]byte, bool) {
	path := pc.blobPath(blobHash)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	return data, true
}

// Has reports whether the blob exists in the prefix cache.
func (pc *PrefixCache) Has(blobHash string) bool {
	entry, ok := pc.index.Get(blobHash)
	if !ok {
		return false
	}
	return entry.Location == "prefix"
}

// Remove deletes the blob from disk and index.
func (pc *PrefixCache) Remove(blobHash string) error {
	entry, ok := pc.index.Get(blobHash)
	if !ok || entry.Location != "prefix" {
		return errors.New("prefix cache: blob not found")
	}

	path := pc.blobPath(blobHash)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("prefix cache: remove %s: %w", blobHash, err)
	}

	pc.usedSize -= entry.Size
	pc.index.Delete(blobHash)
	return nil
}
