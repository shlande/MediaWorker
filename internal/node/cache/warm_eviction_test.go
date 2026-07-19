package cache

import (
	"testing"
	"time"
)

// TestWarmCache_Evictions1h_Zero verifies Evictions1h returns 0 on a fresh cache.
func TestWarmCache_Evictions1h_Zero(t *testing.T) {
	root := t.TempDir()
	wc := NewWarmCache(root, 1<<20, NewMemoryIndex(), nil, nil)

	if got := wc.Evictions1h(); got != 0 {
		t.Fatalf("Evictions1h on fresh cache: want 0, got %d", got)
	}
}

// TestWarmCache_Evictions1h_AfterEvict verifies Evictions1h increments after Put
// triggers eviction.
func TestWarmCache_Evictions1h_AfterEvict(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 10, mi, nil, nil)
	if err := wc.Put("old", []byte("12345678"), 1000); err != nil {
		t.Fatal(err)
	}
	wc.SetPopSource(func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments:   []*SegmentMeta{{BlobHash: "old", Bitrate: 1000, Size: 8}},
		}}
	})

	if err := wc.Put("new", []byte("abcdef"), 1000); err != nil {
		t.Fatal(err)
	}

	if got := wc.Evictions1h(); got != 1 {
		t.Fatalf("Evictions1h after single eviction: want 1, got %d", got)
	}
}

// TestWarmCache_Evictions1h_SlidingWindow verifies expired timestamps are pruned.
func TestWarmCache_Evictions1h_SlidingWindow(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	// Cache capped at 10 bytes so each large Put forces an eviction.
	wc := NewWarmCache(root, 10, mi, nil, nil)

	// Seed with a blob that eviction can target.
	if err := wc.Put("seed", []byte("12345678"), 1000); err != nil {
		t.Fatal(err)
	}
	wc.SetPopSource(func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments:   []*SegmentMeta{{BlobHash: "seed", Bitrate: 1000, Size: 8}},
		}}
	})

	// Inject an old eviction timestamp outside the 1h window.
	oldTs := time.Now().Add(-2 * time.Hour)
	wc.evictMu.Lock()
	wc.evictTimestamps = append(wc.evictTimestamps, oldTs)
	wc.evictMu.Unlock()

	// Force a fresh eviction that lands inside the window.
	if err := wc.Put("fresh", []byte("0123456789"), 1000); err != nil {
		t.Fatal(err)
	}

	// Only the fresh eviction counts; oldTs is pruned.
	if got := wc.Evictions1h(); got != 1 {
		t.Fatalf("Evictions1h after sliding-window prune: want 1, got %d", got)
	}
}
