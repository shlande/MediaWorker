package cache

import (
	"testing"
)

// TestWarmCache_Usage_TracksPuts verifies Usage reports used bytes consistent
// with manual Put calls and total equal to the configured max size.
func TestWarmCache_Usage_TracksPuts(t *testing.T) {
	// Given: an empty warm cache with a 1 MiB cap
	root := t.TempDir()
	wc := NewWarmCache(root, 1<<20, NewMemoryIndex(), nil, nil)

	// When: nothing has been written
	// Then: used is zero and total is the cap
	used, total := wc.Usage()
	if used != 0 {
		t.Fatalf("empty cache used: want 0, got %d", used)
	}
	if total != 1<<20 {
		t.Fatalf("total: want %d, got %d", int64(1<<20), total)
	}

	// When: two blobs are Put
	if err := wc.Put("blob-a", []byte("aaaa"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := wc.Put("blob-b", []byte("bbbbbb"), 1000); err != nil {
		t.Fatal(err)
	}

	// Then: used equals the sum of Put sizes; total unchanged
	used, total = wc.Usage()
	if want := int64(4 + 6); used != want {
		t.Fatalf("used after Puts: want %d, got %d", want, used)
	}
	if total != 1<<20 {
		t.Fatalf("total after Puts: want %d, got %d", int64(1<<20), total)
	}
}

// TestWarmCache_Usage_ReflectsEviction verifies used bytes shrink when Put
// triggers eviction to make room.
func TestWarmCache_Usage_ReflectsEviction(t *testing.T) {
	// Given: a warm cache capped at 10 bytes holding one 8-byte blob
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 10, mi, nil, nil)
	if err := wc.Put("old", []byte("12345678"), 1000); err != nil {
		t.Fatal(err)
	}
	// PopSource exposing the old blob so Evict has a candidate.
	wc.SetPopSource(func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments:   []*SegmentMeta{{BlobHash: "old", Bitrate: 1000, Size: 8}},
		}}
	})

	// When: a new 6-byte blob forces eviction of the old one
	if err := wc.Put("new", []byte("abcdef"), 1000); err != nil {
		t.Fatal(err)
	}

	// Then: used reflects only the surviving blob
	used, _ := wc.Usage()
	if used != 6 {
		t.Fatalf("used after eviction: want 6, got %d", used)
	}
}

// TestWarmCache_Count_CountsWarmEntries verifies Count tracks Put/eviction and
// ignores index entries whose Location is not "warm".
func TestWarmCache_Count_CountsWarmEntries(t *testing.T) {
	// Given: a warm cache plus a foreign (non-warm) entry in the shared index
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)
	mi.Put("foreign", &IndexEntry{Location: "prefix", Size: 1})

	if got := wc.Count(); got != 0 {
		t.Fatalf("empty cache count: want 0, got %d", got)
	}

	// When: two blobs are Put
	if err := wc.Put("blob-a", []byte("aaaa"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := wc.Put("blob-b", []byte("bbbbbb"), 1000); err != nil {
		t.Fatal(err)
	}

	// Then: Count is 2 (the foreign entry is not a warm entry)
	if got := wc.Count(); got != 2 {
		t.Fatalf("count after Puts: want 2, got %d", got)
	}
}
