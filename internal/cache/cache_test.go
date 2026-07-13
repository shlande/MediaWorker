package cache

import (
	"os"
	"path/filepath"
	"testing"
)

// ─── Test: MemoryIndex Put/Get/Delete ───

func TestMemoryIndex_PutGet(t *testing.T) {
	mi := NewMemoryIndex()

	entry := &IndexEntry{Location: "warm", Size: 1024}
	mi.Put("abc123", entry)

	got, ok := mi.Get("abc123")
	if !ok {
		t.Fatal("expected entry to exist after Put")
	}
	if got.Location != "warm" || got.Size != 1024 {
		t.Fatalf("expected Location=warm Size=1024, got Location=%s Size=%d", got.Location, got.Size)
	}

	mi.Delete("abc123")
	_, ok = mi.Get("abc123")
	if ok {
		t.Fatal("expected entry to be gone after Delete")
	}
}

// ─── Test: PrefixCache Put/Get/Has ───

func TestPrefixCache_PutGet(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	pc := NewPrefixCache(root, 1<<20 /* 1 MiB */, mi)

	data := []byte("hello prefix cache")
	if err := pc.Put("blob1", data); err != nil {
		t.Fatal(err)
	}

	if !pc.Has("blob1") {
		t.Fatal("Has returned false for existing blob")
	}

	got, ok := pc.Get("blob1")
	if !ok {
		t.Fatal("Get returned false for existing blob")
	}
	if string(got) != string(data) {
		t.Fatalf("data mismatch: got %q, want %q", string(got), string(data))
	}

	entry, ok := mi.Get("blob1")
	if !ok {
		t.Fatal("index missing entry after Put")
	}
	if entry.Location != "prefix" || !entry.IsPrefix {
		t.Fatalf("expected Location=prefix IsPrefix=true, got Location=%s IsPrefix=%v", entry.Location, entry.IsPrefix)
	}
}

// ─── Test: PrefixCache data is never evicted ───

func TestPrefixCache_NoEvict(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	pc := NewPrefixCache(root, 1<<20, mi)

	if err := pc.Put("prefix1", []byte("pinned data")); err != nil {
		t.Fatal(err)
	}

	entry, _ := mi.Get("prefix1")
	if !entry.IsPrefix {
		t.Fatal("expected IsPrefix=true for prefix cache entry")
	}

	// PinChecker marks "prefix1" as pinned; Evict should return ErrCacheFull.
	checker := func(hash string) bool { return hash == "prefix1" }
	ps := func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v1",
			Popularity: 0.1,
			Segments:   []*SegmentMeta{{BlobHash: "prefix1", Bitrate: 5000, Size: 100}},
		}}
	}
	_, err := Evict(checker, ps, mi)
	if err != ErrCacheFull {
		t.Fatalf("expected ErrCacheFull because prefix1 is pinned, got err=%v", err)
	}
}

// ─── Test: WarmCache Put/Get ───

func TestWarmCache_PutGet(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	data := []byte("hello warm cache")
	if err := wc.Put("blob2", data, 3000); err != nil {
		t.Fatal(err)
	}

	got, ok := wc.Get("blob2")
	if !ok {
		t.Fatal("Get returned false for existing blob")
	}
	if string(got) != string(data) {
		t.Fatalf("data mismatch: got %q, want %q", string(got), string(data))
	}

	entry, ok := mi.Get("blob2")
	if !ok {
		t.Fatal("index missing entry after Put")
	}
	if entry.Location != "warm" || entry.Bitrate != 3000 {
		t.Fatalf("expected Location=warm Bitrate=3000, got Location=%s Bitrate=%d", entry.Location, entry.Bitrate)
	}
}

// ─── Test: WarmCache Put eviction integration ───
// Puts enough data via WarmCache.Put (not raw index insert) so usedSize triggers eviction.

func TestWarmCache_PutEvict_LowPopFirst(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()

	noPin := func(hash string) bool { return false }
	videos := func() []*VideoMeta {
		return []*VideoMeta{
			{BlobHash: "lowPop", Popularity: 1.0, Segments: []*SegmentMeta{{BlobHash: "lowPop_seg", Bitrate: 1000, Size: 15}}},
			{BlobHash: "midPop", Popularity: 50.0, Segments: []*SegmentMeta{{BlobHash: "midPop_seg", Bitrate: 2000, Size: 15}}},
			{BlobHash: "highPop", Popularity: 100.0, Segments: []*SegmentMeta{{BlobHash: "highPop_seg", Bitrate: 3000, Size: 15}}},
		}
	}
	// maxSize=50: three 15-byte entries (45 used) + 10 new = 55 → 1 evict (45→30+10=40 ≤ 50).
	wc := NewWarmCache(root, 50, mi, noPin, videos)

	data15 := []byte("123456789012345")
	if err := wc.Put("lowPop_seg", data15, 1000); err != nil {
		t.Fatal(err)
	}
	if err := wc.Put("midPop_seg", data15, 2000); err != nil {
		t.Fatal(err)
	}
	if err := wc.Put("highPop_seg", data15, 3000); err != nil {
		t.Fatal(err)
	}

	if err := wc.Put("newSeg", []byte("1234567890"), 500); err != nil {
		t.Fatal(err)
	}

	if mi.Has("lowPop_seg") {
		t.Fatal("expected lowest-popularity segment lowPop_seg to be evicted")
	}
	if !mi.Has("midPop_seg") {
		t.Fatal("expected midPop_seg to survive eviction")
	}
	if !mi.Has("highPop_seg") {
		t.Fatal("expected highPop_seg to survive eviction")
	}
}

// ─── Test: Evict selects highest bitrate first within same video ───

func TestEvict_HighBitrateFirst(t *testing.T) {
	mi := NewMemoryIndex()

	noPin := func(hash string) bool { return false }
	videos := func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 10.0,
			Segments: []*SegmentMeta{
				{BlobHash: "lowBr", Bitrate: 1000, Size: 1},
				{BlobHash: "midBr", Bitrate: 5000, Size: 1},
				{BlobHash: "highBr", Bitrate: 10000, Size: 1},
			},
		}}
	}

	mi.Put("lowBr", &IndexEntry{Location: "warm", Size: 1, Bitrate: 1000})
	mi.Put("midBr", &IndexEntry{Location: "warm", Size: 1, Bitrate: 5000})
	mi.Put("highBr", &IndexEntry{Location: "warm", Size: 1, Bitrate: 10000})

	seg, err := Evict(noPin, videos, mi)
	if err != nil {
		t.Fatal(err)
	}
	if seg.BlobHash != "highBr" {
		t.Fatalf("expected highest-bitrate segment highBr to be evicted first, got %s", seg.BlobHash)
	}
}

// ─── Test: Evict skips pinned segments ───

func TestEvict_SkipPinned(t *testing.T) {
	mi := NewMemoryIndex()

	pinChecker := func(hash string) bool { return hash == "pinnedSeg" }
	videos := func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments: []*SegmentMeta{
				{BlobHash: "pinnedSeg", Bitrate: 8000, Size: 1},
			},
		}}
	}
	mi.Put("pinnedSeg", &IndexEntry{Location: "warm", Size: 1, Bitrate: 8000})

	_, err := Evict(pinChecker, videos, mi)
	if err != ErrCacheFull {
		t.Fatalf("expected ErrCacheFull because pinnedSeg is pinned, got err=%v", err)
	}
	if !mi.Has("pinnedSeg") {
		t.Fatal("pinnedSeg should not have been evicted")
	}
}

// ─── Test: Evict skips high-latency segments ───

func TestEvict_HighLatencyProtection(t *testing.T) {
	mi := NewMemoryIndex()

	noPin := func(hash string) bool { return false }
	videos := func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments: []*SegmentMeta{
				{BlobHash: "highLat", Bitrate: 8000, Size: 1, LastFetchLatencyMs: 500},
				{BlobHash: "normal", Bitrate: 8000, Size: 1, LastFetchLatencyMs: 10},
			},
		}}
	}
	mi.Put("highLat", &IndexEntry{Location: "warm", Size: 1, Bitrate: 8000})
	mi.Put("normal", &IndexEntry{Location: "warm", Size: 1, Bitrate: 8000})

	seg, err := Evict(noPin, videos, mi)
	if err != nil {
		t.Fatal(err)
	}
	// normal (10ms latency, <= 200ms threshold) should be evicted; highLat (500ms) skipped.
	if seg.BlobHash != "normal" {
		t.Fatalf("expected normal-latency segment 'normal' to be evicted, got %s", seg.BlobHash)
	}
	if !mi.Has("highLat") {
		t.Fatal("expected high-latency segment 'highLat' to survive")
	}
}

// ─── Test: Evict fallback when all segments are high-latency ───

func TestEvict_FallbackAllHighLatency(t *testing.T) {
	mi := NewMemoryIndex()

	noPin := func(hash string) bool { return false }
	videos := func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments: []*SegmentMeta{
				{BlobHash: "h500", Bitrate: 5000, Size: 1, LastFetchLatencyMs: 500},
				{BlobHash: "h300", Bitrate: 5000, Size: 1, LastFetchLatencyMs: 300},
			},
		}}
	}
	mi.Put("h300", &IndexEntry{Location: "warm", Size: 1, Bitrate: 5000})
	mi.Put("h500", &IndexEntry{Location: "warm", Size: 1, Bitrate: 5000})

	// All segments are high-latency. Fallback should evict the one with lowest latency (h300 with 300ms).
	seg, err := Evict(noPin, videos, mi)
	if err != nil {
		t.Fatal(err)
	}
	if seg.BlobHash != "h300" {
		t.Fatalf("expected h300 (300ms) to be evicted as fallback, got %s", seg.BlobHash)
	}
	if !mi.Has("h500") {
		t.Fatal("expected h500 (500ms) to survive")
	}
}

// ─── Test: MemoryIndex UpdateLatency ───

func TestMemoryIndex_UpdateLatency(t *testing.T) {
	mi := NewMemoryIndex()

	entry := &IndexEntry{Location: "warm", Size: 1024}
	mi.Put("blobX", entry)

	mi.UpdateLatency("blobX", 350)
	got, ok := mi.Get("blobX")
	if !ok {
		t.Fatal("entry missing after UpdateLatency")
	}
	if got.LastFetchLatencyMs != 350 {
		t.Fatalf("expected LastFetchLatencyMs=350, got %d", got.LastFetchLatencyMs)
	}

	// UpdateLatency on non-existent key should not panic.
	mi.UpdateLatency("nonexistent", 100)
}

// ─── Test: WarmCache Put returns ErrCacheFull when nothing evictable ───

func TestWarmCache_ErrCacheFull(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()

	// All segments are pinned.
	allPinned := func(hash string) bool { return true }
	videos := func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments:   []*SegmentMeta{{BlobHash: "pinnedSeg", Bitrate: 1000, Size: 10}},
		}}
	}
	wc := NewWarmCache(root, 10, mi, allPinned, videos)

	// Put pinnedSeg via cache so usedSize advances.
	if err := wc.Put("pinnedSeg", []byte("aaaaaaaaaa"), 1000); err != nil {
		t.Fatal(err)
	}

	// Now cached is full (10/10), next Put triggers eviction but everything is pinned.
	err := wc.Put("bigBlob", []byte("bbbbbbbbbb"), 500)
	if err != ErrCacheFull {
		t.Fatalf("expected ErrCacheFull, got %v", err)
	}
}

// ─── Test: ColdCache Put/Get with simple LRU eviction ───

func TestColdCache_PutGet_Evict(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	cc := NewColdCache(root, 512, mi)

	data1 := make([]byte, 200)
	data2 := make([]byte, 200)
	data3 := make([]byte, 200)

	if err := cc.Put("c1", data1); err != nil {
		t.Fatal(err)
	}
	if err := cc.Put("c2", data2); err != nil {
		t.Fatal(err)
	}

	// Touch c1 to make c2 the LRU
	if _, ok := cc.Get("c1"); !ok {
		t.Fatal("Get c1 failed")
	}

	// Third blob forces eviction of c2 (the oldest LRU)
	if err := cc.Put("c3", data3); err != nil {
		t.Fatal(err)
	}

	if mi.Has("c2") {
		t.Fatal("expected c2 (oldest LRU) to be evicted")
	}
	if !mi.Has("c1") {
		t.Fatal("expected c1 to survive (was touched)")
	}
	if !mi.Has("c3") {
		t.Fatal("expected c3 to be present")
	}
}

// ─── Test: PrefixCache Remove and Has on missing ───

func TestPrefixCache_RemoveAndMissing(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	pc := NewPrefixCache(root, 1<<20, mi)

	if pc.Has("nonexistent") {
		t.Fatal("Has returned true for missing blob")
	}
	if _, ok := pc.Get("nonexistent"); ok {
		t.Fatal("Get returned true for missing blob")
	}

	if err := pc.Put("rm", []byte("remove me")); err != nil {
		t.Fatal(err)
	}
	if err := pc.Remove("rm"); err != nil {
		t.Fatal(err)
	}
	if pc.Has("rm") {
		t.Fatal("Has returned true after Remove")
	}
}

// ─── Test: WarmCache Has and Get on missing ───

func TestWarmCache_HasAndMissing(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	if wc.Has("nonexistent") {
		t.Fatal("Has returned true for missing blob")
	}
	if _, ok := wc.Get("nonexistent"); ok {
		t.Fatal("Get returned true for missing blob")
	}
}

// ─── Test: PrefixCache Available() ───

func TestPrefixCache_Available(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	pc := NewPrefixCache(root, 1024, mi)

	if pc.Available() != 1024 {
		t.Fatalf("expected Available=1024, got %d", pc.Available())
	}

	data := make([]byte, 512)
	if err := pc.Put("a", data); err != nil {
		t.Fatal(err)
	}
	if pc.Available() != 512 {
		t.Fatalf("expected Available=512, got %d", pc.Available())
	}
}

// ─── Test: Cache disk persistence ───

func TestCache_DiskPersistence(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	pc := NewPrefixCache(root, 1<<20, mi)

	data := []byte("persist me")
	if err := pc.Put("persist", data); err != nil {
		t.Fatal(err)
	}

	diskPath := filepath.Join(root, "persist")
	if _, err := os.Stat(diskPath); os.IsNotExist(err) {
		t.Fatal("expected file to exist on disk")
	}

	diskData, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(diskData) != string(data) {
		t.Fatalf("disk data mismatch: got %q, want %q", string(diskData), string(data))
	}
}

// ─── Test: MemoryIndex Has ───

func TestMemoryIndex_Has(t *testing.T) {
	mi := NewMemoryIndex()

	if mi.Has("absent") {
		t.Fatal("Has returned true for missing key")
	}
	mi.Put("present", &IndexEntry{Location: "warm"})
	if !mi.Has("present") {
		t.Fatal("Has returned false for existing key")
	}
}

// ─── Test: MemoryIndex All returns independent snapshot ───

func TestMemoryIndex_All(t *testing.T) {
	mi := NewMemoryIndex()
	mi.Put("k1", &IndexEntry{Location: "warm", Size: 10})
	mi.Put("k2", &IndexEntry{Location: "cold", Size: 20})

	all := mi.All()
	if len(all) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(all))
	}

	// Snapshot must be independent.
	all["k3"] = &IndexEntry{Location: "prefix"}
	_, ok := mi.Get("k3")
	if ok {
		t.Fatal("snapshot mutation leaked into original map")
	}
}

// ─── Test: WarmCache nil injections are safe ───

func TestWarmCache_NilInjectionsSafe(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	data := []byte("hello")
	if err := wc.Put("seg", data, 1000); err != nil {
		t.Fatal(err)
	}
	got, ok := wc.Get("seg")
	if !ok {
		t.Fatal("Get returned false after Put with nil injections")
	}
	if string(got) != string(data) {
		t.Fatal("data mismatch with nil injections")
	}
}

// ─── Test: WarmCache Put with empty PopSource returns ErrCacheFull ───

func TestWarmCache_Put_EmptyPopSource(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	alwaysEmpty := func() []*VideoMeta { return nil }
	wc := NewWarmCache(root, 10, mi, nil, alwaysEmpty)

	// Fill cache via Put so usedSize=5.
	if err := wc.Put("seg1", []byte("hello"), 1000); err != nil {
		t.Fatal(err)
	}

	// Next Put of 10 bytes exceeds maxSize=10 (5+10=15 > 10).
	// Eviction called but popSource empty → ErrCacheFull.
	err := wc.Put("seg2", []byte("world_more"), 500)
	if err != ErrCacheFull {
		t.Fatalf("expected ErrCacheFull with empty popSource, got %v", err)
	}
}
