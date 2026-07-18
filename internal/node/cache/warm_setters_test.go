package cache

import (
	"sync"
	"testing"
)

// TestWarmCache_SetPinChecker_ReplacesDefault verifies that SetPinChecker
// atomically replaces the always-false default so pinned blobs survive
// eviction.
func TestWarmCache_SetPinChecker_ReplacesDefault(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	wc.SetPinChecker(func(hash string) bool { return hash == "pinned" })

	videos := func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments:   []*SegmentMeta{{BlobHash: "pinned", Bitrate: 5000, Size: 1}},
		}}
	}
	wc.SetPopSource(videos)
	mi.Put("pinned", &IndexEntry{Location: "warm", Size: 1, Bitrate: 5000})

	_, err := Evict(wc.pinChecker, wc.popSource, mi)
	if err != ErrCacheFull {
		t.Fatalf("expected ErrCacheFull because pinned is protected, got %v", err)
	}
}

// TestWarmCache_SetPopSource_ReplacesDefault verifies that SetPopSource
// atomically replaces the always-empty default so Evict can find candidates.
func TestWarmCache_SetPopSource_ReplacesDefault(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	called := false
	wc.SetPopSource(func() []*VideoMeta {
		called = true
		return nil
	})

	_, _ = Evict(wc.pinChecker, wc.popSource, mi)
	if !called {
		t.Fatal("expected SetPopSource function to be invoked by Evict")
	}
}

// TestWarmCache_Setters_ConcurrentSafety runs SetPinChecker and SetPopSource
// concurrently with Put to verify the mutex prevents data races. Run with
// -race to catch any unsynchronized access.
func TestWarmCache_Setters_ConcurrentSafety(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	// Seed the index so Get/Has have something to read.
	if err := wc.Put("seed", []byte("seed-data"), 1000); err != nil {
		t.Fatal(err)
	}

	var done sync.WaitGroup
	stop := make(chan struct{})

	// Writer goroutine A: continuously swap PinChecker.
	done.Add(1)
	go func() {
		defer done.Done()
		for {
			select {
			case <-stop:
				return
			default:
				wc.SetPinChecker(func(hash string) bool { return false })
				wc.SetPinChecker(func(hash string) bool { return hash == "x" })
				wc.SetPinChecker(nil)
			}
		}
	}()

	// Writer goroutine B: continuously swap PopSource.
	done.Add(1)
	go func() {
		defer done.Done()
		for {
			select {
			case <-stop:
				return
			default:
				wc.SetPopSource(func() []*VideoMeta { return nil })
				wc.SetPopSource(func() []*VideoMeta {
					return []*VideoMeta{{BlobHash: "x", Popularity: 1, Segments: []*SegmentMeta{{BlobHash: "x"}}}}
				})
				wc.SetPopSource(nil)
			}
		}
	}()

	// Reader goroutine: continuously Put + Get (Put reads pinChecker/popSource
	// under RLock via the Evict path).
	done.Add(1)
	go func() {
		defer done.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				_ = wc.Put("blob", []byte("d"), 1000)
				_, _ = wc.Get("seed")
				_ = wc.Has("seed")
				// Rotate the seed entry so Put keeps making room.
				if i%50 == 0 {
					mi.Delete("blob")
				}
			}
		}
	}()

	// Let it run briefly under -race.
	close(stop)
	done.Wait()
}

// TestWarmCache_SetPinChecker_NilInstallsDefault verifies that passing nil to
// SetPinChecker installs the always-false default rather than leaving a nil
// function pointer (which would panic when Evict calls it).
func TestWarmCache_SetPinChecker_NilInstallsDefault(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	wc.SetPinChecker(nil)

	mi.Put("seg", &IndexEntry{Location: "warm", Size: 1, Bitrate: 1000})
	wc.SetPopSource(func() []*VideoMeta {
		return []*VideoMeta{{
			BlobHash:   "v",
			Popularity: 1.0,
			Segments:   []*SegmentMeta{{BlobHash: "seg", Bitrate: 1000, Size: 1}},
		}}
	})

	seg, err := Evict(wc.pinChecker, wc.popSource, mi)
	if err != nil {
		t.Fatalf("expected successful evict, got %v", err)
	}
	if seg.BlobHash != "seg" {
		t.Fatalf("expected seg, got %s", seg.BlobHash)
	}
}

// TestWarmCache_SetPopSource_NilInstallsDefault verifies that passing nil to
// SetPopSource installs the always-empty default.
func TestWarmCache_SetPopSource_NilInstallsDefault(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	wc.SetPopSource(nil)

	_, err := Evict(wc.pinChecker, wc.popSource, mi)
	if err != ErrCacheFull {
		t.Fatalf("expected ErrCacheFull with nil→empty PopSource, got %v", err)
	}
}
