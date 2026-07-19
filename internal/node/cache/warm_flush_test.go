package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestWarmCache_Flush_ClearsCache verifies that Put 3 files → Flush → Usage=0
// and on-disk files are gone.
func TestWarmCache_Flush_ClearsCache(t *testing.T) {
	root := t.TempDir()
	wc := NewWarmCache(root, 1<<20, NewMemoryIndex(), nil, nil)

	if err := wc.Put("a", []byte("aaaa"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := wc.Put("b", []byte("bbbbbb"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := wc.Put("c", []byte("cc"), 1000); err != nil {
		t.Fatal(err)
	}

	used, _ := wc.Usage()
	if used != 12 {
		t.Fatalf("used before flush: want 12, got %d", used)
	}

	if err := wc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	used, _ = wc.Usage()
	if used != 0 {
		t.Fatalf("used after flush: want 0, got %d", used)
	}

	if wc.Count() != 0 {
		t.Fatalf("Count after flush: want 0, got %d", wc.Count())
	}

	// On-disk files should be gone.
	for _, hash := range []string{"a", "b", "c"} {
		path := filepath.Join(root, hash)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("file %s should be removed after flush, stat err=%v", hash, err)
		}
	}
}

// TestWarmCache_Flush_EmptyCache verifies flushing an empty cache is a no-op.
func TestWarmCache_Flush_EmptyCache(t *testing.T) {
	root := t.TempDir()
	wc := NewWarmCache(root, 1<<20, NewMemoryIndex(), nil, nil)

	if err := wc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush on empty cache: %v", err)
	}

	used, _ := wc.Usage()
	if used != 0 {
		t.Fatalf("used after empty flush: want 0, got %d", used)
	}
}

// TestWarmCache_Flush_RejectsPutDuringFlush verifies Put returns ErrCacheFlushing
// while a flush is in progress.
func TestWarmCache_Flush_RejectsPutDuringFlush(t *testing.T) {
	root := t.TempDir()
	wc := NewWarmCache(root, 1<<20, NewMemoryIndex(), nil, nil)

	if err := wc.Put("a", []byte("aaaa"), 1000); err != nil {
		t.Fatal(err)
	}

	// Manually set flushing and verify Put is rejected.
	wc.mu.Lock()
	wc.flushing = true
	wc.mu.Unlock()

	err := wc.Put("b", []byte("bb"), 1000)
	if err != ErrCacheFlushing {
		t.Fatalf("Put during flush: want ErrCacheFlushing, got %v", err)
	}

	// Reset flushing — Put should work again.
	wc.mu.Lock()
	wc.flushing = false
	wc.mu.Unlock()

	if err := wc.Put("b", []byte("bb"), 1000); err != nil {
		t.Fatalf("Put after flush reset: %v", err)
	}
}

// TestWarmCache_Flush_DoesNotDeleteNonWarm verifies Flush only removes entries
// with Location="warm" — non-warm index entries survive.
func TestWarmCache_Flush_DoesNotDeleteNonWarm(t *testing.T) {
	root := t.TempDir()
	mi := NewMemoryIndex()
	wc := NewWarmCache(root, 1<<20, mi, nil, nil)

	if err := wc.Put("warm-blob", []byte("data"), 1000); err != nil {
		t.Fatal(err)
	}
	mi.Put("prefix-blob", &IndexEntry{Location: "prefix", Size: 10})

	if err := wc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if wc.Count() != 0 {
		t.Fatalf("warm count after flush: want 0, got %d", wc.Count())
	}

	// prefix entry survives.
	if _, ok := mi.Get("prefix-blob"); !ok {
		t.Fatal("prefix entry should survive flush")
	}
}

// TestWarmCache_Flush_RecalcUsedSize verifies usedSize is recomputed by
// directory walk, not by trusting per-entry decrements.
func TestWarmCache_Flush_RecalcUsedSize(t *testing.T) {
	root := t.TempDir()
	wc := NewWarmCache(root, 1<<20, NewMemoryIndex(), nil, nil)

	if err := wc.Put("x", []byte("hello"), 1000); err != nil {
		t.Fatal(err)
	}

	// Simulate accounting drift: usedSize overstated.
	wc.usedSize = 9999

	if err := wc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	used, _ := wc.Usage()
	if used != 0 {
		t.Fatalf("used after flush with drift: want 0 (recalc), got %d", used)
	}
}

// TestWarmCache_Flush_PutWorksAfterFlush verifies Put succeeds after flush
// completes (flushing flag is cleared).
func TestWarmCache_Flush_PutWorksAfterFlush(t *testing.T) {
	root := t.TempDir()
	wc := NewWarmCache(root, 1<<20, NewMemoryIndex(), nil, nil)

	if err := wc.Put("before", []byte("data"), 1000); err != nil {
		t.Fatal(err)
	}

	if err := wc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := wc.Put("after", []byte("newdata"), 1000); err != nil {
		t.Fatalf("Put after flush: %v", err)
	}

	used, _ := wc.Usage()
	if used != 7 {
		t.Fatalf("used after post-flush Put: want 7, got %d", used)
	}
}
