package pinstore

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─── Helpers ───

func newTestPinStore(t *testing.T, fetchFunc func(string) ([]byte, error)) *PinStore {
	t.Helper()
	dbPath := t.TempDir()
	storagePath := t.TempDir()
	ps, err := NewPinStore(dbPath, storagePath, 1<<30, fetchFunc)
	if err != nil {
		t.Fatalf("NewPinStore: %v", err)
	}
	t.Cleanup(func() { ps.Close() })
	return ps
}

// fetchHelper wraps a fetchFunc so the test can wait for the async goroutine.
type fetchHelper struct {
	fn   func(string) ([]byte, error)
	done chan struct{} // closed when fetchFunc returns
}

func (fh *fetchHelper) Fetch(hash string) ([]byte, error) {
	defer close(fh.done)
	return fh.fn(hash)
}

// waitFetch waits for the async fetch goroutine and gives it time to finish
// writing to storage and setting Ready.
func waitFetch(fh *fetchHelper) {
	<-fh.done
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// ─── Tests ───

func TestApplyPin_IsPinned(t *testing.T) {
	fh := &fetchHelper{
		fn:   func(hash string) ([]byte, error) { return []byte("data"), nil },
		done: make(chan struct{}),
	}
	ps := newTestPinStore(t, fh.Fetch)

	// Given: no pins
	// When: ApplyPin
	ps.ApplyPin("abc123", "init", 7)

	// Then: IsPinned returns true
	if !ps.IsPinned("abc123") {
		t.Error("IsPinned should return true after ApplyPin")
	}
}

func TestApplyUnpin_IsPinned(t *testing.T) {
	fh := &fetchHelper{
		fn:   func(hash string) ([]byte, error) { return []byte("data"), nil },
		done: make(chan struct{}),
	}
	ps := newTestPinStore(t, fh.Fetch)

	// Given: a pinned blob
	ps.ApplyPin("abc123", "init", 7)

	// When: ApplyUnpin
	ps.ApplyUnpin("abc123")

	// Then: IsPinned returns false
	if ps.IsPinned("abc123") {
		t.Error("IsPinned should return false after ApplyUnpin")
	}
}

func TestApplyPin_Idempotent(t *testing.T) {
	var callCount atomic.Int32
	fh := &fetchHelper{
		fn: func(hash string) ([]byte, error) {
			callCount.Add(1)
			return []byte("data"), nil
		},
		done: make(chan struct{}),
	}
	ps := newTestPinStore(t, fh.Fetch)

	// Given: first ApplyPin
	ps.ApplyPin("abc123", "init", 7)
	waitFetch(fh)

	// When: second ApplyPin (idempotent)
	fh2 := &fetchHelper{
		fn:   func(hash string) ([]byte, error) { callCount.Add(1); return []byte("data"), nil },
		done: make(chan struct{}),
	}
	// ApplyPin is idempotent — second call is no-op — so fetchFunc is NOT called again.
	ps.ApplyPin("abc123", "init", 7) // uses internal check, not fh2

	// Then: only one fetch call occurred
	if callCount.Load() != 1 {
		t.Errorf("fetchFunc called %d times, want 1", callCount.Load())
	}
	// Entry still exists
	if !ps.IsPinned("abc123") {
		t.Error("IsPinned should still be true after idempotent ApplyPin")
	}
	_ = fh2 // unused — idempotency check prevents second fetch
}

func TestApplyUnpin_Idempotent(t *testing.T) {
	ps := newTestPinStore(t, func(hash string) ([]byte, error) { return []byte("data"), nil })

	// Given: no pins
	// When/Then: ApplyUnpin on non-pinned is a no-op (no error, no panic)
	ps.ApplyUnpin("nonexistent")

	if ps.IsPinned("nonexistent") {
		t.Error("IsPinned should return false for non-pinned blob")
	}
}

func TestIsReady_AfterFetch(t *testing.T) {
	fh := &fetchHelper{
		fn:   func(hash string) ([]byte, error) { return []byte("hello"), nil },
		done: make(chan struct{}),
	}
	ps := newTestPinStore(t, fh.Fetch)

	// Given: ApplyPin triggers async fetch
	ps.ApplyPin("abc123", "init", 5)
	waitFetch(fh)

	// Then: IsReady returns true (data fetched + stored)
	if !ps.IsReady("abc123") {
		t.Error("IsReady should return true after successful async fetch")
	}
}

func TestIsReady_FetchFail(t *testing.T) {
	fh := &fetchHelper{
		fn:   func(hash string) ([]byte, error) { return nil, errors.New("fetch failed") },
		done: make(chan struct{}),
	}
	ps := newTestPinStore(t, fh.Fetch)

	// Given: ApplyPin triggers async fetch that fails
	ps.ApplyPin("abc123", "init", 5)
	<-fh.done // fetchFunc returned with error
	time.Sleep(20 * time.Millisecond)

	// Then: IsReady returns false (Ready stays false on fetch error)
	if ps.IsReady("abc123") {
		t.Error("IsReady should return false when fetch fails")
	}
	// IsPinned is still true (pin record exists)
	if !ps.IsPinned("abc123") {
		t.Error("IsPinned should remain true even when fetch fails")
	}
}

func TestFetchPinnedBlob_PanicRecovery(t *testing.T) {
	fh := &fetchHelper{
		fn:   func(hash string) ([]byte, error) { panic("boom") },
		done: make(chan struct{}),
	}
	ps := newTestPinStore(t, fh.Fetch)

	// Given: ApplyPin triggers async fetch that panics
	ps.ApplyPin("abc123", "init", 5)
	<-fh.done // panic recovered, fetchFunc's defers ran
	time.Sleep(20 * time.Millisecond)

	// Then: IsReady returns false (panic recovered, Ready stays false)
	if ps.IsReady("abc123") {
		t.Error("IsReady should return false when fetch panics and is recovered")
	}
	// IsPinned is still true
	if !ps.IsPinned("abc123") {
		t.Error("IsPinned should remain true after panic recovery")
	}
}

func TestRestore(t *testing.T) {
	// Create a PinStore, apply 10 pins, close it, reopen, restore.
	dbPath := t.TempDir()
	storagePath := t.TempDir()

	var wg sync.WaitGroup
	ps1, err := NewPinStore(dbPath, storagePath, 1<<30, func(hash string) ([]byte, error) {
		defer wg.Done()
		return []byte("data-" + hash), nil
	})
	if err != nil {
		t.Fatalf("NewPinStore: %v", err)
	}

	const numBlobs = 10
	wg.Add(numBlobs)
	for i := range numBlobs {
		hash := fmt.Sprintf("blob-%03d", i)
		ps1.ApplyPin(hash, "init", 100)
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond) // let remaining stores complete

	if err := ps1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and restore.
	ps2, err := NewPinStore(dbPath, storagePath, 1<<30, func(hash string) ([]byte, error) {
		return []byte("data-" + hash), nil
	})
	if err != nil {
		t.Fatalf("NewPinStore (reopen): %v", err)
	}
	defer ps2.Close()

	if err := ps2.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// All 10 blobs should be in the index.
	for i := range numBlobs {
		hash := fmt.Sprintf("blob-%03d", i)
		if !ps2.IsPinned(hash) {
			t.Errorf("blob %s not found after Restore", hash)
		}
	}
}

func TestQuerySpace(t *testing.T) {
	// Use a done-tracking counter instead of channels to avoid close-on-closed panic.
	var fetchCount atomic.Int32
	nopFetch := func(hash string) ([]byte, error) {
		fetchCount.Add(1)
		return make([]byte, 42), nil
	}
	ps := newTestPinStore(t, nopFetch)

	// Given: 3 pinned blobs with known sizes
	ps.ApplyPin("hash-a", "init", 100)
	ps.ApplyPin("hash-b", "media", 200)
	ps.ApplyPin("hash-c", "thumbnail", 50)

	// Wait for all 3 async fetch goroutines to complete.
	for fetchCount.Load() < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)

	// When: QuerySpace
	info := ps.QuerySpace()

	// Then: correct counts
	if info.PinnedCount != 3 {
		t.Errorf("PinnedCount = %d, want 3", info.PinnedCount)
	}
	if info.TotalPinnedSize != 350 {
		t.Errorf("TotalPinnedSize = %d, want 350", info.TotalPinnedSize)
	}
	if info.AvailableBytes <= 0 {
		t.Errorf("AvailableBytes = %d, should be > 0", info.AvailableBytes)
	}
}

func TestHandleQueryPinSpace(t *testing.T) {
	ps := newTestPinStore(t, func(hash string) ([]byte, error) { return []byte("x"), nil })

	ps.ApplyPin("hash-a", "init", 128)
	// Don't wait for fetch — HandleQueryPinSpace returns space info, not Ready status.

	info := ps.HandleQueryPinSpace()
	if info.PinnedCount != 1 {
		t.Errorf("PinnedCount = %d, want 1", info.PinnedCount)
	}
}
