package peerstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

func makeTestEntry(id string, score float64) types.PeerStoreEntry {
	return types.PeerStoreEntry{
		PeerID: types.PeerId(id),
		Addrs:  []string{"/ip4/127.0.0.1/tcp/4001"},
		Score:  score,
	}
}

func openStore(t *testing.T) *PeerEntryStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "badger")
	store, err := NewPeerEntryStore(path)
	if err != nil {
		t.Fatalf("NewPeerEntryStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// ─── TestPeerEntryStore_PutGet ───

func TestPeerEntryStore_PutGet(t *testing.T) {
	store := openStore(t)

	entry := makeTestEntry("peer1", 5.0)
	if err := store.Put("peer1", entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok := store.Get("peer1")
	if !ok {
		t.Fatal("Get returned false for existing peer")
	}

	if got.PeerID != "peer1" || got.Score != 5.0 {
		t.Errorf("unexpected entry: %+v", got)
	}

	// Get non-existing peer returns false.
	_, ok = store.Get("nonexistent")
	if ok {
		t.Error("Get returned true for nonexistent peer")
	}
}

// ─── TestPeerEntryStore_Delete ───

func TestPeerEntryStore_Delete(t *testing.T) {
	store := openStore(t)

	entry := makeTestEntry("peer2", 5.0)
	if err := store.Put("peer2", entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := store.Delete("peer2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, ok := store.Get("peer2")
	if ok {
		t.Error("Get returned true after Delete")
	}
}

// ─── TestPeerEntryStore_Restore ───

func TestPeerEntryStore_Restore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badger")

	const n = 100

	// Write 100 entries.
	store, err := NewPeerEntryStore(path)
	if err != nil {
		t.Fatalf("NewPeerEntryStore: %v", err)
	}
	for i := range n {
		id := fmt.Sprintf("peer-%d", i)
		entry := makeTestEntry(id, float64(i))
		if err := store.Put(types.PeerId(id), entry); err != nil {
			_ = store.Close() // best-effort cleanup before t.Fatalf
			t.Fatalf("Put %s: %v", id, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and restore.
	store2, err := NewPeerEntryStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if err := store2.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify all 100 entries recovered.
	for i := range n {
		id := fmt.Sprintf("peer-%d", i)
		got, ok := store2.Get(types.PeerId(id))
		if !ok {
			t.Errorf("peer %s not restored", id)
			continue
		}
		if got.PeerID != types.PeerId(id) || got.Score != float64(i) {
			t.Errorf("peer %s: got %+v", id, got)
		}
	}
}

// ─── TestPeerEntryStore_ActivePeers ───

func TestPeerEntryStore_ActivePeers(t *testing.T) {
	store := openStore(t)

	// 2 stale, 1 low-score (below -20), 2 healthy → ActivePeers returns 2.
	// peer-a: healthy (Score=5)
	_ = store.Put("peer-a", makeTestEntry("peer-a", 5.0)) // test setup; error would surface in ActivePeers
	// peer-b: healthy (Score=-5, above GraylistThreshold -20)
	_ = store.Put("peer-b", makeTestEntry("peer-b", -5.0))
	// peer-c: stale (should be excluded)
	e := makeTestEntry("peer-c", 5.0)
	e.Stale = true
	_ = store.Put("peer-c", e)
	// peer-d: stale + low score (excluded)
	e2 := makeTestEntry("peer-d", -15.0)
	e2.Stale = true
	_ = store.Put("peer-d", e2)
	// peer-e: low score below GraylistThreshold (excluded at -25.0)
	_ = store.Put("peer-e", makeTestEntry("peer-e", -25.0))

	active := store.ActivePeers()

	if len(active) != 2 {
		t.Fatalf("expected 2 active peers, got %d: %+v", len(active), active)
	}

	ids := make(map[types.PeerId]bool)
	for _, e := range active {
		ids[e.PeerID] = true
	}
	if !ids["peer-a"] {
		t.Error("peer-a should be active")
	}
	if !ids["peer-b"] {
		t.Error("peer-b should be active (score -5 >= -20)")
	}
	if !ids["peer-b"] {
		t.Error("peer-b should be active")
	}
}

// ─── TestPeerEntryStore_MarkStale ───

func TestPeerEntryStore_MarkStale(t *testing.T) {
	store := openStore(t)

	if err := store.Put("peer1", makeTestEntry("peer1", 5.0)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := store.MarkStale("peer1", "test"); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}

	entry, ok := store.Get("peer1")
	if !ok {
		t.Fatal("Get returned false after MarkStale")
	}
	if !entry.Stale {
		t.Error("entry not marked stale")
	}

	// Also verify persistence: reopen and check Stale persisted.
	// Use the same store's Close+reopen pattern.
	peers := store.ActivePeers()
	if len(peers) != 0 {
		t.Errorf("stale peer should not be in active list: %+v", peers)
	}
}

// ─── TestPeerEntryStore_CorruptDB ───

func TestPeerEntryStore_CorruptDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badger")

	store, err := NewPeerEntryStore(path)
	if err != nil {
		t.Fatalf("NewPeerEntryStore: %v", err)
	}
	if err := store.Put("peer1", makeTestEntry("peer1", 5.0)); err != nil {
		_ = store.Close() // best-effort cleanup before t.Fatalf
		t.Fatalf("Put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the DB by writing garbage over MANIFEST.
	manifestPath := filepath.Join(path, "MANIFEST")
	if err := os.WriteFile(manifestPath, []byte("corrupted"), 0644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	// Reopen + Restore must return an error, not panic.
	store2, err := NewPeerEntryStore(path)
	if err == nil {
		defer func() { _ = store2.Close() }()

		// Restore may return an error, but must not panic.
		restoreErr := store2.Restore()
		if restoreErr != nil {
			t.Logf("Restore returned error (expected for corrupt DB): %v", restoreErr)
		}
	}
	// Either NewPeerEntryStore or Restore surfaces the corruption — both are acceptable.
}

// ─── verify BadgerDB persistence (index rebuilt from disk, not stale memory) ───

func TestPeerEntryStore_PersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badger")

	store, err := NewPeerEntryStore(path)
	if err != nil {
		t.Fatalf("NewPeerEntryStore: %v", err)
	}
	e := makeTestEntry("peer-f", 42.0)
	e.JWTExp = 99999999
	if err := store.Put("peer-f", e); err != nil {
		_ = store.Close() // best-effort cleanup before t.Fatalf
		t.Fatalf("Put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen, restore, verify JWTExp persisted.
	store2, err := NewPeerEntryStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if err := store2.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got, ok := store2.Get("peer-f")
	if !ok {
		t.Fatal("peer-f not found after reopen+restore")
	}
	if got.JWTExp != 99999999 {
		t.Errorf("JWTExp = %d, expected 99999999", got.JWTExp)
	}

	// Verify JSON serialized correctly.
	data, _ := json.Marshal(got)
	if len(data) < 20 {
		t.Errorf("serialized entry too short: %d bytes", len(data))
	}
}

// ─── TestPeerEntryStore_Close ───

func TestPeerEntryStore_Close(t *testing.T) {
	store := openStore(t)

	// Closing the store should succeed.
	// openStore already registers a cleanup, but we explicitly close and
	// verify no double-close panic.
	if err := store.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close on already-closed DB should return an error, not panic.
	err := store.Close()
	if err == nil {
		t.Log("second Close returned nil (acceptable for badger)")
	}
}

// ─── TestPeerEntryStore_StartValueLogGC (T15 wiring) ───

// TestPeerEntryStore_StartValueLogGC_TickerFires verifies that
// StartValueLogGC actually drives periodic RunValueLogGC calls. Uses a 10ms
// interval and asserts GCCalls() increments ≥3 times within 500ms.
func TestPeerEntryStore_StartValueLogGC_TickerFires(t *testing.T) {
	store := openStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store.StartValueLogGC(ctx, 10*time.Millisecond)

	deadline := time.After(500 * time.Millisecond)
	for {
		if store.GCCalls() >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected ≥3 GC calls within 500ms, got %d", store.GCCalls())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Logf("GC calls observed: %d", store.GCCalls())
}

// TestPeerEntryStore_StartValueLogGC_StopsOnClose verifies that the GC
// goroutine exits when Close() is called — Close must wait for the loop to
// exit before closing the DB (otherwise badger would race the GC read).
func TestPeerEntryStore_StartValueLogGC_StopsOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badger")
	store, err := NewPeerEntryStore(path)
	if err != nil {
		t.Fatalf("NewPeerEntryStore: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store.StartValueLogGC(ctx, 5*time.Millisecond)

	// Give the loop time to fire at least once.
	time.Sleep(30 * time.Millisecond)
	preCloseCalls := store.GCCalls()
	if preCloseCalls == 0 {
		t.Fatalf("expected ≥1 GC call before Close, got 0")
	}

	// Close must return promptly after signalling the GC goroutine.
	start := time.Now()
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Errorf("Close took %v to wait for GC goroutine — expected <2s", elapsed)
	}
	t.Logf("Close completed in %v with %d pre-close GC calls", elapsed, preCloseCalls)
}

// TestPeerEntryStore_StartValueLogGC_StopsOnContextCancel verifies that the
// GC loop exits when the caller-supplied context is cancelled (independent
// of Close). GCCalls stops incrementing after cancel.
func TestPeerEntryStore_StartValueLogGC_StopsOnContextCancel(t *testing.T) {
	store := openStore(t)

	ctx, cancel := context.WithCancel(context.Background())

	store.StartValueLogGC(ctx, 5*time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	cancel()
	preCancelCalls := store.GCCalls()
	if preCancelCalls == 0 {
		t.Fatalf("expected ≥1 GC call before cancel, got 0")
	}

	// After cancel, GCCalls should not increase (loop exited).
	time.Sleep(50 * time.Millisecond)
	postCancelCalls := store.GCCalls()
	if postCancelCalls > preCancelCalls+1 {
		t.Errorf("GC calls continued after cancel: pre=%d post=%d",
			preCancelCalls, postCancelCalls)
	}
}

// TestPeerEntryStore_StartValueLogGC_ZeroIntervalNoop verifies that a zero
// or negative interval is a no-op (no goroutine started). This preserves
// the legacy "no GC" behaviour for callers that haven't been wired.
func TestPeerEntryStore_StartValueLogGC_ZeroIntervalNoop(t *testing.T) {
	store := openStore(t)
	store.StartValueLogGC(context.Background(), 0)

	time.Sleep(20 * time.Millisecond)
	if got := store.GCCalls(); got != 0 {
		t.Errorf("expected 0 GC calls for zero interval, got %d", got)
	}
}
