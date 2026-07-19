package pinstore

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// waitState polls Get until the entry reaches want or the deadline passes.
func waitState(t *testing.T, ps *PinStore, hash, want string) (entry PinEntry) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var ok bool
		entry, ok = ps.Get(hash)
		if ok && entry.State == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pin %s did not reach state %q (found=%v state=%q)", hash, want, ok, entry.State)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Given a legacy record (no state field, ready=true), When decoded, Then it
// maps to State=ready with the Ready fast path set.
func TestDecodePinEntry_LegacyReadyTrueMapsToReady(t *testing.T) {
	legacy := `{"blob_hash":"h1","blob_type":"mp4_init_segment","size":7,"pinned_at":"2026-07-20T00:00:00Z","ready":true}`
	entry, err := decodePinEntry([]byte(legacy))
	if err != nil {
		t.Fatalf("decodePinEntry: %v", err)
	}
	if entry.State != PinStateReady {
		t.Errorf("State = %q, want %q", entry.State, PinStateReady)
	}
	if !entry.Ready.Load() {
		t.Error("Ready fast path should be true for legacy ready=true record")
	}
}

// Given a legacy record (no state field, ready=false), When decoded, Then it
// maps to State=pulling.
func TestDecodePinEntry_LegacyReadyFalseMapsToPulling(t *testing.T) {
	legacy := `{"blob_hash":"h1","blob_type":"mp4_init_segment","size":7,"pinned_at":"2026-07-20T00:00:00Z","ready":false}`
	entry, err := decodePinEntry([]byte(legacy))
	if err != nil {
		t.Fatalf("decodePinEntry: %v", err)
	}
	if entry.State != PinStatePulling {
		t.Errorf("State = %q, want %q", entry.State, PinStatePulling)
	}
	if entry.Ready.Load() {
		t.Error("Ready fast path should be false for legacy ready=false record")
	}
}

// Given an entry with content_id/state/last_error, When encoded and decoded,
// Then all fields survive the round trip.
func TestPinEntryCodec_RoundTrip(t *testing.T) {
	in := &PinEntry{
		BlobHash:  "h1",
		BlobType:  "mp4_init_segment",
		Role:      "init",
		Size:      42,
		PinnedAt:  time.Now().UTC().Truncate(time.Second),
		ContentID: "cont_9",
		State:     PinStateFailed,
		LastError: "boom",
	}
	data, err := encodePinEntry(in)
	if err != nil {
		t.Fatalf("encodePinEntry: %v", err)
	}
	out, err := decodePinEntry(data)
	if err != nil {
		t.Fatalf("decodePinEntry: %v", err)
	}
	if out.ContentID != "cont_9" || out.State != PinStateFailed || out.LastError != "boom" {
		t.Errorf("round trip mismatch: %+v", out)
	}
	if out.Ready.Load() {
		t.Error("failed entry must not have Ready fast path set")
	}
}

// Given legacy-format bytes already sitting in BadgerDB, When Restore runs,
// Then the entry comes back with the mapped state.
func TestRestore_LegacyRecordMapsState(t *testing.T) {
	dbPath := t.TempDir()
	storagePath := t.TempDir()
	ps, err := NewPinStore(dbPath, storagePath, 1<<30, func(string) ([]byte, error) { return []byte("x"), nil })
	if err != nil {
		t.Fatalf("NewPinStore: %v", err)
	}
	defer func() { _ = ps.Close() }()

	legacy := []byte(`{"blob_hash":"h-old","blob_type":"mp4_init_segment","size":7,"pinned_at":"2026-07-20T00:00:00Z","ready":true}`)
	if err := ps.db.Update(func(txn *badger.Txn) error {
		return txn.Set(makePinKey("h-old"), legacy)
	}); err != nil {
		t.Fatalf("seed legacy record: %v", err)
	}

	if err := ps.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	entry, ok := ps.Get("h-old")
	if !ok {
		t.Fatal("legacy record missing after Restore")
	}
	if entry.State != PinStateReady {
		t.Errorf("State = %q, want %q", entry.State, PinStateReady)
	}
}

// Given a fresh ApplyPin with a blocked fetch, When read back, Then the entry
// is pulling, not ready, and carries the content id.
func TestApplyPin_InitialStatePulling(t *testing.T) {
	block := make(chan struct{})
	ps := newTestPinStore(t, func(string) ([]byte, error) { <-block; return []byte("x"), nil })
	t.Cleanup(func() { close(block) })

	ps.ApplyPin("h1", "mp4_init_segment", "init", 7, "cont_1")

	entry, ok := ps.Get("h1")
	if !ok {
		t.Fatal("Get should find the pin")
	}
	if entry.State != PinStatePulling {
		t.Errorf("State = %q, want %q", entry.State, PinStatePulling)
	}
	if entry.ContentID != "cont_1" {
		t.Errorf("ContentID = %q, want %q", entry.ContentID, "cont_1")
	}
	if ps.IsReady("h1") {
		t.Error("IsReady should be false while pulling")
	}
}

// Given a fetch that errors, When the fetch completes, Then the entry is
// failed with last_error set and IsReady stays false.
func TestFetchFailure_StateFailedWithLastError(t *testing.T) {
	ps := newTestPinStore(t, func(string) ([]byte, error) { return nil, errors.New("origin down") })

	ps.ApplyPin("h1", "mp4_init_segment", "init", 7, "")
	entry := waitState(t, ps, "h1", PinStateFailed)

	if entry.LastError != "origin down" {
		t.Errorf("LastError = %q, want %q", entry.LastError, "origin down")
	}
	if ps.IsReady("h1") {
		t.Error("IsReady should be false for a failed pin")
	}
}

// Given a fetch whose error exceeds 512 characters, When it fails, Then
// last_error is truncated to 512 characters.
func TestFetchFailure_LastErrorTruncated(t *testing.T) {
	longErr := strings.Repeat("e", 600)
	ps := newTestPinStore(t, func(string) ([]byte, error) { return nil, errors.New(longErr) })

	ps.ApplyPin("h1", "mp4_init_segment", "init", 7, "")
	entry := waitState(t, ps, "h1", PinStateFailed)

	if got := len([]rune(entry.LastError)); got != maxLastErrorLen {
		t.Errorf("LastError length = %d, want %d", got, maxLastErrorLen)
	}
}

// Given a failed pin, When RetryPin is called with a now-healthy fetch, Then
// the pin resets to pulling, refetches, and lands ready with last_error
// cleared; a concurrent retry while pulling is refused.
func TestRetryPin_ResetsFailedPin(t *testing.T) {
	ps := newTestPinStore(t, func(string) ([]byte, error) { return nil, errors.New("origin down") })
	ps.ApplyPin("h1", "mp4_init_segment", "init", 7, "")
	waitState(t, ps, "h1", PinStateFailed)

	block := make(chan struct{})
	var unblock sync.Once
	ps.fetchFunc = func(string) ([]byte, error) { <-block; return []byte("x"), nil }
	t.Cleanup(func() { unblock.Do(func() { close(block) }) })

	if !ps.RetryPin("h1") {
		t.Fatal("RetryPin should accept a failed pin")
	}
	entry := waitState(t, ps, "h1", PinStatePulling)
	if entry.LastError != "" {
		t.Errorf("LastError = %q, want cleared", entry.LastError)
	}
	if ps.RetryPin("h1") {
		t.Error("RetryPin while pulling should be refused (idempotent)")
	}

	unblock.Do(func() { close(block) })
	waitState(t, ps, "h1", PinStateReady)
	if !ps.IsReady("h1") {
		t.Error("IsReady should be true after successful retry")
	}
}

// Given a ready pin and a missing hash, When RetryPin is called, Then both
// are refused.
func TestRetryPin_NotFailedReturnsFalse(t *testing.T) {
	fh := &fetchHelper{
		fn:   func(string) ([]byte, error) { return []byte("x"), nil },
		done: make(chan struct{}),
	}
	ps := newTestPinStore(t, fh.Fetch)
	ps.ApplyPin("h1", "mp4_init_segment", "init", 7, "")
	waitFetch(fh)
	waitState(t, ps, "h1", PinStateReady)

	if ps.RetryPin("h1") {
		t.Error("RetryPin on a ready pin should return false")
	}
	if ps.RetryPin("h-missing") {
		t.Error("RetryPin on a missing pin should return false")
	}
}

// Given ready/pulling/failed pins across roles and content ids, When List is
// filtered, Then each filter dimension narrows the result set.
func TestList_ThreeFilters(t *testing.T) {
	block := make(chan struct{})
	fetch := func(hash string) ([]byte, error) {
		switch hash {
		case "h-ready":
			return []byte("x"), nil
		case "h-fail":
			return nil, errors.New("boom")
		default:
			<-block
			return []byte("x"), nil
		}
	}
	ps := newTestPinStore(t, fetch)
	t.Cleanup(func() { close(block) })

	ps.ApplyPin("h-ready", "mp4_init_segment", "init", 10, "c1")
	ps.ApplyPin("h-fail", "m4s_media_segment", "media", 20, "c1")
	ps.ApplyPin("h-pull", "m4s_media_segment", "media", 30, "c2")

	waitState(t, ps, "h-ready", PinStateReady)
	waitState(t, ps, "h-fail", PinStateFailed)
	waitState(t, ps, "h-pull", PinStatePulling)

	boolPtr := func(b bool) *bool { return &b }
	cases := []struct {
		name   string
		filter PinFilter
		want   int
	}{
		{"no filter", PinFilter{}, 3},
		{"ready=true", PinFilter{Ready: boolPtr(true)}, 1},
		{"ready=false matches pulling+failed", PinFilter{Ready: boolPtr(false)}, 2},
		{"role=media", PinFilter{Role: "media"}, 2},
		{"content_id=c1", PinFilter{ContentID: "c1"}, 2},
		{"content_id=c1 + ready=false", PinFilter{ContentID: "c1", Ready: boolPtr(false)}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ps.List(tc.filter)
			if len(got) != tc.want {
				t.Errorf("List(%+v) returned %d entries, want %d", tc.filter, len(got), tc.want)
			}
		})
	}

	// Snapshot fidelity: the listed entry carries the stored fields.
	got := ps.List(PinFilter{ContentID: "c1", Role: "init"})
	if len(got) != 1 {
		t.Fatalf("List(content+role) = %d entries, want 1", len(got))
	}
	if got[0].BlobHash != "h-ready" || got[0].State != PinStateReady || got[0].ContentID != "c1" || got[0].Role != "init" || got[0].Size != 10 {
		t.Errorf("snapshot mismatch: %s %s %s %s %d", got[0].BlobHash, got[0].State, got[0].ContentID, got[0].Role, got[0].Size)
	}
}
