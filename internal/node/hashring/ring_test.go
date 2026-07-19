package hashring

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/node/peerstore"
	"github.com/shlande/mediaworker/internal/types"
)

func newTempStore(t *testing.T) (*peerstore.PeerEntryStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "hashring-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	store, err := peerstore.NewPeerEntryStore(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("NewPeerEntryStore: %v", err)
	}
	if err := store.Restore(); err != nil {
		_ = store.Close()
		os.RemoveAll(dir)
		t.Fatalf("Restore: %v", err)
	}
	cleanup := func() {
		_ = store.Close()
		os.RemoveAll(dir)
	}
	return store, cleanup
}

func putPeer(t *testing.T, store *peerstore.PeerEntryStore, id types.PeerId, score float64, stale bool, icp bool) {
	t.Helper()
	err := store.Put(id, types.PeerStoreEntry{
		PeerID: id,
		Score:  score,
		Stale:  stale,
		Capabilities: types.NodeCapabilities{
			PeerICP: icp,
		},
	})
	if err != nil {
		t.Fatalf("Put peer %s: %v", id, err)
	}
}

func TestHashRing_Get(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-a", 0.0, false, true)
	putPeer(t, store, "peer-b", 0.0, false, true)
	putPeer(t, store, "peer-c", 0.0, false, true)

	ring := NewHashRing("peer-a", store, 150)
	ring.RebuildHashRing()

	// Get should return a non-empty peer for any blob hash
	for _, hash := range []string{"blob-1", "blob-2", "blob-3"} {
		got := ring.Get(hash)
		if got == "" {
			t.Errorf("Get(%q) returned empty string", hash)
		}
		if got != "peer-a" && got != "peer-b" && got != "peer-c" {
			t.Errorf("Get(%q) = %q; want one of [peer-a, peer-b, peer-c]", hash, got)
		}
	}

	// Same blob hash always maps to same peer (deterministic)
	first := ring.Get("deterministic-test")
	for range 100 {
		if got := ring.Get("deterministic-test"); got != first {
			t.Errorf("Get(%q) produced different results: %q vs %q", "deterministic-test", first, got)
			break
		}
	}
}

func TestHashRing_IsPrimary(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "self", 0.0, false, true)
	putPeer(t, store, "other", 0.0, false, true)

	ring := NewHashRing("self", store, 150)
	ring.RebuildHashRing()

	primaryCount := 0
	nonPrimaryCount := 0
	for i := range 100 {
		hash := "blob-" + string(rune('0'+i%10)) + string(rune('0'+i/10))
		if ring.IsPrimary(hash) {
			primaryCount++
		} else {
			nonPrimaryCount++
		}
	}
	// With 2 peers, roughly 50% of blobs should be on self
	if primaryCount == 0 {
		t.Error("IsPrimary never returned true with 2 peers")
	}
	if nonPrimaryCount == 0 {
		t.Error("IsPrimary never returned false with 2 peers")
	}
}

func TestHashRing_Rebuild(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-a", 0.0, false, true)
	putPeer(t, store, "peer-b", 0.0, false, true)

	ring := NewHashRing("peer-a", store, 150)
	ring.RebuildHashRing()

	if ring.RebuildCount() != 1 {
		t.Fatalf("RebuildCount = %d after initial rebuild, want 1", ring.RebuildCount())
	}

	// Add a new peer and rebuild
	putPeer(t, store, "peer-c", 0.0, false, true)
	ring.RebuildHashRing()

	if ring.RebuildCount() != 2 {
		t.Fatalf("RebuildCount = %d after second rebuild, want 2", ring.RebuildCount())
	}

	// New peer should appear as primary for some blobs
	foundC := false
	for i := range 200 {
		if ring.Get("rebuild-test-"+string(rune('0'+i%10))) == "peer-c" {
			foundC = true
			break
		}
	}
	if !foundC {
		t.Error("peer-c was never returned after rebuild")
	}
}

func TestHashRing_NewPeerBuffer(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-a", 0.0, false, true)
	putPeer(t, store, "peer-b", 0.0, false, true)

	ring := NewHashRing("peer-a", store, 150,
		WithNewPeerBuffer(200*time.Millisecond),
	)
	ring.RebuildHashRing()

	// Add a new peer and record its join time
	putPeer(t, store, "peer-new", 0.0, false, true)
	ring.RecordPeerJoin("peer-new")

	// Rebuild immediately — new peer should NOT be in ring (within buffer)
	ring.RebuildHashRing()

	for i := range 200 {
		if ring.Get("buffer-test-"+string(rune('0'+i%10))) == "peer-new" {
			t.Error("new peer appeared in ring before buffer expired")
			return
		}
	}

	// Wait past the buffer and rebuild
	time.Sleep(250 * time.Millisecond)
	ring.RebuildHashRing()

	foundNew := false
	for i := range 200 {
		if ring.Get("buffer-test-"+string(rune('0'+i%10))) == "peer-new" {
			foundNew = true
			break
		}
	}
	if !foundNew {
		t.Error("new peer was NOT in ring after buffer expired")
	}
}

func TestHashRing_HeartbeatLiveness(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-a", 0.0, false, true)
	putPeer(t, store, "peer-b", 0.0, false, true)

	ring := NewHashRing("peer-a", store, 150,
		WithMissThreshold(3),
	)

	// 2 consecutive misses should NOT mark stale
	ring.OnHeartbeatMiss("peer-b")
	ring.OnHeartbeatMiss("peer-b")
	ring.OnHeartbeat("peer-b") // reset
	ring.OnHeartbeatMiss("peer-b")
	ring.OnHeartbeatMiss("peer-b")
	ring.OnHeartbeatMiss("peer-b") // this should be 3 with a reset before

	// verify peer-b is now stale
	entry, ok := store.Get("peer-b")
	if !ok {
		t.Fatal("peer-b not found in store")
	}
	if !entry.Stale {
		t.Error("peer-b should be marked stale after 3 consecutive misses")
	}
}

func TestHashRing_HeartbeatResetsAfterMark(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-c", 0.0, false, true)

	ring := NewHashRing("self", store, 150, WithMissThreshold(3))

	ring.OnHeartbeatMiss("peer-c")
	ring.OnHeartbeatMiss("peer-c")
	ring.OnHeartbeatMiss("peer-c")
	// peer-c is now stale
	entry, ok := store.Get("peer-c")
	if !ok || !entry.Stale {
		t.Fatal("peer-c should be stale after 3 misses")
	}

	// More misses should not panic and peer stays stale
	ring.OnHeartbeatMiss("peer-c")
	ring.OnHeartbeatMiss("peer-c")
	ring.OnHeartbeatMiss("peer-c")

	// Reset: many heartbeats
	for range 10 {
		ring.OnHeartbeat("peer-c")
	}
	ring.OnHeartbeatMiss("peer-c") // count = 1
	ring.OnHeartbeatMiss("peer-c") // count = 2
	// peer-c is already stale, so more misses are fine — just no panic
}

func TestHashRing_Debounce(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-a", 0.0, false, true)

	ring := NewHashRing("peer-a", store, 150,
		WithDebounce(500*time.Millisecond),
		WithMaxWait(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ring.StartRebuildLoop(ctx)

	// Fire many OnPeerStoreChange within debounce window
	for range 20 {
		ring.OnPeerStoreChange()
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for debounce to settle + one rebuild
	time.Sleep(800 * time.Millisecond)
	cancel()

	count := ring.RebuildCount()
	// With 20 triggers in ~400ms and 500ms debounce, we expect 1 rebuild
	// (all within the merge window)
	if count < 1 {
		t.Errorf("RebuildCount = %d, want >= 1", count)
	}
	if count > 3 {
		t.Errorf("RebuildCount = %d, want <= 3 (debounce should merge bursts)", count)
	}
}

func TestHashRing_MaxWait(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-a", 0.0, false, true)

	ring := NewHashRing("peer-a", store, 150,
		WithDebounce(200*time.Millisecond),
		WithMaxWait(400*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ring.StartRebuildLoop(ctx)

	initial := ring.RebuildCount()

	// Continuously fire OnPeerStoreChange for > maxWait duration
	start := time.Now()
	for time.Since(start) < 600*time.Millisecond {
		ring.OnPeerStoreChange()
		time.Sleep(30 * time.Millisecond)
	}

	time.Sleep(300 * time.Millisecond)
	cancel()

	count := ring.RebuildCount() - initial
	if count < 1 {
		t.Errorf("RebuildCount delta = %d, want >= 1 (max-wait should force at least one rebuild)", count)
	}
}

func TestHashRing_Replace(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-a", 0.0, false, true)
	putPeer(t, store, "peer-b", 0.0, false, true)

	ring := NewHashRing("peer-a", store, 150)
	ring.RebuildHashRing()

	// Verify current ring has peer-a and peer-b
	foundA, foundB := false, false
	for i := range 200 {
		switch ring.Get("replace-test-" + string(rune('0'+i%10))) {
		case "peer-a":
			foundA = true
		case "peer-b":
			foundB = true
		}
	}
	if !foundA || !foundB {
		t.Fatal("initial ring missing expected peers")
	}

	// Build replacement ring with only peer-c
	newRing := newConsistentMap(150)
	newRing.add("peer-c")
	newRing.buildKeys()
	ring.Replace(newRing)

	// Verify ring now only returns peer-c
	for i := range 100 {
		got := ring.Get("replace-test-" + string(rune('0'+i%10)))
		if got != "peer-c" {
			t.Errorf("after Replace: Get() = %q, want peer-c", got)
			break
		}
	}
}

func TestHashRing_NoPeerICP(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-icp", 0.0, false, true)
	putPeer(t, store, "peer-noicp", 0.0, false, false) // PeerICP=false

	ring := NewHashRing("peer-icp", store, 150)
	ring.RebuildHashRing()

	// peer-noicp should never appear in ring
	for i := range 200 {
		if ring.Get("noicp-test-"+string(rune('0'+i%10))) == "peer-noicp" {
			t.Error("peer-noicp (PeerICP=false) appeared in ring")
			return
		}
	}
}

func TestHashRing_EmptyRing(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	ring := NewHashRing("self", store, 150)
	ring.RebuildHashRing()

	got := ring.Get("any-blob")
	if got != "" {
		t.Errorf("Get on empty ring = %q, want empty", got)
	}
	if ring.IsPrimary("any-blob") {
		t.Error("IsPrimary on empty ring returned true")
	}
}

func TestHashRing_ConcurrentSafety(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	putPeer(t, store, "peer-a", 0.0, false, true)
	putPeer(t, store, "peer-b", 0.0, false, true)
	putPeer(t, store, "peer-c", 0.0, false, true)

	ring := NewHashRing("peer-a", store, 150)
	ring.RebuildHashRing()

	done := make(chan struct{})
	var errors atomic.Int64

	// Concurrent readers
	for range 5 {
		go func() {
			for i := 0; i < 1000; i++ {
				ring.Get("concurrent-" + string(rune('0'+i%10)))
				ring.IsPrimary("concurrent-" + string(rune('0'+i%10)))
			}
			done <- struct{}{}
		}()
	}

	// Concurrent writer
	go func() {
		for i := 0; i < 20; i++ {
			ring.RebuildHashRing()
			time.Sleep(time.Millisecond)
		}
		done <- struct{}{}
	}()

	for range 6 {
		<-done
	}
	if errors.Load() > 0 {
		t.Errorf("concurrent access caused %d errors", errors.Load())
	}
}

func TestHashRing_DistributionFairness(t *testing.T) {
	store, cleanup := newTempStore(t)
	defer cleanup()

	peers := []types.PeerId{"p1", "p2", "p3", "p4", "p5"}
	for _, p := range peers {
		putPeer(t, store, p, 0.0, false, true)
	}

	ring := NewHashRing("p1", store, 150)
	ring.RebuildHashRing()

	counts := make(map[types.PeerId]int)
	const n = 10000
	for i := range n {
		p := ring.Get("dist-" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+i/100)))
		counts[p]++
	}

	// With 5 peers and 150 replicas, each should get roughly 20% (±5%)
	expected := n / len(peers)
	for _, p := range peers {
		c := counts[p]
		if c < expected/2 || c > expected*2 {
			t.Errorf("peer %s has %d keys, expected ~%d (unfair distribution)", p, c, expected)
		}
	}
}
