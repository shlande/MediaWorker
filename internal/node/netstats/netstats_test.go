package netstats

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
)

// Given a tracker, when hole-punch outcomes are recorded, then the counters
// accumulate (ok, total) exactly.
func TestTracker_HolePunchingCounters(t *testing.T) {
	tr := New()
	tr.TrackHolePunching(true)
	tr.TrackHolePunching(false)
	tr.TrackHolePunching(true)

	ok, total := tr.HolePunchingStats()
	if ok != 2 || total != 3 {
		t.Fatalf("stats = (%d, %d), want (2, 3)", ok, total)
	}
}

// Given a fresh tracker, when read, then reachability is "unknown" and DCUtR
// is unavailable (locked go-libp2p v0.48.0 has no hole-punch event).
func TestTracker_ZeroValueDefaults(t *testing.T) {
	tr := New()
	if got := tr.Reachability(); got != ReachabilityUnknown {
		t.Fatalf("reachability = %q, want unknown", got)
	}
	if tr.DCUtRAvailable() {
		t.Fatal("DCUtR must be unavailable before an event source is wired")
	}
	ok, total := tr.HolePunchingStats()
	if ok != 0 || total != 0 {
		t.Fatalf("stats = (%d, %d), want (0, 0)", ok, total)
	}
}

// Given a tracker subscribed to a real event bus, when AutoNAT emits
// EvtLocalReachabilityChanged, then the tracker reflects the latest value.
func TestSubscribe_TracksReachabilityEvents(t *testing.T) {
	bus := eventbus.NewBus()
	tr := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := Subscribe(ctx, tr, bus); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	em, err := bus.Emitter(new(event.EvtLocalReachabilityChanged))
	if err != nil {
		t.Fatalf("Emitter: %v", err)
	}
	if err := em.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPrivate}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for tr.Reachability() != ReachabilityPrivate && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := tr.Reachability(); got != ReachabilityPrivate {
		t.Fatalf("reachability = %q, want private", got)
	}

	// DCUtR stays unavailable (no event source on the locked version).
	if tr.DCUtRAvailable() {
		t.Fatal("DCUtRAvailable must stay false on go-libp2p v0.48.0")
	}
}

// Given a cancelled subscription context, when the consumer exits, then no
// further updates are applied.
func TestSubscribe_StopsOnContextCancel(t *testing.T) {
	bus := eventbus.NewBus()
	tr := New()
	ctx, cancel := context.WithCancel(context.Background())

	if err := Subscribe(ctx, tr, bus); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()

	em, err := bus.Emitter(new(event.EvtLocalReachabilityChanged))
	if err != nil {
		t.Fatalf("Emitter: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let the consumer observe cancellation
	if err := em.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPublic}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if got := tr.Reachability(); got != ReachabilityUnknown {
		t.Fatalf("reachability = %q after cancel, want unknown (no updates applied)", got)
	}
}
