// Package netstats tracks NAT-related network statistics for the node admin
// API: DCUtR (hole punching) counters and the current reachability state,
// fed by libp2p event-bus subscriptions wired in main.
//
// DCUtR note: the go.mod-locked go-libp2p (v0.48.0) has NO hole-punching
// event-bus event — outcomes only flow through holepunch.Tracer, which
// requires host-construction wiring (out of scope). The counters therefore
// stay 0 and the tracker reports DCUtRAvailable() == false so API responses
// can carry dcutr_counters_unavailable: true.
package netstats

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
)

// Reachability labels emitted by Tracker.Reachability.
const (
	ReachabilityUnknown = "unknown"
	ReachabilityPublic  = "public"
	ReachabilityPrivate = "private"
)

// Tracker records hole-punch counters and the latest reachability value.
// The zero value is ready to use: reachability starts "unknown" and DCUtR is
// unavailable until an event source is wired.
type Tracker struct {
	mu           sync.RWMutex // guards reachability
	reachability string
	punchOK      atomic.Int64
	punchTotal   atomic.Int64
	dcutrAvail   atomic.Bool
}

// New returns a zero-value Tracker.
func New() *Tracker { return &Tracker{reachability: ReachabilityUnknown} }

// TrackHolePunching records one DCUtR outcome.
func (t *Tracker) TrackHolePunching(success bool) {
	t.punchTotal.Add(1)
	if success {
		t.punchOK.Add(1)
	}
}

// HolePunchingStats returns the cumulative (ok, total) outcomes.
func (t *Tracker) HolePunchingStats() (ok, total int) {
	return int(t.punchOK.Load()), int(t.punchTotal.Load())
}

// SetReachability stores the label for the latest reachability event.
func (t *Tracker) SetReachability(label string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reachability = label
}

// Reachability returns the latest reachability label ("unknown" until the
// first EvtLocalReachabilityChanged arrives).
func (t *Tracker) Reachability() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.reachability
}

// SetDCUtRAvailable marks whether a DCUtR event source exists on the locked
// libp2p version. False => counters stay 0 and responses flag unavailability.
func (t *Tracker) SetDCUtRAvailable(available bool) {
	t.dcutrAvail.Store(available)
}

// DCUtRAvailable reports whether hole-punch counters are actually fed.
func (t *Tracker) DCUtRAvailable() bool { return t.dcutrAvail.Load() }

// reachabilityLabel maps network.Reachability to its API label.
func reachabilityLabel(r network.Reachability) string {
	switch r {
	case network.ReachabilityPublic:
		return ReachabilityPublic
	case network.ReachabilityPrivate:
		return ReachabilityPrivate
	case network.ReachabilityUnknown:
		return ReachabilityUnknown
	}
	return ReachabilityUnknown
}

// Subscribe wires the tracker into the host event bus: it consumes
// EvtLocalReachabilityChanged (AutoNAT) until ctx is cancelled. The locked
// go-libp2p v0.48.0 emits that event as a VALUE type and has no hole-punching
// event, so DCUtR is marked unavailable here. The consumer goroutine is
// started by Subscribe; the caller owns ctx (main passes rootCtx).
func Subscribe(ctx context.Context, t *Tracker, bus event.Bus) error {
	sub, err := bus.Subscribe(new(event.EvtLocalReachabilityChanged))
	if err != nil {
		return err
	}
	t.SetDCUtRAvailable(false)
	go func() {
		defer func() { _ = sub.Close() }()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-sub.Out():
				if !ok {
					return
				}
				if e, ok := ev.(event.EvtLocalReachabilityChanged); ok {
					t.SetReachability(reachabilityLabel(e.Reachability))
				}
			}
		}
	}()
	return nil
}
