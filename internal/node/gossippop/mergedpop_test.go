package gossippop

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// TestMergedPopularity_Snapshot_ThresholdFilter verifies that Snapshot only
// returns entries whose TotalWeight >= MinTrustedWeight (the same threshold
// used by getVideoPopularity). Below-threshold entries are omitted entirely
// rather than returned with heat 0, so the cache eviction PopSource never
// ranks an untrusted blob.
func TestMergedPopularity_Snapshot_ThresholdFilter(t *testing.T) {
	mp := NewMergedPopularity()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	// blobBelow: TotalWeight = 2.0 < MinTrustedWeight (5.0) → omitted.
	u1 := signUpdate(t, types.PeerId("peer1"), priv, map[string]int64{"blobBelow": 10})
	if err := mp.OnPopularityUpdate(u1, 2.0, pub); err != nil {
		t.Fatalf("update1: %v", err)
	}

	// blobAbove: TotalWeight = 6.0 > MinTrustedWeight → included with weighted heat.
	u2 := signUpdate(t, types.PeerId("peer1"), priv, map[string]int64{"blobAbove": 20})
	if err := mp.OnPopularityUpdate(u2, 6.0, pub); err != nil {
		t.Fatalf("update2: %v", err)
	}

	snap := mp.Snapshot()
	if _, ok := snap["blobBelow"]; ok {
		t.Fatal("expected blobBelow to be omitted (TotalWeight < MinTrustedWeight)")
	}
	heat, ok := snap["blobAbove"]
	if !ok {
		t.Fatal("expected blobAbove to be present (TotalWeight >= MinTrustedWeight)")
	}
	if heat != 20.0 {
		t.Fatalf("expected heat 20.0, got %f", heat)
	}
}

// TestMergedPopularity_Snapshot_Empty verifies that Snapshot returns a non-nil
// empty map for a fresh MergedPopularity (so callers can safely range over it).
func TestMergedPopularity_Snapshot_Empty(t *testing.T) {
	mp := NewMergedPopularity()
	snap := mp.Snapshot()
	if snap == nil {
		t.Fatal("expected non-nil map")
	}
	if len(snap) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(snap))
	}
}

// TestMergedPopularity_Snapshot_IndependentCopy verifies that mutating the
// returned map does not affect the internal state.
func TestMergedPopularity_Snapshot_IndependentCopy(t *testing.T) {
	mp := NewMergedPopularity()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	u := signUpdate(t, types.PeerId("peer1"), priv, map[string]int64{"blob1": 10})
	if err := mp.OnPopularityUpdate(u, 6.0, pub); err != nil {
		t.Fatalf("update: %v", err)
	}

	snap := mp.Snapshot()
	snap["injected"] = 999.0

	snap2 := mp.Snapshot()
	if _, ok := snap2["injected"]; ok {
		t.Fatal("mutation of returned map leaked into internal state")
	}
}

// TestMergedPopularity_Snapshot_ConcurrentWithUpdate runs Snapshot readers
// concurrently with OnPopularityUpdate writers to verify the RWMutex prevents
// data races. Run with -race.
func TestMergedPopularity_Snapshot_ConcurrentWithUpdate(t *testing.T) {
	mp := NewMergedPopularity()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	var done sync.WaitGroup
	stop := make(chan struct{})

	done.Add(1)
	go func() {
		defer done.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				u := signUpdate(t, types.PeerId("peer1"), priv,
					map[string]int64{"blob": int64(i + 1)})
				_ = mp.OnPopularityUpdate(u, 6.0, pub)
				i++
			}
		}
	}()

	done.Add(1)
	go func() {
		defer done.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = mp.Snapshot()
				_ = mp.getVideoPopularity("blob")
			}
		}
	}()

	close(stop)
	done.Wait()
}

// TestGossipSub_Snapshot_PropagatesAcrossHosts is the plan's acceptance
// integration test: node A publishes a popularity update; node B's
// MergedPopularity.Snapshot() must show the blob with heat > 0. This proves
// the gossip → Snapshot → eviction PopSource pipeline is wired end-to-end.
func TestGossipSub_Snapshot_PropagatesAcrossHosts(t *testing.T) {
	psk := genPSK(t)
	nodeA := newTestNode(t, psk)
	nodeB := newTestNode(t, psk)

	connectNodes(t, nodeA, nodeB)

	pidA := types.PeerId(nodeA.host.ID().String())
	pidB := types.PeerId(nodeB.host.ID().String())
	preSeedScore(nodeA.scorer, pidB, 11)
	preSeedScore(nodeB.scorer, pidA, 11)

	mpB := NewMergedPopularity()
	subB, err := nodeB.ps.Subscribe(PopularityTopic)
	if err != nil {
		t.Fatalf("subscribe B: %v", err)
	}
	defer subB.Cancel()

	time.Sleep(2 * time.Second)

	// Publish a signed update from A.
	go func() {
		rawPriv, err := nodeA.identity.PrivKey.Raw()
		if err != nil {
			t.Errorf("raw priv A: %v", err)
			return
		}
		update := PopularityUpdate{
			PeerID:    nodeA.identity.PeerID,
			Timestamp: time.Now().Unix(),
			Counts:    map[string]int64{"snap-blob": 7},
		}
		payload, _ := update.payloadForSigning()
		update.Sig = ed25519.Sign(ed25519.PrivateKey(rawPriv), payload)
		data := mustEncode(t, update)
		nodeA.topic.Publish(context.Background(), data)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := subB.Next(ctx)
	if err != nil {
		t.Fatalf("subB.Next: %v", err)
	}

	pubKey := nodeB.host.Peerstore().PubKey(msg.ReceivedFrom)
	if pubKey == nil {
		t.Fatal("pubkey not found in peerstore")
	}
	rawPub, err := pubKey.Raw()
	if err != nil {
		t.Fatalf("raw pub: %v", err)
	}

	var update PopularityUpdate
	if err := json.Unmarshal(msg.Data, &update); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	sourceScore := nodeB.scorer.GetScore(pidA)
	if err := mpB.OnPopularityUpdate(&update, sourceScore, rawPub); err != nil {
		t.Fatalf("on popularity update: %v", err)
	}

	snap := mpB.Snapshot()
	heat, ok := snap["snap-blob"]
	if !ok {
		t.Fatalf("expected snap-blob in Snapshot, got keys=%v", snapKeys(snap))
	}
	if heat <= 0 {
		t.Fatalf("expected positive heat, got %f", heat)
	}

	// 11 ICP successes × 0.5 = 5.5 source score; weighted heat = (5.5 * 7) / 5.5 = 7.0.
	if heat != 7.0 {
		t.Fatalf("expected heat 7.0, got %f", heat)
	}
}

func snapKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
