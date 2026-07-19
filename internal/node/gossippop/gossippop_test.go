package gossippop

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/node/libp2phost"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Helpers ───

// testNode wraps all the components needed for GossipSub integration tests.
type testNode struct {
	identity *sharedid.NodeIdentity
	host     host.Host
	ps       *pubsub.PubSub
	scorer   *PeerScorer
	topic    *pubsub.Topic
}

// genPSK returns a fresh 32-byte test PSK.
func genPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("gen psk: %v", err)
	}
	return types.PSK(psk)
}

// newTestNode creates a libp2p host with a fresh Ed25519 identity and a shared
// PSK, listening on a random TCP port. Returns a testNode with all components.
func newTestNode(t *testing.T, psk types.PSK) *testNode {
	t.Helper()
	ctx := context.Background()

	tmpDir := t.TempDir()
	identity, err := sharedid.LoadOrGenerateIdentity(tmpDir + "/ed25519.key")
	if err != nil {
		t.Fatalf("load identity: %v", err)
	}

	h, err := libp2phost.NewEdgeHost(identity, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("new edge host: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	scorer := NewPeerScorer()
	ps, err := NewGossipSub(ctx, h, scorer)
	if err != nil {
		t.Fatalf("new gossipsub: %v", err)
	}

	topic, err := ps.Join(PopularityTopic)
	if err != nil {
		t.Fatalf("join topic: %v", err)
	}

	return &testNode{
		identity: identity,
		host:     h,
		ps:       ps,
		scorer:   scorer,
		topic:    topic,
	}
}

// mustEncode marshals v to JSON, failing the test on error.
func mustEncode(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// connectNodes connects two test nodes by adding each other's addresses to
// their peerstores and dialing.
func connectNodes(t *testing.T, a, b *testNode) {
	t.Helper()
	a.host.Peerstore().AddAddrs(b.host.ID(), b.host.Addrs(), time.Hour)
	b.host.Peerstore().AddAddrs(a.host.ID(), a.host.Addrs(), time.Hour)

	if err := a.host.Connect(context.Background(), b.host.Peerstore().PeerInfo(b.host.ID())); err != nil {
		t.Fatalf("connect %s → %s: %v", a.host.ID(), b.host.ID(), err)
	}
}

// preSeedScore calls RecordICPSuccess n times on the scorer for the given
// PeerId to push the score above MinTrustedWeight.
func preSeedScore(scorer *PeerScorer, pid types.PeerId, n int) {
	for i := 0; i < n; i++ {
		scorer.RecordICPSuccess(pid)
	}
}

// ─── 1. LocalPopularity unit tests ───

func TestLocalPopularity_HitSnapshot(t *testing.T) {
	// Given: a fresh LocalPopularity
	lp := NewLocalPopularity()

	// When: we hit blob1 5 times
	for i := 0; i < 5; i++ {
		lp.Hit("blob1")
	}

	// Then: Snapshot returns 5 for blob1
	snap := lp.Snapshot()
	if snap["blob1"] != 5 {
		t.Fatalf("expected 5, got %d", snap["blob1"])
	}
	// Then: blob2 is absent
	if _, ok := snap["blob2"]; ok {
		t.Fatal("blob2 should not be in snapshot")
	}
}

func TestLocalPopularity_SnapshotEmpty(t *testing.T) {
	// Given: a fresh LocalPopularity
	lp := NewLocalPopularity()

	// When: snapshot called without any hits
	snap := lp.Snapshot()

	// Then: result is empty (not nil)
	if len(snap) != 0 {
		t.Fatalf("expected empty snapshot, got %d entries", len(snap))
	}
}

// ─── 2. PeerScorer unit tests ───

func TestPeerScorer_ICPSuccess(t *testing.T) {
	// Given: a fresh PeerScorer
	s := NewPeerScorer()
	pid := types.PeerId("peerA")

	// When: recording 3 ICP successes
	s.RecordICPSuccess(pid)
	s.RecordICPSuccess(pid)
	s.RecordICPSuccess(pid)

	// Then: score is 3 * 0.5 = 1.5
	score := s.GetScore(pid)
	if score != 1.5 {
		t.Fatalf("expected 1.5, got %f", score)
	}
}

func TestPeerScorer_ICPTimeout(t *testing.T) {
	// Given: a peer with score first boosted
	s := NewPeerScorer()
	pid := types.PeerId("peerB")
	preSeedScore(s, pid, 4) // score = 2.0

	// When: recording a timeout
	s.RecordICPTimeout(pid)

	// Then: score decreases to 1.0
	score := s.GetScore(pid)
	if score != 1.0 {
		t.Fatalf("expected 1.0, got %f", score)
	}
}

func TestPeerScorer_Misbehavior(t *testing.T) {
	// Given: a peer with score 0
	s := NewPeerScorer()
	pid := types.PeerId("peerC")

	// When: recording one misbehavior (score = -5.0, NOT below -20.0)
	s.RecordMisbehavior(pid, MisbehaviorInvalidSig)

	// Then: score is -5.0, not graylisted
	if s.GetScore(pid) != -5.0 {
		t.Fatalf("expected -5.0, got %f", s.GetScore(pid))
	}
	if s.IsGraylisted(pid) {
		t.Fatal("peer should not be graylisted at -5.0")
	}

	// When: recording misbehavior until below GraylistThreshold (-20.0)
	// Each misbehavior is -5.0, so 4 total = -20.0
	s.RecordMisbehavior(pid, MisbehaviorPoisonedHeat) // -10.0
	s.RecordMisbehavior(pid, MisbehaviorPoisonedHeat) // -15.0
	s.RecordMisbehavior(pid, MisbehaviorPoisonedHeat) // -20.0 → graylisted

	// Then: score is ≤ GraylistThreshold, graylisted
	if s.GetScore(pid) > GraylistThreshold {
		t.Fatalf("expected score <= %f, got %f", GraylistThreshold, s.GetScore(pid))
	}
	if !s.IsGraylisted(pid) {
		t.Fatal("peer should be graylisted")
	}
}

func TestPeerScorer_BandwidthContributed(t *testing.T) {
	// Given: a fresh PeerScorer
	s := NewPeerScorer()
	pid := types.PeerId("peerD")

	// When: recording 2 GB contributed
	s.RecordBandwidthContributed(pid, 2_000_000_000)

	// Then: score increases by 2.0 (bytes/1e9)
	if s.GetScore(pid) != 2.0 {
		t.Fatalf("expected 2.0, got %f", s.GetScore(pid))
	}
}

// ─── 3. MergedPopularity unit tests ───

func TestMergedPopularity_WeightedMerge(t *testing.T) {
	// Given: a MergedPopularity + ed25519 signer
	mp := NewMergedPopularity()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	// When: first update with score 3.0, reporting blob1 count 10
	u1 := signUpdate(t, types.PeerId("peer1"), priv, map[string]int64{"blob1": 10})
	if err := mp.OnPopularityUpdate(u1, 3.0, pub); err != nil {
		t.Fatalf("update1: %v", err)
	}

	// When: second update with score 6.0, reporting blob1 count 20
	u2 := signUpdate(t, types.PeerId("peer1"), priv, map[string]int64{"blob1": 20})
	if err := mp.OnPopularityUpdate(u2, 6.0, pub); err != nil {
		t.Fatalf("update2: %v", err)
	}

	// Then: weighted average = (3*10 + 6*20) / (3+6) = 150/9 ≈ 16.667
	// And TotalWeight = 9.0 > MinTrustedWeight → getVideoPopularity returns heat
	heat := mp.getVideoPopularity("blob1")
	expected := (3.0*10 + 6.0*20) / (3.0 + 6.0)
	if heat != expected {
		t.Fatalf("expected %f, got %f", expected, heat)
	}
}

func TestMergedPopularity_LowScoreDropped(t *testing.T) {
	// Given: MergedPopularity + signer
	mp := NewMergedPopularity()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	// When: update from peer with score < GraylistThreshold (-10.0)
	u := signUpdate(t, types.PeerId("badpeer"), priv, map[string]int64{"blob1": 10})
	err = mp.OnPopularityUpdate(u, GraylistThreshold-1, pub)

	// Then: update is dropped
	if err == nil {
		t.Fatal("expected error for low score, got nil")
	}

	// Then: blob1 has no entry
	heat := mp.getVideoPopularity("blob1")
	if heat != 0 {
		t.Fatalf("expected 0 heat, got %f", heat)
	}
}

func TestMergedPopularity_BelowMinTrustedWeight(t *testing.T) {
	// Given: MergedPopularity + signer
	mp := NewMergedPopularity()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	// When: update from a peer with score 2.0 (TotalWeight = 2.0 < MinTrustedWeight 5.0)
	u := signUpdate(t, types.PeerId("peer1"), priv, map[string]int64{"blob1": 10})
	if err := mp.OnPopularityUpdate(u, 2.0, pub); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Then: getVideoPopularity returns 0 (not enough trusted weight)
	heat := mp.getVideoPopularity("blob1")
	if heat != 0 {
		t.Fatalf("expected 0 (below MinTrustedWeight), got %f", heat)
	}

	// When: another update pushes TotalWeight to 4.0 (still < 5.0)
	u2 := signUpdate(t, types.PeerId("peer1"), priv, map[string]int64{"blob1": 20})
	if err := mp.OnPopularityUpdate(u2, 2.0, pub); err != nil {
		t.Fatalf("update2: %v", err)
	}
	heat = mp.getVideoPopularity("blob1")
	if heat != 0 {
		t.Fatalf("expected 0 (still below MinTrustedWeight), got %f", heat)
	}

	// When: final update pushes past threshold
	u3 := signUpdate(t, types.PeerId("peer1"), priv, map[string]int64{"blob1": 30})
	if err := mp.OnPopularityUpdate(u3, 2.0, pub); err != nil {
		t.Fatalf("update3: %v", err)
	}

	// Then: now TotalWeight = 6.0 > 5.0, getVideoPopularity returns weighted heat
	heat = mp.getVideoPopularity("blob1")
	expected := (2.0*10 + 2.0*20 + 2.0*30) / 6.0 // = 120/6 = 20
	if heat != expected {
		t.Fatalf("expected %f, got %f", expected, heat)
	}
}

// ─── 4. GossipSub integration tests ───

func TestGossipSub_2NodePopularitySync(t *testing.T) {
	// Given: two hosts sharing a PSK
	psk := genPSK(t)

	nodeA := newTestNode(t, psk)
	nodeB := newTestNode(t, psk)

	connectNodes(t, nodeA, nodeB)

	// Pre-seed scores: 11 * 0.5 = 5.5 > MinTrustedWeight (5.0)
	pidA := types.PeerId(nodeA.host.ID().String())
	pidB := types.PeerId(nodeB.host.ID().String())
	preSeedScore(nodeA.scorer, pidB, 11)
	preSeedScore(nodeB.scorer, pidA, 11)

	// Host B subscribes to the topic for receiving.
	mpB := NewMergedPopularity()
	subB, err := nodeB.ps.Subscribe(PopularityTopic)
	if err != nil {
		t.Fatalf("subscribe B: %v", err)
	}
	defer subB.Cancel()

	// Wait for mesh to form via heartbeat exchanges.
	time.Sleep(2 * time.Second)

	// When: node A records blob hits and publishes.
	lpA := NewLocalPopularity()
	lpA.Hit("blob1")
	lpA.Hit("blob1")
	lpA.Hit("blob1") // count = 3

	// Publish immediately.
	go func() {
		snapshot := lpA.Snapshot()
		update := PopularityUpdate{
			PeerID:    nodeA.identity.PeerID,
			Timestamp: time.Now().Unix(),
			Counts:    snapshot,
		}
		payload, _ := update.payloadForSigning()
		rawPriv, err := nodeA.identity.PrivKey.Raw()
		if err != nil {
			t.Errorf("raw priv key A: %v", err)
			return
		}
		update.Sig = ed25519.Sign(ed25519.PrivateKey(rawPriv), payload)
		data := mustEncode(t, update)
		_ = nodeA.topic.Publish(context.Background(), data) // best-effort: test fixture
	}()

	// When: node B receives and processes.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := subB.Next(ctx)
	if err != nil {
		t.Fatalf("subB.Next: %v", err)
	}

	var update PopularityUpdate
	if err := json.Unmarshal(msg.Data, &update); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Verify signature using pubkey from node B's peerstore.
	pubKey := nodeB.host.Peerstore().PubKey(msg.ReceivedFrom)
	if pubKey == nil {
		t.Fatal("pubkey not found in peerstore")
	}
	rawPub, err := pubKey.Raw()
	if err != nil {
		t.Fatalf("raw pub: %v", err)
	}
	sourceScore := nodeB.scorer.GetScore(pidA)

	if err := mpB.OnPopularityUpdate(&update, sourceScore, rawPub); err != nil {
		t.Fatalf("on popularity update: %v", err)
	}

	// Then: node B's merged popularity has correct weighted value (5.5 * 3 / 5.5 = 3.0).
	heat := mpB.getVideoPopularity("blob1")
	if heat <= 0 {
		t.Fatalf("expected positive heat for blob1, got %f", heat)
	}
	if heat != 3.0 {
		t.Fatalf("expected heat 3.0, got %f", heat)
	}
}

func TestGossipSub_3NodePoisoningDefense(t *testing.T) {
	// Given: three nodes — one attacker (low score) and two honest (high score).
	psk := genPSK(t)

	nodeH1 := newTestNode(t, psk)
	nodeH2 := newTestNode(t, psk)
	nodeAtt := newTestNode(t, psk)

	// Connect all to each other.
	connectNodes(t, nodeH1, nodeH2)
	connectNodes(t, nodeH1, nodeAtt)
	connectNodes(t, nodeH2, nodeAtt)

	pidH1 := types.PeerId(nodeH1.host.ID().String())
	pidH2 := types.PeerId(nodeH2.host.ID().String())
	pidAtt := types.PeerId(nodeAtt.host.ID().String())

	// Both honest nodes give each other high scores.
	preSeedScore(nodeH1.scorer, pidH2, 11)
	preSeedScore(nodeH2.scorer, pidH1, 11)

	// Honest node 2 greylists the attacker.
	nodeH2.scorer.RecordMisbehavior(pidAtt, MisbehaviorInvalidSig)
	nodeH2.scorer.RecordMisbehavior(pidAtt, MisbehaviorInvalidSig)
	nodeH2.scorer.RecordMisbehavior(pidAtt, MisbehaviorInvalidSig)
	nodeH2.scorer.RecordMisbehavior(pidAtt, MisbehaviorInvalidSig)

	if !nodeH2.scorer.IsGraylisted(pidAtt) {
		t.Fatal("attacker should be graylisted on H2")
	}

	// Honest node 2's merged popularity view.
	mpH2 := NewMergedPopularity()

	subH2, err := nodeH2.ps.Subscribe(PopularityTopic)
	if err != nil {
		t.Fatalf("subscribe H2: %v", err)
	}
	defer subH2.Cancel()

	// Wait for mesh to form.
	time.Sleep(2 * time.Second)

	// Pre-compute pubkeys from identities.
	rawPubH1, _ := nodeH1.identity.PrivKey.GetPublic().Raw()
	rawPubAtt, _ := nodeAtt.identity.PrivKey.GetPublic().Raw()

	// When: attacker publishes poisoned update.
	rawPrivAtt, _ := nodeAtt.identity.PrivKey.Raw()
	attUpdate := createUpdate(t, nodeAtt.identity.PeerID, ed25519.PrivateKey(rawPrivAtt), map[string]int64{"blob1": 99999})
	_ = nodeAtt.topic.Publish(context.Background(), mustEncode(t, attUpdate)) // best-effort: test fixture

	// When: honest node 1 publishes truthful update.
	rawPrivH1, _ := nodeH1.identity.PrivKey.Raw()
	h1Update := createUpdate(t, nodeH1.identity.PeerID, ed25519.PrivateKey(rawPrivH1), map[string]int64{"blob1": 10})
	_ = nodeH1.topic.Publish(context.Background(), mustEncode(t, h1Update)) // best-effort: test fixture

	// Then: process messages sequentially — attacker's dropped, honest's merged.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Process up to 2 messages.
	for i := 0; i < 2; i++ {
		msg, err := subH2.Next(ctx)
		if err != nil {
			t.Logf("message %d: %v (continuing)", i, err)
			break
		}

		var update PopularityUpdate
		if err := json.Unmarshal(msg.Data, &update); err != nil {
			t.Logf("unmarshal error for message %d: %v", i, err)
			continue
		}

		var rawPub []byte
		sourceScore := 0.0
		switch msg.ReceivedFrom {
		case nodeH1.host.ID():
			sourceScore = nodeH2.scorer.GetScore(pidH1)
			rawPub = rawPubH1
		case nodeAtt.host.ID():
			sourceScore = nodeH2.scorer.GetScore(pidAtt)
			rawPub = rawPubAtt
		default:
			t.Logf("unknown sender: %s", msg.ReceivedFrom)
			continue
		}

		if err := mpH2.OnPopularityUpdate(&update, sourceScore, rawPub); err != nil {
			t.Logf("update %d from %s rejected: %v", i, msg.ReceivedFrom, err)
		} else {
			t.Logf("update %d from %s accepted, counts=%v", i, msg.ReceivedFrom, update.Counts)
		}
	}

	// Then: blob1's heat should equal the honest count (10), not poisoned (99999).
	heat := mpH2.getVideoPopularity("blob1")
	if heat != 10.0 {
		t.Fatalf("expected heat 10.0 (honest update only), got %f", heat)
	}
}

// ─── Signing helpers ───

// signUpdate creates and signs a PopularityUpdate for testing.
func signUpdate(t *testing.T, peerID types.PeerId, priv ed25519.PrivateKey, counts map[string]int64) *PopularityUpdate {
	t.Helper()
	u := &PopularityUpdate{
		PeerID:    peerID,
		Timestamp: time.Now().Unix(),
		Counts:    counts,
	}
	payload, err := u.payloadForSigning()
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	u.Sig = ed25519.Sign(priv, payload)
	return u
}

// createUpdate creates and signs a PopularityUpdate (alias for readability).
func createUpdate(t *testing.T, peerID types.PeerId, priv ed25519.PrivateKey, counts map[string]int64) *PopularityUpdate {
	t.Helper()
	return signUpdate(t, peerID, priv, counts)
}

// ─── 5. HandlePopularityMessage trust-guard tests (T19) ───

// stubHostAdapter returns a fixed Ed25519 public key for any peer.ID.
type stubHostAdapter struct {
	pub ed25519.PublicKey
}

func (s stubHostAdapter) Peerstore() interface {
	PubKey(peer.ID) ed25519.PublicKey
} {
	return stubPubKeyView{pub: s.pub}
}

type stubPubKeyView struct {
	pub ed25519.PublicKey
}

func (v stubPubKeyView) PubKey(_ peer.ID) ed25519.PublicKey { return v.pub }

// stubPeerEntryLookup is a configurable PeerEntryLookup for testing the
// stale/unknown guard. Returns the configured stale/unknown verdict per peer.
type stubPeerEntryLookup struct {
	known map[types.PeerId]bool // true = known AND not stale; absent = unknown
}

func (s stubPeerEntryLookup) StaleOrUnknown(peerID types.PeerId) bool {
	known, ok := s.known[peerID]
	if !ok {
		return true // unknown
	}
	return !known // known but stale
}

// makeSignedMessage builds a pubsub.Message carrying a signed PopularityUpdate
// from srcPeer, signed with priv. The embedded From is also set to srcPeer so
// signature verification against the source peer's key succeeds.
func makeSignedMessage(t *testing.T, srcPeer peer.ID, peerID types.PeerId, priv ed25519.PrivateKey, counts map[string]int64) *pubsub.Message {
	t.Helper()
	update := signUpdate(t, peerID, priv, counts)
	data, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}
	return &pubsub.Message{
		Message:      &pb.Message{Data: data, From: []byte(srcPeer)},
		ReceivedFrom: srcPeer,
	}
}

// runHandle wires MergedPopularity + PeerScorer + stub host + stub lookup and
// invokes HandlePopularityMessage. Returns the MergedPopularity for assertion.
func runHandle(t *testing.T, scorer *PeerScorer, pub ed25519.PublicKey, lookup PeerEntryLookup, msg *pubsub.Message) *MergedPopularity {
	t.Helper()
	mp := NewMergedPopularity()
	HandlePopularityMessage(mp, scorer, msg, stubHostAdapter{pub: pub}, lookup)
	return mp
}

// TestHandlePopularityMessage_StalePeerDiscarded asserts that an update from a
// peer marked Stale in the PeerEntryStore is discarded — no entry is created.
func TestHandlePopularityMessage_StalePeerDiscarded(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	srcPeer := peer.ID("stale-peer-id")
	srcPeerID := types.PeerId(srcPeer.String())

	scorer := NewPeerScorer()
	preSeedScore(scorer, srcPeerID, 11) // high score — would normally pass

	lookup := stubPeerEntryLookup{known: map[types.PeerId]bool{srcPeerID: false}} // known but Stale=true

	msg := makeSignedMessage(t, srcPeer, srcPeerID, priv, map[string]int64{"blob1": 99999})

	mp := runHandle(t, scorer, pub, lookup, msg)

	if heat := mp.getVideoPopularity("blob1"); heat != 0 {
		t.Fatalf("stale peer update must be discarded; got heat=%f", heat)
	}
	if snap := mp.Snapshot(); len(snap) != 0 {
		t.Fatalf("stale peer update must not appear in Snapshot; got %v", snap)
	}
}

// TestHandlePopularityMessage_UnknownPeerDiscarded asserts that an update from
// a peer absent from the PeerEntryStore is discarded.
func TestHandlePopularityMessage_UnknownPeerDiscarded(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	srcPeer := peer.ID("unknown-peer-id")
	srcPeerID := types.PeerId(srcPeer.String())

	scorer := NewPeerScorer()
	preSeedScore(scorer, srcPeerID, 11) // high score — would normally pass

	lookup := stubPeerEntryLookup{known: map[types.PeerId]bool{}} // empty → everyone unknown

	msg := makeSignedMessage(t, srcPeer, srcPeerID, priv, map[string]int64{"blob1": 99999})

	mp := runHandle(t, scorer, pub, lookup, msg)

	if heat := mp.getVideoPopularity("blob1"); heat != 0 {
		t.Fatalf("unknown peer update must be discarded; got heat=%f", heat)
	}
	if snap := mp.Snapshot(); len(snap) != 0 {
		t.Fatalf("unknown peer update must not appear in Snapshot; got %v", snap)
	}
}

// TestHandlePopularityMessage_NormalPeerMerges asserts the happy path: when
// the peer is known and not stale, the update merges into the local view.
// This guards against the trust guard accidentally over-rejecting.
func TestHandlePopularityMessage_NormalPeerMerges(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	srcPeer := peer.ID("known-good-peer-id")
	srcPeerID := types.PeerId(srcPeer.String())

	scorer := NewPeerScorer()
	preSeedScore(scorer, srcPeerID, 11) // score 5.5 > MinTrustedWeight

	lookup := stubPeerEntryLookup{known: map[types.PeerId]bool{srcPeerID: true}} // known, not stale

	msg := makeSignedMessage(t, srcPeer, srcPeerID, priv, map[string]int64{"blob1": 7})

	mp := runHandle(t, scorer, pub, lookup, msg)

	heat := mp.getVideoPopularity("blob1")
	if heat != 7.0 {
		t.Fatalf("normal peer update should merge to heat=7.0 (single source, score>MinTrustedWeight); got %f", heat)
	}
}

// TestHandlePopularityMessage_ForgedSignatureDiscarded asserts defense layer 2
// (signature verification, mergedpop.go:86-93): a forged signature causes the
// update to be discarded even when the peer is known, not stale, and has a
// high score.
func TestHandlePopularityMessage_ForgedSignatureDiscarded(t *testing.T) {
	goodPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen good key: %v", err)
	}
	// Different key used to sign → signature won't verify against goodPub.
	_, evilPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen evil key: %v", err)
	}

	srcPeer := peer.ID("forged-sig-peer-id")
	srcPeerID := types.PeerId(srcPeer.String())

	scorer := NewPeerScorer()
	preSeedScore(scorer, srcPeerID, 11)

	lookup := stubPeerEntryLookup{known: map[types.PeerId]bool{srcPeerID: true}}

	// Sign with evilPriv, but the host adapter will return goodPub for verification.
	update := &PopularityUpdate{
		PeerID:    srcPeerID,
		Timestamp: time.Now().Unix(),
		Counts:    map[string]int64{"blob1": 100},
	}
	payload, err := update.payloadForSigning()
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	update.Sig = ed25519.Sign(evilPriv, payload)

	data, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := &pubsub.Message{
		Message:      &pb.Message{Data: data, From: []byte(srcPeer)},
		ReceivedFrom: srcPeer,
	}

	mp := runHandle(t, scorer, goodPub, lookup, msg)

	if heat := mp.getVideoPopularity("blob1"); heat != 0 {
		t.Fatalf("forged-signature update must be discarded; got heat=%f", heat)
	}
}

// TestHandlePopularityMessage_GraylistedPeerDiscarded asserts defense layer 3
// (GraylistThreshold floor, mergedpop.go:96): an update from a peer whose
// score is at/below GraylistThreshold is discarded even if known & not stale.
func TestHandlePopularityMessage_GraylistedPeerDiscarded(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	srcPeer := peer.ID("graylisted-peer-id")
	srcPeerID := types.PeerId(srcPeer.String())

	scorer := NewPeerScorer()
	// 5 misbehaviors × -5.0 = -25.0 ≤ GraylistThreshold (-20.0)
	for i := 0; i < 5; i++ {
		scorer.RecordMisbehavior(srcPeerID, MisbehaviorInvalidSig)
	}
	if !scorer.IsGraylisted(srcPeerID) {
		t.Fatalf("peer should be graylisted at score %f", scorer.GetScore(srcPeerID))
	}

	lookup := stubPeerEntryLookup{known: map[types.PeerId]bool{srcPeerID: true}}

	msg := makeSignedMessage(t, srcPeer, srcPeerID, priv, map[string]int64{"blob1": 99999})

	mp := runHandle(t, scorer, pub, lookup, msg)

	if heat := mp.getVideoPopularity("blob1"); heat != 0 {
		t.Fatalf("graylisted peer update must be discarded; got heat=%f", heat)
	}
	if snap := mp.Snapshot(); len(snap) != 0 {
		t.Fatalf("graylisted peer update must not appear in Snapshot; got %v", snap)
	}
}

// TestHandlePopularityMessage_LowScoreDoesNotBreachMinTrustedWeight asserts
// defense layer 4 (MinTrustedWeight output gate, mergedpop.go:124-133):
// a single low-score peer's contribution is merged into MergedPopularity but
// is filtered out of Snapshot() and getVideoPopularity() because
// TotalWeight < MinTrustedWeight. This is the critical gate that prevents a
// single malicious peer from poisoning eviction ranking.
func TestHandlePopularityMessage_LowScoreDoesNotBreachMinTrustedWeight(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	srcPeer := peer.ID("low-score-peer-id")
	srcPeerID := types.PeerId(srcPeer.String())

	scorer := NewPeerScorer()
	preSeedScore(scorer, srcPeerID, 3) // score 1.5 > GraylistThreshold but < MinTrustedWeight (5.0)

	lookup := stubPeerEntryLookup{known: map[types.PeerId]bool{srcPeerID: true}}

	msg := makeSignedMessage(t, srcPeer, srcPeerID, priv, map[string]int64{"blob1": 99999})

	mp := runHandle(t, scorer, pub, lookup, msg)

	// The entry exists internally (TotalWeight = 1.5, below MinTrustedWeight=5.0).
	mp.mu.RLock()
	entry, ok := mp.entries["blob1"]
	mp.mu.RUnlock()
	if !ok {
		t.Fatal("low-score update should be merged into internal entries (gate is on output, not input)")
	}
	if entry.TotalWeight >= MinTrustedWeight {
		t.Fatalf("TotalWeight=%f must be below MinTrustedWeight=%f to exercise the gate",
			entry.TotalWeight, MinTrustedWeight)
	}
	heatBefore := entry.WeightedHeat
	weightBefore := entry.TotalWeight

	// But Snapshot filters it out — the eviction PopSource never sees it.
	if snap := mp.Snapshot(); len(snap) != 0 {
		t.Fatalf("low-score contribution must NOT breach MinTrustedWeight gate; Snapshot got %v", snap)
	}
	// And getVideoPopularity returns 0 (fallback).
	if heat := mp.getVideoPopularity("blob1"); heat != 0 {
		t.Fatalf("low-score heat must be gated to 0; got %f", heat)
	}

	// Defense layer 5 (zero-immunity): a follow-up update from a peer whose
	// sourceScore=0 (the zero-score/zero-weight injection case the incremental
	// formula at mergedpop.go:110-114 is naturally immune to) must not change
	// WeightedHeat OR TotalWeight. Use a separate scorer so the source peer's
	// score is exactly 0 (RecordICPSuccess was never called for it).
	zeroScorer := NewPeerScorer() // srcPeerID score == 0
	msg2 := makeSignedMessage(t, srcPeer, srcPeerID, priv, map[string]int64{"blob1": 99999})
	HandlePopularityMessage(mp, zeroScorer, msg2, stubHostAdapter{pub: pub}, lookup)

	mp.mu.RLock()
	entry2 := mp.entries["blob1"]
	mp.mu.RUnlock()
	if entry2.WeightedHeat != heatBefore {
		t.Fatalf("zero-score injection must not change WeightedHeat; before=%f after=%f",
			heatBefore, entry2.WeightedHeat)
	}
	if entry2.TotalWeight != weightBefore {
		t.Fatalf("zero-score injection must not change TotalWeight; before=%f after=%f",
			weightBefore, entry2.TotalWeight)
	}
}

// TestHandlePopularityMessage_NilLookupSkipsGuard asserts that a nil
// PeerEntryLookup preserves backward-compatible behaviour: the trust guard is
// skipped, and existing defenses (signature, graylist, MinTrustedWeight)
// remain in force.
func TestHandlePopularityMessage_NilLookupSkipsGuard(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	srcPeer := peer.ID("nil-lookup-peer-id")
	srcPeerID := types.PeerId(srcPeer.String())

	scorer := NewPeerScorer()
	preSeedScore(scorer, srcPeerID, 11) // > MinTrustedWeight

	msg := makeSignedMessage(t, srcPeer, srcPeerID, priv, map[string]int64{"blob1": 7})

	mp := NewMergedPopularity()
	HandlePopularityMessage(mp, scorer, msg, stubHostAdapter{pub: pub}, nil)

	heat := mp.getVideoPopularity("blob1")
	if heat != 7.0 {
		t.Fatalf("nil lookup should skip guard and merge normally; got heat=%f", heat)
	}
}
