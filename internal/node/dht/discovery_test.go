package dht

import (
	"context"
	"crypto/rand"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	discoveryrouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"

	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Test helpers ───────────────────────────────────────────────────────────

func testPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("generate PSK: %v", err)
	}
	return types.PSK(psk)
}

func testHost(t *testing.T, psk types.PSK) host.Host {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	id := &sharedid.NodeIdentity{PrivKey: priv, PeerID: types.PeerId(pid.String())}
	h, err := libp2phost.NewEdgeHost(id, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func tempPeerStore(t *testing.T) *peerstore.PeerEntryStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "peers.db")
	s, err := peerstore.NewPeerEntryStore(dbPath)
	if err != nil {
		t.Fatalf("create PeerEntryStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// connectMutual establishes bi-directional p2p connections and waits for
// the DHT routing tables to stabilize.
func connectMutual(t *testing.T, ctx context.Context, a, b host.Host) {
	t.Helper()
	piA := peer.AddrInfo{ID: a.ID(), Addrs: a.Addrs()}
	piB := peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}
	if err := a.Connect(ctx, piB); err != nil {
		t.Fatalf("connect %s -> %s: %v", a.ID(), b.ID(), err)
	}
	if err := b.Connect(ctx, piA); err != nil {
		t.Fatalf("connect %s -> %s: %v", b.ID(), a.ID(), err)
	}
	time.Sleep(time.Second)
}

// populateRoutingTables triggers DHT FindPeer in both directions so both
// routing tables learn about each other.
func populateRoutingTables(ctx context.Context, a, b *kaddht.IpfsDHT, aid, bid peer.ID) {
	go a.FindPeer(ctx, bid)
	b.FindPeer(ctx, aid)
	time.Sleep(500 * time.Millisecond) // let async queries land
}

// ─── Tests ──────────────────────────────────────────────────────────────────

func TestDHT_AdvertiseFindPeers(t *testing.T) {
	psk := testPSK(t)
	hostA := testHost(t, psk)
	hostB := testHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	storeA := tempPeerStore(t)
	storeB := tempPeerStore(t)

	dhtA, err := kaddht.New(ctx, hostA, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create dht A: %v", err)
	}
	dhtB, err := kaddht.New(ctx, hostB, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create dht B: %v", err)
	}

	connectMutual(t, ctx, hostA, hostB)
	populateRoutingTables(ctx, dhtA, dhtB, hostA.ID(), hostB.ID())

	discA := NewEdgeDiscovery(hostA, storeA, nil, "edge", 15*time.Minute, 0, kaddht.ModeServer)
	discA.dht = dhtA
	discA.routingDisc = discoveryrouting.NewRoutingDiscovery(dhtA)

	discB := NewEdgeDiscovery(hostB, storeB,
		[]peer.AddrInfo{{ID: hostA.ID(), Addrs: hostA.Addrs()}},
		"edge", 15*time.Minute, 0, kaddht.ModeServer)
	discB.dht = dhtB
	discB.routingDisc = discoveryrouting.NewRoutingDiscovery(dhtB)

	// Given: A advertises and B discovers
	ttl, err := discA.routingDisc.Advertise(ctx, "edge", discovery.TTL(15*time.Minute))
	if err != nil {
		t.Fatalf("A advertise: %v", err)
	}
	t.Logf("A advertised with TTL: %v", ttl)

	peerChan, err := discB.routingDisc.FindPeers(ctx, "edge", discovery.Limit(50))
	if err != nil {
		t.Fatalf("B FindPeers: %v", err)
	}

	var foundA bool
	for p := range peerChan {
		if p.ID == hostA.ID() {
			foundA = true
		}
		addrs := make([]string, 0, len(p.Addrs))
		for _, a := range p.Addrs {
			addrs = append(addrs, a.String())
		}
		if err := storeB.Put(peerstore.PeerIdFromPeerID(p.ID),
			peerstore.PeerStoreEntryFromDiscovery(p.ID, addrs)); err != nil {
			t.Logf("store peer %s: %v", p.ID, err)
		}
	}

	// Then: B should have discovered A.
	if !foundA {
		t.Fatalf("B did not discover A")
	}

	entry, found := storeB.Get(peerstore.PeerIdFromPeerID(hostA.ID()))
	if !found {
		t.Fatalf("A not in B's PeerEntryStore")
	}
	if len(entry.Addrs) == 0 {
		t.Errorf("discovered peer has no addresses")
	}
	t.Logf("B discovered A: peer=%s addrs=%v", entry.PeerID, entry.Addrs)
}

func TestDHT_AdvertiseTTLReturned(t *testing.T) {
	psk := testPSK(t)
	hostA := testHost(t, psk)
	hostB := testHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	storeA := tempPeerStore(t)

	dhtA, err := kaddht.New(ctx, hostA, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create dht A: %v", err)
	}
	dhtB, err := kaddht.New(ctx, hostB, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create dht B: %v", err)
	}

	connectMutual(t, ctx, hostA, hostB)
	populateRoutingTables(ctx, dhtA, dhtB, hostA.ID(), hostB.ID())

	discA := NewEdgeDiscovery(hostA, storeA, nil, "edge", 15*time.Minute, 0, kaddht.ModeServer)
	discA.dht = dhtA
	discA.routingDisc = discoveryrouting.NewRoutingDiscovery(dhtA)

	// Given: a DHT with a connected peer in its routing table
	// When: we call Advertise
	ttl, err := discA.routingDisc.Advertise(ctx, "edge", discovery.TTL(15*time.Minute))
	// Then: a non-zero effective TTL is returned
	if err != nil {
		t.Fatalf("advertise: %v", err)
	}
	if ttl == 0 {
		t.Errorf("advertise returned zero TTL, expected non-zero")
	}
	t.Logf("advertise effective TTL: %v", ttl)
}

func TestDHT_BootstrapUnreachable(t *testing.T) {
	psk := testPSK(t)
	h := testHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := tempPeerStore(t)

	// Given: a bootstrap addr with an unreachable peer (wrong PSK → reject)
	unreachable := peer.AddrInfo{
		ID:    "12D3KooWUnreachablePeerId0000000000001",
		Addrs: h.Addrs(),
	}

	disc := NewEdgeDiscovery(
		h, store,
		[]peer.AddrInfo{unreachable},
		"edge", 15*time.Minute, 0,
		kaddht.ModeClient,
	)

	// When: Start is called
	err := disc.Start(ctx)
	// Then: it returns the bootstrap connection error
	if err == nil {
		t.Errorf("expected error from unreachable bootstrap, got nil")
	}
	t.Logf("expected bootstrap error: %v", err)
}

func TestDHT_HeartbeatLoop(t *testing.T) {
	psk := testPSK(t)
	hostA := testHost(t, psk)
	hostB := testHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	storeA := tempPeerStore(t)

	dhtA, err := kaddht.New(ctx, hostA, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create dht A: %v", err)
	}
	dhtB, err := kaddht.New(ctx, hostB, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create dht B: %v", err)
	}

	connectMutual(t, ctx, hostA, hostB)
	populateRoutingTables(ctx, dhtA, dhtB, hostA.ID(), hostB.ID())

	discA := NewEdgeDiscovery(hostA, storeA, nil, "edge", 1*time.Second, 0, kaddht.ModeServer)
	discA.dht = dhtA
	discA.routingDisc = discoveryrouting.NewRoutingDiscovery(dhtA)

	// Start the heartbeat and discover loops.
	go discA.heartbeatLoop(ctx)
	go discA.discoverLoop(ctx)

	// Given: a running heartbeat loop with fast interval
	// When: we wait for multiple heartbeat cycles
	time.Sleep(4 * time.Second)

	// Then: Advertise still works (heartbeat loop hasn't crashed)
	ttl, err := discA.routingDisc.Advertise(ctx, "edge", discovery.TTL(15*time.Minute))
	if err != nil {
		t.Fatalf("advertise after heartbeat: %v", err)
	}
	if ttl == 0 {
		t.Errorf("advertise returned zero TTL after heartbeat loop")
	}
	t.Logf("advertise TTL after heartbeat: %v", ttl)
}

func TestDHT_PeX(t *testing.T) {
	psk := testPSK(t)
	h := testHost(t, psk)

	store := tempPeerStore(t)

	disc := NewEdgeDiscovery(
		h, store,
		nil,
		"edge", 15*time.Minute, 0,
		kaddht.ModeServer,
	)

	// Given: a PeerRecord for a synthetic peer
	rec := peer.PeerRecord{
		PeerID: "12D3KooWSyntheticPeer1234567890abcdef",
		Addrs:  h.Addrs(),
		Seq:    peer.TimestampSeq(),
	}

	// When: OnGossipSubPruneWithPX is called
	disc.OnGossipSubPruneWithPX(h.ID(), []peer.PeerRecord{rec})

	// Then: the peer is in the PeerEntryStore
	entry, found := store.Get(peerstore.PeerIdFromPeerID(rec.PeerID))
	if !found {
		t.Fatalf("PeX peer was not stored in PeerEntryStore")
	}
	if len(entry.Addrs) == 0 {
		t.Errorf("PeX peer stored but has no addresses")
	}
	if entry.LastSeen == 0 {
		t.Errorf("PeX peer stored but LastSeen is zero")
	}
	t.Logf("PeX peer stored: peer=%s addrs=%v last_seen=%d", entry.PeerID, entry.Addrs, entry.LastSeen)
}

// TestDHT_AdvertiseIntervalWired verifies that the advertiseInterval parameter
// (T15) actually drives the heartbeat re-advertise ticker — i.e., the dormant
// config field is now wired through. We construct an EdgeDiscovery with a
// short advertiseInterval and a long advertiseTTL, run the heartbeat loop,
// and confirm that re-advertise fires at the interval (not at TTL/2).
func TestDHT_AdvertiseIntervalWired(t *testing.T) {
	psk := testPSK(t)
	hostA := testHost(t, psk)
	hostB := testHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	storeA := tempPeerStore(t)

	dhtA, err := kaddht.New(ctx, hostA, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create dht A: %v", err)
	}
	dhtB, err := kaddht.New(ctx, hostB, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create dht B: %v", err)
	}

	connectMutual(t, ctx, hostA, hostB)
	populateRoutingTables(ctx, dhtA, dhtB, hostA.ID(), hostB.ID())

	// TTL of 30m (so TTL/2 = 15m — would not fire within the test window).
	// advertiseInterval of 500ms — should drive the heartbeat ticker.
	const (
		longTTL    = 30 * time.Minute
		shortInter = 500 * time.Millisecond
	)

	discA := NewEdgeDiscovery(hostA, storeA, nil, "edge", longTTL, shortInter, kaddht.ModeServer)
	discA.dht = dhtA
	discA.routingDisc = discoveryrouting.NewRoutingDiscovery(dhtA)

	// Count re-advertises by capturing the logger. The count is atomic
	// because the heartbeat goroutine writes while the test reads.
	var heartbeats atomic.Uint64
	countingLogger := slog.New(slog.NewTextHandler(&countingWriter{count: &heartbeats},
		&slog.HandlerOptions{Level: slog.LevelDebug}))
	discA.logger = countingLogger.With("component", "dht")

	go discA.heartbeatLoop(ctx)

	// Poll for ≥3 heartbeats at 500ms each. Bounded by a 2s deadline.
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if heartbeats.Load() >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Errorf("expected ≥3 heartbeat re-advertise log lines at interval=%v, got %d",
				shortInter, heartbeats.Load())
			return
		case <-ticker.C:
		}
	}
	t.Logf("heartbeats observed in 2s at interval=%v: %d", shortInter, heartbeats.Load())
}

// countingWriter is an io.Writer that counts lines written. Each Advertise
// log entry produces one line containing "heartbeat re-advertised" or
// "advertised in DHT namespace".
type countingWriter struct {
	count *atomic.Uint64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	if bytes := string(p); bytes != "" {
		if containsAdvertiseMsg(bytes) {
			w.count.Add(1)
		}
	}
	return len(p), nil
}

func containsAdvertiseMsg(s string) bool {
	// Match either "advertised in DHT namespace" (success) or
	// "heartbeat re-advertise" (success or failure). The wiring assertion
	// is "the ticker fired and called Advertise" — the result is irrelevant
	// for proving the dormant field now drives the loop.
	return contains(s, "advertised in DHT namespace") || contains(s, "heartbeat re-advertise")
}

func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
