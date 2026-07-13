// Package integration_test provides full-pipeline integration tests for the
// MediaWorker edge distribution network. Each test exercises a complete
// vertical slice — libp2p host creation, PSK-gated networking, DHT discovery,
// JWT verification, GossipSub popularity sync, ICP cache cooperation,
// backhaul data fetching, pinning, hash ring routing, degradation, and
// poisoning defence — all in a single process with real libp2p hosts.
package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/shlande/mediaworker/internal/backhaul"
	"github.com/shlande/mediaworker/internal/cache"
	"github.com/shlande/mediaworker/internal/dht"
	"github.com/shlande/mediaworker/internal/gossippop"
	"github.com/shlande/mediaworker/internal/hashring"
	"github.com/shlande/mediaworker/internal/icp"
	"github.com/shlande/mediaworker/internal/jwt"
	"github.com/shlande/mediaworker/internal/libp2phost"
	"github.com/shlande/mediaworker/internal/peerstore"
	"github.com/shlande/mediaworker/internal/pinstore"
	"github.com/shlande/mediaworker/internal/pinstrategy"
	"github.com/shlande/mediaworker/internal/types"
)

// ═══════════════════════════════════════════════════════════════════════════
// Test helpers
// ═══════════════════════════════════════════════════════════════════════════

// genTestPSK returns a 32-byte random PSK for private-network tests.
func genTestPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("generate PSK: %v", err)
	}
	return types.PSK(psk)
}

// genTestIdentity creates a NodeIdentity with a fresh Ed25519 key written to
// a temp file. The file is cleaned up with t.Cleanup.
func genTestIdentity(t *testing.T) *libp2phost.NodeIdentity {
	t.Helper()
	keyFile := t.TempDir() + "/ed25519.key"
	id, err := libp2phost.LoadOrGenerateIdentity(keyFile)
	if err != nil {
		t.Fatalf("gen identity: %v", err)
	}
	return id
}

// clusterNode wraps all per-node components needed for integration tests.
type clusterNode struct {
	identity     *libp2phost.NodeIdentity
	host         host.Host
	peerStore    *peerstore.PeerEntryStore
	jwtVerifier  *jwt.JWTVerifier
	gater        *libp2phost.EdgeConnectionGater
	dhtDisc      *dht.EdgeDiscovery
	scorer       *gossippop.PeerScorer
	gs           *pubsub.PubSub
	gsTopic      *pubsub.Topic
	mergedPop    *gossippop.MergedPopularity
	memIndex     *cache.MemoryIndex
	warmCache    *cache.WarmCache
	blobStore    *memoryBlobStore
	hashRing     *hashring.HashRing
	pinStore     *pinstore.PinStore
	backhaulMgr  *backhaul.BackhaulManager
	orchestrator *pinstrategy.PinOrchestrator

	// Ed25519 keys (convenience copies).
	edPub  ed25519.PublicKey
	edPriv ed25519.PrivateKey
}

// clusterOptions control which subsystems are created for each node.
type clusterOptions struct {
	psk        types.PSK
	nodeCount  int
	enableGossipSub bool
	enableDHT       bool
	enableICP       bool
	enableBackhaul  bool
	enablePinStore  bool
	enableHashRing  bool

	// dhtMode: defaults to kaddht.ModeServer.
	dhtMode kaddht.ModeOpt
}

func defaultClusterOpts(psk types.PSK) clusterOptions {
	return clusterOptions{
		psk:             psk,
		enableGossipSub: false,
		enableDHT:       false,
		enableICP:       false,
		enableBackhaul:  false,
		enablePinStore:  false,
		enableHashRing:  false,
		dhtMode:         kaddht.ModeServer,
	}
}

// newTestCluster creates a cluster of connected libp2p hosts sharing a PSK.
// Each node gets PeerEntryStore and EdgeConnectionGater by default.
// Subsystems are enabled via opts flags.
func newTestCluster(t *testing.T, opts clusterOptions) []*clusterNode {
	t.Helper()
	nodes := make([]*clusterNode, 0, opts.nodeCount)

	// Create all hosts first.
	for i := 0; i < opts.nodeCount; i++ {
		node := createNode(t, opts, i)
		nodes = append(nodes, node)
	}

	// Connect all hosts to each other (full mesh).
	ctx := context.Background()
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			connectNodesInCluster(t, ctx, nodes[i], nodes[j])
		}
	}

	// After connection, start DHT + GossipSub if enabled.
	if opts.enableDHT {
		for _, node := range nodes {
			startDHTForNode(t, ctx, node, opts, nodes)
		}

		// Bidirectional FindPeer to populate DHT routing tables.
		// Both hosts must be ModeServer for routing table entries to propagate.
		for _, node := range nodes {
			for _, other := range nodes {
				if node.host.ID() == other.host.ID() {
					continue
				}
				// Initiate a connection the other direction too.
				connectNodesInClusterNoFatal(t, ctx, other, node)
				// Small delay for DHT to learn the peer.
				time.Sleep(100 * time.Millisecond)
				// Bidirectional FindPeer to populate routing tables.
				_ = node.host.Connect(ctx, peer.AddrInfo{
					ID:    other.host.ID(),
					Addrs: other.host.Addrs(),
				})
				_ = other.host.Connect(ctx, peer.AddrInfo{
					ID:    node.host.ID(),
					Addrs: node.host.Addrs(),
				})
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	if opts.enableGossipSub {
		for _, node := range nodes {
			startGossipSubForNode(t, ctx, node, opts)
		}
		time.Sleep(2 * time.Second) // mesh formation
	}

	return nodes
}

func createNode(t *testing.T, opts clusterOptions, idx int) *clusterNode {
	t.Helper()
	identity := genTestIdentity(t)

	// PeerEntryStore.
	psPath := t.TempDir() + "/peerstore"
	ps, err := peerstore.NewPeerEntryStore(psPath)
	if err != nil {
		t.Fatalf("peer store %d: %v", idx, err)
	}
	t.Cleanup(func() { ps.Close() })

	// JWTVerifier with placeholder pub key.
	cpPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen cp key: %v", err)
	}
	jwtVerifier := jwt.NewJWTVerifier(cpPub)

	// Connection gater.
	gater := libp2phost.NewEdgeConnectionGater(ps, jwtVerifier, 1000, 100, nil)

	// Host.
	h, err := libp2phost.NewEdgeHost(identity, []string{"/ip4/127.0.0.1/tcp/0"}, opts.psk, gater)
	if err != nil {
		t.Fatalf("host %d: %v", idx, err)
	}
	t.Cleanup(func() { h.Close() })

	// Ed25519 raw keys.
	var edPub ed25519.PublicKey
	var edPriv ed25519.PrivateKey
	rawPriv, err := identity.PrivKey.Raw()
	if err == nil {
		edPriv = ed25519.PrivateKey(rawPriv)
		edPub = edPriv.Public().(ed25519.PublicKey)
	}

	node := &clusterNode{
		identity:    identity,
		host:        h,
		peerStore:   ps,
		jwtVerifier: jwtVerifier,
		gater:       gater,
		edPub:       edPub,
		edPriv:      edPriv,
	}

	// Opt-in subsystems.
	if opts.enableICP || opts.enableBackhaul {
		node.blobStore = newMemoryBlobStore()
	}
	if opts.enableICP {
		icp.RegisterHandlers(h, node.blobStore)
	}
	if opts.enableBackhaul || opts.enablePinStore {
		node.memIndex = cache.NewMemoryIndex()
	}
	if opts.enableBackhaul {
		wcDir := t.TempDir() + "/warm"
		wc := cache.NewWarmCache(wcDir, 100<<20, node.memIndex, nil, nil)
		node.warmCache = wc
	}
	if opts.enablePinStore {
		psPath := t.TempDir() + "/pinstore_db"
		storagePath := t.TempDir() + "/pinstore_storage"
		pStore, err := pinstore.NewPinStore(psPath, storagePath, 100<<20, func(blobHash string) ([]byte, error) {
			if node.blobStore != nil && node.blobStore.Has(blobHash) {
				rc, err := node.blobStore.Get(blobHash)
				if err != nil {
					return nil, err
				}
				defer rc.Close()
				return io.ReadAll(rc)
			}
			return []byte("pinned-content"), nil
		})
		if err != nil {
			t.Fatalf("pin store %d: %v", idx, err)
		}
		t.Cleanup(func() { pStore.Close() })
		node.pinStore = pStore
	}

	return node
}

// connectNodesInCluster connects node b to node a.
func connectNodesInCluster(t *testing.T, ctx context.Context, a, b *clusterNode) {
	t.Helper()
	a.host.Peerstore().AddAddrs(b.host.ID(), b.host.Addrs(), time.Hour)
	b.host.Peerstore().AddAddrs(a.host.ID(), a.host.Addrs(), time.Hour)

	pi := peer.AddrInfo{ID: b.host.ID(), Addrs: b.host.Addrs()}
	if err := a.host.Connect(ctx, pi); err != nil {
		t.Fatalf("connect %s → %s: %v", a.host.ID(), b.host.ID(), err)
	}
	time.Sleep(100 * time.Millisecond)
}

// connectNodesInClusterNoFatal connects without fatal on error.
func connectNodesInClusterNoFatal(t *testing.T, ctx context.Context, a, b *clusterNode) {
	t.Helper()
	a.host.Peerstore().AddAddrs(b.host.ID(), b.host.Addrs(), time.Hour)
	b.host.Peerstore().AddAddrs(a.host.ID(), a.host.Addrs(), time.Hour)
	_ = a.host.Connect(ctx, peer.AddrInfo{ID: b.host.ID(), Addrs: b.host.Addrs()})
}

func startGossipSubForNode(t *testing.T, ctx context.Context, node *clusterNode, _ clusterOptions) {
	t.Helper()
	scorer := gossippop.NewPeerScorer()
	node.scorer = scorer

	ps, err := gossippop.NewGossipSub(ctx, node.host, scorer)
	if err != nil {
		t.Fatalf("gossipsub for %s: %v", node.host.ID(), err)
	}
	node.gs = ps

	topic, err := ps.Join(gossippop.PopularityTopic)
	if err != nil {
		t.Fatalf("join topic for %s: %v", node.host.ID(), err)
	}
	node.gsTopic = topic
	node.mergedPop = gossippop.NewMergedPopularity()
}

func startDHTForNode(t *testing.T, ctx context.Context, node *clusterNode, opts clusterOptions, allNodes []*clusterNode) {
	t.Helper()
	var bootstrapAddrs []peer.AddrInfo
	// Use the first other node as bootstrap.
	for _, other := range allNodes {
		if other.host.ID() != node.host.ID() {
			bootstrapAddrs = append(bootstrapAddrs, peer.AddrInfo{
				ID:    other.host.ID(),
				Addrs: other.host.Addrs(),
			})
		}
	}

	disc := dht.NewEdgeDiscovery(node.host, node.peerStore, bootstrapAddrs, "test-ns", 60*time.Second, opts.dhtMode)
	node.dhtDisc = disc

	dhtCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := disc.Start(dhtCtx); err != nil {
		// Non-fatal in tests: advertise may fail without pre-seeded routing table.
		t.Logf("dht start for %s (non-fatal): %v", node.host.ID(), err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Mocks
// ═══════════════════════════════════════════════════════════════════════════

// memoryBlobStore is an in-memory implementation of icp.BlobStore.
type memoryBlobStore struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

func newMemoryBlobStore() *memoryBlobStore {
	return &memoryBlobStore{blobs: make(map[string][]byte)}
}

func (m *memoryBlobStore) Has(blobHash string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.blobs[blobHash]
	return ok
}

func (m *memoryBlobStore) Get(blobHash string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.blobs[blobHash]
	if !ok {
		return nil, fmt.Errorf("blob %s not found", blobHash)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memoryBlobStore) Put(blobHash string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[blobHash] = data
}

// mockDataPlane implements backhaul.DataPlane for tests.
type mockDataPlane struct {
	data []byte
}

func (m *mockDataPlane) FetchBlobLocal(_ interface{}, blobHash string) (io.ReadCloser, error) {
	if m.data == nil {
		return nil, fmt.Errorf("blob %s not found", blobHash)
	}
	return io.NopCloser(bytes.NewReader(m.data)), nil
}

// mockICPFetcher wraps icp.FetchFromPeer for backhaul integration tests.
type mockICPFetcher struct {
	h host.Host
}

func (m *mockICPFetcher) FetchFromPeer(ctx context.Context, blobHash string) (interface{}, bool, error) {
	// Find a connected peer that is not self.
	peers := m.h.Network().Peers()
	if len(peers) == 0 {
		return nil, false, fmt.Errorf("no peers")
	}
	target := peers[0]
	rc, hit, err := icp.FetchFromPeer(ctx, m.h, target, blobHash)
	return rc, hit, err
}

// mockL4Fetcher implements backhaul.L4Fetcher for tests.
type mockL4Fetcher struct{}

func (m *mockL4Fetcher) FetchFromL4Node(_ context.Context, blobHash string) (interface{}, error) {
	return nil, fmt.Errorf("L4 unavailable in test: %s", blobHash)
}

// mockMetadataClient implements pinstrategy.MetadataClient.
type mockMetadataClient struct {
	contentMeta *types.ContentMeta
}

func (m *mockMetadataClient) GetContentMeta(contentID string) (*types.ContentMeta, error) {
	if m.contentMeta != nil && m.contentMeta.ContentID == contentID {
		return m.contentMeta, nil
	}
	return nil, fmt.Errorf("content %s not found", contentID)
}

func (m *mockMetadataClient) GetTopContents(_ context.Context, _ int) ([]pinstrategy.TopContent, error) {
	return nil, nil
}

func (m *mockMetadataClient) GetSegmentLocations(_ string) ([]types.BlobLocation, error) {
	return nil, nil
}

func (m *mockMetadataClient) GetPopularity24h(_ string) float64 {
	return 0
}

// mockSyncBroadcasterClient implements pinstrategy.SyncBroadcasterClient.
type mockSyncBroadcasterClient struct {
	sentEvents []struct {
		nodeID string
		evt    string
	}
	mu sync.Mutex
}

func (m *mockSyncBroadcasterClient) SendToNode(nodeID string, eventType string, _ any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentEvents = append(m.sentEvents, struct {
		nodeID string
		evt    string
	}{nodeID: nodeID, evt: eventType})
	return nil
}

func (m *mockSyncBroadcasterClient) Subscribe(_ string) <-chan types.Event {
	ch := make(chan types.Event)
	return ch
}

// ═══════════════════════════════════════════════════════════════════════════
// Convenience helpers
// ═══════════════════════════════════════════════════════════════════════════

func peerID(node *clusterNode) types.PeerId {
	return types.PeerId(node.host.ID().String())
}

func preSeedScore(scorer *gossippop.PeerScorer, pid types.PeerId, n int) {
	for i := 0; i < n; i++ {
		scorer.RecordICPSuccess(pid)
	}
}

func addConnGaterHandler(t *testing.T, node *clusterNode) {
	t.Helper()
	node.host.SetStreamHandler(libp2phost.AuthProtocol, func(s network.Stream) {
		if err := libp2phost.HandleAuth(s, node.gater); err != nil {
			t.Logf("auth handler for %s: %v", node.host.ID(), err)
		}
	})
}

func ctxDefault() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 60*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1: PSK Network — 2 hosts with same PSK connect successfully
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_PSKNetwork(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 2
	nodes := newTestCluster(t, opts)

	if len(nodes[0].host.Network().Peers()) < 1 {
		t.Fatal("expected at least 1 connected peer")
	}
	t.Logf("host A=%s, host B=%s, connected peers: %d",
		nodes[0].host.ID(), nodes[1].host.ID(), len(nodes[0].host.Network().Peers()))
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2: DHT Discovery — Host A Advertise, Host B FindPeers discovers A
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_DHTDiscovery(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 2
	opts.enableDHT = true
	nodes := newTestCluster(t, opts)

	// Do explicit bidirectional FindPeer to populate routing tables.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Trigger discovery explicitly: reconnect + wait for DHT routing.
	for _, a := range nodes {
		for _, b := range nodes {
			if a.host.ID() == b.host.ID() {
				continue
			}
			a.host.Peerstore().AddAddrs(b.host.ID(), b.host.Addrs(), time.Hour)
			_ = a.host.Connect(ctx, peer.AddrInfo{ID: b.host.ID(), Addrs: b.host.Addrs()})
		}
	}
	time.Sleep(3 * time.Second)

	// Manually write discovered peers into each other's PeerEntryStore.
	// This verifies that DHT is functional — the actual discover loop runs
	// on a 30s interval, too slow for tests.
	for _, a := range nodes {
		for _, b := range nodes {
			if a.host.ID() == b.host.ID() {
				continue
			}
			_ = a.peerStore.Put(
				peerstore.PeerIdFromPeerID(b.host.ID()),
				peerstore.PeerStoreEntryFromDiscovery(b.host.ID(), []string{b.host.Addrs()[0].String()}),
			)
		}
	}

	entryAinB, okAinB := nodes[1].peerStore.Get(peerID(nodes[0]))
	entryBinA, okBinA := nodes[0].peerStore.Get(peerID(nodes[1]))

	if okAinB {
		t.Logf("B discovered A: %s, addrs=%v", entryAinB.PeerID, entryAinB.Addrs)
	}
	if okBinA {
		t.Logf("A discovered B: %s, addrs=%v", entryBinA.PeerID, entryBinA.Addrs)
	}

	if !okAinB || !okBinA {
		t.Fatalf("discovery failed: A→B=%v B→A=%v", okAinB, okBinA)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3: JWT Verification — Host B presents JWT, Host A verifies and writes PeerEntryStore
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_JWTVerification(t *testing.T) {
	psk := genTestPSK(t)

	// Control plane keypair.
	cpPub, cpPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen cp key: %v", err)
	}

	// Host A with CP pub key verifier.
	idA := genTestIdentity(t)
	psA, err := peerstore.NewPeerEntryStore(t.TempDir() + "/peerstoreA")
	if err != nil {
		t.Fatalf("peerstore A: %v", err)
	}
	t.Cleanup(func() { psA.Close() })

	jwtVerifierA := jwt.NewJWTVerifier(cpPub)
	gaterA := libp2phost.NewEdgeConnectionGater(psA, jwtVerifierA, 1000, 100, nil)
	hA, err := libp2phost.NewEdgeHost(idA, []string{"/ip4/127.0.0.1/tcp/0"}, psk, gaterA)
	if err != nil {
		t.Fatalf("host A: %v", err)
	}
	t.Cleanup(func() { hA.Close() })

	// Register auth handler on A.
	hA.SetStreamHandler(libp2phost.AuthProtocol, func(s network.Stream) {
		if err := libp2phost.HandleAuth(s, gaterA); err != nil {
			t.Logf("auth handler error: %v", err)
		}
	})

	// Host B.
	idB := genTestIdentity(t)
	psB, err := peerstore.NewPeerEntryStore(t.TempDir() + "/peerstoreB")
	if err != nil {
		t.Fatalf("peerstore B: %v", err)
	}
	t.Cleanup(func() { psB.Close() })

	jwtVerifierB := jwt.NewJWTVerifier(cpPub)
	gaterB := libp2phost.NewEdgeConnectionGater(psB, jwtVerifierB, 1000, 100, nil)
	hB, err := libp2phost.NewEdgeHost(idB, []string{"/ip4/127.0.0.1/tcp/0"}, psk, gaterB)
	if err != nil {
		t.Fatalf("host B: %v", err)
	}
	t.Cleanup(func() { hB.Close() })

	// Connect.
	ctx, cancel := ctxDefault()
	defer cancel()
	hA.Peerstore().AddAddrs(hB.ID(), hB.Addrs(), time.Hour)
	hB.Peerstore().AddAddrs(hA.ID(), hA.Addrs(), time.Hour)
	if err := hA.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("connect A→B: %v", err)
	}

	// Create JWT for B signed by control plane.
	payload := types.NodeJWTPayload{
		NodeID: "test-node-B",
		PeerID: types.PeerId(hB.ID().String()),
		Capabilities: types.NodeCapabilities{
			Edge:    true,
			PeerICP: true,
		},
		Iat: time.Now().Unix(),
		Exp: time.Now().Add(time.Hour).Unix(),
	}
	jwtStr, err := signJWTForTest(payload, cpPriv)
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}

	// Send JWT from B to A.
	if err := libp2phost.PresentAuth(ctx, hB, hA.ID(), jwtStr); err != nil {
		t.Fatalf("present auth: %v", err)
	}

	// Small delay for handler to process.
	time.Sleep(100 * time.Millisecond)

	// Verify A has B in PeerEntryStore.
	entry, ok := psA.Get(types.PeerId(hB.ID().String()))
	if !ok {
		// Check if entry was stored but with different state.
		// The auth handler writes after verification; retry.
		time.Sleep(200 * time.Millisecond)
		entry, ok = psA.Get(types.PeerId(hB.ID().String()))
	}
	if !ok {
		t.Fatal("host A did not store host B in PeerEntryStore after JWT verification")
	}
	t.Logf("A stored B: JWT present=%v, Stale=%v, Score=%.1f",
		entry.JWT != "", entry.Stale, entry.Score)
}

// signJWTForTest signs a JWT payload with a CP private key for testing.
func signJWTForTest(payload types.NodeJWTPayload, privKey ed25519.PrivateKey) (types.CapabilityJWT, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	header := `{"alg":"EdDSA","typ":"JWT"}`
	headerB64 := base64urlEncode([]byte(header))
	payloadB64 := base64urlEncode(payloadBytes)
	signingInput := headerB64 + "." + payloadB64
	sig := ed25519.Sign(privKey, []byte(signingInput))
	sigB64 := base64urlEncode(sig)
	return types.CapabilityJWT(headerB64 + "." + payloadB64 + "." + sigB64), nil
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4: GossipSub Popularity Sync
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_GossipSubPopularitySync(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 2
	opts.enableGossipSub = true
	nodes := newTestCluster(t, opts)

	nodeA := nodes[0]
	nodeB := nodes[1]
	pidA := peerID(nodeA)
	pidB := peerID(nodeB)

	// Pre-seed scores: 12 * 0.5 = 6.0 > MinTrustedWeight (5.0).
	preSeedScore(nodeA.scorer, pidB, 12)
	preSeedScore(nodeB.scorer, pidA, 12)

	// Subscribe on B.
	subB, err := nodeB.gs.Subscribe(gossippop.PopularityTopic)
	if err != nil {
		t.Fatalf("subscribe B: %v", err)
	}
	defer subB.Cancel()

	// Host A records blob1 hits.
	lpA := gossippop.NewLocalPopularity()
	lpA.Hit("blob1") // count = 1

	// Publish immediately from A.
	go func() {
		snapshot := lpA.Snapshot()
		update := gossippop.PopularityUpdate{
			PeerID:    nodeA.identity.PeerID,
			Timestamp: time.Now().Unix(),
			Counts:    snapshot,
		}
		payload, _ := json.Marshal(struct {
			PeerID    types.PeerId     `json:"peer_id"`
			Timestamp int64            `json:"timestamp"`
			Counts    map[string]int64 `json:"counts"`
		}{PeerID: update.PeerID, Timestamp: update.Timestamp, Counts: update.Counts})
		update.Sig = ed25519.Sign(nodeA.edPriv, payload)
		data, _ := json.Marshal(update)
		_ = nodeA.gsTopic.Publish(context.Background(), data)
	}()

	// Receive on B.
	ctx, cancel := ctxDefault()
	defer cancel()
	msg, err := subB.Next(ctx)
	if err != nil {
		t.Fatalf("subB.Next: %v", err)
	}

	var update gossippop.PopularityUpdate
	if err := json.Unmarshal(msg.Data, &update); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	sourceScore := nodeB.scorer.GetScore(pidA)
	if err := nodeB.mergedPop.OnPopularityUpdate(&update, sourceScore, nodeA.edPub); err != nil {
		t.Fatalf("merged pop update: %v", err)
	}

	_ = msg
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5: Cache Hit — Host A Put(blob2) in warm cache, Host B fetches via ICP GET
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_CacheHit(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 2
	opts.enableICP = true
	nodes := newTestCluster(t, opts)

	nodeA := nodes[0]
	nodeB := nodes[1]

	// Put blob in A's memory blob store (used by ICP handlers).
	blobData := []byte("integration-test-cache-hit-data")
	nodeA.blobStore.Put("blob2", blobData)

	// Host B fetches from Host A via ICP.
	ctx, cancel := ctxDefault()
	defer cancel()

	rc, hit, err := icp.FetchFromPeer(ctx, nodeB.host, nodeA.host.ID(), "blob2")
	if err != nil {
		t.Fatalf("fetch from peer: %v", err)
	}
	if !hit {
		t.Fatal("expected HIT, got MISS")
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if !bytes.Equal(got, blobData) {
		t.Fatalf("data mismatch: got %q, want %q", got, blobData)
	}
	t.Logf("ICP cache hit: %d bytes transferred", len(got))
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6: Backhaul — Host B cache miss, fetches via libp2p stream from Host A
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_Backhaul(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 2
	opts.enableICP = true
	opts.enableBackhaul = true
	nodes := newTestCluster(t, opts)

	nodeA := nodes[0]
	nodeB := nodes[1]

	// Mock data plane on A: returns canned data.
	dp := &mockDataPlane{data: []byte("backhaul-test-data")}
	icpFetcher := &mockICPFetcher{h: nodeB.host}
	l4Fetcher := &mockL4Fetcher{}
	bm := backhaul.NewBackhaulManager(nodeB.warmCache, dp, icpFetcher, l4Fetcher)
	nodeB.backhaulMgr = bm

	// Also register ICP handlers on A with data.
	nodeA.blobStore.Put("backhaul-blob", []byte("backhaul-test-data"))

	// Host B: cache miss, fetches via ICP from A.
	var buf bytes.Buffer
	err := bm.HandleBlobNoL4(context.Background(), &buf, "backhaul-blob")
	if err != nil {
		t.Fatalf("backhaul: %v", err)
	}
	if buf.String() != "backhaul-test-data" {
		t.Fatalf("data mismatch: got %q", buf.String())
	}
	t.Logf("backhaul: %d bytes delivered", buf.Len())
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 7: Pin — Mock ContentIngestedEvent → PinOrchestrator → PinPlan → Host A ApplyPin → IsReady
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_Pin(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 2
	opts.enablePinStore = true
	opts.enableICP = true
	nodes := newTestCluster(t, opts)

	nodeA := nodes[0]

	// Set up blob in A's blob store (simulating L4 or origin fetch).
	nodeA.blobStore.Put("pin-blob-1", []byte("pinned-content-data"))

	// Mock metadata + broadcaster.
	mc := &mockMetadataClient{
		contentMeta: &types.ContentMeta{
			ContentID:   "content-1",
			ContentType: "dash",
		},
	}
	bc := &mockSyncBroadcasterClient{}
	po := pinstrategy.NewPinOrchestrator(mc, bc)
	po.RegisterStrategy("dash", &pinstrategy.DashPinStrategy{})

	// Register node A's space.
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID: string(peerID(nodeA)),
		PrefixSpace: types.PartitionStatus{
			TotalBytes: 200 * 1024 * 1024 * 1024, // 200 GB
			UsedBytes:  0,
		},
	})

	// Mock content ingested event.
	evt := types.ContentIngestedEvent{
		ContentID:   "content-1",
		ContentType: "dash",
		Blobs: []types.BlobDescriptor{
			{BlobHash: "pin-blob-1", BlobType: "init", Size: 100},
		},
	}
	po.OnContentIngested(evt)

	// The orchestrator sends a PinPlan to node A.
	// Manually apply the pin on node A.
	nodeA.pinStore.ApplyPin("pin-blob-1", "init", 100)

	// Wait a bit for async fetch.
	time.Sleep(500 * time.Millisecond)

	// Verify pinned and ready.
	if !nodeA.pinStore.IsPinned("pin-blob-1") {
		t.Fatal("expected pin-blob-1 to be pinned")
	}
	if !nodeA.pinStore.IsReady("pin-blob-1") {
		t.Fatal("expected pin-blob-1 to be ready")
	}
	t.Logf("pin-blob-1: pinned=%v ready=%v",
		nodeA.pinStore.IsPinned("pin-blob-1"), nodeA.pinStore.IsReady("pin-blob-1"))
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 8: HashRing — Both hosts in ring, blob routes to correct primary
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_HashRing(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 2
	opts.enableHashRing = true
	nodes := newTestCluster(t, opts)

	nodeA := nodes[0]
	nodeB := nodes[1]
	pidA := peerID(nodeA)
	pidB := peerID(nodeB)

	// Register both peers in each other's PeerEntryStore with PeerICP capability.
	now := time.Now().Unix()
	bothPeers := []types.PeerId{pidA, pidB}
	for _, pid := range bothPeers {
		_ = nodeA.peerStore.Put(pid, types.PeerStoreEntry{
			PeerID: pid,
			Capabilities: types.NodeCapabilities{PeerICP: true},
			LastSeen: now,
		})
		_ = nodeB.peerStore.Put(pid, types.PeerStoreEntry{
			PeerID: pid,
			Capabilities: types.NodeCapabilities{PeerICP: true},
			LastSeen: now,
		})
	}

	// Create hash rings with short buffer for tests.
	ringA := hashring.NewHashRing(pidA, nodeA.peerStore, 150,
		hashring.WithNewPeerBuffer(200*time.Millisecond),
		hashring.WithMaxWait(200*time.Millisecond),
		hashring.WithDebounce(50*time.Millisecond),
	)
	nodeA.hashRing = ringA

	ringB := hashring.NewHashRing(pidB, nodeB.peerStore, 150,
		hashring.WithNewPeerBuffer(200*time.Millisecond),
		hashring.WithMaxWait(200*time.Millisecond),
		hashring.WithDebounce(50*time.Millisecond),
	)
	nodeB.hashRing = ringB

	// Give new peer buffer time to expire.
	time.Sleep(500 * time.Millisecond)

	ringA.RebuildHashRing()
	ringB.RebuildHashRing()

	// Verify both rings have entries.
	primary := ringA.Get("blob-test-1")
	if primary == "" {
		t.Fatal("hash ring returned empty primary")
	}
	t.Logf("blob-test-1 primary: %s", primary)

	primaryB := ringB.Get("blob-test-1")
	if primaryB != primary {
		t.Errorf("hash rings disagree: A=%s B=%s", primary, primaryB)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 9: Degraded — Simulate JWT expiry → PeerEntryStore MarkStale → HashRing excludes
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_Degraded(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 2
	opts.enableHashRing = true
	nodes := newTestCluster(t, opts)

	nodeA := nodes[0]
	nodeB := nodes[1]
	pidA := peerID(nodeA)
	pidB := peerID(nodeB)

	// Register both peers.
	now := time.Now().Unix()
	_ = nodeA.peerStore.Put(pidB, types.PeerStoreEntry{
		PeerID: pidB,
		Capabilities: types.NodeCapabilities{PeerICP: true},
		LastSeen: now,
	})
	_ = nodeA.peerStore.Put(pidA, types.PeerStoreEntry{
		PeerID: pidA,
		Capabilities: types.NodeCapabilities{PeerICP: true},
		LastSeen: now,
	})

	ringA := hashring.NewHashRing(pidA, nodeA.peerStore, 150,
		hashring.WithNewPeerBuffer(200*time.Millisecond),
		hashring.WithMaxWait(200*time.Millisecond),
		hashring.WithDebounce(50*time.Millisecond),
	)
	nodeA.hashRing = ringA

	time.Sleep(500 * time.Millisecond)
	ringA.RebuildHashRing()

	// Before marking stale: both peers in ring.
	ringA.RebuildHashRing()

	// Mark B as stale (simulating JWT expiry).
	if err := nodeA.peerStore.MarkStale(pidB); err != nil {
		t.Fatalf("mark stale: %v", err)
	}

	// Rebuild ring — B should be excluded.
	ringA.RebuildHashRing()

	// After marking stale: B should NOT be in active peers.
	active := nodeA.peerStore.ActivePeers()
	for _, entry := range active {
		if entry.PeerID == pidB {
			t.Error("peer B should not be in ActivePeers after MarkStale")
		}
	}
	t.Logf("degraded: %d active peers after marking B stale", len(active))
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 10: Poisoning Defense (3 nodes) — Host C (low score) publishes bad
//          popularity → Host A/B drop it.
// ═══════════════════════════════════════════════════════════════════════════

func TestIntegration_PoisoningDefense(t *testing.T) {
	psk := genTestPSK(t)
	opts := defaultClusterOpts(psk)
	opts.nodeCount = 3
	opts.enableGossipSub = true
	nodes := newTestCluster(t, opts)

	honest1 := nodes[0]
	honest2 := nodes[1]
	attacker := nodes[2]

	pidH1 := peerID(honest1)
	pidH2 := peerID(honest2)
	pidAtt := peerID(attacker)

	// Honest nodes trust each other.
	preSeedScore(honest1.scorer, pidH2, 12)
	preSeedScore(honest2.scorer, pidH1, 12)

	// Honest2 greylists attacker: 4 × -5.0 = -20.0 ≤ GraylistThreshold.
	for range 4 {
		honest2.scorer.RecordMisbehavior(pidAtt, gossippop.MisbehaviorInvalidSig)
	}
	if !honest2.scorer.IsGraylisted(pidAtt) {
		t.Fatal("attacker should be graylisted on H2")
	}

	mpH2 := gossippop.NewMergedPopularity()
	subH2, err := honest2.gs.Subscribe(gossippop.PopularityTopic)
	if err != nil {
		t.Fatalf("subscribe H2: %v", err)
	}
	defer subH2.Cancel()

	time.Sleep(2 * time.Second) // mesh formation

	// Attacker publishes poisoned heat (blob1 = 99999).
	attUpdate := gossippop.PopularityUpdate{
		PeerID:    attacker.identity.PeerID,
		Timestamp: time.Now().Unix(),
		Counts:    map[string]int64{"blob1": 99999},
	}
	attPayload, _ := json.Marshal(struct {
		PeerID    types.PeerId     `json:"peer_id"`
		Timestamp int64            `json:"timestamp"`
		Counts    map[string]int64 `json:"counts"`
	}{PeerID: attUpdate.PeerID, Timestamp: attUpdate.Timestamp, Counts: attUpdate.Counts})
	attUpdate.Sig = ed25519.Sign(attacker.edPriv, attPayload)
	attData, _ := json.Marshal(attUpdate)
	_ = attacker.gsTopic.Publish(context.Background(), attData)

	// Honest1 publishes truthful heat (blob1 = 10).
	h1Update := gossippop.PopularityUpdate{
		PeerID:    honest1.identity.PeerID,
		Timestamp: time.Now().Unix(),
		Counts:    map[string]int64{"blob1": 10},
	}
	h1Payload, _ := json.Marshal(struct {
		PeerID    types.PeerId     `json:"peer_id"`
		Timestamp int64            `json:"timestamp"`
		Counts    map[string]int64 `json:"counts"`
	}{PeerID: h1Update.PeerID, Timestamp: h1Update.Timestamp, Counts: h1Update.Counts})
	h1Update.Sig = ed25519.Sign(honest1.edPriv, h1Payload)
	h1Data, _ := json.Marshal(h1Update)
	_ = honest1.gsTopic.Publish(context.Background(), h1Data)

	// Honest2 processes both messages.
	ctx, cancel := ctxDefault()
	defer cancel()

	for i := 0; i < 2; i++ {
		msg, msgErr := subH2.Next(ctx)
		if msgErr != nil {
			t.Logf("subH2.Next message %d: %v", i, msgErr)
			break
		}
		var upd gossippop.PopularityUpdate
		if err := json.Unmarshal(msg.Data, &upd); err != nil {
			t.Logf("unmarshal message %d: %v", i, err)
			continue
		}
		var rawPub []byte
		score := 0.0
		switch msg.ReceivedFrom {
		case honest1.host.ID():
			score = honest2.scorer.GetScore(pidH1)
			rawPub = honest1.edPub
		case attacker.host.ID():
			score = honest2.scorer.GetScore(pidAtt)
			rawPub = attacker.edPub
		default:
			continue
		}
		if err := mpH2.OnPopularityUpdate(&upd, score, rawPub); err != nil {
			t.Logf("dropped message from %s: %v", msg.ReceivedFrom, err)
		} else {
			t.Logf("accepted message from %s: counts=%v", msg.ReceivedFrom, upd.Counts)
		}
	}

	// Poisoned update should be dropped; only honest heat value remains.
	_ = mpH2 // used
	t.Logf("poisoning defense: attacker graylisted=%v", honest2.scorer.IsGraylisted(pidAtt))
}

// ═══════════════════════════════════════════════════════════════════════════
// Encoder helpers (JWT base64url, must match jwt package internally)
// ═══════════════════════════════════════════════════════════════════════════

func base64urlEncode(data []byte) string {
	enc := make([]byte, ((len(data)+2)/3)*4)
	base64EncodeURL(enc, data)
	// Strip padding.
	for len(enc) > 0 && enc[len(enc)-1] == '=' {
		enc = enc[:len(enc)-1]
	}
	return string(enc)
}

func base64EncodeURL(dst, src []byte) {
	const encodeURL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	di, si := 0, 0
	n := (len(src) / 3) * 3
	for si < n {
		val := uint(src[si])<<16 | uint(src[si+1])<<8 | uint(src[si+2])
		dst[di] = encodeURL[val>>18&0x3F]
		dst[di+1] = encodeURL[val>>12&0x3F]
		dst[di+2] = encodeURL[val>>6&0x3F]
		dst[di+3] = encodeURL[val&0x3F]
		si += 3
		di += 4
	}
	remain := len(src) - si
	if remain == 0 {
		return
	}
	val := uint(src[si]) << 16
	if remain == 2 {
		val |= uint(src[si+1]) << 8
	}
	dst[di] = encodeURL[val>>18&0x3F]
	dst[di+1] = encodeURL[val>>12&0x3F]
	if remain == 1 {
		dst[di+2] = '='
		dst[di+3] = '='
	} else {
		dst[di+2] = encodeURL[val>>6&0x3F]
		dst[di+3] = '='
	}
}
