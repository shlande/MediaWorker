package libp2phost

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/config"
	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/node/jwt"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

// dummyConnMultiaddrs is a minimal network.ConnMultiaddrs for tests.
type dummyConnMultiaddrs struct {
	local  multiaddr.Multiaddr
	remote multiaddr.Multiaddr
}

func (d dummyConnMultiaddrs) LocalMultiaddr() multiaddr.Multiaddr  { return d.local }
func (d dummyConnMultiaddrs) RemoteMultiaddr() multiaddr.Multiaddr { return d.remote }

// dummyConn is a minimal network.Conn for InterceptUpgraded tests.
type dummyConn struct {
	remotePeer peer.ID
}

func (d dummyConn) Close() error                                      { return nil }
func (d dummyConn) CloseWithError(network.ConnErrorCode) error        { return nil }
func (d dummyConn) LocalPeer() peer.ID                                { return "" }
func (d dummyConn) LocalPrivateKey() crypto.PrivKey                   { return nil }
func (d dummyConn) RemotePeer() peer.ID                               { return d.remotePeer }
func (d dummyConn) RemotePublicKey() crypto.PubKey                    { return nil }
func (d dummyConn) ConnState() network.ConnectionState                { return network.ConnectionState{} }
func (d dummyConn) Stat() network.ConnStats                           { return network.ConnStats{} }
func (d dummyConn) ID() string                                        { return "dummy" }
func (d dummyConn) NewStream(context.Context) (network.Stream, error) { return nil, nil }
func (d dummyConn) GetStreams() []network.Stream                      { return nil }
func (d dummyConn) IsClosed() bool                                    { return false }
func (d dummyConn) Scope() network.ConnScope                          { return nil }
func (d dummyConn) ConnMultiaddrs() network.ConnMultiaddrs            { return nil }
func (d dummyConn) LocalMultiaddr() multiaddr.Multiaddr               { return nil }
func (d dummyConn) RemoteMultiaddr() multiaddr.Multiaddr              { return nil }
func (d dummyConn) As(_ any) bool                                     { return false }

// newTestStore creates a PeerEntryStore backed by a temp BadgerDB directory.
func newTestStore(t *testing.T) *peerstore.PeerEntryStore {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "peerstore")
	store, err := peerstore.NewPeerEntryStore(dir)
	if err != nil {
		t.Fatalf("create peer entry store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// newTestGater creates an EdgeConnectionGater for unit tests.
func newTestGater(t *testing.T) (*EdgeConnectionGater, *peerstore.PeerEntryStore) {
	t.Helper()
	store := newTestStore(t)
	_, cpPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate control-plane key: %v", err)
	}
	verifier := jwt.NewJWTVerifier(cpPriv.Public().(ed25519.PublicKey))

	gater := NewEdgeConnectionGater(store, verifier, rate.Limit(50), 100, nil)
	return gater, store
}

// newTestIdentity creates a NodeIdentity for tests (no disk write).
func newTestIdentity(t *testing.T) *sharedid.NodeIdentity {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	return &sharedid.NodeIdentity{
		PrivKey: priv,
		PeerID:  types.PeerId(pid.String()),
	}
}

// testJWTService creates a JWTService for signing test JWTs.
// Uses a 1ms rate-limit interval so multiple JWTs can be signed from the
// same IP in a single test run.
func testJWTService(t *testing.T) (*cpjwt.JWTService, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate control-plane key: %v", err)
	}
	svc := cpjwt.NewJWTService(priv, cpjwt.NewPeerIdSet(), cpjwt.NewRateLimiter(1*time.Millisecond),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})
	return svc, pub
}

// signJWTForPeer signs a capability JWT for a peer using JWTService.
func signJWTForPeer(t *testing.T, svc *cpjwt.JWTService, peerID types.PeerId, nodePrivKey crypto.PrivKey) types.CapabilityJWT {
	t.Helper()
	sig, err := nodePrivKey.Sign([]byte(peerID))
	if err != nil {
		t.Fatalf("sign peer ID: %v", err)
	}
	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: sig,
	}
	resp, err := svc.HandleJWTRequest(req, "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}
	return resp.JWT
}

// ─── InterceptPeerDial / InterceptAddrDial ──────────────────────────────────

func TestInterceptPeerDial_AllowAll(t *testing.T) {
	gater, _ := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	if !gater.InterceptPeerDial(pid) {
		t.Error("InterceptPeerDial should allow all peers")
	}
}

func TestInterceptAddrDial_AllowAll(t *testing.T) {
	gater, _ := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	ma, err := multiaddr.NewMultiaddr("/ip4/10.0.0.1/tcp/9001")
	if err != nil {
		t.Fatalf("create multiaddr: %v", err)
	}

	if !gater.InterceptAddrDial(pid, ma) {
		t.Error("InterceptAddrDial should allow all addresses")
	}
}

// ─── InterceptAccept ────────────────────────────────────────────────────────

func TestInterceptAccept_IPRateLimit(t *testing.T) {
	// Given: a gater with rate limit 2/sec and burst 1
	store := newTestStore(t)
	_, cpPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	verifier := jwt.NewJWTVerifier(cpPriv.Public().(ed25519.PublicKey))
	gater := NewEdgeConnectionGater(store, verifier, rate.Limit(2), 1, nil)

	remoteMA, _ := multiaddr.NewMultiaddr("/ip4/10.0.1.1/tcp/9001")
	localMA, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	addrs := dummyConnMultiaddrs{local: localMA, remote: remoteMA}

	// When: first connection → allow
	if !gater.InterceptAccept(addrs) {
		t.Error("first connection should be allowed")
	}

	// When: second connection immediately → reject (burst=1)
	if gater.InterceptAccept(addrs) {
		t.Error("second connection should be rate-limited")
	}
}

func TestInterceptAccept_CIDRAllowlist(t *testing.T) {
	store := newTestStore(t)
	_, cpPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	verifier := jwt.NewJWTVerifier(cpPriv.Public().(ed25519.PublicKey))

	// Given: allow only 10.0.0.0/24
	allowedPFX := netip.MustParsePrefix("10.0.0.0/24")
	gater := NewEdgeConnectionGater(store, verifier, rate.Limit(1000), 1000,
		[]netip.Prefix{allowedPFX})

	localMA, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")

	// When: IP in allowlist → allow
	remoteIn, _ := multiaddr.NewMultiaddr("/ip4/10.0.0.5/tcp/9001")
	if !gater.InterceptAccept(dummyConnMultiaddrs{local: localMA, remote: remoteIn}) {
		t.Error("IP in CIDR allowlist should be allowed")
	}

	// When: IP outside allowlist → reject
	remoteOut, _ := multiaddr.NewMultiaddr("/ip4/192.168.1.1/tcp/9001")
	if gater.InterceptAccept(dummyConnMultiaddrs{local: localMA, remote: remoteOut}) {
		t.Error("IP not in CIDR allowlist should be rejected")
	}
}

// ─── InterceptSecured ───────────────────────────────────────────────────────

func TestInterceptSecured_UnknownPeer(t *testing.T) {
	gater, _ := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	// When: unknown peer → allow (will verify JWT via stream)
	if !gater.InterceptSecured(network.DirInbound, pid, nil) {
		t.Error("unknown peer should be allowed (will verify JWT via stream)")
	}
}

func TestInterceptSecured_StalePeer(t *testing.T) {
	gater, store := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	if err := store.Put(id.PeerID, types.PeerStoreEntry{
		PeerID: id.PeerID,
		Stale:  true,
		Score:  0.0,
	}); err != nil {
		t.Fatalf("put entry: %v", err)
	}

	// When: stale peer → reject
	if gater.InterceptSecured(network.DirInbound, pid, nil) {
		t.Error("stale peer should be rejected")
	}
}

func TestInterceptSecured_LowScore(t *testing.T) {
	gater, store := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	if err := store.Put(id.PeerID, types.PeerStoreEntry{
		PeerID: id.PeerID,
		Stale:  false,
		Score:  peerstore.GraylistThreshold - 5.0, // below GraylistThreshold
	}); err != nil {
		t.Fatalf("put entry: %v", err)
	}

	// When: low-score peer → reject
	if gater.InterceptSecured(network.DirInbound, pid, nil) {
		t.Error("peer with Score < GraylistThreshold should be rejected")
	}
}

func TestInterceptSecured_HealthyPeer(t *testing.T) {
	gater, store := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	if err := store.Put(id.PeerID, types.PeerStoreEntry{
		PeerID: id.PeerID,
		Stale:  false,
		Score:  5.0,
	}); err != nil {
		t.Fatalf("put entry: %v", err)
	}

	// When: healthy peer → allow
	if !gater.InterceptSecured(network.DirInbound, pid, nil) {
		t.Error("healthy peer should be allowed")
	}
}

func TestInterceptSecured_BoundaryScore(t *testing.T) {
	gater, store := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	if err := store.Put(id.PeerID, types.PeerStoreEntry{
		PeerID: id.PeerID,
		Stale:  false,
		Score:  peerstore.GraylistThreshold, // exactly at threshold
	}); err != nil {
		t.Fatalf("put entry: %v", err)
	}

	// When: peer at boundary → allow (>= threshold)
	if !gater.InterceptSecured(network.DirInbound, pid, nil) {
		t.Error("peer with Score == GraylistThreshold should be allowed")
	}
}

// ─── InterceptUpgraded ──────────────────────────────────────────────────────

func TestInterceptUpgraded_UnknownPeer(t *testing.T) {
	gater, _ := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	allow, reason := gater.InterceptUpgraded(dummyConn{remotePeer: pid})
	if !allow {
		t.Error("unknown peer should be allowed at upgrade")
	}
	if reason != 0 {
		t.Errorf("reason should be 0, got %d", reason)
	}
}

func TestInterceptUpgraded_ExpiredJWT(t *testing.T) {
	gater, store := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	now := time.Now().Unix()
	if err := store.Put(id.PeerID, types.PeerStoreEntry{
		PeerID: id.PeerID,
		Stale:  false,
		Score:  5.0,
		JWTExp: now - 1, // expired 1s ago
	}); err != nil {
		t.Fatalf("put entry: %v", err)
	}

	allow, reason := gater.InterceptUpgraded(dummyConn{remotePeer: pid})
	if allow {
		t.Error("peer with expired JWT should be rejected")
	}
	if reason != 0 {
		t.Errorf("reason should be 0, got %d", reason)
	}

	// Then: entry should be marked Stale
	staleEntry, ok := store.Get(id.PeerID)
	if !ok {
		t.Fatal("entry should still exist (marked stale)")
	}
	if !staleEntry.Stale {
		t.Error("expired peer should be marked Stale")
	}
}

func TestInterceptUpgraded_HealthyPeer(t *testing.T) {
	gater, store := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	now := time.Now().Unix()
	if err := store.Put(id.PeerID, types.PeerStoreEntry{
		PeerID: id.PeerID,
		Stale:  false,
		Score:  5.0,
		JWTExp: now + 3600, // valid for 1h
	}); err != nil {
		t.Fatalf("put entry: %v", err)
	}

	allow, reason := gater.InterceptUpgraded(dummyConn{remotePeer: pid})
	if !allow {
		t.Error("peer with valid JWT should be allowed")
	}
	if reason != 0 {
		t.Errorf("reason should be 0, got %d", reason)
	}
}

func TestInterceptUpgraded_when_JWTExp_is_zero_expect_allow(t *testing.T) {
	gater, store := newTestGater(t)

	id := newTestIdentity(t)
	pid, err := peer.Decode(string(id.PeerID))
	if err != nil {
		t.Fatalf("decode peer ID: %v", err)
	}

	// Given: discovery-state entry with JWTExp=0 (no JWT yet, not expired)
	if err := store.Put(id.PeerID, types.PeerStoreEntry{
		PeerID: id.PeerID,
		Stale:  false,
		Score:  0.0,
		JWTExp: 0,
	}); err != nil {
		t.Fatalf("put entry: %v", err)
	}

	// When: InterceptUpgraded — must NOT reject + NOT mark stale
	allow, reason := gater.InterceptUpgraded(dummyConn{remotePeer: pid})
	if !allow {
		t.Error("peer with JWTExp=0 should be allowed (no JWT yet, not expired)")
	}
	if reason != 0 {
		t.Errorf("reason should be 0, got %d", reason)
	}

	entry, ok := store.Get(id.PeerID)
	if !ok {
		t.Fatal("entry should still exist")
	}
	if entry.Stale {
		t.Error("JWTExp=0 peer must NOT be marked stale")
	}
}

// ─── Auth protocol ──────────────────────────────────────────────────────────

func TestAuthProtocol(t *testing.T) {
	// Given: control-plane JWT service + verifier
	jwtSvc, cpPub := testJWTService(t)
	verifier := jwt.NewJWTVerifier(cpPub)

	psk := genTestPSK(t)

	id1 := newTestIdentity(t)
	id2 := newTestIdentity(t)

	store1 := newTestStore(t)
	gater1 := NewEdgeConnectionGater(store1, verifier, rate.Limit(1000), 1000, nil)

	store2 := newTestStore(t)
	gater2 := NewEdgeConnectionGater(store2, verifier, rate.Limit(1000), 1000, nil)

	h1, err := NewEdgeHost(id1, []string{"/ip4/127.0.0.1/tcp/0"}, psk, gater1)
	if err != nil {
		t.Fatalf("create host1: %v", err)
	}
	defer func() { _ = h1.Close() }()

	h2, err := NewEdgeHost(id2, []string{"/ip4/127.0.0.1/tcp/0"}, psk, gater2)
	if err != nil {
		t.Fatalf("create host2: %v", err)
	}
	defer func() { _ = h2.Close() }()

	// Given: h2 registers the auth protocol handler with completion signal
	authDone := make(chan struct{})
	h2.SetStreamHandler(AuthProtocol, func(s network.Stream) {
		defer close(authDone)
		if err := HandleAuth(s, gater2); err != nil {
			t.Logf("HandleAuth error: %v", err)
		}
	})

	// Given: h1 has a valid JWT signed by the control plane
	host1JWT := signJWTForPeer(t, jwtSvc, id1.PeerID, id1.PrivKey)

	// When: h1 connects to h2 and presents its JWT
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pi := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}
	if err := h1.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := PresentAuth(ctx, h1, h2.ID(), host1JWT); err != nil {
		t.Fatalf("PresentAuth: %v", err)
	}

	// Wait for HandleAuth to complete.
	select {
	case <-authDone:
	case <-ctx.Done():
		t.Fatal("timed out waiting for HandleAuth")
	}

	// Then: h2's PeerEntryStore should contain h1's entry
	entry, ok := store2.Get(id1.PeerID)
	if !ok {
		t.Fatal("store2 should contain h1's entry after PresentAuth")
	}
	if entry.PeerID != id1.PeerID {
		t.Errorf("entry PeerID = %q, want %q", entry.PeerID, id1.PeerID)
	}
	if entry.Stale {
		t.Error("entry should not be stale")
	}
	if entry.JWT != host1JWT {
		t.Error("entry JWT should match presented JWT")
	}
}

// TestAuthProtocol_PreservesAddrsAndScore verifies that HandleAuth merges
// Addrs and Score from a pre-existing discovery-sourced entry instead of
// clobbering them with zero values.
//
// Given: a peerstore entry for h1 with known Addrs and non-zero Score
// When:  h1 presents Auth to h2 (HandleAuth calls Put with Score=0, Addrs=empty)
// Then:  the resulting entry has the new JWTExp AND preserved Addrs AND preserved Score
func TestAuthProtocol_PreservesAddrsAndScore(t *testing.T) {
	jwtSvc, cpPub := testJWTService(t)
	verifier := jwt.NewJWTVerifier(cpPub)

	psk := genTestPSK(t)

	id1 := newTestIdentity(t)
	id2 := newTestIdentity(t)

	store1 := newTestStore(t)
	gater1 := NewEdgeConnectionGater(store1, verifier, rate.Limit(1000), 1000, nil)

	store2 := newTestStore(t)
	gater2 := NewEdgeConnectionGater(store2, verifier, rate.Limit(1000), 1000, nil)

	preSeededAddrs := []string{"/ip4/10.1.2.3/tcp/9001", "/ip4/10.1.2.4/tcp/9001"}
	preSeededScore := float64(42.5)

	// Given: store2 already has a discovery-sourced entry for h1 with Addrs + Score
	if err := store2.Put(id1.PeerID, types.PeerStoreEntry{
		PeerID:   id1.PeerID,
		Addrs:    preSeededAddrs,
		Score:    preSeededScore,
		LastSeen: time.Now().Unix() - 60,
		Stale:    false,
	}); err != nil {
		t.Fatalf("pre-seed store2: %v", err)
	}

	h1, err := NewEdgeHost(id1, []string{"/ip4/127.0.0.1/tcp/0"}, psk, gater1)
	if err != nil {
		t.Fatalf("create host1: %v", err)
	}
	defer func() { _ = h1.Close() }()

	h2, err := NewEdgeHost(id2, []string{"/ip4/127.0.0.1/tcp/0"}, psk, gater2)
	if err != nil {
		t.Fatalf("create host2: %v", err)
	}
	defer func() { _ = h2.Close() }()

	authDone := make(chan struct{})
	h2.SetStreamHandler(AuthProtocol, func(s network.Stream) {
		defer close(authDone)
		if err := HandleAuth(s, gater2); err != nil {
			t.Logf("HandleAuth error: %v", err)
		}
	})

	host1JWT := signJWTForPeer(t, jwtSvc, id1.PeerID, id1.PrivKey)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pi := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}
	if err := h1.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// When: h1 authenticates to h2
	if err := PresentAuth(ctx, h1, h2.ID(), host1JWT); err != nil {
		t.Fatalf("PresentAuth: %v", err)
	}

	select {
	case <-authDone:
	case <-ctx.Done():
		t.Fatal("timed out waiting for HandleAuth")
	}

	// Then: entry has new auth data AND preserved Addrs AND preserved Score
	entry, ok := store2.Get(id1.PeerID)
	if !ok {
		t.Fatal("store2 should contain h1's entry after PresentAuth")
	}
	if entry.Stale {
		t.Error("entry should not be stale after successful auth")
	}
	if entry.JWT != host1JWT {
		t.Error("entry JWT should match presented JWT")
	}

	// Fail-first assertion: Addrs must be preserved (the bug this test catches)
	if len(entry.Addrs) == 0 {
		t.Error("Addrs clobbered: pre-seeded Addrs lost after HandleAuth — merge bug")
	}
	if len(entry.Addrs) != len(preSeededAddrs) {
		t.Errorf("Addrs count mismatch: got %d, want %d", len(entry.Addrs), len(preSeededAddrs))
	}
	for i, want := range preSeededAddrs {
		if i >= len(entry.Addrs) || entry.Addrs[i] != want {
			t.Errorf("Addrs[%d] = %q, want %q", i, entry.Addrs[i], want)
		}
	}

	if entry.Score != preSeededScore {
		t.Errorf("Score clobbered: got %f, want %f (merge bug)", entry.Score, preSeededScore)
	}
}
