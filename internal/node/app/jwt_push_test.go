package app

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"

	"github.com/shlande/mediaworker/internal/config"
	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	nodejwt "github.com/shlande/mediaworker/internal/node/jwt"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// newPSK returns a PSK derived from a seed (matches jwt_test.go pattern).
func newPSK(seed string) (pnet.PSK, error) {
	hash := sjwt.SHA256Sum([]byte(seed))
	return pnet.PSK(hash[:]), nil
}

// createHost creates a libp2p host bound to localhost with the given key and PSK.
func createHost(t *testing.T, privKey ed25519.PrivateKey, psk pnet.PSK) crypto.PrivKey {
	t.Helper()
	libp2pPriv, err := crypto.UnmarshalEd25519PrivateKey(privKey)
	if err != nil {
		t.Fatalf("unmarshal private key: %v", err)
	}
	return libp2pPriv
}

func newBadgerPeerStore(t *testing.T) *peerstore.PeerEntryStore {
	t.Helper()
	dir := t.TempDir()
	ps, err := peerstore.NewPeerEntryStore(dir + "/ps.db")
	if err != nil {
		t.Fatalf("NewPeerEntryStore: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close() })
	return ps
}

func newMockJWTService(t *testing.T, cpPriv ed25519.PrivateKey, policy config.JWTPolicyConfig) (endpoint string, closeFn func()) {
	t.Helper()
	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(),
		cpjwt.NewRateLimiter(1*time.Millisecond),
		cpjwt.NewAuditLog(nil), policy)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/node/jwt", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req types.JWTRequest
		_ = json.Unmarshal(body, &req)
		resp, err := svc.HandleJWTRequest(req, r.RemoteAddr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	return "http://" + ln.Addr().String() + "/v1/node/jwt", func() { _ = ln.Close() }
}

// ---------------------------------------------------------------------------
// Test: runJWTRefreshLoop pushes refreshed JWT to authenticated peer
// ---------------------------------------------------------------------------

func TestRunJWTRefreshLoop_PushesToAuthenticatedPeer(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	_, aPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate A key: %v", err)
	}
	_, bPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate B key: %v", err)
	}

	psk, err := newPSK("test-loop-push")
	if err != nil {
		t.Fatalf("newPSK: %v", err)
	}

	aPriv := createHost(t, aPrivRaw, psk)
	bPriv := createHost(t, bPrivRaw, psk)

	hostA, err := libp2p.New(
		libp2p.Identity(aPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostA: %v", err)
	}
	defer func() { _ = hostA.Close() }()

	hostB, err := libp2p.New(
		libp2p.Identity(bPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostB: %v", err)
	}
	defer func() { _ = hostB.Close() }()

	if err := hostA.Connect(context.Background(), peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatalf("connect A→B: %v", err)
	}

	aPeerID := types.PeerId(hostA.ID().String())
	bPeerID := types.PeerId(hostB.ID().String())

	// Peer stores: both use real BadgerDB-backed stores.
	psA := newBadgerPeerStore(t)
	psB := newBadgerPeerStore(t)

	// Seed B's store with A entry — use a SHORT expiry so the refreshed
	// JWT (from a 1m TTL policy) has a higher Exp and passes dedup.
	if err := psB.Put(aPeerID, types.PeerStoreEntry{
		PeerID: aPeerID,
		JWTExp: time.Now().Add(1 * time.Second).Unix(),
		JWT:    "old-test-jwt",
	}); err != nil {
		t.Fatalf("seed B store: %v", err)
	}

	// A's store needs B entry with valid JWTExp so pushRefreshedJWT
	// considers B authenticated.
	if err := psA.Put(bPeerID, types.PeerStoreEntry{
		PeerID: bPeerID,
		JWTExp: time.Now().Add(1 * time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("seed A store: %v", err)
	}

	policy := config.JWTPolicyConfig{
		TTL:                  "1m",
		RefreshBeforeSeconds: 0,
		BandwidthQuotaBytes:  50_000_000,
		DefaultCapabilities:  config.JWTPolicyDefaultCapabilities{Edge: true, PeerICP: true},
	}
	endpoint, closeCP := newMockJWTService(t, cpPriv, policy)
	defer closeCP()

	clientA := nodejwt.NewJWTClient(aPrivRaw, aPeerID, endpoint, types.NodeCapabilities{Edge: true, PeerICP: true})
	_, err = clientA.RequestJWT(context.Background())
	if err != nil {
		t.Fatalf("initial RequestJWT: %v", err)
	}

	// B registers the push handler.
	verifierB := nodejwt.NewJWTVerifier(cpPub)
	jwtPeerStoreB := newPeerStoreWriterAdapter(psB)

	var pushCount atomic.Int64
	hostB.SetStreamHandler(nodejwt.JWTRefreshProtocolID, func(s network.Stream) {
		_, err := nodejwt.HandleJWTPush(s, verifierB, jwtPeerStoreB)
		if err != nil {
			t.Errorf("HandleJWTPush on B: %v", err)
			return
		}
		pushCount.Add(1)
	})

	durations := &config.RefreshDurations{}
	durations.Store(100*time.Millisecond, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	go runJWTRefreshLoop(ctx, clientA, cpPub, durations,
		slog.Default(), nil,
		hostA, psA)

	<-ctx.Done()
	time.Sleep(200 * time.Millisecond) // flush final push

	if pushCount.Load() < 1 {
		t.Errorf("expected ≥1 push after refresh cycles, got %d", pushCount.Load())
	}

	// Verify B's JWTExp for A was updated to something future-dated.
	entryAfter, ok := psB.Get(aPeerID)
	if !ok {
		t.Fatal("B should still have A entry after pushes")
	}
	if entryAfter.JWTExp <= time.Now().Add(15*time.Second).Unix() {
		t.Errorf("B JWTExp should be far in the future, got %d (now=%d)",
			entryAfter.JWTExp, time.Now().Unix())
	}
	if entryAfter.JWT == "old-test-jwt" {
		t.Error("B JWT should be updated to the refreshed value, not the old one")
	}
}

// ---------------------------------------------------------------------------
// Test: connected-but-never-authenticated peer does NOT get a push
// ---------------------------------------------------------------------------

func TestRunJWTRefreshLoop_SkipsUnauthenticatedPeer(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	_ = cpPub
	_, aPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate A key: %v", err)
	}
	_, bPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate B key: %v", err)
	}

	psk, err := newPSK("test-loop-skip-unauth")
	if err != nil {
		t.Fatalf("newPSK: %v", err)
	}

	aPriv := createHost(t, aPrivRaw, psk)
	bPriv := createHost(t, bPrivRaw, psk)

	hostA, err := libp2p.New(
		libp2p.Identity(aPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostA: %v", err)
	}
	defer func() { _ = hostA.Close() }()

	hostB, err := libp2p.New(
		libp2p.Identity(bPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostB: %v", err)
	}
	defer func() { _ = hostB.Close() }()

	if err := hostA.Connect(context.Background(), peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatalf("connect A→B: %v", err)
	}

	aPeerID := types.PeerId(hostA.ID().String())
	bPeerID := types.PeerId(hostB.ID().String())

	// A's store: NO entry for B (never authenticated).
	psA := newBadgerPeerStore(t)
	_ = psA

	policy := config.JWTPolicyConfig{
		TTL:                  "100ms",
		RefreshBeforeSeconds: 0,
		BandwidthQuotaBytes:  50_000_000,
		DefaultCapabilities:  config.JWTPolicyDefaultCapabilities{Edge: true, PeerICP: true},
	}
	endpoint, closeCP := newMockJWTService(t, cpPriv, policy)
	defer closeCP()

	clientA := nodejwt.NewJWTClient(aPrivRaw, aPeerID, endpoint, types.NodeCapabilities{Edge: true, PeerICP: true})
	_, err = clientA.RequestJWT(context.Background())
	if err != nil {
		t.Fatalf("initial RequestJWT: %v", err)
	}

	var pushReceived atomic.Int64
	hostB.SetStreamHandler(nodejwt.JWTRefreshProtocolID, func(s network.Stream) {
		pushReceived.Add(1)
		_ = s.Reset()
	})

	durations := &config.RefreshDurations{}
	durations.Store(30*time.Millisecond, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	go runJWTRefreshLoop(ctx, clientA, cpPub, durations,
		slog.Default(), nil,
		hostA, psA)

	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)

	if pushReceived.Load() > 0 {
		t.Errorf("unauthenticated peer should not receive JWT pushes, got %d", pushReceived.Load())
	}

	_ = bPeerID
	_ = bPrivRaw
	_ = hex.EncodeToString
}

// ---------------------------------------------------------------------------
// Test: pushRefreshedJWT unit — skips peer not in store
// ---------------------------------------------------------------------------

func TestPushRefreshedJWT_SkipsPeerNotInStore(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	_, aPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate A key: %v", err)
	}
	_, bPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate B key: %v", err)
	}

	psk, err := newPSK("test-push-skip-nostore")
	if err != nil {
		t.Fatalf("newPSK: %v", err)
	}

	aPriv := createHost(t, aPrivRaw, psk)
	bPriv := createHost(t, bPrivRaw, psk)

	hostA, err := libp2p.New(
		libp2p.Identity(aPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostA: %v", err)
	}
	defer func() { _ = hostA.Close() }()

	hostB, err := libp2p.New(
		libp2p.Identity(bPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostB: %v", err)
	}
	defer func() { _ = hostB.Close() }()

	if err := hostA.Connect(context.Background(), peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatalf("connect A→B: %v", err)
	}

	aPeerID := types.PeerId(hostA.ID().String())

	policy := config.JWTPolicyConfig{
		TTL:                  "1h",
		RefreshBeforeSeconds: 300,
		BandwidthQuotaBytes:  50_000_000,
		DefaultCapabilities:  config.JWTPolicyDefaultCapabilities{Edge: true, PeerICP: true},
	}
	endpoint, closeCP := newMockJWTService(t, cpPriv, policy)
	defer closeCP()

	clientA := nodejwt.NewJWTClient(aPrivRaw, aPeerID, endpoint, types.NodeCapabilities{Edge: true, PeerICP: true})
	_, err = clientA.RequestJWT(context.Background())
	if err != nil {
		t.Fatalf("RequestJWT: %v", err)
	}

	var streamReceived atomic.Int64
	hostB.SetStreamHandler(nodejwt.JWTRefreshProtocolID, func(s network.Stream) {
		streamReceived.Add(1)
		_ = s.Reset()
	})

	// A's store is fresh (no entry for B).
	psA := newBadgerPeerStore(t)

	// This should iterate hostA.Network().Peers() → find B → psA.Get(B) returns false → skip.
	pushRefreshedJWT(context.Background(), clientA, hostA, psA, slog.Default())
	time.Sleep(100 * time.Millisecond)

	if streamReceived.Load() > 0 {
		t.Error("peer not in store should NOT receive a JWT push")
	}

	_ = bPrivRaw
}

// ---------------------------------------------------------------------------
// Test: pushRefreshedJWT skips expired peer
// ---------------------------------------------------------------------------

func TestPushRefreshedJWT_SkipsExpiredPeer(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	_, aPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate A key: %v", err)
	}
	_, bPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate B key: %v", err)
	}

	psk, err := newPSK("test-push-skip-expired")
	if err != nil {
		t.Fatalf("newPSK: %v", err)
	}

	aPriv := createHost(t, aPrivRaw, psk)
	bPriv := createHost(t, bPrivRaw, psk)

	hostA, err := libp2p.New(
		libp2p.Identity(aPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostA: %v", err)
	}
	defer func() { _ = hostA.Close() }()

	hostB, err := libp2p.New(
		libp2p.Identity(bPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostB: %v", err)
	}
	defer func() { _ = hostB.Close() }()

	if err := hostA.Connect(context.Background(), peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatalf("connect A→B: %v", err)
	}

	aPeerID := types.PeerId(hostA.ID().String())
	bPeerID := types.PeerId(hostB.ID().String())

	// A's store has B entry but JWTExp is in the past.
	psA := newBadgerPeerStore(t)
	if err := psA.Put(bPeerID, types.PeerStoreEntry{
		PeerID: bPeerID,
		JWTExp: time.Now().Add(-1 * time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("seed A store: %v", err)
	}

	policy := config.JWTPolicyConfig{
		TTL:                  "1h",
		RefreshBeforeSeconds: 300,
		BandwidthQuotaBytes:  50_000_000,
		DefaultCapabilities:  config.JWTPolicyDefaultCapabilities{Edge: true, PeerICP: true},
	}
	endpoint, closeCP := newMockJWTService(t, cpPriv, policy)
	defer closeCP()

	clientA := nodejwt.NewJWTClient(aPrivRaw, aPeerID, endpoint, types.NodeCapabilities{Edge: true, PeerICP: true})
	_, err = clientA.RequestJWT(context.Background())
	if err != nil {
		t.Fatalf("RequestJWT: %v", err)
	}

	var streamReceived atomic.Int64
	hostB.SetStreamHandler(nodejwt.JWTRefreshProtocolID, func(s network.Stream) {
		streamReceived.Add(1)
		_ = s.Reset()
	})

	pushRefreshedJWT(context.Background(), clientA, hostA, psA, slog.Default())
	time.Sleep(100 * time.Millisecond)

	if streamReceived.Load() > 0 {
		t.Error("expired peer should NOT receive a JWT push")
	}

	_ = bPrivRaw
}

// ---------------------------------------------------------------------------
// Test: pushRefreshedJWT sends to valid peer (unit — single push)
// ---------------------------------------------------------------------------

func TestPushRefreshedJWT_SendsToValidPeer(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	_, aPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate A key: %v", err)
	}
	_, bPrivRaw, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate B key: %v", err)
	}

	psk, err := newPSK("test-push-valid")
	if err != nil {
		t.Fatalf("newPSK: %v", err)
	}

	aPriv := createHost(t, aPrivRaw, psk)
	bPriv := createHost(t, bPrivRaw, psk)

	hostA, err := libp2p.New(
		libp2p.Identity(aPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostA: %v", err)
	}
	defer func() { _ = hostA.Close() }()

	hostB, err := libp2p.New(
		libp2p.Identity(bPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
	if err != nil {
		t.Fatalf("create hostB: %v", err)
	}
	defer func() { _ = hostB.Close() }()

	if err := hostA.Connect(context.Background(), peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatalf("connect A→B: %v", err)
	}

	aPeerID := types.PeerId(hostA.ID().String())
	bPeerID := types.PeerId(hostB.ID().String())

	psA := newBadgerPeerStore(t)
	if err := psA.Put(bPeerID, types.PeerStoreEntry{
		PeerID: bPeerID,
		JWTExp: time.Now().Add(1 * time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("seed A store: %v", err)
	}

	psB := newBadgerPeerStore(t)
	if err := psB.Put(aPeerID, types.PeerStoreEntry{
		PeerID: aPeerID,
		JWTExp: time.Now().Add(10 * time.Minute).Unix(),
		JWT:    "pre-push-jwt",
	}); err != nil {
		t.Fatalf("seed B store: %v", err)
	}

	policy := config.JWTPolicyConfig{
		TTL:                  "1h",
		RefreshBeforeSeconds: 300,
		BandwidthQuotaBytes:  50_000_000,
		DefaultCapabilities:  config.JWTPolicyDefaultCapabilities{Edge: true, PeerICP: true},
	}
	endpoint, closeCP := newMockJWTService(t, cpPriv, policy)
	defer closeCP()

	clientA := nodejwt.NewJWTClient(aPrivRaw, aPeerID, endpoint, types.NodeCapabilities{Edge: true, PeerICP: true})
	_, err = clientA.RequestJWT(context.Background())
	if err != nil {
		t.Fatalf("RequestJWT: %v", err)
	}

	verifierB := nodejwt.NewJWTVerifier(cpPub)
	jwtPeerStoreB := newPeerStoreWriterAdapter(psB)

	var pushOK atomic.Int64
	hostB.SetStreamHandler(nodejwt.JWTRefreshProtocolID, func(s network.Stream) {
		_, err := nodejwt.HandleJWTPush(s, verifierB, jwtPeerStoreB)
		if err != nil {
			t.Errorf("HandleJWTPush: %v", err)
			return
		}
		pushOK.Add(1)
	})

	bBefore, _ := psB.Get(aPeerID)

	pushRefreshedJWT(context.Background(), clientA, hostA, psA, slog.Default())
	time.Sleep(200 * time.Millisecond)

	if pushOK.Load() != 1 {
		t.Fatalf("expected 1 push, got %d", pushOK.Load())
	}

	bAfter, ok := psB.Get(aPeerID)
	if !ok {
		t.Fatal("B should still have A entry")
	}
	if bAfter.JWTExp <= bBefore.JWTExp {
		t.Errorf("B JWTExp should be updated: before=%d after=%d", bBefore.JWTExp, bAfter.JWTExp)
	}
	if bAfter.JWT == "pre-push-jwt" {
		t.Error("B JWT should be refreshed, not old value")
	}

	_ = bPrivRaw
}
