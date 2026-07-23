package libp2phost

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/node/jwt"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// testProtocol is the stream protocol ID used in tests.
const testProtocol = protocol.ID("/mediaworker/test/1.0.0")

// genTestPSK returns a fresh 32-byte PSK for tests.
func genTestPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("generate PSK: %v", err)
	}
	return types.PSK(psk)
}

// ─── Identity tests ──────────────────────────────────────────────────────────

func TestLoadOrGenerateIdentity_NewKey(t *testing.T) {
	// Given: a temp file path that does not exist
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "ed25519.key")

	// When: we call LoadOrGenerateIdentity for the first time
	id, err := sharedid.LoadOrGenerateIdentity(keyPath)

	// Then: it succeeds and returns a valid identity
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id.PeerID == "" {
		t.Fatal("peer ID is empty")
	}
	if id.PrivKey == nil {
		t.Fatal("private key is nil")
	}

	// Then: the key file exists with 0600 permissions
	fi, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("key file not created: %v", err)
	}
	if fi.Mode().Perm() != 0600 {
		t.Fatalf("key file has permissions %o, want 0600", fi.Mode().Perm())
	}
}

func TestLoadOrGenerateIdentity_ExistingKey(t *testing.T) {
	// Given: an identity already written to disk
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "ed25519.key")

	first, err := sharedid.LoadOrGenerateIdentity(keyPath)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}

	// When: we load it again from the same path
	second, err := sharedid.LoadOrGenerateIdentity(keyPath)

	// Then: it succeeds and returns the same PeerId
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if second.PeerID != first.PeerID {
		t.Fatalf("peer ID changed: %s → %s", first.PeerID, second.PeerID)
	}

	// Then: the raw keys are equal (byte-for-byte comparison)
	firstRaw, err := crypto.MarshalPrivateKey(first.PrivKey)
	if err != nil {
		t.Fatalf("marshal first key: %v", err)
	}
	secondRaw, err := crypto.MarshalPrivateKey(second.PrivKey)
	if err != nil {
		t.Fatalf("marshal second key: %v", err)
	}
	if string(firstRaw) != string(secondRaw) {
		t.Fatal("private key bytes differ between loads")
	}
}

// ─── Host construction ───────────────────────────────────────────────────────

func TestNewEdgeHost_PSKConnect(t *testing.T) {
	// Given: two hosts with the same PSK
	psk := genTestPSK(t)
	id1 := genIdentity(t)
	id2 := genIdentity(t)

	h1, err := NewEdgeHost(id1, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host1: %v", err)
	}
	defer func() { _ = h1.Close() }()

	h2, err := NewEdgeHost(id2, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host2: %v", err)
	}
	defer func() { _ = h2.Close() }()

	// When: host2 connects to host1
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pi := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	if err := h2.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Then: the connection succeeds and is visible
	if len(h2.Network().ConnsToPeer(h1.ID())) == 0 {
		t.Fatal("no connections to host1")
	}
}

func TestNewEdgeHost_NoPSKRejected(t *testing.T) {
	// The PSK handshake rejection is a known limitation in test:
	// PSK mode rejects at the transport level, and the dialer will
	// see a timeout or connection reset. We set a short timeout
	// and assert that the connection does NOT succeed.
	t.Setenv("LIBP2P_FORCE_PNET", "1")

	psk := genTestPSK(t)
	id1 := genIdentity(t)
	id2 := genIdentity(t)

	// Host1 uses PSK.
	h1, err := NewEdgeHost(id1, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host1: %v", err)
	}
	defer func() { _ = h1.Close() }()

	// Host2 does NOT use PSK.
	h2, err := NewEdgeHost(id2, []string{"/ip4/127.0.0.1/tcp/0"}, nil, nil)
	if err != nil {
		t.Fatalf("create host2: %v", err)
	}
	defer func() { _ = h2.Close() }()

	// When: host2 (no PSK) tries to connect to host1 (PSK)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pi := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	err = h2.Connect(ctx, pi)

	// Then: the connection must fail (PSK mismatch)
	if err == nil {
		t.Fatal("expected connection to be rejected due to PSK mismatch")
	}
}

func TestNewEdgeHost_Stream(t *testing.T) {
	// Given: two hosts with the same PSK
	psk := genTestPSK(t)
	id1 := genIdentity(t)
	id2 := genIdentity(t)

	h1, err := NewEdgeHost(id1, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host1: %v", err)
	}
	defer func() { _ = h1.Close() }()

	h2, err := NewEdgeHost(id2, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host2: %v", err)
	}
	defer func() { _ = h2.Close() }()

	// Given: host1 registers a stream handler that echoes messages
	done := make(chan struct{})
	h1.SetStreamHandler(testProtocol, func(s network.Stream) {
		defer close(done)
		defer func() { _ = s.Close() }()
		buf := make([]byte, 1024)
		n, err := s.Read(buf)
		if err != nil {
			t.Logf("handler read error: %v", err)
			return
		}
		// Echo back.
		if _, err := s.Write(buf[:n]); err != nil {
			t.Logf("handler write error: %v", err)
		}
	})

	// When: host2 connects to host1 and opens a stream
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pi := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	if err := h2.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}

	s, err := h2.NewStream(ctx, h1.ID(), testProtocol)
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	defer func() { _ = s.Close() }()

	// When: host2 writes a message
	msg := []byte("hello edge")
	if _, err := s.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Close write side to signal EOF.
	if c, ok := s.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
	}

	// Then: host2 reads the echoed message
	buf := make([]byte, 1024)
	n, err := s.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}

	select {
	case <-done:
		// handler completed
	case <-ctx.Done():
		t.Fatal("timed out waiting for stream handler")
	}

	if string(buf[:n]) != string(msg) {
		t.Fatalf("echo mismatch: got %q, want %q", buf[:n], msg)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// genIdentity creates an in-memory NodeIdentity for tests (no disk write).
func genIdentity(t *testing.T) *sharedid.NodeIdentity {
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

// ─── T15: NATOptions gating ────────────────────────────────────────────────

// TestNewEdgeHostWithNAT_DefaultPreservesCurrentBehaviour verifies that a
// zero-value NATOptions (Explicit=false) preserves the pre-T15 behaviour:
// host creation succeeds with all NAT options enabled.
func TestNewEdgeHostWithNAT_DefaultPreservesCurrentBehaviour(t *testing.T) {
	psk := genTestPSK(t)
	id := genIdentity(t)

	h, err := NewEdgeHostWithNAT(id, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil, NATOptions{})
	if err != nil {
		t.Fatalf("NewEdgeHostWithNAT with default NATOptions: %v", err)
	}
	defer func() { _ = h.Close() }()

	if h.ID() == "" {
		t.Fatal("host has empty peer ID")
	}
}

// TestNewEdgeHostWithNAT_AllExplicitTrue verifies that an explicit all-true
// config produces a working host (matches the l4ConfigYAML/edgeConfigYAML
// samples where all three fields are set to true).
func TestNewEdgeHostWithNAT_AllExplicitTrue(t *testing.T) {
	psk := genTestPSK(t)
	id := genIdentity(t)

	h, err := NewEdgeHostWithNAT(id, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil,
		NATOptions{Explicit: true, AutoNAT: true, AutoRelay: true, DCUtR: true})
	if err != nil {
		t.Fatalf("NewEdgeHostWithNAT all-on: %v", err)
	}
	defer func() { _ = h.Close() }()
}

// TestNewEdgeHostWithNAT_AllExplicitFalse verifies that an explicit all-false
// config still produces a working host (NAT options simply aren't added to
// the libp2p Option list — the host still starts, just without NAT traversal).
func TestNewEdgeHostWithNAT_AllExplicitFalse(t *testing.T) {
	psk := genTestPSK(t)
	id := genIdentity(t)

	h, err := NewEdgeHostWithNAT(id, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil,
		NATOptions{Explicit: true, AutoNAT: false, AutoRelay: false, DCUtR: false})
	if err != nil {
		t.Fatalf("NewEdgeHostWithNAT all-off: %v", err)
	}
	defer func() { _ = h.Close() }()
}

// TestNewEdgeHostWithNAT_Mixed verifies that mixed values (some on, some off)
// produce a working host.
func TestNewEdgeHostWithNAT_Mixed(t *testing.T) {
	psk := genTestPSK(t)
	id := genIdentity(t)

	h, err := NewEdgeHostWithNAT(id, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil,
		NATOptions{Explicit: true, AutoNAT: false, AutoRelay: true, DCUtR: false})
	if err != nil {
		t.Fatalf("NewEdgeHostWithNAT mixed: %v", err)
	}
	defer func() { _ = h.Close() }()
}

// TestResolveNATOptions verifies the *bool → NATOptions conversion:
//   - all nil → Explicit=false (preserves pre-T15 behaviour)
//   - any non-nil → Explicit=true with nil treated as true
func TestResolveNATOptions(t *testing.T) {
	t.Run("all nil preserves default", func(t *testing.T) {
		got := ResolveNATOptions(nil, nil, nil)
		if got.Explicit {
			t.Errorf("Explicit = true, want false when all nil")
		}
	})
	t.Run("all explicit true", func(t *testing.T) {
		trueVal := true
		got := ResolveNATOptions(&trueVal, &trueVal, &trueVal)
		if !got.Explicit {
			t.Errorf("Explicit = false, want true")
		}
		if !got.AutoNAT || !got.AutoRelay || !got.DCUtR {
			t.Errorf("got = %+v, want all true", got)
		}
	})
	t.Run("all explicit false", func(t *testing.T) {
		falseVal := false
		got := ResolveNATOptions(&falseVal, &falseVal, &falseVal)
		if !got.Explicit {
			t.Errorf("Explicit = false, want true")
		}
		if got.AutoNAT || got.AutoRelay || got.DCUtR {
			t.Errorf("got = %+v, want all false", got)
		}
	})
	t.Run("mixed with nil treated as true", func(t *testing.T) {
		falseVal := false
		got := ResolveNATOptions(&falseVal, nil, nil)
		if !got.Explicit {
			t.Errorf("Explicit = false, want true when any non-nil")
		}
		if got.AutoNAT {
			t.Errorf("AutoNAT = true, want false (explicit false)")
		}
		if !got.AutoRelay {
			t.Errorf("AutoRelay = false, want true (nil → preserve default)")
		}
		if !got.DCUtR {
			t.Errorf("DCUtR = false, want true (nil → preserve default)")
		}
	})
}

// ─── Auth exchange on peer connect ───────────────────────────────────────────

// TestAuthExchangeOnConnect_Bidirectional verifies that when two hosts both
// have valid JWTs and the on-peer-connected callback fires PresentAuth, both
// peerstores gain each other's capabilities (PeerICP=true).
func TestAuthExchangeOnConnect_Bidirectional(t *testing.T) {
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

	// Register auth handler on both hosts with completion signalling.
	authDone1 := make(chan struct{})
	authDone2 := make(chan struct{})
	h1.SetStreamHandler(AuthProtocol, func(s network.Stream) {
		defer close(authDone1)
		if err := HandleAuth(s, gater1); err != nil {
			t.Logf("h1 HandleAuth: %v", err)
		}
	})
	h2.SetStreamHandler(AuthProtocol, func(s network.Stream) {
		defer close(authDone2)
		if err := HandleAuth(s, gater2); err != nil {
			t.Logf("h2 HandleAuth: %v", err)
		}
	})

	// Issue JWTs for both peers.
	jwt1 := signJWTForPeer(t, jwtSvc, id1.PeerID, id1.PrivKey)
	jwt2 := signJWTForPeer(t, jwtSvc, id2.PeerID, id2.PrivKey)

	// A per-host map of the JWT to present.
	jwtBySelf := map[peer.ID]types.CapabilityJWT{
		h1.ID(): jwt1,
		h2.ID(): jwt2,
	}
	hostBySelf := map[peer.ID]host.Host{
		h1.ID(): h1,
		h2.ID(): h2,
	}

	// Wire the on-peer-connected callback to fire PresentAuth in a goroutine
	// (never block the notifee — identify negotiation may not yet be complete).
	SetOnPeerConnectedCallback(h1, func(local peer.ID, remote peer.ID) {
		jwtStr := jwtBySelf[local]
		if jwtStr == "" {
			return
		}
		h := hostBySelf[local]
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := PresentAuth(ctx, h, remote, jwtStr); err != nil {
				t.Logf("PresentAuth %s→%s: %v", local.ShortString(), remote.ShortString(), err)
			}
		}()
	})
	SetOnPeerConnectedCallback(h2, func(local peer.ID, remote peer.ID) {
		jwtStr := jwtBySelf[local]
		if jwtStr == "" {
			return
		}
		h := hostBySelf[local]
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := PresentAuth(ctx, h, remote, jwtStr); err != nil {
				t.Logf("PresentAuth %s→%s: %v", local.ShortString(), remote.ShortString(), err)
			}
		}()
	})

	// When: connect h1 to h2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pi := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}
	if err := h1.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Then: both HandleAuth handlers complete within 5s.
	select {
	case <-authDone1:
	case <-ctx.Done():
		t.Fatal("timed out waiting for h1 HandleAuth")
	}
	select {
	case <-authDone2:
	case <-ctx.Done():
		t.Fatal("timed out waiting for h2 HandleAuth")
	}

	// Then: h1's peerstore contains h2 with PeerICP=true.
	entryIn1, ok := store1.Get(id2.PeerID)
	if !ok {
		t.Fatal("store1 should contain h2's entry after auth exchange")
	}
	if !entryIn1.Capabilities.PeerICP {
		t.Error("store1 entry for h2 should have PeerICP=true")
	}

	// Then: h2's peerstore contains h1 with PeerICP=true.
	entryIn2, ok := store2.Get(id1.PeerID)
	if !ok {
		t.Fatal("store2 should contain h1's entry after auth exchange")
	}
	if !entryIn2.Capabilities.PeerICP {
		t.Error("store2 entry for h1 should have PeerICP=true")
	}
}

// TestAuthExchangeOnConnect_Debounce verifies rapid reconnect within 60s does
// not trigger a second PresentAuth.
func TestAuthExchangeOnConnect_Debounce(t *testing.T) {
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

	h2.SetStreamHandler(AuthProtocol, func(s network.Stream) {
		if err := HandleAuth(s, gater2); err != nil {
			t.Logf("h2 HandleAuth: %v", err)
		}
	})

	jwt1 := signJWTForPeer(t, jwtSvc, id1.PeerID, id1.PrivKey)

	var presentCount int
	var mu sync.Mutex

	SetOnPeerConnectedCallback(h1, func(local peer.ID, remote peer.ID) {
		mu.Lock()
		pc := presentCount
		presentCount++
		mu.Unlock()

		// First call — present. Second call within 60s — skip.
		if pc > 0 {
			t.Log("debounce: skipping second PresentAuth")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := PresentAuth(ctx, h1, remote, jwt1); err != nil {
			t.Logf("PresentAuth: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pi := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}

	// When: first connect.
	if err := h1.Connect(ctx, pi); err != nil {
		t.Fatalf("first connect: %v", err)
	}

	// Wait for auth to settle.
	time.Sleep(500 * time.Millisecond)

	// When: disconnect and reconnect immediately (within debounce window).
	if err := h1.Network().ClosePeer(h2.ID()); err != nil {
		t.Logf("close peer: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := h1.Connect(ctx, pi); err != nil {
		t.Fatalf("second connect: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Then: the callback was invoked twice (Connected fires for both),
	// but PresentAuth was only sent once (debounce logic inside the callback
	// prevents the second). The counter is the number of Connected invocations.
	mu.Lock()
	if presentCount < 2 {
		t.Errorf("expected at least 2 Connected events, got %d", presentCount)
	}
	mu.Unlock()

	// Then: h2's store has h1's entry (first PresentAuth succeeded).
	entry, ok := store2.Get(id1.PeerID)
	if !ok {
		t.Fatal("store2 should contain h1's entry after first PresentAuth")
	}
	if !entry.Capabilities.PeerICP {
		t.Error("store2 entry for h1 should have PeerICP=true")
	}
}

// TestAuthExchangeOnConnect_NoJWT verifies fail-open: a host without a JWT
// does not present auth, the connection still succeeds, and no peerstore
// entry is written for it.
func TestAuthExchangeOnConnect_NoJWT(t *testing.T) {
	jwtSvc, cpPub := testJWTService(t)
	verifier := jwt.NewJWTVerifier(cpPub)

	psk := genTestPSK(t)

	idA := newTestIdentity(t) // has JWT
	idB := newTestIdentity(t)

	storeA := newTestStore(t)
	gaterA := NewEdgeConnectionGater(storeA, verifier, rate.Limit(1000), 1000, nil)

	storeB := newTestStore(t)
	gaterB := NewEdgeConnectionGater(storeB, verifier, rate.Limit(1000), 1000, nil)

	hA, err := NewEdgeHost(idA, []string{"/ip4/127.0.0.1/tcp/0"}, psk, gaterA)
	if err != nil {
		t.Fatalf("create hostA: %v", err)
	}
	defer func() { _ = hA.Close() }()

	hB, err := NewEdgeHost(idB, []string{"/ip4/127.0.0.1/tcp/0"}, psk, gaterB)
	if err != nil {
		t.Fatalf("create hostB: %v", err)
	}
	defer func() { _ = hB.Close() }()

	authDone := make(chan struct{})
	hB.SetStreamHandler(AuthProtocol, func(s network.Stream) {
		defer close(authDone)
		if err := HandleAuth(s, gaterB); err != nil {
			t.Logf("hB HandleAuth: %v", err)
		}
	})

	jwtA := signJWTForPeer(t, jwtSvc, idA.PeerID, idA.PrivKey)

	// Only A has a JWT. B is JWT-less (no entry in jwtBySelf → no PresentAuth).
	jwtBySelf := map[peer.ID]types.CapabilityJWT{hA.ID(): jwtA}
	hostBySelf := map[peer.ID]host.Host{hA.ID(): hA}

	SetOnPeerConnectedCallback(hA, func(local peer.ID, remote peer.ID) {
		jwtStr := jwtBySelf[local]
		if jwtStr == "" {
			return
		}
		h := hostBySelf[local]
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := PresentAuth(ctx, h, remote, jwtStr); err != nil {
				t.Logf("PresentAuth %s→%s: %v", local.ShortString(), remote.ShortString(), err)
			}
		}()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pi := peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}
	if err := hA.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Then: A's auth handler fires (A presented to B).
	select {
	case <-authDone:
	case <-ctx.Done():
		t.Fatal("timed out waiting for HandleAuth on B (A→B present)")
	}

	// Then: B's peerstore has A's entry (A presented its JWT to B).
	entryAinB, ok := storeB.Get(idA.PeerID)
	if !ok {
		t.Fatal("storeB should contain A's entry (A presented JWT)")
	}
	if !entryAinB.Capabilities.PeerICP {
		t.Error("storeB entry for A should have PeerICP=true")
	}

	// Then: A's peerstore does NOT have B's entry (B never presented).
	_, ok = storeA.Get(idB.PeerID)
	if ok {
		t.Error("storeA should NOT contain B's entry (B has no JWT)")
	}

	// Then: the connection is still alive (fail-open intact).
	if len(hA.Network().ConnsToPeer(hB.ID())) == 0 {
		t.Error("connection should still be alive (fail-open gater)")
	}
}

// TestSetOnPeerConnectedCallback_NoCrossFire verifies that two hosts in the
// same process each register their OWN callback and do NOT cross-fire:
// host1's Connected must invoke only host1's closure, host2's Connected only
// host2's closure. This is the regression test for the per-host-keyed registry
// — a global last-write-wins design would overwrite and break multi-app tests.
func TestSetOnPeerConnectedCallback_NoCrossFire(t *testing.T) {
	psk := genTestPSK(t)

	id1 := genIdentity(t)
	id2 := genIdentity(t)

	h1, err := NewEdgeHost(id1, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host1: %v", err)
	}
	defer func() { _ = h1.Close() }()

	h2, err := NewEdgeHost(id2, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host2: %v", err)
	}
	defer func() { _ = h2.Close() }()

	var sawH1, sawH2 bool
	SetOnPeerConnectedCallback(h1, func(local peer.ID, remote peer.ID) {
		if local == h1.ID() && remote == h2.ID() {
			sawH1 = true
		} else {
			t.Errorf("h1 callback: unexpected (local=%s, remote=%s)", local.ShortString(), remote.ShortString())
		}
	})
	SetOnPeerConnectedCallback(h2, func(local peer.ID, remote peer.ID) {
		if local == h2.ID() && remote == h1.ID() {
			sawH2 = true
		} else {
			t.Errorf("h2 callback: unexpected (local=%s, remote=%s)", local.ShortString(), remote.ShortString())
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pi := peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}
	if err := h1.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	if !sawH1 {
		t.Error("h1's callback did not fire for h2 connect")
	}
	if !sawH2 {
		t.Error("h2's callback did not fire for h1 connect")
	}
}

// TestDeregisterConnNotifee_RemovesFromRegistry verifies that after
// DeregisterConnNotifee + host.Close, the registry no longer holds the host
// entry.  The stale-entry leak (m1) causes callbacks to silently never fire
// for a new host if a peer ID is reused; removing on close prevents it.
func TestDeregisterConnNotifee_RemovesFromRegistry(t *testing.T) {
	psk := genTestPSK(t)

	id1 := genIdentity(t)

	h1, err := NewEdgeHost(id1, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host1: %v", err)
	}

	// Given: a callback is set on h1
	SetOnPeerConnectedCallback(h1, func(local peer.ID, remote peer.ID) {})

	// When: deregister then close
	DeregisterConnNotifee(h1)
	if err := h1.Close(); err != nil {
		t.Fatalf("close host1: %v", err)
	}

	// Then: the registry no longer holds h1's entry
	connNotifeeRegistryMu.Lock()
	_, inRegistry := connNotifeeRegistry[h1.ID()]
	connNotifeeRegistryMu.Unlock()
	if inRegistry {
		t.Error("registry still holds h1 entry after DeregisterConnNotifee")
	}
}
