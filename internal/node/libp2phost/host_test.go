package libp2phost

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

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
