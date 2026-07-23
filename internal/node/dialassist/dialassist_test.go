package dialassist

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/shlande/mediaworker/internal/node/libp2phost"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

type fakeAddrSource struct {
	addrs map[peer.ID][]string
}

func (s *fakeAddrSource) AddrsOf(pid peer.ID) ([]string, bool) {
	addrs, ok := s.addrs[pid]
	return addrs, ok
}

func genTestPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("generate PSK: %v", err)
	}
	return types.PSK(psk)
}

func genTestHost(t *testing.T, psk types.PSK) host.Host {
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
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func TestReseedAndRetry_AddrSourceHasAddrs_Success(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	addrStrs := make([]string, len(h1.Addrs()))
	for i, a := range h1.Addrs() {
		addrStrs[i] = a.String()
	}
	if len(addrStrs) == 0 {
		t.Fatal("h1 has no listen addrs")
	}

	src := &fakeAddrSource{
		addrs: map[peer.ID][]string{h1.ID(): addrStrs},
	}

	if len(h2.Peerstore().Addrs(h1.ID())) > 0 {
		t.Fatal("precondition: h2 peerstore unexpectedly has addrs for h1 before test")
	}

	testProto := protocol.ID("/test/dialassist/1.0.0")
	h1.SetStreamHandler(testProto, func(s network.Stream) {
		_ = s.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dial := func(ctx context.Context) (network.Stream, error) {
		dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
		defer dialCancel()
		return h2.NewStream(dialCtx, h1.ID(), testProto)
	}

	stream, err := ReseedAndRetry(ctx, h2, h1.ID(), src, dial)
	if err != nil {
		t.Fatalf("ReseedAndRetry: expected success after reseed, got: %v", err)
	}
	_ = stream.Close()

	if len(h2.Peerstore().Addrs(h1.ID())) == 0 {
		t.Error("after reseed, h2 peerstore should have addrs for h1")
	}
}

func TestReseedAndRetry_NoAddrsInSource_ReturnsOriginalError(t *testing.T) {
	psk := genTestPSK(t)
	h := genTestHost(t, psk)

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	targetID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}

	src := &fakeAddrSource{addrs: map[peer.ID][]string{}}

	dialErr := errors.New("no good addresses")

	dial := func(ctx context.Context) (network.Stream, error) {
		return nil, dialErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = ReseedAndRetry(ctx, h, targetID, src, dial)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, dialErr) {
		t.Errorf("expected original dial error, got: %v", err)
	}
}

func TestReseedAndRetry_NilSource_ReturnsOriginalError(t *testing.T) {
	psk := genTestPSK(t)
	h := genTestHost(t, psk)

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	targetID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}

	dialErr := errors.New("dial failed")

	dial := func(ctx context.Context) (network.Stream, error) {
		return nil, dialErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = ReseedAndRetry(ctx, h, targetID, nil, dial)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, dialErr) {
		t.Errorf("expected original dial error, got: %v", err)
	}
}

func TestReseedAndRetry_FirstDialSucceeds_NoReseed(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pi := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}
	if err := h2.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}

	testProto := protocol.ID("/test/dialassist/happy/1.0.0")
	h1.SetStreamHandler(testProto, func(s network.Stream) {
		_ = s.Close()
	})

	src := &fakeAddrSource{
		addrs: map[peer.ID][]string{h1.ID(): {"/ip4/127.0.0.1/tcp/1"}},
	}

	dial := func(ctx context.Context) (network.Stream, error) {
		dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
		defer dialCancel()
		return h2.NewStream(dialCtx, h1.ID(), testProto)
	}

	stream, err := ReseedAndRetry(ctx, h2, h1.ID(), src, dial)
	if err != nil {
		t.Fatalf("expected success on first dial, got: %v", err)
	}
	_ = stream.Close()
}

func TestParseAddrs_AllValid(t *testing.T) {
	input := []string{
		"/ip4/127.0.0.1/tcp/9001",
		"/ip4/10.0.0.1/tcp/9001",
	}
	result := ParseAddrs(input)
	if len(result) != 2 {
		t.Fatalf("expected 2 addrs, got %d", len(result))
	}
	if result[0].String() != input[0] {
		t.Errorf("got %q, want %q", result[0], input[0])
	}
	if result[1].String() != input[1] {
		t.Errorf("got %q, want %q", result[1], input[1])
	}
}

func TestParseAddrs_MixedValidInvalid(t *testing.T) {
	input := []string{
		"/ip4/127.0.0.1/tcp/9001",
		"not-a-multiaddr",
		"/ip4/10.0.0.1/tcp/9001",
	}
	result := ParseAddrs(input)
	if len(result) != 2 {
		t.Fatalf("expected 2 valid addrs (1 skipped), got %d", len(result))
	}
	if result[0].String() != input[0] {
		t.Errorf("first: got %q, want %q", result[0], input[0])
	}
	if result[1].String() != input[2] {
		t.Errorf("second: got %q, want %q", result[1], input[2])
	}
}

func TestParseAddrs_Empty(t *testing.T) {
	result := ParseAddrs(nil)
	if len(result) != 0 {
		t.Errorf("expected 0 addrs from nil, got %d", len(result))
	}
	result = ParseAddrs([]string{})
	if len(result) != 0 {
		t.Errorf("expected 0 addrs from empty, got %d", len(result))
	}
}
