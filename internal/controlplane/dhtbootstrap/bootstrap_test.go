package dhtbootstrap

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	discoveryrouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

// genTestPSK returns a fresh 32-byte PSK for tests.
func genTestPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("generate PSK: %v", err)
	}
	return types.PSK(psk)
}

// genTestIdentity creates an in-memory NodeIdentity for tests (no disk write).
func genTestIdentity(t *testing.T) *identity.NodeIdentity {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	return &identity.NodeIdentity{
		PrivKey: priv,
		PeerID:  types.PeerId(pid.String()),
	}
}

// bootstrapConfig returns a minimal DHTBootstrapConfig suitable for tests.
func bootstrapConfig(listenAddr, namespace string) config.DHTBootstrapConfig {
	return config.DHTBootstrapConfig{
		ListenAddrs:       []string{listenAddr},
		Namespace:         namespace,
		AdvertiseTTL:      "15m",
		AdvertiseInterval: "5m",
		BootstrapPeers:    nil,
	}
}

// nodeHost creates a simplified libp2p host (node-side, with PSK) for test
// peers that connect to the bootstrap host. Unlike the full NewEdgeHost, this
// avoids AutoRelay/DCUtR because tests run on localhost.
func nodeHost(t *testing.T, id *identity.NodeIdentity, psk types.PSK) host.Host {
	t.Helper()
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatalf("create peerstore: %v", err)
	}
	opts := []libp2p.Option{
		libp2p.Identity(id.PrivKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.Peerstore(ps),
	}
	if len(psk) > 0 {
		opts = append(opts, libp2p.PrivateNetwork(pnet.PSK(psk)))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		t.Fatalf("create node host: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// connectMutual establishes bi-directional p2p connections and waits for the
// DHT routing tables to stabilize.
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

// ─── Constructor tests ─────────────────────────────────────────────────────

func TestNewBootstrapHost_NilIdentity(t *testing.T) {
	// Given: a nil identity
	// When: calling NewBootstrapHost
	_, err := NewBootstrapHost(nil, bootstrapConfig("/ip4/127.0.0.1/tcp/0", "test"), nil)
	// Then: error is returned
	if err == nil {
		t.Fatal("expected error for nil identity")
	}
}

func TestNewBootstrapHost_EmptyNamespace(t *testing.T) {
	// Given: an empty namespace
	id := genTestIdentity(t)
	cfg := bootstrapConfig("/ip4/127.0.0.1/tcp/0", "")
	// When: calling NewBootstrapHost
	_, err := NewBootstrapHost(id, cfg, nil)
	// Then: error is returned
	if err == nil {
		t.Fatal("expected error for empty namespace")
	}
}

func TestNewBootstrapHost_InvalidAdvertiseTTL(t *testing.T) {
	// Given: an unparseable advertise_ttl
	id := genTestIdentity(t)
	cfg := config.DHTBootstrapConfig{
		ListenAddrs:       []string{"/ip4/127.0.0.1/tcp/0"},
		Namespace:         "test",
		AdvertiseTTL:      "not-a-duration",
		AdvertiseInterval: "5m",
	}
	// When: calling NewBootstrapHost
	_, err := NewBootstrapHost(id, cfg, nil)
	// Then: parse error is returned
	if err == nil {
		t.Fatal("expected error for invalid advertise_ttl")
	}
}

func TestNewBootstrapHost_InvalidAdvertiseInterval(t *testing.T) {
	// Given: an unparseable advertise_interval
	id := genTestIdentity(t)
	cfg := config.DHTBootstrapConfig{
		ListenAddrs:       []string{"/ip4/127.0.0.1/tcp/0"},
		Namespace:         "test",
		AdvertiseTTL:      "15m",
		AdvertiseInterval: "not-a-duration",
	}
	// When: calling NewBootstrapHost
	_, err := NewBootstrapHost(id, cfg, nil)
	// Then: parse error is returned
	if err == nil {
		t.Fatal("expected error for invalid advertise_interval")
	}
}

func TestNewBootstrapHost_Success(t *testing.T) {
	// Given: a valid identity and config
	id := genTestIdentity(t)
	psk := genTestPSK(t)
	cfg := bootstrapConfig("/ip4/127.0.0.1/tcp/0", "test")

	// When: calling NewBootstrapHost
	bh, err := NewBootstrapHost(id, cfg, psk)
	if err != nil {
		t.Fatalf("NewBootstrapHost: %v", err)
	}
	defer bh.Close()

	// Then: the host is created and the underlying host is accessible
	h := bh.Host()
	if h == nil {
		t.Fatal("Host() returned nil")
	}
	if h.ID().String() != string(id.PeerID) {
		t.Fatalf("host peer ID %s does not match identity %s", h.ID(), id.PeerID)
	}
}

// ─── FindPeers discovery test ───────────────────────────────────────────────

func TestBootstrapHost_FindPeers(t *testing.T) {
	// Given: a BootstrapHost started with PSK, and a node-side DHT peer
	psk := genTestPSK(t)

	bootID := genTestIdentity(t)
	nodeID := genTestIdentity(t)

	bootCfg := bootstrapConfig("/ip4/127.0.0.1/tcp/0", "edge")
	bh, err := NewBootstrapHost(bootID, bootCfg, psk)
	if err != nil {
		t.Fatalf("NewBootstrapHost: %v", err)
	}
	defer bh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := bh.Start(ctx); err != nil {
		t.Fatalf("bootstrap Start: %v", err)
	}

	// Create a node-side host and DHT that will discover the bootstrap.
	nodeH := nodeHost(t, nodeID, psk)
	nodeDHT, err := kaddht.New(ctx, nodeH, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create node DHT: %v", err)
	}
	defer nodeDHT.Close()

	// Connect node → bootstrap and let routing tables stabilize.
	pi := peer.AddrInfo{ID: bh.Host().ID(), Addrs: bh.Host().Addrs()}
	if err := nodeH.Connect(ctx, pi); err != nil {
		t.Fatalf("node connect to bootstrap: %v", err)
	}
	time.Sleep(2 * time.Second) // wait for DHT bootstrap

	// Given: the node-side DHT is connected to the bootstrap
	// When: the node calls FindPeers on the namespace
	nodeDisc := discoveryrouting.NewRoutingDiscovery(nodeDHT)
	peerChan, err := nodeDisc.FindPeers(ctx, "edge", discovery.Limit(10))
	if err != nil {
		t.Fatalf("node FindPeers: %v", err)
	}

	var foundBoot bool
	for p := range peerChan {
		if p.ID == bh.Host().ID() {
			foundBoot = true
		}
	}

	// Then: the bootstrap host is discovered
	if !foundBoot {
		t.Fatal("node did not discover the bootstrap host via FindPeers")
	}
}

// ─── PSK mismatch test ──────────────────────────────────────────────────────

func TestBootstrapHost_PSKMismatch(t *testing.T) {
	// Given: a BootstrapHost with PSK, and a node-side host WITHOUT PSK
	bootPSK := genTestPSK(t)

	bootID := genTestIdentity(t)
	nodeID := genTestIdentity(t)

	bootCfg := bootstrapConfig("/ip4/127.0.0.1/tcp/0", "edge")
	bh, err := NewBootstrapHost(bootID, bootCfg, bootPSK)
	if err != nil {
		t.Fatalf("NewBootstrapHost: %v", err)
	}
	defer bh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := bh.Start(ctx); err != nil {
		t.Fatalf("bootstrap Start: %v", err)
	}

	// Create a node host WITHOUT PSK — connection should be rejected.
	nodeH := nodeHost(t, nodeID, nil)

	// When: the node (no PSK) tries to connect to the bootstrap (PSK)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	pi := peer.AddrInfo{ID: bh.Host().ID(), Addrs: bh.Host().Addrs()}
	err = nodeH.Connect(ctx2, pi)

	// Then: the connection is rejected
	if err == nil {
		t.Fatal("expected connection to be rejected due to PSK mismatch")
	}
}

// ─── Heartbeat loop test ────────────────────────────────────────────────────

func TestBootstrapHost_Heartbeat(t *testing.T) {
	// Given: a BootstrapHost started with a short re-advertise interval
	psk := genTestPSK(t)

	bootID := genTestIdentity(t)
	nodeID := genTestIdentity(t)

	cfg := config.DHTBootstrapConfig{
		ListenAddrs:       []string{"/ip4/127.0.0.1/tcp/0"},
		Namespace:         "edge",
		AdvertiseTTL:      "1s",
		AdvertiseInterval: "500ms",
	}
	bh, err := NewBootstrapHost(bootID, cfg, psk)
	if err != nil {
		t.Fatalf("NewBootstrapHost: %v", err)
	}
	defer bh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := bh.Start(ctx); err != nil {
		t.Fatalf("bootstrap Start: %v", err)
	}

	// Create a node host that can query the bootstrap.
	nodeH := nodeHost(t, nodeID, psk)
	nodeDHT, err := kaddht.New(ctx, nodeH, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		t.Fatalf("create node DHT: %v", err)
	}
	defer nodeDHT.Close()

	pi := peer.AddrInfo{ID: bh.Host().ID(), Addrs: bh.Host().Addrs()}
	if err := nodeH.Connect(ctx, pi); err != nil {
		t.Fatalf("node connect to bootstrap: %v", err)
	}
	time.Sleep(2 * time.Second)

	// When: we wait for several heartbeat cycles
	time.Sleep(3 * time.Second)

	// Then: the bootstrap host can still be discovered (heartbeat hasn't crashed)
	nodeDisc := discoveryrouting.NewRoutingDiscovery(nodeDHT)
	peerChan, err := nodeDisc.FindPeers(ctx, "edge", discovery.Limit(10))
	if err != nil {
		t.Fatalf("node FindPeers after heartbeat: %v", err)
	}

	var foundBoot bool
	for p := range peerChan {
		if p.ID == bh.Host().ID() {
			foundBoot = true
		}
	}
	if !foundBoot {
		t.Fatal("bootstrap host not discovered after heartbeat cycles")
	}
}
