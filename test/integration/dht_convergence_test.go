package integration_test

import (
	"context"
	"testing"
	"time"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/config"
	cpdht "github.com/shlande/mediaworker/internal/controlplane/dhtbootstrap"
	"github.com/shlande/mediaworker/internal/node/dht"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/node/peerstore"
)

// TestDHTBootstrapConvergence locks the todo-56 bug: the CP bootstrap host's
// DHT routing table must contain the edge nodes so that peers can discover
// each other via DHT (kb.ErrLookupFailure "failed to find any peer in table"
// surfaces whenever a lookup runs against an empty table).
//
// The test starts the real CP BootstrapHost (ModeServer) plus two real edge
// hosts (ModeServer, PSK-gated) wired exactly as cmd/edge-node/main.go wires
// EdgeDiscovery, then polls the CP routing table.
func TestDHTBootstrapConvergence(t *testing.T) {
	psk := genTestPSK(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// CP bootstrap host — mirrors cmd/control-plane/main.go wiring.
	boot, err := cpdht.NewBootstrapHost(genTestIdentity(t), config.DHTBootstrapConfig{
		ListenAddrs:       []string{"/ip4/127.0.0.1/tcp/0"},
		Namespace:         "edge",
		AdvertiseTTL:      "2m",
		AdvertiseInterval: "30s",
	}, psk)
	if err != nil {
		t.Fatalf("new bootstrap host: %v", err)
	}
	t.Cleanup(func() { _ = boot.Close() })
	if err := boot.Start(ctx); err != nil {
		t.Fatalf("start bootstrap host: %v", err)
	}
	cpInfo := peer.AddrInfo{ID: boot.Host().ID(), Addrs: boot.Host().Addrs()}
	t.Logf("CP bootstrap peer: %s addrs=%v", cpInfo.ID, cpInfo.Addrs)

	// Two edge nodes — mirrors cmd/edge-node/main.go EdgeDiscovery assembly.
	edgeHosts := make([]*dht.EdgeDiscovery, 0, 2)
	for i := 0; i < 2; i++ {
		h, err := libp2phost.NewEdgeHost(genTestIdentity(t), []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
		if err != nil {
			t.Fatalf("edge host %d: %v", i, err)
		}
		t.Cleanup(func() { _ = h.Close() })

		ps, err := peerstore.NewPeerEntryStore(t.TempDir() + "/peerstore")
		if err != nil {
			t.Fatalf("edge peer store %d: %v", i, err)
		}
		t.Cleanup(func() { _ = ps.Close() })

		disc := dht.NewEdgeDiscovery(h, ps, []peer.AddrInfo{cpInfo},
			"edge", 2*time.Minute, 30*time.Second, kaddht.ModeServer)
		if err := disc.Start(ctx); err != nil {
			t.Fatalf("edge discovery %d: %v", i, err)
		}
		edgeHosts = append(edgeHosts, disc)
		t.Logf("edge %d peer: %s", i, h.ID())
	}

	_ = edgeHosts

	// Assert: CP DHT routing table converges to hold both edge peers.
	deadline := time.Now().Add(15 * time.Second)
	for {
		size := boot.RoutingTableSize()
		if size >= 2 {
			t.Logf("CP routing table converged: size=%d", size)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("CP DHT routing table did not converge: size=%d, want >= 2", size)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestDHTBootstrapPeerIDDrift reproduces the live "failed to find any peer in
// table" failure mode (todo-56, hypothesis H1): when the edge's
// bootstrap_peers carries a peer ID that does not match the CP bootstrap
// host's identity (e.g. stale ID after CP identity regeneration), the edge
// can never join the CP DHT and the CP routing table stays empty. Red
// sibling of TestDHTBootstrapConvergence: same harness, only the bootstrap
// peer ID differs — toggling it toggles the bug.
func TestDHTBootstrapPeerIDDrift(t *testing.T) {
	psk := genTestPSK(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	boot, err := cpdht.NewBootstrapHost(genTestIdentity(t), config.DHTBootstrapConfig{
		ListenAddrs:       []string{"/ip4/127.0.0.1/tcp/0"},
		Namespace:         "edge",
		AdvertiseTTL:      "2m",
		AdvertiseInterval: "30s",
	}, psk)
	if err != nil {
		t.Fatalf("new bootstrap host: %v", err)
	}
	t.Cleanup(func() { _ = boot.Close() })
	if err := boot.Start(ctx); err != nil {
		t.Fatalf("start bootstrap host: %v", err)
	}

	// A peer ID that is NOT the CP's identity, pinned to the CP's real
	// addresses — simulates a stale bootstrap_peers entry.
	wrongID, err := peer.IDFromPrivateKey(genTestIdentity(t).PrivKey)
	if err != nil {
		t.Fatalf("derive wrong peer id: %v", err)
	}
	stale := peer.AddrInfo{ID: wrongID, Addrs: boot.Host().Addrs()}

	h, err := libp2phost.NewEdgeHost(genTestIdentity(t), []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("edge host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ps, err := peerstore.NewPeerEntryStore(t.TempDir() + "/peerstore")
	if err != nil {
		t.Fatalf("edge peer store: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close() })

	disc := dht.NewEdgeDiscovery(h, ps, []peer.AddrInfo{stale},
		"edge", 2*time.Minute, 30*time.Second, kaddht.ModeServer)
	if err := disc.Start(ctx); err == nil {
		t.Fatal("edge discovery Start should fail when bootstrap_peers peer ID is stale")
	} else {
		t.Logf("edge Start failed as expected: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	if size := boot.RoutingTableSize(); size != 0 {
		t.Fatalf("CP routing table should stay empty with stale bootstrap peer ID, got size=%d", size)
	}
}
