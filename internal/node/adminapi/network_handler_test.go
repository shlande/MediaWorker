package adminapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/test"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/shlande/mediaworker/internal/node/netstats"
	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Fakes for the narrow dependency views
// ---------------------------------------------------------------------------

type fakeHost struct{ addrs []ma.Multiaddr }

func (f fakeHost) Addrs() []ma.Multiaddr { return f.addrs }

// netFakeConn embeds network.Conn (nil) and overrides only the two methods
// the handler calls; any other call panics, which no test path triggers.
type netFakeConn struct {
	network.Conn
	dir    network.Direction
	remote ma.Multiaddr
}

func (c netFakeConn) Stat() network.ConnStats {
	return network.ConnStats{Stats: network.Stats{Direction: c.dir}}
}
func (c netFakeConn) RemoteMultiaddr() ma.Multiaddr { return c.remote }

type netFakeConnSource struct{ conns []network.Conn }

func (f netFakeConnSource) Conns() []network.Conn { return f.conns }

type fakeDHT struct{ size int }

func (f fakeDHT) RoutingTableSize() int { return f.size }

type fakeRing struct {
	size int
	pct  float64
	ok   bool
}

func (f fakeRing) Size() int                    { return f.size }
func (f fakeRing) PositionPct() (float64, bool) { return f.pct, f.ok }

type fakePeerStore struct{ entries []types.PeerStoreEntry }

func (f fakePeerStore) List() []types.PeerStoreEntry { return f.entries }

func mustAddr(t *testing.T, s string) ma.Multiaddr {
	t.Helper()
	a, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatalf("bad multiaddr %q: %v", s, err)
	}
	return a
}

func fullNetworkDeps(t *testing.T) NetworkDeps {
	t.Helper()
	stats := netstats.New()
	stats.SetReachability(netstats.ReachabilityPrivate)
	relayAddr := fmt.Sprintf("/ip4/10.0.0.3/tcp/4001/p2p/%s/p2p-circuit/p2p/%s",
		test.RandPeerIDFatal(t), test.RandPeerIDFatal(t))
	return NetworkDeps{
		Host: fakeHost{addrs: []ma.Multiaddr{
			mustAddr(t, "/ip4/0.0.0.0/tcp/9001"),
			mustAddr(t, "/ip4/0.0.0.0/udp/9001/quic"),
		}},
		Conns: netFakeConnSource{conns: []network.Conn{
			netFakeConn{dir: network.DirInbound, remote: mustAddr(t, "/ip4/10.0.0.1/tcp/4001")},
			netFakeConn{dir: network.DirOutbound, remote: mustAddr(t, "/ip4/10.0.0.2/tcp/4001")},
			netFakeConn{dir: network.DirOutbound, remote: mustAddr(t, relayAddr)},
		}},
		DHT:     fakeDHT{size: 7},
		DHTMode: "server",
		Ring:    fakeRing{size: 3, pct: 0.42, ok: true},
		Peers: fakePeerStore{entries: []types.PeerStoreEntry{
			{
				PeerID:       "12D3KooWPeer",
				Addrs:        []string{"/ip4/10.0.0.9/tcp/4001"},
				Capabilities: types.NodeCapabilities{Edge: true, PeerICP: true},
				Score:        3.5,
				Stale:        false,
				LastSeen:     1_700_000_000,
			},
		}},
		Stats: stats,
	}
}

func newNetworkServer(t *testing.T, deps NetworkDeps) *Server {
	t.Helper()
	srv := NewServer(testToken)
	RegisterNetworkRoutes(srv, deps)
	return srv
}

// Given fully-wired deps, when GET /v1/network fires, then every contract
// field is populated from the components.
func TestNetwork_AllFields(t *testing.T) {
	srv := newNetworkServer(t, fullNetworkDeps(t))

	rr := doGet(t, srv, "/v1/network", testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp networkResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.ListenAddrs) != 2 || resp.ListenAddrs[0] != "/ip4/0.0.0.0/tcp/9001" {
		t.Fatalf("listen_addrs = %v", resp.ListenAddrs)
	}
	if resp.Conn.Total != 3 || resp.Conn.In != 1 || resp.Conn.Out != 2 {
		t.Fatalf("conn = %+v, want {3,1,2}", resp.Conn)
	}
	if resp.DHT.Mode != "server" || resp.DHT.TableSize != 7 {
		t.Fatalf("dht = %+v", resp.DHT)
	}
	if !resp.GossipSub.Subscribed {
		t.Fatal("gossipsub.subscribed = false, want true")
	}
	if resp.NAT.Reachability != netstats.ReachabilityPrivate {
		t.Fatalf("nat.reachability = %q, want private", resp.NAT.Reachability)
	}
	if resp.NAT.RelayCircuits != 1 {
		t.Fatalf("nat.relay_circuits = %d, want 1 (one /p2p-circuit conn)", resp.NAT.RelayCircuits)
	}
	if resp.HashRing.PositionPct == nil || *resp.HashRing.PositionPct != 0.42 {
		t.Fatalf("hash_ring.position_pct = %v, want 0.42", resp.HashRing.PositionPct)
	}
	if resp.HashRing.PeersOnRing != 3 {
		t.Fatalf("hash_ring.peers_on_ring = %d, want 3", resp.HashRing.PeersOnRing)
	}
}

// Given the locked go-libp2p version (no hole-punch event), when reading the
// NAT card, then dcutr_success_rate is null and the unavailable flag is set.
func TestNetwork_DCUtRUnavailableDegrades(t *testing.T) {
	deps := fullNetworkDeps(t) // tracker with no event source: unavailable, 0 counters
	srv := newNetworkServer(t, deps)

	rr := doGet(t, srv, "/v1/network", testToken)
	var resp networkResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.NAT.DCUtRSuccessRate != nil {
		t.Fatalf("dcutr_success_rate = %v, want null (no outcomes recorded)", *resp.NAT.DCUtRSuccessRate)
	}
	if !resp.NAT.DCUtRCountersUnavail {
		t.Fatal("dcutr_counters_unavailable = false, want true on go-libp2p v0.48.0")
	}

	// The raw JSON must contain null for the rate (not omit the field).
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	var nat map[string]json.RawMessage
	if err := json.Unmarshal(raw["nat"], &nat); err != nil {
		t.Fatalf("decode nat: %v", err)
	}
	if string(nat["dcutr_success_rate"]) != "null" {
		t.Fatalf("raw dcutr_success_rate = %s, want null", nat["dcutr_success_rate"])
	}
	if string(nat["dcutr_counters_unavailable"]) != "true" {
		t.Fatalf("raw dcutr_counters_unavailable = %s, want true", nat["dcutr_counters_unavailable"])
	}
}

// Given a ring without self (empty), when reading the network card, then
// position_pct is null.
func TestNetwork_PositionPctNullWhenSelfAbsent(t *testing.T) {
	deps := fullNetworkDeps(t)
	deps.Ring = fakeRing{size: 0, ok: false}
	srv := newNetworkServer(t, deps)

	rr := doGet(t, srv, "/v1/network", testToken)
	var resp networkResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.HashRing.PositionPct != nil {
		t.Fatalf("position_pct = %v, want null for self-absent ring", *resp.HashRing.PositionPct)
	}
}

// Given recorded DCUtR outcomes with an available source, when reading the
// NAT card, then the success rate is the ok/total fraction and the flag clears.
func TestNetwork_DCUtRRateWhenAvailable(t *testing.T) {
	deps := fullNetworkDeps(t)
	stats := netstats.New()
	stats.SetDCUtRAvailable(true)
	stats.TrackHolePunching(true)
	stats.TrackHolePunching(true)
	stats.TrackHolePunching(false)
	deps.Stats = stats
	srv := newNetworkServer(t, deps)

	rr := doGet(t, srv, "/v1/network", testToken)
	var resp networkResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.NAT.DCUtRSuccessRate == nil {
		t.Fatal("dcutr_success_rate = null, want 2/3")
	}
	if got := *resp.NAT.DCUtRSuccessRate; got < 0.66 || got > 0.67 {
		t.Fatalf("dcutr_success_rate = %v, want ~0.667", got)
	}
	if resp.NAT.DCUtRCountersUnavail {
		t.Fatal("dcutr_counters_unavailable = true, want false when source is wired")
	}
}

// Given a populated peer store, when GET /v1/peers fires, then every entry is
// mapped to the contract shape (capabilities as name strings, no JWT leak).
func TestPeers_AllFields(t *testing.T) {
	srv := newNetworkServer(t, fullNetworkDeps(t))

	rr := doGet(t, srv, "/v1/peers", testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var out []peerView
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len = %d, want 1", len(out))
	}
	p := out[0]
	if p.PeerID != "12D3KooWPeer" || p.Score != 3.5 || p.Stale || p.LastSeen != 1_700_000_000 {
		t.Fatalf("peer = %+v", p)
	}
	if len(p.Capabilities) != 2 || p.Capabilities[0] != "edge" || p.Capabilities[1] != "peer_icp" {
		t.Fatalf("capabilities = %v, want [edge peer_icp]", p.Capabilities)
	}
	if len(p.Addrs) != 1 || p.Addrs[0] != "/ip4/10.0.0.9/tcp/4001" {
		t.Fatalf("addrs = %v", p.Addrs)
	}

	// Secret hygiene: the raw payload must not carry the peer's JWT.
	var raw []map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if _, leaked := raw[0]["jwt"]; leaked {
		t.Fatal("response leaks the peer JWT field")
	}
}

// Given a nil peer store view, when GET /v1/peers fires, then the response is
// an empty array (not null, not 500).
func TestPeers_NilStoreReturnsEmptyArray(t *testing.T) {
	deps := fullNetworkDeps(t)
	deps.Peers = nil
	srv := newNetworkServer(t, deps)

	rr := doGet(t, srv, "/v1/peers", testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Body.String(); got != "[]\n" {
		t.Fatalf("body = %q, want empty JSON array", got)
	}
}

// Given the admin token middleware, when the endpoints are hit without a
// token, then both return 401.
func TestNetwork_RequiresAdminToken(t *testing.T) {
	srv := newNetworkServer(t, fullNetworkDeps(t))
	for _, path := range []string{"/v1/network", "/v1/peers"} {
		if rr := doGet(t, srv, path, ""); rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s without token: status = %d, want 401", path, rr.Code)
		}
	}
}
