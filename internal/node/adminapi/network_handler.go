package adminapi

import (
	"net/http"
	"strings"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/shlande/mediaworker/internal/node/netstats"
	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Narrow dependency views (todo 49 passes the live components directly —
// host.Host satisfies HostView, host.Network() satisfies ConnSource,
// *dht.EdgeDiscovery satisfies DHTView, *hashring.HashRing satisfies RingView,
// *peerstore.PeerEntryStore satisfies PeerStoreView).
// ---------------------------------------------------------------------------

// HostView is the slice of libp2p host state the network handler reads.
type HostView interface {
	Addrs() []ma.Multiaddr
}

// ConnSource provides the live connection list (host.Network()).
type ConnSource interface {
	Conns() []network.Conn
}

// DHTView reports the DHT routing-table size (*dht.EdgeDiscovery).
type DHTView interface {
	RoutingTableSize() int
}

// RingView reports hash-ring membership and self's position (*hashring.HashRing).
type RingView interface {
	Size() int
	PositionPct() (pct float64, ok bool)
}

// PeerStoreView lists every peer entry, unfiltered (*peerstore.PeerEntryStore).
type PeerStoreView interface {
	List() []types.PeerStoreEntry
}

// NetworkDeps carries the components RegisterNetworkRoutes reads from.
type NetworkDeps struct {
	Host    HostView
	Conns   ConnSource
	DHT     DHTView  // nil tolerated: table_size 0
	DHTMode string   // cfg.Node.Libp2p.DHT.Mode verbatim
	Ring    RingView // nil tolerated: position_pct/peers_on_ring null/0
	Peers   PeerStoreView
	Stats   *netstats.Tracker // nil tolerated: reachability "unknown", dcutr unavailable
}

// ---------------------------------------------------------------------------
// GET /v1/network
// ---------------------------------------------------------------------------

type connBreakdown struct {
	Total int `json:"total"`
	In    int `json:"in"`
	Out   int `json:"out"`
}

type dhtStatus struct {
	Mode      string `json:"mode"`
	TableSize int    `json:"table_size"`
}

type gossipsubStatus struct {
	Subscribed bool `json:"subscribed"` // static true: the topic is joined during assembly
}

type natStatus struct {
	Reachability         string   `json:"reachability"`
	RelayCircuits        int      `json:"relay_circuits"`
	DCUtRSuccessRate     *float64 `json:"dcutr_success_rate"`
	DCUtRCountersUnavail bool     `json:"dcutr_counters_unavailable"`
}

type hashRingStatus struct {
	PositionPct *float64 `json:"position_pct"` // null when self is absent from the ring
	PeersOnRing int      `json:"peers_on_ring"`
}

type networkResponse struct {
	ListenAddrs []string        `json:"listen_addrs"`
	Conn        connBreakdown   `json:"conn"`
	DHT         dhtStatus       `json:"dht"`
	GossipSub   gossipsubStatus `json:"gossipsub"`
	NAT         natStatus       `json:"nat"`
	HashRing    hashRingStatus  `json:"hash_ring"`
}

// ---------------------------------------------------------------------------
// GET /v1/peers
// ---------------------------------------------------------------------------

type peerView struct {
	PeerID       string   `json:"peer_id"`
	Capabilities []string `json:"capabilities"`
	Score        float64  `json:"score"`
	Stale        bool     `json:"stale"`
	LastSeen     int64    `json:"last_seen"`
	Addrs        []string `json:"addrs"`
}

// RegisterNetworkRoutes mounts GET /v1/network and GET /v1/peers on srv. Per
// orchestrator decision D1 it does NOT edit main.go — todo 49 consolidates
// all node-admin route mounts and constructs NetworkDeps from the live
// components.
func RegisterNetworkRoutes(srv *Server, deps NetworkDeps) {
	srv.Handle("GET /v1/network", func(w http.ResponseWriter, r *http.Request) {
		resp := networkResponse{
			ListenAddrs: listenAddrStrings(deps.Host),
			Conn:        countConns(deps.Conns),
			DHT:         dhtStatus{Mode: deps.DHTMode},
			GossipSub:   gossipsubStatus{Subscribed: true},
			NAT:         natStatus{Reachability: netstats.ReachabilityUnknown},
		}
		if deps.DHT != nil {
			resp.DHT.TableSize = deps.DHT.RoutingTableSize()
		}
		relay, dcutrRate, dcutrUnavailable := natCounters(deps.Conns, deps.Stats)
		resp.NAT.RelayCircuits = relay
		resp.NAT.DCUtRSuccessRate = dcutrRate
		resp.NAT.DCUtRCountersUnavail = dcutrUnavailable
		if deps.Stats != nil {
			resp.NAT.Reachability = deps.Stats.Reachability()
		}
		if deps.Ring != nil {
			resp.HashRing.PeersOnRing = deps.Ring.Size()
			if pct, ok := deps.Ring.PositionPct(); ok {
				resp.HashRing.PositionPct = &pct
			}
		}
		WriteJSON(w, http.StatusOK, resp)
	})

	srv.Handle("GET /v1/peers", func(w http.ResponseWriter, r *http.Request) {
		out := []peerView{}
		if deps.Peers != nil {
			for _, e := range deps.Peers.List() {
				out = append(out, peerView{
					PeerID:       string(e.PeerID),
					Capabilities: capabilityNames(e.Capabilities),
					Score:        e.Score,
					Stale:        e.Stale,
					LastSeen:     e.LastSeen,
					Addrs:        e.Addrs,
				})
			}
		}
		WriteJSON(w, http.StatusOK, out)
	})
}

// listenAddrStrings formats host listen addresses (logAddrs pattern).
func listenAddrStrings(h HostView) []string {
	out := []string{}
	if h == nil {
		return out
	}
	for _, a := range h.Addrs() {
		out = append(out, a.String())
	}
	return out
}

// countConns totals connections and splits them by direction.
func countConns(cs ConnSource) connBreakdown {
	var b connBreakdown
	if cs == nil {
		return b
	}
	for _, c := range cs.Conns() {
		b.Total++
		switch c.Stat().Direction {
		case network.DirInbound:
			b.In++
		case network.DirOutbound:
			b.Out++
		case network.DirUnknown:
			// Counted in total only; neither in nor out.
		}
	}
	return b
}

// natCounters derives relay-circuit count (connections whose remote multiaddr
// contains /p2p-circuit) and the DCUtR success rate. The rate is null when no
// outcomes were recorded or the locked libp2p exposes no hole-punch event
// (go-libp2p v0.48.0 — the unavailable flag is set in that case).
func natCounters(cs ConnSource, stats *netstats.Tracker) (relay int, rate *float64, unavailable bool) {
	if cs != nil {
		for _, c := range cs.Conns() {
			if strings.Contains(c.RemoteMultiaddr().String(), "/p2p-circuit") {
				relay++
			}
		}
	}
	if stats == nil {
		return relay, nil, true
	}
	unavailable = !stats.DCUtRAvailable()
	ok, total := stats.HolePunchingStats()
	if total == 0 {
		return relay, nil, unavailable
	}
	r := float64(ok) / float64(total)
	return relay, &r, unavailable
}
