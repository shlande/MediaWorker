// Package dht implements private DHT discovery for the edge network.
package dht

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	discoveryrouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"

	"github.com/shlande/mediaworker/internal/node/peerstore"
)

// EdgeDiscovery manages private DHT node discovery for the edge network.
// It uses a private DHT (not connected to public IPFS), PSK-gated at the
// libp2p layer, with RoutingDiscovery for Advertise/FindPeers and an
// application-level PeerEntryStore for peer metadata persistence.
type EdgeDiscovery struct {
	host              host.Host
	dht               *kaddht.IpfsDHT
	routingDisc       *discoveryrouting.RoutingDiscovery
	peerEntryStore    *peerstore.PeerEntryStore
	namespace         string
	bootstrapAddrs    []peer.AddrInfo
	advertiseTTL      time.Duration
	advertiseInterval time.Duration
	dhtMode           kaddht.ModeOpt
	logger            *slog.Logger
}

// NewEdgeDiscovery creates a new EdgeDiscovery. bootstrapAddrs is the list
// of self-hosted DHT bootstrap peers. dhtMode controls whether this node
// operates as ModeServer (publicly reachable) or ModeClient (NAT).
//
// advertiseInterval drives the heartbeat re-advertise ticker. When zero or
// negative, the heartbeat falls back to advertiseTTL/2 (the pre-T15
// behaviour) so callers passing a legacy zero value preserve old semantics.
func NewEdgeDiscovery(
	h host.Host,
	store *peerstore.PeerEntryStore,
	bootstrapAddrs []peer.AddrInfo,
	namespace string,
	advertiseTTL time.Duration,
	advertiseInterval time.Duration,
	dhtMode kaddht.ModeOpt,
) *EdgeDiscovery {
	return &EdgeDiscovery{
		host:              h,
		peerEntryStore:    store,
		bootstrapAddrs:    bootstrapAddrs,
		namespace:         namespace,
		advertiseTTL:      advertiseTTL,
		advertiseInterval: advertiseInterval,
		dhtMode:           dhtMode,
		logger:            slog.Default().With("component", "dht"),
	}
}

// Start initializes the private DHT, advertises the node, discovers peers,
// and starts the heartbeat/discover background loops.
func (d *EdgeDiscovery) Start(ctx context.Context) error {
	// 1. Initialize private DHT — do NOT use DefaultBootstrapPeers.
	idht, err := kaddht.New(ctx, d.host, kaddht.Mode(d.dhtMode))
	if err != nil {
		return fmt.Errorf("dht: create kad-dht: %w", err)
	}
	d.dht = idht

	// 2. Connect to bootstrap peers — try each, succeed on first.
	//    Must happen AFTER DHT creation so the routing table can learn the peer.
	if len(d.bootstrapAddrs) > 0 {
		var connected bool
		for _, addr := range d.bootstrapAddrs {
			if err := d.host.Connect(ctx, addr); err == nil {
				d.logger.Info("connected to bootstrap peer", "peer", addr.ID)
				connected = true
				break
			}
			d.logger.Warn("failed to connect to bootstrap peer", "peer", addr.ID)
		}
		if !connected {
			return fmt.Errorf("dht: failed to connect to any bootstrap peer (%d configured)", len(d.bootstrapAddrs))
		}
	}

	// 3. Create RoutingDiscovery from the DHT content router.
	d.routingDisc = discoveryrouting.NewRoutingDiscovery(d.dht)

	// 4. Advertise ourselves in the private namespace.
	//    This may fail if the routing table is still empty (e.g., single node
	//    with no bootstrap peers). In that case we skip and let the heartbeat
	//    loop retry.
	ttl, err := d.routingDisc.Advertise(ctx, d.namespace, discovery.TTL(d.advertiseTTL))
	if err != nil {
		d.logger.Warn("initial advertise failed (will retry via heartbeat)", "err", err)
	} else {
		d.logger.Info("advertised in DHT namespace",
			"namespace", d.namespace,
			"effective_ttl", ttl,
		)
	}

	// 5. FindPeers — discover other nodes in the namespace.
	peerChan, err := d.routingDisc.FindPeers(ctx, d.namespace, discovery.Limit(50))
	if err != nil {
		d.logger.Warn("initial FindPeers failed (will retry via discover loop)", "err", err)
	} else {
		for p := range peerChan {
			if p.ID == d.host.ID() {
				continue
			}
			addrs := make([]string, 0, len(p.Addrs))
			for _, a := range p.Addrs {
				addrs = append(addrs, a.String())
			}
			isNew := !d.peerEntryStore.Has(peerstore.PeerIdFromPeerID(p.ID))
			if err := d.peerEntryStore.Put(
				peerstore.PeerIdFromPeerID(p.ID),
				peerstore.PeerStoreEntryFromDiscovery(p.ID, addrs),
			); err != nil {
				d.logger.Warn("failed to store discovered peer", "peer", p.ID, "err", err)
			} else if isNew {
				d.logger.Info("discovered new peer via DHT", "peer", p.ID, "addrs", addrs)
			} else {
				d.logger.Debug("refreshed known peer via DHT", "peer", p.ID, "addrs", addrs)
			}
		}
	}

	// 6. Start background loops.
	go d.heartbeatLoop(ctx)
	go d.discoverLoop(ctx)

	return nil
}

// heartbeatLoop periodically re-advertises in the DHT namespace. The interval
// is the configured advertiseInterval when positive; otherwise it falls back
// to half the advertiseTTL (pre-T15 behaviour) so the record never expires.
//
// When the interval comes from the advertiseInterval config field (operator
// intent), it is respected as-is. When it falls back to advertiseTTL/2
// (legacy behaviour), a 30s floor protects against pathological TTLs that
// would hammer the DHT.
func (d *EdgeDiscovery) heartbeatLoop(ctx context.Context) {
	interval := d.advertiseInterval
	fromConfig := interval > 0
	if !fromConfig {
		interval = d.advertiseTTL / 2
	}
	if !fromConfig && interval < 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	advertisedOnce := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ttl, err := d.routingDisc.Advertise(ctx, d.namespace, discovery.TTL(d.advertiseTTL))
			if err != nil {
				d.logger.Error("heartbeat re-advertise failed", "err", err)
				continue
			}
			if !advertisedOnce {
				d.logger.Info("advertised in DHT namespace",
					"namespace", d.namespace,
					"effective_ttl", ttl,
				)
				advertisedOnce = true
			} else {
				d.logger.Debug("heartbeat re-advertised",
					"namespace", d.namespace,
					"effective_ttl", ttl,
				)
			}
		}
	}
}

// discoverLoop periodically runs FindPeers to refresh the PeerEntryStore
// with newly discovered nodes.
func (d *EdgeDiscovery) discoverLoop(ctx context.Context) {
	interval := d.advertiseTTL / 2
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peerChan, err := d.routingDisc.FindPeers(ctx, d.namespace, discovery.Limit(50))
			if err != nil {
				d.logger.Error("discover loop findpeers failed", "err", err)
				continue
			}
			for p := range peerChan {
				if p.ID == d.host.ID() {
					continue
				}
				addrs := make([]string, 0, len(p.Addrs))
				for _, a := range p.Addrs {
					addrs = append(addrs, a.String())
				}
				isNew := !d.peerEntryStore.Has(peerstore.PeerIdFromPeerID(p.ID))
				if err := d.peerEntryStore.Put(
					peerstore.PeerIdFromPeerID(p.ID),
					peerstore.PeerStoreEntryFromDiscovery(p.ID, addrs),
				); err != nil {
					d.logger.Warn("discover loop: failed to store peer", "peer", p.ID, "err", err)
				} else if isNew {
					d.logger.Info("discovered new peer via DHT", "peer", p.ID, "addrs", addrs)
				} else {
					d.logger.Debug("refreshed known peer via DHT", "peer", p.ID, "addrs", addrs)
				}
			}
		}
	}
}

// OnGossipSubPruneWithPX handles PeX (Peer Exchange) notifications from
// GossipSub prune messages. GossipSub validates peer records via its own
// signed envelope mechanism; each record from a trusted message is stored
// in the PeerEntryStore.
func (d *EdgeDiscovery) OnGossipSubPruneWithPX(p peer.ID, pxPeers []peer.PeerRecord) {
	d.logger.Debug("received PeX from prune", "from", p, "count", len(pxPeers))
	added := 0
	for _, rec := range pxPeers {
		addrs := make([]string, 0, len(rec.Addrs))
		for _, a := range rec.Addrs {
			addrs = append(addrs, a.String())
		}
		if err := d.peerEntryStore.Put(
			peerstore.PeerIdFromPeerID(rec.PeerID),
			peerstore.PeerStoreEntryFromDiscovery(rec.PeerID, addrs),
		); err != nil {
			d.logger.Warn("PeX: failed to store peer", "peer", rec.PeerID, "err", err)
			continue
		}
		added++
	}
	d.logger.Debug("PeX processing complete", "total", len(pxPeers), "added", added)
}
