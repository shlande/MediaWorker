// Package dhtbootstrap manages the control plane's DHT bootstrap host.
// It creates a publicly reachable libp2p host (no NAT traversal), initializes
// a ModeServer DHT, advertises the control plane in the private namespace,
// and runs a re-advertise heartbeat loop.
package dhtbootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
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

// BootstrapHost is the control plane's DHT bootstrap node. It runs a
// publicly reachable libp2p host (no AutoRelay/DCUtR/NAT), initializes a
// ModeServer DHT, advertises itself in the private namespace, and runs a
// re-advertise heartbeat loop.
type BootstrapHost struct {
	host           host.Host
	dht            *kaddht.IpfsDHT
	routingDisc    *discoveryrouting.RoutingDiscovery
	namespace      string
	advertiseTTL   time.Duration
	advertiseIntv  time.Duration
	bootstrapAddrs []peer.AddrInfo
	logger         *slog.Logger
}

// NewBootstrapHost creates a BootstrapHost with a simplified libp2p stack
// suitable for a publicly reachable control plane:
//
//   - PSK private network admission (when psk is provided)
//   - No AutoRelay, DCUtR, or NAT traversal (the control plane is publicly
//     reachable and does not need relay assistance)
//   - No ConnectionGater IP filtering (a permissive gater is used)
//
// The DHT and re-advertise heartbeat are started by calling Start.
func NewBootstrapHost(
	id *identity.NodeIdentity,
	cfg config.DHTBootstrapConfig,
	psk types.PSK,
) (*BootstrapHost, error) {
	if id == nil {
		return nil, fmt.Errorf("identity is required")
	}
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	advTTL, err := time.ParseDuration(cfg.AdvertiseTTL)
	if err != nil {
		return nil, fmt.Errorf("parse advertise_ttl %q: %w", cfg.AdvertiseTTL, err)
	}
	advIntv, err := time.ParseDuration(cfg.AdvertiseInterval)
	if err != nil {
		return nil, fmt.Errorf("parse advertise_interval %q: %w", cfg.AdvertiseInterval, err)
	}

	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		return nil, fmt.Errorf("create peerstore: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(id.PrivKey),
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.Peerstore(ps),
	}

	if len(psk) > 0 {
		opts = append(opts, libp2p.PrivateNetwork(pnet.PSK(psk)))
	}

	// No AutoRelay, DCUtR, NAT, or HolePunching — the control plane is
	// publicly reachable and does not participate in the relay mesh.

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}

	// Parse bootstrap peer multiaddrs into AddrInfo.
	var bootAddrs []peer.AddrInfo
	for _, maStr := range cfg.BootstrapPeers {
		ai, err := peer.AddrInfoFromString(maStr)
		if err != nil {
			_ = h.Close()
			return nil, fmt.Errorf("parse bootstrap peer multiaddr %q: %w", maStr, err)
		}
		bootAddrs = append(bootAddrs, *ai)
	}

	return &BootstrapHost{
		host:           h,
		namespace:      cfg.Namespace,
		advertiseTTL:   advTTL,
		advertiseIntv:  advIntv,
		bootstrapAddrs: bootAddrs,
		logger: slog.Default().With(
			"component", "dhtbootstrap",
			"peer_id", id.PeerID,
		),
	}, nil
}

// Start initializes the ModeServer DHT, connects to any configured bootstrap
// peers, advertises this host in the namespace, and starts the re-advertise
// heartbeat goroutine.
func (b *BootstrapHost) Start(ctx context.Context) error {
	// 1. Initialize DHT in ModeServer.
	dht, err := kaddht.New(ctx, b.host, kaddht.Mode(kaddht.ModeServer))
	if err != nil {
		return fmt.Errorf("dht: create kad-dht: %w", err)
	}
	b.dht = dht

	// 2. Connect to bootstrap peers (best-effort; this is a bootstrap host
	//    itself, so external bootstrap peers are optional).
	if len(b.bootstrapAddrs) > 0 {
		var connected bool
		for _, ai := range b.bootstrapAddrs {
			if err := b.host.Connect(ctx, ai); err == nil {
				b.logger.Info("connected to bootstrap peer", "peer", ai.ID)
				connected = true
				break
			}
			b.logger.Warn("failed to connect to bootstrap peer", "peer", ai.ID)
		}
		if !connected {
			b.logger.Warn("could not connect to any bootstrap peer (this host is a bootstrap itself)")
		}
	}

	// 3. Create RoutingDiscovery.
	b.routingDisc = discoveryrouting.NewRoutingDiscovery(b.dht)

	// 4. Initial advertise.
	ttl, err := b.routingDisc.Advertise(ctx, b.namespace, discovery.TTL(b.advertiseTTL))
	if err != nil {
		b.logger.Warn("initial advertise failed (will retry via heartbeat)", "err", err)
	} else {
		b.logger.Info("advertised in DHT namespace",
			"namespace", b.namespace,
			"effective_ttl", ttl,
		)
	}

	// 5. Start re-advertise heartbeat.
	go b.heartbeatLoop(ctx)

	return nil
}

// Host returns the underlying libp2p host. Callers (e.g. SyncBroadcaster)
// use this to bootstrap a GossipSub overlay on top of the same host.
func (b *BootstrapHost) Host() host.Host {
	return b.host
}

// RoutingTableSize returns the number of peers in the DHT routing table.
// It is safe to call only after Start has succeeded; before Start it
// returns 0. Exposed for observability and convergence tests.
func (b *BootstrapHost) RoutingTableSize() int {
	if b.dht == nil {
		return 0
	}
	return b.dht.RoutingTable().Size()
}

// Close shuts down the DHT and the underlying libp2p host.
func (b *BootstrapHost) Close() error {
	if b.dht != nil {
		_ = b.dht.Close()
	}
	if b.host != nil {
		return b.host.Close()
	}
	return nil
}

// heartbeatLoop periodically re-advertises in the DHT namespace. The
// interval is taken from the configured AdvertiseInterval.
func (b *BootstrapHost) heartbeatLoop(ctx context.Context) {
	if b.advertiseIntv <= 0 {
		b.logger.Warn("advertise interval is zero or negative, heartbeat loop disabled")
		return
	}
	ticker := time.NewTicker(b.advertiseIntv)
	defer ticker.Stop()

	advertisedOnce := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ttl, err := b.routingDisc.Advertise(ctx, b.namespace, discovery.TTL(b.advertiseTTL))
			if err != nil {
				b.logger.Error("heartbeat re-advertise failed", "err", err)
				continue
			}
			if !advertisedOnce {
				b.logger.Info("advertised in DHT namespace (heartbeat)",
					"namespace", b.namespace,
					"effective_ttl", ttl,
				)
				advertisedOnce = true
			} else {
				b.logger.Debug("heartbeat re-advertised",
					"namespace", b.namespace,
					"effective_ttl", ttl,
				)
			}
		}
	}
}
