// Package libp2phost manages the libp2p host lifecycle.
package libp2phost

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	circuit "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/multiformats/go-multiaddr"

	"github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// NATOptions gates the libp2p NAT traversal options. The zero value (all
// fields false) preserves the pre-T15 behaviour where every option is
// enabled. Callers passing explicit values should set Enabled=true to
// honour the gate; a false value disables that specific option.
//
// This struct is intentionally decoupled from config.NATTraversalConfig so
// the libp2phost package does not depend on internal/config — the caller
// (main.go) is responsible for the *bool → NATOptions conversion via
// ResolveNATOptions.
type NATOptions struct {
	// AutoNAT controls libp2p.EnableNATService. When false (zero value),
	// AutoNAT is enabled (preserving pre-T15 behaviour). When true, AutoNAT
	// is enabled AND a log declaration is emitted. To DISABLE AutoNAT,
	// callers must set Explicit=true and AutoNAT=false.
	AutoNAT bool
	// AutoRelay controls libp2p.EnableAutoRelayWithPeerSource. Same
	// false=enabled semantics as AutoNAT.
	AutoRelay bool
	// DCUtR controls libp2p.EnableHolePunching. Same false=enabled semantics.
	DCUtR bool
	// Explicit marks that the caller has explicitly set the fields above.
	// When false (zero value), all three NAT options are enabled
	// unconditionally (preserving pre-T15 behaviour). When true, each
	// option is gated by its corresponding bool field.
	Explicit bool
}

// ResolveNATOptions converts a config.NATTraversalConfig (which uses *bool
// for nil-distinguishes-omitted semantics) into a libp2phost.NATOptions
// struct. The conversion preserves the pre-T15 behaviour: a zero-value
// NATTraversalConfig (all nil) produces a zero-value NATOptions (Explicit=false),
// which NewEdgeHostWithNAT interprets as "enable everything".
//
// Callers MUST pass the result to NewEdgeHostWithNAT — passing a
// config.NATTraversalConfig directly is a type error (intentional).
func ResolveNATOptions(autoNAT, autoRelay, dcutr *bool) NATOptions {
	if autoNAT == nil && autoRelay == nil && dcutr == nil {
		return NATOptions{Explicit: false}
	}
	return NATOptions{
		Explicit:  true,
		AutoNAT:   autoNAT == nil || *autoNAT,
		AutoRelay: autoRelay == nil || *autoRelay,
		DCUtR:     dcutr == nil || *dcutr,
	}
}

// NewEdgeHost creates a libp2p host configured for the MediaWorker edge
// private network with the default NAT traversal stack (AutoNAT, AutoRelay
// with empty peer source, DCUtR, relay client + relay service). This is the
// pre-T15 behaviour preserved bit-for-bit.
//
// Callers that need to gate NAT traversal by config should use
// NewEdgeHostWithNAT instead.
func NewEdgeHost(
	identity *identity.NodeIdentity,
	listenAddrs []string,
	psk types.PSK,
	gater connmgr.ConnectionGater,
) (host.Host, error) {
	return NewEdgeHostWithNAT(identity, listenAddrs, psk, gater, NATOptions{})
}

// NewEdgeHostWithNAT is like NewEdgeHost but accepts a NATOptions struct
// that gates the libp2p NAT traversal options. A zero-value NATOptions
// (Explicit=false) preserves the pre-T15 behaviour: every option is enabled.
// When Explicit=true, each option is gated by its corresponding bool field.
//
// AutoRelay: when enabled, the host is configured with
// EnableAutoRelayWithPeerSource using an empty peer source (returns an
// immediately-closed channel — no relay candidates). This matches the
// pre-T15 behaviour. Operators wanting real relay candidates must wire a
// peer source in a future task; we do NOT invent a relay list config here
// (plan line 230 — "无列表时 Warn 跳过" is honoured by the empty peer source
// returning zero candidates, which AutoRelay handles gracefully).
func NewEdgeHostWithNAT(
	nodeIdentity *identity.NodeIdentity,
	listenAddrs []string,
	psk types.PSK,
	gater connmgr.ConnectionGater,
	nat NATOptions,
) (host.Host, error) {
	if nodeIdentity == nil {
		return nil, fmt.Errorf("identity is required")
	}
	if len(listenAddrs) == 0 {
		return nil, fmt.Errorf("at least one listen address is required")
	}

	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		return nil, fmt.Errorf("create peerstore: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(nodeIdentity.PrivKey),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.Peerstore(ps),
	}

	if len(psk) > 0 {
		opts = append(opts, libp2p.PrivateNetwork(pnet.PSK(psk)))
	}

	if gater != nil {
		opts = append(opts, libp2p.ConnectionGater(gater))
	}

	// Relay client — always enabled (relies on AutoRelay to find relays).
	opts = append(opts, libp2p.EnableRelay())

	// Relay provider (activates when publicly reachable). Always enabled —
	// any node that is publicly reachable can act as a relay for the mesh.
	opts = append(opts,
		libp2p.EnableRelayService(circuit.WithResources(circuit.DefaultResources())))

	logger := slog.Default().With("component", "libp2p_host")

	// Resolve effective settings: when not explicit, all options enabled
	// (preserves pre-T15 behaviour).
	autoNATEff := !nat.Explicit || nat.AutoNAT
	autoRelayEff := !nat.Explicit || nat.AutoRelay
	dcutrEff := !nat.Explicit || nat.DCUtR

	if autoNATEff {
		opts = append(opts, libp2p.EnableNATService())
		logger.Info("NAT traversal option enabled", "option", "autonat",
			"source", natSource(nat.Explicit, nat.AutoNAT))
	} else {
		logger.Info("NAT traversal option disabled", "option", "autonat")
	}

	if autoRelayEff {
		opts = append(opts,
			libp2p.EnableAutoRelayWithPeerSource(peerSourceFunc))
		logger.Info("NAT traversal option enabled", "option", "auto_relay",
			"source", natSource(nat.Explicit, nat.AutoRelay),
			"peer_source", "empty (TODO: wire PeerEntryStore)",
		)
	} else {
		logger.Info("NAT traversal option disabled", "option", "auto_relay")
	}

	if dcutrEff {
		opts = append(opts, libp2p.EnableHolePunching())
		logger.Info("NAT traversal option enabled", "option", "dcutr",
			"source", natSource(nat.Explicit, nat.DCUtR))
	} else {
		logger.Info("NAT traversal option disabled", "option", "dcutr")
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}

	registerConnNotifee(h)

	return h, nil
}

// natSource returns "config" when the field was explicitly set, "default"
// when it was omitted (preserves pre-T15 behaviour).
func natSource(explicit, value bool) string {
	if !explicit {
		return "default"
	}
	return "config"
}

// peerSourceFunc is an empty AutoRelay peer source. It returns an empty
// channel — AutoRelay will not find any relay candidates until
// PeerEntryStore is wired up.
//
// TODO: Replace with a real peer source that reads from PeerEntryStore.
func peerSourceFunc(_ context.Context, _ int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	close(ch)
	return ch
}

// OnPeerConnectedCallback is the signature for peer-connected hooks.
// local is the peer ID of the host that fired the event; remote is the
// connected peer. The callback MUST NOT block — the caller should launch
// its own goroutine when the hook requires I/O or significant work.
type OnPeerConnectedCallback func(local peer.ID, remote peer.ID)

// connNotifeeRegistry maps host peer IDs to their connNotifee instances.
// Per-host keying avoids cross-app contamination: when two node apps run in
// one process (e.g. integration tests), each calls SetOnPeerConnectedCallback
// with its OWN host handle and callback; the setter only targets that host's
// notifee.
var (
	connNotifeeRegistryMu sync.Mutex
	connNotifeeRegistry   = map[peer.ID]*connNotifee{}
)

// SetOnPeerConnectedCallback registers cb to be invoked on every Connected
// event for the given host. Only the notifee belonging to h is affected;
// other hosts in the same process are untouched. If h was not created by
// NewEdgeHost* (not in the registry), the call is silently ignored. cb must
// be safe to invoke from any goroutine.
func SetOnPeerConnectedCallback(h host.Host, cb OnPeerConnectedCallback) {
	connNotifeeRegistryMu.Lock()
	defer connNotifeeRegistryMu.Unlock()
	if n, ok := connNotifeeRegistry[h.ID()]; ok {
		n.setCallback(cb)
	} else {
		slog.Default().Debug("SetOnPeerConnectedCallback: host not in registry (not created by NewEdgeHost*)",
			"host", h.ID().ShortString())
	}
}

// DeregisterConnNotifee removes the host's connNotifee from the registry and
// unregisters it from the host's network.Notifiee list. Callers should invoke
// this BEFORE closing the host so the registry does not leak entries and stale
// callbacks for a closed host never fire.
func DeregisterConnNotifee(h host.Host) {
	connNotifeeRegistryMu.Lock()
	n, ok := connNotifeeRegistry[h.ID()]
	if !ok {
		connNotifeeRegistryMu.Unlock()
		return
	}
	delete(connNotifeeRegistry, h.ID())
	connNotifeeRegistryMu.Unlock()

	h.Network().StopNotify(n)
}

// registerConnNotifee wires a network.Notifee that logs peer connect/disconnect
// events at Info level. This is the primary signal for diagnosing peer mesh
// formation — when two edge nodes discover each other via DHT, a Connected
// event fires here.
func registerConnNotifee(h host.Host) {
	logger := slog.Default().With("component", "libp2p_host", "self", h.ID().ShortString())
	n := &connNotifee{logger: logger}
	h.Network().Notify(n)

	connNotifeeRegistryMu.Lock()
	connNotifeeRegistry[h.ID()] = n
	connNotifeeRegistryMu.Unlock()
}

type connNotifee struct {
	logger *slog.Logger

	mu       sync.RWMutex
	onPeerCB OnPeerConnectedCallback
}

func (n *connNotifee) setCallback(cb OnPeerConnectedCallback) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onPeerCB = cb
}

func (n *connNotifee) Connected(_ network.Network, conn network.Conn) {
	remote := conn.RemotePeer()
	local := conn.LocalPeer()
	dir := "inbound"
	if conn.Stat().Direction == network.DirOutbound {
		dir = "outbound"
	}
	n.logger.Info("peer connected", "peer", remote.ShortString(), "direction", dir)

	n.mu.RLock()
	cb := n.onPeerCB
	n.mu.RUnlock()
	if cb != nil {
		cb(local, remote)
	}
}

func (n *connNotifee) Disconnected(_ network.Network, conn network.Conn) {
	remote := conn.RemotePeer()
	n.logger.Info("peer disconnected", "peer", remote.ShortString())
}

func (n *connNotifee) ListenOpen(_ network.Network, _ multiaddr.Multiaddr)  {}
func (n *connNotifee) ListenClose(_ network.Network, _ multiaddr.Multiaddr) {}
func (n *connNotifee) Listen(_ network.Network, _ multiaddr.Multiaddr)      {}
