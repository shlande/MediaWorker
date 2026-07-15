// Package libp2phost manages the libp2p host lifecycle.
package libp2phost

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	circuit "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"

	"github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// NewEdgeHost creates a libp2p host configured for the MediaWorker edge
// private network. It wires PSK admission, NAT traversal (AutoNAT, relay,
// DCUtR hole punching), a memory peerstore, and a connection gater.
//
// The host listens on TCP addresses only. PSK mode does not support QUIC or
// WebTransport; see https://github.com/libp2p/go-libp2p/issues/for-psk-quic.
//
// Parameters:
//   - identity: the node's Ed25519 key and derived PeerId
//   - listenAddrs: multiaddr strings (e.g. "/ip4/0.0.0.0/tcp/9001")
//   - psk: 32-byte pre-shared key for private network admission
//   - gater: connection gater for filtering peers (may be nil for no-op)
func NewEdgeHost(
	identity *identity.NodeIdentity,
	listenAddrs []string,
	psk types.PSK,
	gater connmgr.ConnectionGater,
) (host.Host, error) {
	if identity == nil {
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
		libp2p.Identity(identity.PrivKey),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.Peerstore(ps),
	}

	// PSK private network admission (TCP only — PSK does not support QUIC).
	if len(psk) > 0 {
		opts = append(opts, libp2p.PrivateNetwork(pnet.PSK(psk)))
	}

	// Connection gater (may be nil).
	if gater != nil {
		opts = append(opts, libp2p.ConnectionGater(gater))
	}

	// NAT traversal stack.
	opts = append(opts,
		libp2p.EnableRelay(),                                                      // relay client
		libp2p.EnableRelayService(circuit.WithResources(circuit.DefaultResources())), // relay provider (activates when publicly reachable)
		libp2p.EnableHolePunching(),                                               // DCUtR
		libp2p.EnableNATService(),                                                 // AutoNAT
	)

	// AutoRelay with peer source.
	// TODO: Wire up PeerEntryStore to provide real relay candidates. For now,
	// the peer source returns an empty channel — AutoRelay will not find any
	// relays until PeerEntryStore is integrated in a later task.
	opts = append(opts,
		libp2p.EnableAutoRelayWithPeerSource(
			peerSourceFunc,
		),
	)

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}

	return h, nil
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
