// Package libp2phost manages the libp2p host lifecycle.
package libp2phost

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	circuit "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"

	"github.com/shlande/mediaworker/internal/types"
)

// NodeIdentity holds the node's Ed25519 private key and derived PeerId.
// The private key is persistent: the same key path yields the same identity
// across restarts. Losing the key file means losing the node identity.
type NodeIdentity struct {
	PrivKey crypto.PrivKey // Ed25519, persisted at keyPath with 0600 perms
	PeerID  types.PeerId   // derived from PrivKey.GetPublic()
}

// LoadOrGenerateIdentity loads an existing Ed25519 private key from keyPath
// (file mode 0600). If the file does not exist, a new Ed25519 keypair is
// generated, written to keyPath, and returned. The PeerId is derived from the
// public key via peer.IDFromPublicKey.
func LoadOrGenerateIdentity(keyPath string) (*NodeIdentity, error) {
	privKey, err := loadOrCreateKey(keyPath)
	if err != nil {
		return nil, err
	}

	peerID, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("derive peer ID: %w", err)
	}

	return &NodeIdentity{
		PrivKey: privKey,
		PeerID:  types.PeerId(peerID.String()),
	}, nil
}

// loadOrCreateKey reads the serialized private key from path. If the file
// does not exist, a new Ed25519 key is generated and written.
func loadOrCreateKey(path string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		return crypto.UnmarshalPrivateKey(data)
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ed25519 key: %w", err)
	}

	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}

	if err := os.WriteFile(path, raw, 0600); err != nil {
		return nil, fmt.Errorf("write key file: %w", err)
	}

	return priv, nil
}

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
	identity *NodeIdentity,
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
