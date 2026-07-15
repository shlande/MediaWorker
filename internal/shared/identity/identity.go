// Package identity provides shared node identity loading/generation that is
// used by both the control plane (dhtbootstrap) and node (libp2phost) roles.
package identity

import (
	"crypto/rand"
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

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
	privKey, err := LoadOrCreateKey(keyPath)
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

// LoadOrCreateKey reads the serialized private key from path. If the file
// does not exist, a new Ed25519 key is generated and written.
func LoadOrCreateKey(path string) (crypto.PrivKey, error) {
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
