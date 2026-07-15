package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// pemEd25519PrivateKey is the PEM block type used for Ed25519 private keys.
const pemEd25519PrivateKey = "ED25519 PRIVATE KEY"

// LoadOrGenerateControlPlaneKey loads an Ed25519 private key from PEM file at
// path. If the file does not exist, it generates a new key, PEM-encodes it and
// writes it to path, then returns the new key.
//
// The key is stored as a PEM-encoded crypto/ed25519 private key (PKCS#8),
// NOT in libp2p protobuf format. This key is used for JWT signing, not for
// libp2p host identity.
func LoadOrGenerateControlPlaneKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read control-plane key: %w", err)
		}
		// Generate new key pair.
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate ed25519 key: %w", err)
		}
		if err := SaveControlPlaneKey(path, priv); err != nil {
			return nil, err
		}
		return priv, nil
	}

	block, _ := pem.Decode(data)
	if block == nil || block.Type != pemEd25519PrivateKey {
		return nil, fmt.Errorf("control-plane key: invalid PEM block (type=%q)", pemLabel(block))
	}

	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("control-plane key: parse PKCS#8: %w", err)
	}

	edPriv, ok := priv.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("control-plane key: not an Ed25519 key (got %T)", priv)
	}
	return edPriv, nil
}

// SaveControlPlaneKey PEM-encodes the Ed25519 private key in PKCS#8 format
// and writes it to path with 0o600 permissions.
func SaveControlPlaneKey(path string, key ed25519.PrivateKey) error {
	b, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal PKCS#8 ed25519 key: %w", err)
	}

	block := &pem.Block{
		Type:  pemEd25519PrivateKey,
		Bytes: b,
	}

	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		return fmt.Errorf("write control-plane key: %w", err)
	}
	return nil
}

// pemLabel returns the PEM block type label, or "<nil>" if block is nil.
func pemLabel(block *pem.Block) string {
	if block == nil {
		return "<nil>"
	}
	return block.Type
}