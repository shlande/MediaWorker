// Package jwt provides shared JWT utilities used by both the control-plane
// jwt service and the node-side jwt client/protocol.
package jwt

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

var (
	ErrInvalidPeerID       = errors.New("jwt: invalid peer ID")
	ErrInvalidSignature    = errors.New("jwt: invalid peer signature")
	ErrRateLimited         = errors.New("jwt: rate limited")
	ErrJWTExpired          = errors.New("jwt: token expired")
	ErrJWTBadSignature     = errors.New("jwt: bad control-plane signature")
	ErrPeerIDMismatch      = errors.New("jwt: peer ID mismatch")
	ErrJWTStaleOrDuplicate = errors.New("jwt: stale or duplicate JWT")
)

// ---------------------------------------------------------------------------
// JWT header (constant)
// ---------------------------------------------------------------------------

const jwtHeader = `{"alg":"EdDSA","typ":"JWT"}`

// ---------------------------------------------------------------------------
// JWT signing and verification
// ---------------------------------------------------------------------------

// SignJWT returns a compact JWT: base64url(header).base64url(payload).base64url(sig)
func SignJWT(payload types.NodeJWTPayload, privKey ed25519.PrivateKey) (types.CapabilityJWT, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	headerB64 := base64urlEncode([]byte(jwtHeader))
	payloadB64 := base64urlEncode(payloadBytes)
	signingInput := headerB64 + "." + payloadB64

	sig := ed25519.Sign(privKey, []byte(signingInput))
	sigB64 := base64urlEncode(sig)

	return types.CapabilityJWT(headerB64 + "." + payloadB64 + "." + sigB64), nil
}

// VerifyJWT decodes and verifies a CapabilityJWT. It checks:
//   - The JWT is well-formed (3 parts)
//   - The Ed25519 signature is valid against controlPlanePubKey
//   - The PeerID in the payload matches expectedPeerID
//   - Exp is not in the past (no grace period)
//
// Returns the decoded payload on success.
func VerifyJWT(jwtStr types.CapabilityJWT, controlPlanePubKey ed25519.PublicKey, expectedPeerID types.PeerId) (*types.NodeJWTPayload, error) {
	parts := splitJWT(string(jwtStr))
	if len(parts) != 3 {
		return nil, fmt.Errorf("jwt: invalid format, expected 3 parts, got %d", len(parts))
	}

	signingInput := parts[0] + "." + parts[1]
	sig, err := base64urlDecode(parts[2])
	if err != nil {
		return nil, fmt.Errorf("jwt: decode signature: %w", err)
	}

	if !ed25519.Verify(controlPlanePubKey, []byte(signingInput), sig) {
		return nil, ErrJWTBadSignature
	}

	payloadBytes, err := base64urlDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("jwt: decode payload: %w", err)
	}

	var payload types.NodeJWTPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("jwt: unmarshal payload: %w", err)
	}

	if payload.PeerID != expectedPeerID {
		return nil, ErrPeerIDMismatch
	}

	if time.Now().Unix() >= payload.Exp {
		return nil, ErrJWTExpired
	}

	return &payload, nil
}

// VerifyJWTAnyPeerID is like VerifyJWT but without PeerId binding check.
// Used by JWT push protocol where the receiver verifies the JWT's embedded PeerID.
func VerifyJWTAnyPeerID(jwtStr types.CapabilityJWT, controlPlanePubKey ed25519.PublicKey) (*types.NodeJWTPayload, error) {
	parts := splitJWT(string(jwtStr))
	if len(parts) != 3 {
		return nil, fmt.Errorf("jwt: invalid format, expected 3 parts, got %d", len(parts))
	}

	signingInput := parts[0] + "." + parts[1]
	sig, err := base64urlDecode(parts[2])
	if err != nil {
		return nil, fmt.Errorf("jwt: decode signature: %w", err)
	}

	if !ed25519.Verify(controlPlanePubKey, []byte(signingInput), sig) {
		return nil, ErrJWTBadSignature
	}

	payloadBytes, err := base64urlDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("jwt: decode payload: %w", err)
	}

	var payload types.NodeJWTPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("jwt: unmarshal payload: %w", err)
	}

	if time.Now().Unix() >= payload.Exp {
		return nil, ErrJWTExpired
	}

	return &payload, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ExtractEd25519PubKey extracts the Ed25519 public key from a PeerID.
// PeerID format: base58(multihash(pubkey)).
func ExtractEd25519PubKey(peerID types.PeerId) (ed25519.PublicKey, error) {
	pid, err := peer.Decode(string(peerID))
	if err != nil {
		return nil, fmt.Errorf("decode peer ID: %w", err)
	}
	pubKey, err := pid.ExtractPublicKey()
	if err != nil {
		return nil, fmt.Errorf("extract public key: %w", err)
	}
	if pubKey.Type() != crypto.Ed25519 {
		return nil, fmt.Errorf("unsupported key type: %s", pubKey.Type())
	}
	raw, err := pubKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("get raw public key: %w", err)
	}
	return ed25519.PublicKey(raw), nil
}

// GenerateNodeID creates a random 16-byte UUID-v4-style node identifier.
func GenerateNodeID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16],
	)
}

// GenerateEd25519Key creates a new random Ed25519 key pair.
func GenerateEd25519Key() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// GeneratePeerID creates a libp2p peer.ID from an Ed25519 private key.
func GeneratePeerID(privKey ed25519.PrivateKey) (types.PeerId, error) {
	libp2pPriv, err := crypto.UnmarshalEd25519PrivateKey(privKey)
	if err != nil {
		return "", fmt.Errorf("unmarshal private key: %w", err)
	}
	pid, err := peer.IDFromPrivateKey(libp2pPriv)
	if err != nil {
		return "", fmt.Errorf("peer ID from private key: %w", err)
	}
	return types.PeerId(pid.String()), nil
}

// SignPeerID signs a PeerID string with the given Ed25519 private key.
func SignPeerID(privKey ed25519.PrivateKey, peerID types.PeerId) []byte {
	return ed25519.Sign(privKey, []byte(peerID))
}

// SHA256Sum is used for PSK generation in tests.
func SHA256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

// splitJWT splits a compact JWT string into its three base64url-encoded parts.
func splitJWT(token string) []string {
	parts := make([]string, 0, 3)
	start := 0
	for i := 0; i < len(token); i++ {
		if token[i] == '.' {
			parts = append(parts, token[start:i])
			start = i + 1
		}
	}
	parts = append(parts, token[start:])
	return parts
}

// base64urlEncode returns standard base64-encoding with URL-safe characters and no padding.
func base64urlEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

// base64urlDecode decodes a base64url-encoded string (no padding).
func base64urlDecode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
