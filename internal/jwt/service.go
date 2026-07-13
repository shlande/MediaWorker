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
	ErrInvalidPeerID    = errors.New("jwt: invalid peer ID")
	ErrInvalidSignature = errors.New("jwt: invalid peer signature")
	ErrRateLimited      = errors.New("jwt: rate limited")
	ErrJWTExpired       = errors.New("jwt: token expired")
	ErrJWTBadSignature  = errors.New("jwt: bad control-plane signature")
	ErrPeerIDMismatch   = errors.New("jwt: peer ID mismatch")
	ErrJWTStaleOrDuplicate = errors.New("jwt: stale or duplicate JWT")
)

// ---------------------------------------------------------------------------
// JWT header (constant)
// ---------------------------------------------------------------------------

const jwtHeader = `{"alg":"EdDSA","typ":"JWT"}`

// ---------------------------------------------------------------------------
// JWTService - control-plane side
// ---------------------------------------------------------------------------

// JWTService signs capability JWTs for nodes. It lives on the control plane and
// is called by nodes via HTTP POST (the endpoint wiring is outside this package).
type JWTService struct {
	privKey     ed25519.PrivateKey
	pubKey      ed25519.PublicKey
	l4Whitelist *PeerIdSet
	rateLimiter *RateLimiter
	auditLog    *AuditLog
}

// NewJWTService creates a JWTService. privKey is the control plane's Ed25519
// private key used to sign all issued JWTs.
func NewJWTService(privKey ed25519.PrivateKey, l4Whitelist *PeerIdSet, rateLimiter *RateLimiter, auditLog *AuditLog) *JWTService {
	pubKey := privKey.Public().(ed25519.PublicKey)
	return &JWTService{
		privKey:     privKey,
		pubKey:      pubKey,
		l4Whitelist: l4Whitelist,
		rateLimiter: rateLimiter,
		auditLog:    auditLog,
	}
}

// HandleJWTRequest validates a node's JWT request and returns a signed JWT.
//
// Steps:
//  1. Extract the node's Ed25519 public key from req.PeerID
//  2. Verify req.SignedPeerID (node signing its own PeerID to prove ownership)
//  3. Rate-limit by remoteIP (1 req/hour)
//  4. Check L4 whitelist to decide L4Backhaul capability
//  5. Build the NodeJWTPayload and sign it as a JWT
func (s *JWTService) HandleJWTRequest(req types.JWTRequest, remoteIP string) (*types.JWTResponse, error) {
	// 1. Extract public key from PeerID
	nodePubKey, err := extractEd25519PubKey(req.PeerID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPeerID, err)
	}

	// 2. Verify the node signed its own PeerID
	if !ed25519.Verify(nodePubKey, []byte(req.PeerID), req.SignedPeerID) {
		return nil, ErrInvalidSignature
	}

	// 3. Rate limit
	if !s.rateLimiter.Allow(remoteIP) {
		return nil, ErrRateLimited
	}

	// 4. L4 whitelist check
	isL4 := s.l4Whitelist.Contains(req.PeerID)

	// 5. Build payload
	now := time.Now()
	payload := types.NodeJWTPayload{
		NodeID: generateNodeID(),
		PeerID: req.PeerID,
		Capabilities: types.NodeCapabilities{
			Edge:          true,
			PeerICP:       true,
			L4Backhaul:    isL4,
			RelayProvider: false, // auto-detected later by control-plane probe
		},
		BandwidthQuota: 50_000_000,
		Iat:            now.Unix(),
		Exp:            now.Add(1 * time.Hour).Unix(),
	}

	// 6. Sign
	jwtStr, err := signJWT(payload, s.privKey)
	if err != nil {
		return nil, fmt.Errorf("jwt: sign: %w", err)
	}

	// 7. Audit
	s.auditLog.Log(req.PeerID, remoteIP, isL4, payload.BandwidthQuota, payload.Exp)

	return &types.JWTResponse{
		JWT:           jwtStr,
		RefreshBefore: 300, // 5 minutes
	}, nil
}

// PubKey returns the control plane's public key for verification.
func (s *JWTService) PubKey() ed25519.PublicKey {
	return s.pubKey
}

// ---------------------------------------------------------------------------
// JWT signing and verification
// ---------------------------------------------------------------------------

// signJWT returns a compact JWT: base64url(header).base64url(payload).base64url(sig)
func signJWT(payload types.NodeJWTPayload, privKey ed25519.PrivateKey) (types.CapabilityJWT, error) {
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

// extractEd25519PubKey extracts the Ed25519 public key from a PeerID.
// PeerID format: base58(multihash(pubkey)).
func extractEd25519PubKey(peerID types.PeerId) (ed25519.PublicKey, error) {
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

// generateNodeID creates a random 16-byte UUID-v4-style node identifier.
func generateNodeID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16],
	)
}

// splitJWT splits a compact JWT string into its three base64url-encoded parts.
func splitJWT(token string) []string {
	// Use a simple loop rather than strings.Split to avoid importing strings for one use.
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

// SHA256 is used for PSK generation in tests.
func sha256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}
