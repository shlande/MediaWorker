package jwt

import (
	"crypto/ed25519"
	"fmt"
	"time"

	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

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
	nodePubKey, err := sjwt.ExtractEd25519PubKey(req.PeerID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", sjwt.ErrInvalidPeerID, err)
	}

	// 2. Verify the node signed its own PeerID
	if !ed25519.Verify(nodePubKey, []byte(req.PeerID), req.SignedPeerID) {
		return nil, sjwt.ErrInvalidSignature
	}

	// 3. Rate limit
	if !s.rateLimiter.Allow(remoteIP) {
		return nil, sjwt.ErrRateLimited
	}

	// 4. L4 whitelist check
	isL4 := s.l4Whitelist.Contains(req.PeerID)

	// 5. Build payload
	now := time.Now()
	payload := types.NodeJWTPayload{
		NodeID: sjwt.GenerateNodeID(),
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
	jwtStr, err := sjwt.SignJWT(payload, s.privKey)
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
