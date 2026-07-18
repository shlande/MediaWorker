package jwt

import (
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/shlande/mediaworker/internal/config"
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
	policy      config.JWTPolicyConfig
	ttl         time.Duration
}

// NewJWTService creates a JWTService. privKey is the control plane's Ed25519
// private key used to sign all issued JWTs. policy controls TTL, refresh window,
// bandwidth quota, and default capability grants; a zero-value policy falls back
// to the same defaults LoadControlPlaneConfig would apply (1h / 300 / 50M /
// edge+peer_icp=true).
func NewJWTService(privKey ed25519.PrivateKey, l4Whitelist *PeerIdSet, rateLimiter *RateLimiter, auditLog *AuditLog, policy config.JWTPolicyConfig) *JWTService {
	applyPolicyDefaultsInPlace(&policy)
	ttl, err := time.ParseDuration(policy.TTL)
	if err != nil || ttl <= 0 {
		ttl = 1 * time.Hour
	}
	pubKey := privKey.Public().(ed25519.PublicKey)
	return &JWTService{
		privKey:     privKey,
		pubKey:      pubKey,
		l4Whitelist: l4Whitelist,
		rateLimiter: rateLimiter,
		auditLog:    auditLog,
		policy:      policy,
		ttl:         ttl,
	}
}

// applyPolicyDefaultsInPlace mirrors config.applyJWTPolicyDefaults but lives in
// the jwt package so callers that construct a JWTService directly (tests, etc.)
// without going through LoadControlPlaneConfig still get sane defaults. The
// logic is intentionally identical to keep behaviour consistent.
func applyPolicyDefaultsInPlace(p *config.JWTPolicyConfig) {
	if p == nil {
		return
	}
	if p.TTL == "" {
		p.TTL = "1h"
	}
	if p.RefreshBeforeSeconds == 0 {
		p.RefreshBeforeSeconds = 300
	}
	if p.BandwidthQuotaBytes == 0 {
		p.BandwidthQuotaBytes = 50_000_000
	}
	if !p.DefaultCapabilities.Edge && !p.DefaultCapabilities.PeerICP && !p.DefaultCapabilities.RelayProvider {
		p.DefaultCapabilities.Edge = true
		p.DefaultCapabilities.PeerICP = true
	}
}

// HandleJWTRequest validates a node's JWT request and returns a signed JWT.
//
// Steps:
//  1. Extract the node's Ed25519 public key from req.PeerID
//  2. Verify req.SignedPeerID (node signing its own PeerID to prove ownership)
//  3. Rate-limit by remoteIP (1 req/hour)
//  4. Check L4 whitelist to decide L4Backhaul capability (declared L4 ignored)
//  5. Compute granted capabilities as declared ∩ default
//  6. Build the NodeJWTPayload and sign it as a JWT
//
// When req.DeclaredCapabilities == nil the granted capabilities, quota, TTL and
// RefreshBefore match the pre-policy behaviour bit-for-bit
// (edge=true, peer_icp=true, l4=whitelist, relay=false, quota=50M, ttl=1h, refresh=300).
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

	// 4. L4 whitelist check (declared L4 is intentionally ignored).
	isL4 := s.l4Whitelist.Contains(req.PeerID)

	// 5. Compute granted capabilities = declared ∩ default.
	d := s.policy.DefaultCapabilities
	edge := d.Edge
	peerICP := d.PeerICP
	relay := d.RelayProvider
	if req.DeclaredCapabilities != nil {
		edge = req.DeclaredCapabilities.Edge && d.Edge
		peerICP = req.DeclaredCapabilities.PeerICP && d.PeerICP
		relay = req.DeclaredCapabilities.RelayProvider && d.RelayProvider
	}

	// 6. Build payload
	now := time.Now()
	payload := types.NodeJWTPayload{
		NodeID: sjwt.GenerateNodeID(),
		PeerID: req.PeerID,
		Capabilities: types.NodeCapabilities{
			Edge:          edge,
			PeerICP:       peerICP,
			L4Backhaul:    isL4,
			RelayProvider: relay,
		},
		BandwidthQuota: s.policy.BandwidthQuotaBytes,
		Iat:            now.Unix(),
		Exp:            now.Add(s.ttl).Unix(),
	}

	// 7. Sign
	jwtStr, err := sjwt.SignJWT(payload, s.privKey)
	if err != nil {
		return nil, fmt.Errorf("jwt: sign: %w", err)
	}

	// 8. Audit
	s.auditLog.Log(req.PeerID, remoteIP, isL4, payload.BandwidthQuota, payload.Exp)

	return &types.JWTResponse{
		JWT:           jwtStr,
		RefreshBefore: int64(s.policy.RefreshBeforeSeconds),
	}, nil
}

// PubKey returns the control plane's public key for verification.
func (s *JWTService) PubKey() ed25519.PublicKey {
	return s.pubKey
}

// Policy returns the effective policy used by this service. Read-only; callers
// must not mutate the returned struct (it is a copy of the value).
func (s *JWTService) Policy() config.JWTPolicyConfig {
	return s.policy
}
