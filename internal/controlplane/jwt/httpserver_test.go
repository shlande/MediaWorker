package jwt_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// defaultTestPolicy returns the zero-value policy that — after the service's
// internal defaulting — matches the pre-policy behaviour bit-for-bit.
func defaultTestPolicy() config.JWTPolicyConfig {
	return config.JWTPolicyConfig{}
}

func TestJWTHTTPServer_validJWTRequest_returns200(t *testing.T) {
	// Given: a JWTService with a real keypair, empty whitelist, and permissive rate limiter.
	pubKey, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	auditLog := jwt.NewAuditLog(io.Discard)

	service := jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, defaultTestPolicy())
	server := jwt.NewJWTHTTPServer(service)

	// Pick a random available port.
	listenAddr := pickFreeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ctx, listenAddr)
	}()

	// Wait for the server to start.
	waitForServer(t, listenAddr)

	// When: POST /v1/node/jwt with a valid SignedPeerID.
	peerID, err := sjwt.GeneratePeerID(privKey)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}
	signed := sjwt.SignPeerID(privKey, peerID)

	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: signed,
	}
	body, _ := json.Marshal(req)

	resp, err := http.Post("http://"+listenAddr+"/v1/node/jwt", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	// Then: status 200, and the response contains a valid JWT.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var jwtResp types.JWTResponse
	if err := json.NewDecoder(resp.Body).Decode(&jwtResp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if jwtResp.JWT == "" {
		t.Fatal("expected non-empty JWT")
	}
	if jwtResp.RefreshBefore <= 0 {
		t.Fatalf("expected positive RefreshBefore, got %d", jwtResp.RefreshBefore)
	}

	// Verify the JWT is valid.
	payload, err := sjwt.VerifyJWTAnyPeerID(jwtResp.JWT, pubKey)
	if err != nil {
		t.Fatalf("verify issued JWT: %v", err)
	}
	if payload.PeerID != peerID {
		t.Fatalf("expected peer ID %q, got %q", peerID, payload.PeerID)
	}

	cancel()
}

func TestJWTHTTPServer_invalidSignature_returns403(t *testing.T) {
	// Given: a JWTService with a real keypair.
	_, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	auditLog := jwt.NewAuditLog(io.Discard)

	server := jwt.NewJWTHTTPServer(jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, defaultTestPolicy()))

	listenAddr := pickFreeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ctx, listenAddr)
	}()

	waitForServer(t, listenAddr)

	// When: POST /v1/node/jwt with a different keypair's signature.
	peerID, err := sjwt.GeneratePeerID(privKey)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}
	// Sign with a different (random) key — invalid.
	_, wrongPriv, _ := ed25519.GenerateKey(nil)
	signed := ed25519.Sign(wrongPriv, []byte(peerID))

	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: signed,
	}
	body, _ := json.Marshal(req)

	resp, err := http.Post("http://"+listenAddr+"/v1/node/jwt", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	// Then: status 403.
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	cancel()
}

func pickFreeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func waitForServer(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/v1/node/jwt")
		if err == nil {
			resp.Body.Close()
			return // server is up (even if 405 or 404)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not start within 2s")
}

// ---------------------------------------------------------------------------
// Grant-matrix tests (T6)
// ---------------------------------------------------------------------------

// newGrantMatrixSvc builds a JWTService with the given effective policy and a
// permissive rate limiter so the test can focus on grant logic.
func newGrantMatrixSvc(t *testing.T, policy config.JWTPolicyConfig) (*jwt.JWTService, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pubKey, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	auditLog := jwt.NewAuditLog(io.Discard)
	svc := jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, policy)
	return svc, pubKey, privKey
}

// signRequestFor builds a JWTRequest with a valid SignedPeerID and the given
// declared capabilities (nil = omit field).
func signRequestFor(t *testing.T, privKey ed25519.PrivateKey, peerID types.PeerId, declared *types.NodeCapabilities) types.JWTRequest {
	t.Helper()
	return types.JWTRequest{
		PeerID:               peerID,
		SignedPeerID:         sjwt.SignPeerID(privKey, peerID),
		DeclaredCapabilities: declared,
	}
}

// TestGrantMatrix_NilDeclaredMatchesCurrentBehaviour asserts that when
// req.DeclaredCapabilities == nil the granted capabilities, quota, TTL and
// RefreshBefore match the pre-policy behaviour bit-for-bit.
func TestGrantMatrix_NilDeclaredMatchesCurrentBehaviour(t *testing.T) {
	svc, cpPub, _ := newGrantMatrixSvc(t, config.JWTPolicyConfig{})

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}

	resp, err := svc.HandleJWTRequest(signRequestFor(t, nodePriv, peerID, nil), "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	if resp.RefreshBefore != 300 {
		t.Errorf("RefreshBefore = %d, want 300 (legacy default)", resp.RefreshBefore)
	}

	payload, err := sjwt.VerifyJWTAnyPeerID(resp.JWT, cpPub)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}

	if !payload.Capabilities.Edge {
		t.Error("Edge should be true (legacy default)")
	}
	if !payload.Capabilities.PeerICP {
		t.Error("PeerICP should be true (legacy default)")
	}
	if payload.Capabilities.L4Backhaul {
		t.Error("L4Backhaul should be false (non-whitelisted, legacy default)")
	}
	if payload.Capabilities.RelayProvider {
		t.Error("RelayProvider should be false (legacy default)")
	}
	if payload.BandwidthQuota != 50_000_000 {
		t.Errorf("BandwidthQuota = %d, want 50_000_000 (legacy default)", payload.BandwidthQuota)
	}
	if payload.Exp-payload.Iat != 3600 {
		t.Errorf("Exp-Iat = %d, want 3600 (legacy 1h TTL)", payload.Exp-payload.Iat)
	}
}

// TestGrantMatrix_DeclaredEdgeAndRelay_DefaultAllowsRelay_GrantsEdgeAndRelay
// asserts: declared {Edge, Relay} + default allows Relay → grants Edge+Relay
// (PeerICP also granted since default allows it; declared missing PeerICP →
// intersection is false).
func TestGrantMatrix_DeclaredEdgeAndRelay_DefaultAllowsRelay_GrantsEdgeAndRelay(t *testing.T) {
	policy := config.JWTPolicyConfig{
		DefaultCapabilities: config.JWTPolicyDefaultCapabilities{
			Edge:          true,
			PeerICP:       true,
			RelayProvider: true,
		},
	}
	svc, cpPub, _ := newGrantMatrixSvc(t, policy)

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}

	declared := &types.NodeCapabilities{
		Edge:          true,
		PeerICP:       false, // declared false → intersection false
		RelayProvider: true,
		L4Backhaul:    true, // must be ignored
	}
	resp, err := svc.HandleJWTRequest(signRequestFor(t, nodePriv, peerID, declared), "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	payload, err := sjwt.VerifyJWTAnyPeerID(resp.JWT, cpPub)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}

	if !payload.Capabilities.Edge {
		t.Error("Edge should be granted (declared ∩ default = true ∩ true)")
	}
	if payload.Capabilities.PeerICP {
		t.Error("PeerICP should NOT be granted (declared false ∩ default true = false)")
	}
	if !payload.Capabilities.RelayProvider {
		t.Error("RelayProvider should be granted (declared true ∩ default true = true)")
	}
	if payload.Capabilities.L4Backhaul {
		t.Error("L4Backhaul must NOT be granted from declared; whitelist-only (peer not whitelisted)")
	}
}

// TestGrantMatrix_DeclaredL4ButNotWhitelisted_L4NotGranted asserts that a node
// declaring L4Backhaul=true is NOT granted L4 unless it is in the whitelist.
func TestGrantMatrix_DeclaredL4ButNotWhitelisted_L4NotGranted(t *testing.T) {
	svc, cpPub, _ := newGrantMatrixSvc(t, config.JWTPolicyConfig{})

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}

	declared := &types.NodeCapabilities{
		Edge:       true,
		L4Backhaul: true, // declared L4 must be ignored
	}
	resp, err := svc.HandleJWTRequest(signRequestFor(t, nodePriv, peerID, declared), "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	payload, err := sjwt.VerifyJWTAnyPeerID(resp.JWT, cpPub)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}

	if payload.Capabilities.L4Backhaul {
		t.Error("L4Backhaul must NOT be granted: declared L4 is ignored and peer is not whitelisted")
	}
	if !payload.Capabilities.Edge {
		t.Error("Edge should be granted (declared true ∩ default true)")
	}
}

// TestGrantMatrix_DeclaredL4AndWhitelisted_L4Granted asserts the whitelist
// still grants L4 when the peer is whitelisted, regardless of declared value.
func TestGrantMatrix_DeclaredL4AndWhitelisted_L4Granted(t *testing.T) {
	pubKey, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	auditLog := jwt.NewAuditLog(io.Discard)
	svc := jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, config.JWTPolicyConfig{})

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}
	whitelist.Add(peerID)

	// Declared L4=false — but peer IS whitelisted, so L4 should still be granted.
	declared := &types.NodeCapabilities{Edge: true, L4Backhaul: false}
	resp, err := svc.HandleJWTRequest(signRequestFor(t, nodePriv, peerID, declared), "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	payload, err := sjwt.VerifyJWTAnyPeerID(resp.JWT, pubKey)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}
	if !payload.Capabilities.L4Backhaul {
		t.Error("L4Backhaul should be granted: whitelist takes precedence over declared L4 value")
	}
}

// TestGrantMatrix_PolicyOverridesTTLAndQuota asserts non-default policy values
// for TTL, RefreshBefore and BandwidthQuota propagate to the issued JWT.
func TestGrantMatrix_PolicyOverridesTTLAndQuota(t *testing.T) {
	policy := config.JWTPolicyConfig{
		TTL:                  "30m",
		RefreshBeforeSeconds: 120,
		BandwidthQuotaBytes:  99_999_999,
		DefaultCapabilities: config.JWTPolicyDefaultCapabilities{
			Edge:    true,
			PeerICP: true,
		},
	}
	svc, cpPub, _ := newGrantMatrixSvc(t, policy)

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}

	resp, err := svc.HandleJWTRequest(signRequestFor(t, nodePriv, peerID, nil), "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	if resp.RefreshBefore != 120 {
		t.Errorf("RefreshBefore = %d, want 120", resp.RefreshBefore)
	}

	payload, err := sjwt.VerifyJWTAnyPeerID(resp.JWT, cpPub)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}
	if payload.BandwidthQuota != 99_999_999 {
		t.Errorf("BandwidthQuota = %d, want 99_999_999", payload.BandwidthQuota)
	}
	if payload.Exp-payload.Iat != 1800 {
		t.Errorf("Exp-Iat = %d, want 1800 (30m)", payload.Exp-payload.Iat)
	}
}
