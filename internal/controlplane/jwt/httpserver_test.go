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

	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

func TestJWTHTTPServer_validJWTRequest_returns200(t *testing.T) {
	// Given: a JWTService with a real keypair, empty whitelist, and permissive rate limiter.
	pubKey, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	auditLog := jwt.NewAuditLog(io.Discard)

	service := jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog)
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

	server := jwt.NewJWTHTTPServer(jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog))

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
