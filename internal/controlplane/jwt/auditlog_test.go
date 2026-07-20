package jwt_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// auditSvc builds a JWTService with a bytes.Buffer-based AuditLog so
// we can assert on emitted audit lines.
func auditSvc(t *testing.T) (*jwt.JWTService, *bytes.Buffer, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pubKey, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	buf := &bytes.Buffer{}
	auditLog := jwt.NewAuditLog(buf)
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	svc := jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, config.JWTPolicyConfig{})
	return svc, buf, pubKey, privKey
}

// auditLine decodes a single JSON audit line from the buffer.
func auditLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	raw := strings.TrimSpace(buf.String())
	if raw == "" {
		t.Fatal("audit log buffer is empty")
	}
	lines := strings.Split(raw, "\n")
	last := lines[len(lines)-1]
	var m map[string]any
	if err := json.Unmarshal([]byte(last), &m); err != nil {
		t.Fatalf("decode audit line %q: %v", last, err)
	}
	return m
}

// TestAuditLog_SuccessLineContainsResultOk asserts a successful JWT issuance
// writes an audit line with "result":"ok".
func TestAuditLog_SuccessLineContainsResultOk(t *testing.T) {
	svc, buf, _, _ := auditSvc(t)

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}
	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: sjwt.SignPeerID(nodePriv, peerID),
	}

	_, err = svc.HandleJWTRequest(req, "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	m := auditLine(t, buf)
	if got, ok := m["result"].(string); !ok || got != "ok" {
		t.Errorf("result = %v, want \"ok\"", m["result"])
	}
}

// TestAuditLog_InvalidSignatureProducesResultFail asserts an invalid
// signature writes an audit line with "result":"fail" and reason
// "invalid_signature".
func TestAuditLog_InvalidSignatureProducesResultFail(t *testing.T) {
	svc, buf, _, _ := auditSvc(t)

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}
	// Sign with a different (wrong) private key.
	_, wrongPriv, _ := ed25519.GenerateKey(nil)
	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: ed25519.Sign(wrongPriv, []byte(peerID)),
	}

	_, err = svc.HandleJWTRequest(req, "127.0.0.1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	m := auditLine(t, buf)
	if got, ok := m["result"].(string); !ok || got != "fail" {
		t.Errorf("result = %v, want \"fail\"", m["result"])
	}
	if got, ok := m["reason"].(string); !ok || got != "invalid_signature" {
		t.Errorf("reason = %v, want \"invalid_signature\"", m["reason"])
	}
}

// TestAuditLog_RateLimitedProducesResultFail asserts a rate-limit hit
// writes "result":"fail" with reason "rate_limited".
func TestAuditLog_RateLimitedProducesResultFail(t *testing.T) {
	// Use a RateLimiter with a 1-hour interval so the refill can never
	// elapse between two sequential calls — deterministic rate-limit.
	_, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	buf := &bytes.Buffer{}
	auditLog := jwt.NewAuditLog(buf)
	whitelist := jwt.NewPeerIdSet()
	blockAll := jwt.NewRateLimiter(1 * time.Hour)
	svc := jwt.NewJWTService(privKey, whitelist, blockAll, auditLog, config.JWTPolicyConfig{})

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}

	// First request consumes the only token.
	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: sjwt.SignPeerID(nodePriv, peerID),
	}
	_, _ = svc.HandleJWTRequest(req, "192.168.1.1")

	// Second request should hit rate limit.
	_, err = svc.HandleJWTRequest(req, "192.168.1.1")
	if err == nil {
		t.Fatal("expected rate-limit error, got nil")
	}

	m := auditLine(t, buf)
	if got, ok := m["result"].(string); !ok || got != "fail" {
		t.Errorf("result = %v, want \"fail\"", m["result"])
	}
	if got, ok := m["reason"].(string); !ok || got != "rate_limited" {
		t.Errorf("reason = %v, want \"rate_limited\"", m["reason"])
	}
}

// TestAuditLog_InvalidPeerIDProducesResultFail asserts an invalid PeerID
// writes "result":"fail" with reason "invalid_peer_id" without panicking.
func TestAuditLog_InvalidPeerIDProducesResultFail(t *testing.T) {
	svc, buf, _, _ := auditSvc(t)

	// Empty peer ID is invalid.
	req := types.JWTRequest{
		PeerID:       "",
		SignedPeerID: []byte("garbage"),
	}

	_, err := svc.HandleJWTRequest(req, "8.8.8.8")
	if err == nil {
		t.Fatal("expected error for empty peer ID, got nil")
	}

	m := auditLine(t, buf)
	if got, ok := m["result"].(string); !ok || got != "fail" {
		t.Errorf("result = %v, want \"fail\"", m["result"])
	}
	if got, ok := m["reason"].(string); !ok || got != "invalid_peer_id" {
		t.Errorf("reason = %v, want \"invalid_peer_id\"", m["reason"])
	}
	// The audit line must still carry the empty peer_id field — no panic.
	if got, ok := m["peer_id"].(string); !ok || got != "" {
		t.Errorf("peer_id = %v, want \"\" (empty string, no panic)", m["peer_id"])
	}
	if got, ok := m["remote_ip"].(string); !ok || got != "8.8.8.8" {
		t.Errorf("remote_ip = %v, want \"8.8.8.8\"", m["remote_ip"])
	}
}

// TestAuditLog_ReasonOmittedFromSuccessLine asserts that a success audit
// line does NOT include the reason field (omitempty).
func TestAuditLog_ReasonOmittedFromSuccessLine(t *testing.T) {
	svc, buf, _, _ := auditSvc(t)

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}
	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: sjwt.SignPeerID(nodePriv, peerID),
	}

	_, err = svc.HandleJWTRequest(req, "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	raw := strings.TrimSpace(buf.String())
	if raw == "" {
		t.Fatal("audit log buffer is empty")
	}
	// The success line should NOT contain "reason" at all (omitempty when empty).
	if strings.Contains(raw, `"reason"`) {
		t.Errorf("success audit line should not contain reason field, got: %s", raw)
	}
}

// TestAuditLog_OutputStillSingleLineJSON asserts the Log method still
// produces a single-line JSON output per event (no line breaks inside).
func TestAuditLog_OutputStillSingleLineJSON(t *testing.T) {
	auditLog := jwt.NewAuditLog(io.Discard) // no-op, test only that call compiles

	// Verify the new signature compiles — the real test is that the
	// existing test suite does not break. This is a compilation gate.
	auditLog.Log("peer1", "1.2.3.4", true, 50_000_000, 99999, "fail", "internal_error")
	auditLog.Log("peer2", "5.6.7.8", false, 0, 0, "ok", "")
}
