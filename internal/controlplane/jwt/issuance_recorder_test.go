package jwt_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

type recordedIssuance struct {
	peerID types.PeerId
	exp    int64
	l4     bool
}

// TestIssuanceRecorder_SuccessTriggersCallback asserts a successful
// HandleJWTRequest invokes the recorder exactly once with the requesting
// peer ID, the issued JWT's expiry, and the L4 whitelist verdict.
func TestIssuanceRecorder_SuccessTriggersCallback(t *testing.T) {
	pubKey, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	_ = pubKey
	auditLog := jwt.NewAuditLog(&bytes.Buffer{})
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	svc := jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, config.JWTPolicyConfig{})

	var got []recordedIssuance
	svc.SetIssuanceRecorder(func(peerID types.PeerId, exp int64, l4 bool) {
		got = append(got, recordedIssuance{peerID: peerID, exp: exp, l4: l4})
	})

	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	peerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer id: %v", err)
	}
	whitelist.Add(peerID) // L4 whitelisted -> recorder must see l4=true

	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: sjwt.SignPeerID(nodePriv, peerID),
	}
	if _, err := svc.HandleJWTRequest(req, "127.0.0.1"); err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("recorder invoked %d times, want 1", len(got))
	}
	rec := got[0]
	if rec.peerID != peerID {
		t.Errorf("peerID = %q, want %q", rec.peerID, peerID)
	}
	if !rec.l4 {
		t.Error("l4 = false, want true (peer is whitelisted)")
	}

	// exp must match the issued token's payload exp (default TTL 1h).
	wantExpWindow := time.Now().Add(time.Hour).Unix()
	if rec.exp < wantExpWindow-60 || rec.exp > wantExpWindow+60 {
		t.Errorf("exp = %d, want within ±60s of now+1h (%d)", rec.exp, wantExpWindow)
	}
}

// TestIssuanceRecorder_FailureDoesNotTrigger asserts failure branches never
// invoke the recorder.
func TestIssuanceRecorder_FailureDoesNotTrigger(t *testing.T) {
	_, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	auditLog := jwt.NewAuditLog(&bytes.Buffer{})
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	svc := jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, config.JWTPolicyConfig{})

	calls := 0
	svc.SetIssuanceRecorder(func(peerID types.PeerId, exp int64, l4 bool) { calls++ })

	// Invalid signature -> error path.
	_, nodePriv, _ := sjwt.GenerateEd25519Key()
	peerID, _ := sjwt.GeneratePeerID(nodePriv)
	_, wrongPriv, _ := sjwt.GenerateEd25519Key()
	req := types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: sjwt.SignPeerID(wrongPriv, peerID),
	}
	if _, err := svc.HandleJWTRequest(req, "127.0.0.1"); err == nil {
		t.Fatal("expected invalid-signature error, got nil")
	}
	if calls != 0 {
		t.Errorf("recorder invoked %d times on failure path, want 0", calls)
	}
}

// TestIssuanceRecorder_NilByDefault asserts a service without a recorder
// issues JWTs unchanged (nil-tolerant).
func TestIssuanceRecorder_NilByDefault(t *testing.T) {
	svc, _, _, _ := auditSvc(t)

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
	if _, err := svc.HandleJWTRequest(req, "127.0.0.1"); err != nil {
		t.Fatalf("HandleJWTRequest with nil recorder: %v", err)
	}
}
