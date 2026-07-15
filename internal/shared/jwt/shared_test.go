package jwt

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustGenerateKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := GenerateEd25519Key()
	if err != nil {
		t.Fatalf("GenerateEd25519Key: %v", err)
	}
	return pub, priv
}

func mustSignJWT(t *testing.T, payload types.NodeJWTPayload, priv ed25519.PrivateKey) types.CapabilityJWT {
	t.Helper()
	jwt, err := SignJWT(payload, priv)
	if err != nil {
		t.Fatalf("SignJWT: %v", err)
	}
	return jwt
}

// tamperBase64url replaces the last character of a base64url part with a
// different valid base64url character, guaranteeing that base64url decoding
// succeeds but produces a different decoded value.
func tamperBase64url(part string) string {
	if len(part) == 0 {
		return part
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	b := []byte(part)
	last := b[len(b)-1]
	for i := 0; i < len(alphabet); i++ {
		if alphabet[i] != last {
			b[len(b)-1] = alphabet[i]
			break
		}
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// 1. Round-trip: sign payload, verify it, check payload matches
// ---------------------------------------------------------------------------

func TestSignJWT_VerifyJWT_RoundTrip(t *testing.T) {
	pub, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	payload := types.NodeJWTPayload{
		NodeID:         "test-node-1",
		PeerID:         peerID,
		Capabilities:   types.NodeCapabilities{Edge: true},
		BandwidthQuota: 1_000_000,
		Iat:            time.Now().Unix(),
		Exp:            time.Now().Add(time.Hour).Unix(),
	}

	jwtStr := mustSignJWT(t, payload, priv)
	got, err := VerifyJWT(jwtStr, pub, peerID)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}

	if got.NodeID != payload.NodeID {
		t.Errorf("NodeID: got %q, want %q", got.NodeID, payload.NodeID)
	}
	if got.PeerID != payload.PeerID {
		t.Errorf("PeerID: got %q, want %q", got.PeerID, payload.PeerID)
	}
	if got.BandwidthQuota != payload.BandwidthQuota {
		t.Errorf("BandwidthQuota: got %d, want %d", got.BandwidthQuota, payload.BandwidthQuota)
	}
	if !got.Capabilities.Edge {
		t.Error("Capabilities.Edge: want true")
	}
}

// ---------------------------------------------------------------------------
// 2. Tampered signature → ErrJWTBadSignature
// ---------------------------------------------------------------------------

func TestVerifyJWT_TamperedSignature(t *testing.T) {
	_, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}
	otherPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate other key: %v", err)
	}

	payload := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Unix(),
		Exp:    time.Now().Add(time.Hour).Unix(),
	}

	jwtStr := mustSignJWT(t, payload, priv)
	parts := strings.Split(string(jwtStr), ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	// Tamper the signature part (flip a byte in the base64url-encoded sig).
	tamperedSig := tamperBase64url(parts[2])
	tamperedJWT := types.CapabilityJWT(parts[0] + "." + parts[1] + "." + tamperedSig)

	_, err = VerifyJWT(tamperedJWT, otherPub, peerID)
	if err != ErrJWTBadSignature {
		t.Fatalf("expected ErrJWTBadSignature, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 3. Tampered payload → ErrJWTBadSignature (signature no longer matches)
// ---------------------------------------------------------------------------

func TestVerifyJWT_TamperedPayload(t *testing.T) {
	pub, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	payload := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Unix(),
		Exp:    time.Now().Add(time.Hour).Unix(),
	}

	jwtStr := mustSignJWT(t, payload, priv)
	parts := strings.Split(string(jwtStr), ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	// Tamper the payload part.
	tamperedPayload := tamperBase64url(parts[1])
	tamperedJWT := types.CapabilityJWT(parts[0] + "." + tamperedPayload + "." + parts[2])

	_, err = VerifyJWT(tamperedJWT, pub, peerID)
	if err != ErrJWTBadSignature {
		t.Fatalf("expected ErrJWTBadSignature, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 4. Wrong PeerID → ErrPeerIDMismatch
// ---------------------------------------------------------------------------

func TestVerifyJWT_WrongPeerID(t *testing.T) {
	pub, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	payload := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Unix(),
		Exp:    time.Now().Add(time.Hour).Unix(),
	}

	jwtStr := mustSignJWT(t, payload, priv)
	wrongPeerID := types.PeerId("12D3KooWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

	_, err = VerifyJWT(jwtStr, pub, wrongPeerID)
	if err != ErrPeerIDMismatch {
		t.Fatalf("expected ErrPeerIDMismatch, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 5. Expired JWT → ErrJWTExpired
// ---------------------------------------------------------------------------

func TestVerifyJWT_Expired(t *testing.T) {
	pub, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	payload := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Add(-2 * time.Hour).Unix(),
		Exp:    time.Now().Add(-1 * time.Hour).Unix(),
	}

	jwtStr := mustSignJWT(t, payload, priv)

	_, err = VerifyJWT(jwtStr, pub, peerID)
	if err != ErrJWTExpired {
		t.Fatalf("expected ErrJWTExpired, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 6. Malformed token (1 part, 2 parts, 4 parts) → format error
// ---------------------------------------------------------------------------

func TestVerifyJWT_MalformedToken(t *testing.T) {
	_, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}
	payload := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Unix(),
		Exp:    time.Now().Add(time.Hour).Unix(),
	}
	jwtStr := mustSignJWT(t, payload, priv)
	parts := strings.Split(string(jwtStr), ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts for valid JWT, got %d", len(parts))
	}

	tests := []struct {
		name string
		jwt  types.CapabilityJWT
	}{
		{"1 part (no dots)", types.CapabilityJWT(parts[0])},
		{"2 parts (one dot)", types.CapabilityJWT(parts[0] + "." + parts[1])},
		{"4 parts (three dots)", types.CapabilityJWT(parts[0] + "." + parts[1] + "." + parts[2] + "." + parts[0])},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := VerifyJWT(tt.jwt, nil, peerID)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "invalid format") {
				t.Fatalf("expected format error, got: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 7. VerifyJWTAnyPeerID — valid token, no peerID binding check
// ---------------------------------------------------------------------------

func TestVerifyJWTAnyPeerID(t *testing.T) {
	pub, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	payload := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Unix(),
		Exp:    time.Now().Add(time.Hour).Unix(),
	}

	jwtStr := mustSignJWT(t, payload, priv)

	got, err := VerifyJWTAnyPeerID(jwtStr, pub)
	if err != nil {
		t.Fatalf("VerifyJWTAnyPeerID: %v", err)
	}
	if got.PeerID != peerID {
		t.Errorf("PeerID: got %q, want %q", got.PeerID, peerID)
	}

	// Also verify expired is still caught.
	payloadExpired := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Add(-2 * time.Hour).Unix(),
		Exp:    time.Now().Add(-1 * time.Hour).Unix(),
	}
	jwtExpired := mustSignJWT(t, payloadExpired, priv)
	_, err = VerifyJWTAnyPeerID(jwtExpired, pub)
	if err != ErrJWTExpired {
		t.Fatalf("expected ErrJWTExpired, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 8. ExtractEd25519PubKey — valid PeerID
// ---------------------------------------------------------------------------

func TestExtractEd25519PubKey_ValidPeerID(t *testing.T) {
	_, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	pubKey, err := ExtractEd25519PubKey(peerID)
	if err != nil {
		t.Fatalf("ExtractEd25519PubKey: %v", err)
	}

	// Verify the extracted key matches the private key.
	sig := ed25519.Sign(priv, []byte("test-message"))
	if !ed25519.Verify(pubKey, []byte("test-message"), sig) {
		t.Error("extracted public key does not verify signature from private key")
	}
}

// ---------------------------------------------------------------------------
// 9. ExtractEd25519PubKey — invalid PeerID string
// ---------------------------------------------------------------------------

func TestExtractEd25519PubKey_InvalidPeerID(t *testing.T) {
	tests := []struct {
		name   string
		peerID types.PeerId
	}{
		{"empty string", ""},
		{"not base58", "not-a-valid-peer-id!!!"},
		{"random bytes", "abcdefghijklmnop"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ExtractEd25519PubKey(tt.peerID)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 10. GenerateEd25519Key — basic sanity
// ---------------------------------------------------------------------------

func TestGenerateEd25519Key(t *testing.T) {
	pub, priv, err := GenerateEd25519Key()
	if err != nil {
		t.Fatalf("GenerateEd25519Key: %v", err)
	}

	if len(pub) != ed25519.PublicKeySize {
		t.Errorf("public key size: got %d, want %d", len(pub), ed25519.PublicKeySize)
	}
	if len(priv) != ed25519.PrivateKeySize {
		t.Errorf("private key size: got %d, want %d", len(priv), ed25519.PrivateKeySize)
	}

	// Verify the keypair works.
	msg := []byte("hello")
	sig := ed25519.Sign(priv, msg)
	if !ed25519.Verify(pub, msg, sig) {
		t.Error("generated keypair does not verify")
	}
}

// ---------------------------------------------------------------------------
// 11. GeneratePeerID — validity
// ---------------------------------------------------------------------------

func TestGeneratePeerID(t *testing.T) {
	_, priv := mustGenerateKey(t)

	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	if peerID == "" {
		t.Fatal("expected non-empty PeerID")
	}

	// Extract the public key from it — should succeed.
	pubKey, err := ExtractEd25519PubKey(peerID)
	if err != nil {
		t.Fatalf("ExtractEd25519PubKey from generated PeerID: %v", err)
	}

	// The extracted key should match the public key derived from the private key.
	expectedPub := priv.Public().(ed25519.PublicKey)
	if len(pubKey) != len(expectedPub) {
		t.Fatalf("public key length: got %d, want %d", len(pubKey), len(expectedPub))
	}
	for i := range pubKey {
		if pubKey[i] != expectedPub[i] {
			t.Fatalf("public key mismatch at byte %d: got %02x, want %02x", i, pubKey[i], expectedPub[i])
		}
	}
}

// ---------------------------------------------------------------------------
// 12. SignPeerID — sign and verify
// ---------------------------------------------------------------------------

func TestSignPeerID(t *testing.T) {
	_, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	sig := SignPeerID(priv, peerID)
	if len(sig) != ed25519.SignatureSize {
		t.Errorf("signature size: got %d, want %d", len(sig), ed25519.SignatureSize)
	}

	// Verify using the extracted public key.
	pubKey, err := ExtractEd25519PubKey(peerID)
	if err != nil {
		t.Fatalf("ExtractEd25519PubKey: %v", err)
	}

	if !ed25519.Verify(pubKey, []byte(peerID), sig) {
		t.Error("SignPeerID signature does not verify")
	}

	// Wrong message should fail.
	if ed25519.Verify(pubKey, []byte("wrong-message"), sig) {
		t.Error("signature should not verify for wrong message")
	}

	// Wrong key should fail.
	otherPub, _, _ := ed25519.GenerateKey(nil)
	if ed25519.Verify(otherPub, []byte(peerID), sig) {
		t.Error("signature should not verify with wrong public key")
	}
}

// ---------------------------------------------------------------------------
// Optional: base64url decode invalid string test
// ---------------------------------------------------------------------------

func TestVerifyJWT_InvalidBase64url(t *testing.T) {
	// A JWT with invalid base64url in the signature part should fail decoding.
	pub, priv := mustGenerateKey(t)
	peerID, err := GeneratePeerID(priv)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}
	payload := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Unix(),
		Exp:    time.Now().Add(time.Hour).Unix(),
	}
	jwtStr := mustSignJWT(t, payload, priv)
	parts := strings.Split(string(jwtStr), ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	// Replace signature with a clearly invalid base64url string (contains invalid char).
	invalidSig := "!!!not-base64url!!!"
	badJWT := types.CapabilityJWT(parts[0] + "." + parts[1] + "." + invalidSig)

	_, err = VerifyJWT(badJWT, pub, peerID)
	if err == nil {
		t.Fatal("expected error for invalid base64url signature, got nil")
	}
	if !strings.Contains(err.Error(), "decode signature") {
		t.Fatalf("expected decode signature error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Verify a token signed with one key fails verification with a different key
// ---------------------------------------------------------------------------

func TestVerifyJWT_WrongSigningKey(t *testing.T) {
	_, privA := mustGenerateKey(t)
	pubB, _, _ := ed25519.GenerateKey(nil)
	peerID, err := GeneratePeerID(privA)
	if err != nil {
		t.Fatalf("GeneratePeerID: %v", err)
	}

	payload := types.NodeJWTPayload{
		PeerID: peerID,
		Iat:    time.Now().Unix(),
		Exp:    time.Now().Add(time.Hour).Unix(),
	}

	// Sign with key A, verify with key B.
	jwtStr := mustSignJWT(t, payload, privA)
	_, err = VerifyJWT(jwtStr, pubB, peerID)
	if err != ErrJWTBadSignature {
		t.Fatalf("expected ErrJWTBadSignature when verifying with wrong key, got %v", err)
	}
}