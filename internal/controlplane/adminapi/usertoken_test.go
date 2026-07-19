package adminapi

import (
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

func testSecret() []byte {
	return []byte("test-secret-key-for-admin-tokens")
}

func testPayload() UserTokenPayload {
	return UserTokenPayload{
		UserID:   "user-abc-123",
		Username: "admin",
		Roles:    []string{"admin", "operator"},
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(8 * time.Hour).Unix(),
	}
}

// ---------------------------------------------------------------------------
// 1. Round-trip: sign payload, verify it, check payload matches
// ---------------------------------------------------------------------------

func TestUserToken_SignVerify_RoundTrip(t *testing.T) {
	secret := testSecret()
	payload := testPayload()

	token, err := SignUserToken(payload, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	got, err := VerifyUserToken(token, secret)
	if err != nil {
		t.Fatalf("VerifyUserToken: %v", err)
	}

	if got.UserID != payload.UserID {
		t.Fatalf("UserID: got %q, want %q", got.UserID, payload.UserID)
	}
	if got.Username != payload.Username {
		t.Fatalf("Username: got %q, want %q", got.Username, payload.Username)
	}
	if len(got.Roles) != len(payload.Roles) {
		t.Fatalf("Roles length: got %d, want %d", len(got.Roles), len(payload.Roles))
	}
	for i, role := range payload.Roles {
		if got.Roles[i] != role {
			t.Fatalf("Roles[%d]: got %q, want %q", i, got.Roles[i], role)
		}
	}
	if got.Iat != payload.Iat {
		t.Fatalf("Iat: got %d, want %d", got.Iat, payload.Iat)
	}
	if got.Exp != payload.Exp {
		t.Fatalf("Exp: got %d, want %d", got.Exp, payload.Exp)
	}

	// Verify the token has exactly 3 segments.
	if strings.Count(token, ".") != 2 {
		t.Fatalf("token should have 2 dots (3 segments), got %d dots", strings.Count(token, "."))
	}
}

// ---------------------------------------------------------------------------
// 2. Tampered payload → ErrUserTokenBadSignature
// ---------------------------------------------------------------------------

func TestUserToken_TamperedPayload(t *testing.T) {
	secret := testSecret()
	payload := testPayload()

	token, err := SignUserToken(payload, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	tamperedPayload := tamperBase64url(parts[1])
	tamperedToken := parts[0] + "." + tamperedPayload + "." + parts[2]

	_, err = VerifyUserToken(tamperedToken, secret)
	if err != ErrUserTokenBadSignature {
		t.Fatalf("expected ErrUserTokenBadSignature, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 3. Tampered signature → ErrUserTokenBadSignature
// ---------------------------------------------------------------------------

func TestUserToken_TamperedSignature(t *testing.T) {
	secret := testSecret()
	payload := testPayload()

	token, err := SignUserToken(payload, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	tamperedSig := tamperBase64url(parts[2])
	tamperedToken := parts[0] + "." + parts[1] + "." + tamperedSig

	_, err = VerifyUserToken(tamperedToken, secret)
	if err != ErrUserTokenBadSignature {
		t.Fatalf("expected ErrUserTokenBadSignature, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 4. Wrong secret → ErrUserTokenBadSignature
// ---------------------------------------------------------------------------

func TestUserToken_WrongSecret(t *testing.T) {
	secret := testSecret()
	payload := testPayload()

	token, err := SignUserToken(payload, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	wrongSecret := []byte("completely-different-secret-key")

	_, err = VerifyUserToken(token, wrongSecret)
	if err != ErrUserTokenBadSignature {
		t.Fatalf("expected ErrUserTokenBadSignature, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 5. Expired token → ErrUserTokenExpired
// ---------------------------------------------------------------------------

func TestUserToken_Expired(t *testing.T) {
	secret := testSecret()
	payload := UserTokenPayload{
		UserID:   "user-expired",
		Username: "expired-user",
		Roles:    []string{"admin"},
		Iat:      time.Now().Add(-2 * time.Hour).Unix(),
		Exp:      time.Now().Add(-1 * time.Hour).Unix(), // expired 1 hour ago
	}

	token, err := SignUserToken(payload, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	_, err = VerifyUserToken(token, secret)
	if err != ErrUserTokenExpired {
		t.Fatalf("expected ErrUserTokenExpired, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 6. Malformed token (1 part, 2 parts, 4 parts) → ErrUserTokenFormat
// ---------------------------------------------------------------------------

func TestUserToken_MalformedToken(t *testing.T) {
	secret := testSecret()
	payload := testPayload()

	token, err := SignUserToken(payload, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts for valid token, got %d", len(parts))
	}

	tests := []struct {
		name  string
		token string
	}{
		{"1 part (no dots)", parts[0]},
		{"2 parts (one dot)", parts[0] + "." + parts[1]},
		{"4 parts (three dots)", parts[0] + "." + parts[1] + "." + parts[2] + "." + parts[0]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := VerifyUserToken(tt.token, secret)
			if err != ErrUserTokenFormat {
				t.Fatalf("expected ErrUserTokenFormat, got %v", err)
			}
		})
	}
}
