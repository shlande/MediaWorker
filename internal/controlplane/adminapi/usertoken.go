// Package adminapi provides the control-plane admin API primitives.
package adminapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

var (
	ErrUserTokenExpired      = errors.New("adminapi: user token expired")
	ErrUserTokenBadSignature = errors.New("adminapi: bad user token signature")
	ErrUserTokenFormat       = errors.New("adminapi: invalid user token format")
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// UserTokenPayload is the claim set carried inside a user JWT.
type UserTokenPayload struct {
	UserID   string   `json:"user_id"`
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
	Iat      int64    `json:"iat"`
	Exp      int64    `json:"exp"`
}

// ---------------------------------------------------------------------------
// JWT header (constant)
// ---------------------------------------------------------------------------

const userTokenHeader = `{"alg":"HS256","typ":"JWT"}`

// ---------------------------------------------------------------------------
// SignUserToken returns a compact three-segment JWT signed with HMAC-SHA256.
// Format: base64url(header).base64url(payload).base64url(sig)
func SignUserToken(payload UserTokenPayload, secret []byte) (string, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("adminapi: marshal payload: %w", err)
	}

	headerB64 := base64urlEncode([]byte(userTokenHeader))
	payloadB64 := base64urlEncode(payloadBytes)
	signingInput := headerB64 + "." + payloadB64

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := mac.Sum(nil)
	sigB64 := base64urlEncode(sig)

	return headerB64 + "." + payloadB64 + "." + sigB64, nil
}

// ---------------------------------------------------------------------------
// VerifyUserToken decodes and verifies a user token. It checks:
//   - The token is well-formed (3 parts)
//   - The HMAC-SHA256 signature is valid against secret
//   - Exp is not in the past (no grace period)
//
// Returns the decoded payload on success.
func VerifyUserToken(token string, secret []byte) (*UserTokenPayload, error) {
	parts := splitJWT(token)
	if len(parts) != 3 {
		return nil, ErrUserTokenFormat
	}

	signingInput := parts[0] + "." + parts[1]

	sig, err := base64urlDecode(parts[2])
	if err != nil {
		return nil, fmt.Errorf("adminapi: decode signature: %w", err)
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(sig, expectedSig) {
		return nil, ErrUserTokenBadSignature
	}

	payloadBytes, err := base64urlDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("adminapi: decode payload: %w", err)
	}

	var payload UserTokenPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("adminapi: unmarshal payload: %w", err)
	}

	if time.Now().Unix() >= payload.Exp {
		return nil, ErrUserTokenExpired
	}

	return &payload, nil
}

// ---------------------------------------------------------------------------
// Helpers (mirror shared.go internal helpers for self-contained package)
// ---------------------------------------------------------------------------

// splitJWT splits a compact JWT string into its base64url-encoded parts.
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

func base64urlEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func base64urlDecode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
