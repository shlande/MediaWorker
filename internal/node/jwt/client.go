package jwt

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// JWTClient - node side
// ---------------------------------------------------------------------------

// JWTClient requests capability JWTs from the control plane on behalf of a node.
type JWTClient struct {
	privKey      ed25519.PrivateKey
	peerID       types.PeerId
	endpoint     string
	httpClient   *http.Client
	capabilities types.NodeCapabilities

	mu            sync.RWMutex
	currentJWT    types.CapabilityJWT
	degraded      bool

	// retryBackoff controls the base delay for retries. Tests can set this to a
	// small value to speed up retry exhaustion.
	retryBackoff time.Duration
}

// NewJWTClient creates a JWTClient. privKey is the node's Ed25519 private key.
// capabilities is the node's declared capabilities, sent with each JWT request
// so the control plane can apply its `declared ∩ default` grant policy.
func NewJWTClient(privKey ed25519.PrivateKey, peerID types.PeerId, endpoint string, capabilities types.NodeCapabilities) *JWTClient {
	return &JWTClient{
		privKey:      privKey,
		peerID:       peerID,
		endpoint:     endpoint,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		capabilities: capabilities,
	}
}

// RequestJWT sends a signed JWTRequest (with declared capabilities) to the
// control plane and returns the response. On success the returned JWT is
// cached and accessible via CurrentJWT.
func (c *JWTClient) RequestJWT(ctx context.Context) (*types.JWTResponse, error) {
	// Copy capabilities through a pointer so the request's omitempty works:
	// a non-nil pointer (even all-false) signals "I am declaring capabilities".
	declared := c.capabilities
	req := types.JWTRequest{
		PeerID:               c.peerID,
		SignedPeerID:         sjwt.SignPeerID(c.privKey, c.peerID),
		DeclaredCapabilities: &declared,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("jwt client: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("jwt client: create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("jwt client: post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwt client: unexpected status %d", resp.StatusCode)
	}

	var jr types.JWTResponse
	if err := json.NewDecoder(resp.Body).Decode(&jr); err != nil {
		return nil, fmt.Errorf("jwt client: decode response: %w", err)
	}

	c.mu.Lock()
	c.currentJWT = jr.JWT
	c.mu.Unlock()

	return &jr, nil
}

// RequestJWTWithRetry calls RequestJWT with exponential backoff: 1s → 2s →
// 4s → ... → 30s, max 10 retries. If all retries fail, enters degraded mode
// (logs an error, sets a degraded flag — no recovery logic here, that's the
// caller's responsibility).
func (c *JWTClient) RequestJWTWithRetry(ctx context.Context) (*types.JWTResponse, error) {
	const (
		maxRetries = 10
		maxDelay   = 30 * time.Second
	)

	baseDelay := c.retryBackoff
	if baseDelay == 0 {
		baseDelay = 1 * time.Second
	}
	delay := baseDelay

	var lastErr error
	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				c.enterDegradedMode(ctx.Err())
				return nil, ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}

		jr, err := c.RequestJWT(ctx)
		if err == nil {
			return jr, nil
		}
		lastErr = err
	}

	c.enterDegradedMode(lastErr)
	return nil, fmt.Errorf("jwt client: all %d retries failed: %w", maxRetries, lastErr)
}

// CurrentJWT returns the last successfully obtained JWT, or empty string.
func (c *JWTClient) CurrentJWT() types.CapabilityJWT {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentJWT
}

// IsDegraded reports whether the client is in degraded mode (all retries exhausted).
func (c *JWTClient) IsDegraded() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.degraded
}

// enterDegradedMode logs the error and sets the degraded flag. Recovery from
// degraded mode is the caller's responsibility (e.g. periodic retry loop).
func (c *JWTClient) enterDegradedMode(err error) {
	c.mu.Lock()
	c.degraded = true
	c.mu.Unlock()
	log.Printf("jwt client: entering degraded mode: %v", err)
}
