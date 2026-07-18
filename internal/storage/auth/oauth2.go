// Package auth provides OAuth2 token management for cloud drive vendors.
// Supports Baidu and OneDrive refresh_token grants with concurrent dedup
// via golang.org/x/sync/singleflight. Tokens are kept in memory only
// (never persisted to PostgreSQL).
package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/types"
	"golang.org/x/sync/singleflight"
)

// ─── Types ────────────────────────────────────────────────────────────

// TokenState holds the current OAuth2 token and its expiry.
type TokenState struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// OAuth2Config contains the static credentials for a vendor's OAuth2 client.
// These are loaded from Config.CloudAccounts (YAML) at startup and registered
// once; they do not change at runtime.
type OAuth2Config struct {
	ClientID     string
	ClientSecret string
	RefreshToken string
	RedirectURI  string // Required for OneDrive; empty for Baidu
	TokenURL     string
}

// registeredEntry is the per-key value stored in the map: the config is kept
// so refreshToken has access to client_id/client_secret without a separate lookup.
type registeredEntry struct {
	Config OAuth2Config
	State  *TokenState
}

// TokenManager manages in-memory OAuth2 tokens for multiple vendor accounts.
// Uses singleflight.Group to deduplicate concurrent refresh_token calls
// for the same key ("vendor:accountID").
type TokenManager struct {
	mu     sync.Mutex
	states map[string]*registeredEntry
	sfg    singleflight.Group
	httpc  *http.Client
}

// ─── Vendor token URLs ────────────────────────────────────────────────

const (
	baiduTokenURL = "https://openapi.baidu.com/oauth/2.0/token"
)

// onedriveHosts maps OneDrive region to login host.
var onedriveHosts = map[string]string{
	"global": "login.microsoftonline.com",
	"cn":     "login.partner.microsoftonline.cn",
	"us":     "login.microsoftonline.us",
	"de":     "login.microsoftonline.de",
}

// OneDriveTokenURL returns the token endpoint for a given OneDrive region.
// Defaults to global if the region is unknown.
func OneDriveTokenURL(region string) string {
	host, ok := onedriveHosts[region]
	if !ok {
		host = onedriveHosts["global"]
	}
	return fmt.Sprintf("https://%s/common/oauth2/v2.0/token", host)
}

// ─── Constructor ──────────────────────────────────────────────────────

// NewTokenManager creates a TokenManager. If httpc is nil, http.DefaultClient
// is used.
func NewTokenManager(httpc *http.Client) *TokenManager {
	if httpc == nil {
		httpc = http.DefaultClient
	}
	return &TokenManager{
		states: make(map[string]*registeredEntry),
		httpc:  httpc,
	}
}

// ─── Register ─────────────────────────────────────────────────────────

// Register stores the initial OAuth2 config and token state for a vendor account.
// ExpiresAt is set to zero so the first GetAccessToken call forces a refresh
// to obtain a fresh access_token from the refresh_token.
func (tm *TokenManager) Register(vendor types.Vendor, accountID string, cfg OAuth2Config) {
	key := fmt.Sprintf("%s:%s", vendor, accountID)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.states[key] = &registeredEntry{
		Config: cfg,
		State: &TokenState{
			AccessToken:  "",
			RefreshToken: cfg.RefreshToken,
			ExpiresAt:    time.Time{}, // zero → forces immediate refresh
		},
	}
}

// ─── GetAccessToken ───────────────────────────────────────────────────

// refreshResponse is the JSON shape returned by both Baidu and OneDrive
// token endpoints.
type refreshResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"` // seconds
	Error        string `json:"error,omitempty"`
	ErrorDesc    string `json:"error_description,omitempty"`
}

// GetAccessToken returns a valid access token for the given vendor account.
// If the cached token has more than 5 minutes remaining, it is returned
// directly. Otherwise a refresh_token grant is performed. Concurrent calls
// for the same key are deduplicated via singleflight.
func (tm *TokenManager) GetAccessToken(vendor types.Vendor, accountID string) (string, error) {
	key := fmt.Sprintf("%s:%s", vendor, accountID)

	// Fast path: cached token still fresh.
	tm.mu.Lock()
	entry, ok := tm.states[key]
	if ok && entry.State != nil && time.Until(entry.State.ExpiresAt) > 5*time.Minute && entry.State.AccessToken != "" {
		tm.mu.Unlock()
		return entry.State.AccessToken, nil
	}
	tm.mu.Unlock()

	// Slow path: need to refresh. Use singleflight to dedup concurrent calls.
	result, err, _ := tm.sfg.Do(key, func() (any, error) {
		return tm.refreshToken(key)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// refreshToken performs the actual refresh_token HTTP POST and updates the
// cached TokenState. Called inside singleflight.Group.Do so at most one
// goroutine executes per key.
func (tm *TokenManager) refreshToken(key string) (string, error) {
	// Read config + current refresh token under lock.
	tm.mu.Lock()
	entry, ok := tm.states[key]
	if !ok {
		tm.mu.Unlock()
		return "", fmt.Errorf("auth: no registered entry for %s", key)
	}
	cfg := entry.Config
	refreshToken := entry.State.RefreshToken
	tm.mu.Unlock()

	v := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {cfg.ClientID},
		"client_secret": {cfg.ClientSecret},
	}
	if cfg.RedirectURI != "" {
		v.Set("redirect_uri", cfg.RedirectURI)
	}

	tokenURL := cfg.TokenURL
	if tokenURL == "" {
		tokenURL = baiduTokenURL
	}

	resp, err := tm.httpc.PostForm(tokenURL, v)
	if err != nil {
		return "", fmt.Errorf("auth: token request failed for %s: %w", key, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("auth: read response body for %s: %w", key, err)
	}

	var rr refreshResponse
	if err := json.Unmarshal(body, &rr); err != nil {
		return "", fmt.Errorf("auth: parse token response for %s: %w", key, err)
	}

	if rr.Error != "" {
		return "", fmt.Errorf("auth: token error for %s: %s (%s)", key, rr.Error, rr.ErrorDesc)
	}
	if rr.AccessToken == "" {
		return "", fmt.Errorf("auth: empty access_token in response for %s", key)
	}

	newRefresh := rr.RefreshToken
	if newRefresh == "" {
		newRefresh = refreshToken // some vendors don't return a new refresh token
	}

	newState := &TokenState{
		AccessToken:  rr.AccessToken,
		RefreshToken: newRefresh,
		ExpiresAt:    time.Now().Add(time.Duration(rr.ExpiresIn) * time.Second),
	}

	tm.mu.Lock()
	if entry, ok := tm.states[key]; ok {
		entry.State = newState
	}
	tm.mu.Unlock()

	return rr.AccessToken, nil
}
