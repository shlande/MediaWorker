// Package dataplane — HTTP location client for the control-plane blob-location
// query API (T10).
//
// HTTPLocationClient is the edge/dataplane-side counterpart to the control-plane
// location service (T9: GET /v1/blob-locations/{hash}). It satisfies
// dataplane.BlobLocationClient so it can be injected into LocalDataPlane or
// AccountPool in place of the in-process metadata client.
//
// jwtProvider is a `func() string` closure so this package does NOT import
// internal/node/jwt (which would create an import cycle). The edge main wires
// the closure as `func() string { return string(jwtClient.CurrentJWT()) }` —
// types.CapabilityJWT is a `type CapabilityJWT string`, so the conversion is
// trivial and needs no adapter.
package dataplane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

const defaultHTTPLocationTimeout = 5 * time.Second

// HTTPLocationClient queries the control-plane blob-location API
// (GET /v1/blob-locations/{hash}) on behalf of the edge/dataplane.
//
// Satisfies both dataplane.BlobLocationClient (plane.go:18-20) and
// accountpool.BlobLocationClient (pool.go:42-45) — the signatures are
// identical, so no adapter is required.
type HTTPLocationClient struct {
	endpoint string

	// httpClient issues the request. If nil, a default 5s-timeout client is
	// used at call time.
	httpClient *http.Client

	// jwtProvider returns the current capability JWT as a raw signed string
	// ("header.payload.sig"). Invoked on every call so a freshly refreshed
	// JWT is always used.
	jwtProvider func() string
}

var _ BlobLocationClient = (*HTTPLocationClient)(nil)

// NewHTTPLocationClient builds an HTTPLocationClient with the default 5s
// timeout. endpoint is the CP base URL; jwtProvider returns the current JWT
// string (may return "" if no JWT is cached yet — the CP will respond 401).
func NewHTTPLocationClient(endpoint string, jwtProvider func() string) *HTTPLocationClient {
	return &HTTPLocationClient{
		endpoint:    endpoint,
		httpClient:  &http.Client{Timeout: defaultHTTPLocationTimeout},
		jwtProvider: jwtProvider,
	}
}

// NewHTTPLocationClientWithTimeout is like NewHTTPLocationClient but lets the
// caller configure the per-request timeout. A non-positive timeout falls back
// to defaultHTTPLocationTimeout.
func NewHTTPLocationClientWithTimeout(endpoint string, jwtProvider func() string, timeout time.Duration) *HTTPLocationClient {
	if timeout <= 0 {
		timeout = defaultHTTPLocationTimeout
	}
	return &HTTPLocationClient{
		endpoint:    endpoint,
		httpClient:  &http.Client{Timeout: timeout},
		jwtProvider: jwtProvider,
	}
}

// GetBlobLocations queries the control-plane location API for blobHash.
//
// Status semantics (mirrors the CP handler at locationsvc/handler.go:48-92):
//   - 200 → decode `{"locations":[...]}` and return the slice (may be empty
//     if the CP returned an empty array).
//   - 404 → return an empty (non-nil) slice and nil error: no locations are
//     stored for this hash yet.
//   - 401, 403, 5xx, or any other non-2xx → return an error wrapping the
//     status code so callers can branch without type-asserting.
//
// Single-shot: no retries. The caller owns retry policy.
func (c *HTTPLocationClient) GetBlobLocations(ctx context.Context, blobHash string) ([]types.BlobLocation, error) {
	if c.jwtProvider == nil {
		return nil, fmt.Errorf("httplocclient: jwtProvider is nil (misconfigured client)")
	}

	hc := c.httpClient
	if hc == nil {
		hc = &http.Client{Timeout: defaultHTTPLocationTimeout}
	}

	url := c.endpoint + "/v1/blob-locations/" + blobHash
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("httplocclient: create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.jwtProvider())
	req.Header.Set("Accept", "application/json")

	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("httplocclient: request %s: %w", url, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		var body struct {
			Locations []types.BlobLocation `json:"locations"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, fmt.Errorf("httplocclient: decode 200 response: %w", err)
		}
		if body.Locations == nil {
			return []types.BlobLocation{}, nil
		}
		return body.Locations, nil

	case http.StatusNotFound:
		return []types.BlobLocation{}, nil

	default:
		return nil, fmt.Errorf("httplocclient: unexpected status %d from %s", resp.StatusCode, url)
	}
}
