package dataplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Helpers ───

// newTestServer builds an httptest.Server that asserts the request shape and
// dispatches to the per-test handler. It returns the server plus the captured
// request's Authorization header and hash path so tests can assert the client
// sent the right things.
func newTestServer(t *testing.T, status int, body any) (*httptest.Server, *string, *string, *int32) {
	t.Helper()
	var authHeader string
	var hashPath string
	var callCount int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		authHeader = r.Header.Get("Authorization")
		hashPath = r.URL.Path

		// Assert request shape on every call — failures point at the client,
		// not the test setup.
		if r.Method != http.MethodGet {
			t.Errorf("request method = %q, want GET", r.Method)
		}
		if !strings.HasPrefix(r.URL.Path, "/v1/blob-locations/") {
			t.Errorf("request path = %q, want prefix /v1/blob-locations/", r.URL.Path)
		}
		if !strings.HasPrefix(authHeader, "Bearer ") {
			t.Errorf("Authorization header = %q, want Bearer scheme", authHeader)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != nil {
			_ = json.NewEncoder(w).Encode(body)
		}
	}))
	t.Cleanup(ts.Close)
	return ts, &authHeader, &hashPath, &callCount
}

// ─── Tests ───

// TestHTTPLocationClient_200_ParsesLocations verifies the happy path: a 200
// response with `{"locations":[...]}` is decoded into a []types.BlobLocation
// with each element's BlobHash/BackendID/FileID populated.
func TestHTTPLocationClient_200_ParsesLocations(t *testing.T) {
	wantLocations := []types.BlobLocation{
		{BlobHash: "deadbeef", BackendID: "115:acct-1", FileID: "file-aaa"},
		{BlobHash: "deadbeef", BackendID: "baidu:acct-2", FileID: "file-bbb"},
	}
	ts, authHdr, hashP, callCnt := newTestServer(t, http.StatusOK, locationsResponse{Locations: wantLocations})

	const jwt = "header.payload.sig"
	c := NewHTTPLocationClient(ts.URL, func() string { return jwt })

	got, err := c.GetBlobLocations(context.Background(), "deadbeef")
	if err != nil {
		t.Fatalf("GetBlobLocations: unexpected error: %v", err)
	}
	if len(got) != len(wantLocations) {
		t.Fatalf("got %d locations, want %d", len(got), len(wantLocations))
	}
	for i, want := range wantLocations {
		if got[i] != want {
			t.Errorf("location[%d] = %+v, want %+v", i, got[i], want)
		}
	}

	// Verify the client wired the JWT into the Authorization header correctly.
	if *authHdr != "Bearer "+jwt {
		t.Errorf("Authorization = %q, want %q", *authHdr, "Bearer "+jwt)
	}
	// Verify the hash was placed in the path, not the query string.
	if !strings.HasSuffix(*hashP, "/v1/blob-locations/deadbeef") {
		t.Errorf("path = %q, want suffix /v1/blob-locations/deadbeef", *hashP)
	}
	if atomic.LoadInt32(callCnt) != 1 {
		t.Errorf("call count = %d, want 1 (no retry storm)", *callCnt)
	}
}

// TestHTTPLocationClient_404_ReturnsEmptySlice verifies the "no locations"
// branch: 404 → empty (non-nil) slice, nil error. The caller (FetchBlobLocal)
// turns an empty slice into a "no locations for blob" error itself.
func TestHTTPLocationClient_404_ReturnsEmptySlice(t *testing.T) {
	ts, _, _, callCnt := newTestServer(t, http.StatusNotFound, map[string]string{"error": "no locations for hash"})

	c := NewHTTPLocationClient(ts.URL, func() string { return "jwt-token" })

	got, err := c.GetBlobLocations(context.Background(), "no-such-hash")
	if err != nil {
		t.Fatalf("GetBlobLocations: unexpected error on 404: %v", err)
	}
	if got == nil {
		t.Fatal("got nil slice, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("got %d locations, want 0", len(got))
	}
	if atomic.LoadInt32(callCnt) != 1 {
		t.Errorf("call count = %d, want 1 (404 is not retried)", *callCnt)
	}
}

// TestHTTPLocationClient_401_ReturnsError verifies the auth-failure branch:
// missing/expired/malformed JWT → 401 → error. Single request, no retry.
func TestHTTPLocationClient_401_ReturnsError(t *testing.T) {
	ts, _, _, callCnt := newTestServer(t, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})

	c := NewHTTPLocationClient(ts.URL, func() string { return "expired-or-empty" })

	got, err := c.GetBlobLocations(context.Background(), "anyhash")
	if err == nil {
		t.Fatal("expected error on 401, got nil")
	}
	if got != nil {
		t.Errorf("got non-nil slice on 401: %v", got)
	}
	// Error message should carry the status code so callers can branch
	// ("is this an auth problem or a CP outage?") without type-asserting.
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error %q does not contain status code 401", err.Error())
	}
	if atomic.LoadInt32(callCnt) != 1 {
		t.Errorf("call count = %d, want 1 (no retry storm on 401)", *callCnt)
	}
}

// TestHTTPLocationClient_500_ReturnsError verifies the CP-broken branch: 5xx
// → error. Single request, no retry.
func TestHTTPLocationClient_500_ReturnsError(t *testing.T) {
	ts, _, _, callCnt := newTestServer(t, http.StatusInternalServerError, map[string]string{"error": "metadata query failed"})

	c := NewHTTPLocationClient(ts.URL, func() string { return "valid-jwt" })

	got, err := c.GetBlobLocations(context.Background(), "hash-500")
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}
	if got != nil {
		t.Errorf("got non-nil slice on 500: %v", got)
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error %q does not contain status code 500", err.Error())
	}
	if atomic.LoadInt32(callCnt) != 1 {
		t.Errorf("call count = %d, want 1 (no retry storm on 5xx)", *callCnt)
	}
}

// TestHTTPLocationClient_403_ReturnsError covers the no-Edge-capability case:
// JWT valid but the CP rejects it for lacking Edge. Plan line 189 only lists
// 200/404/401/500 as required branches; 403 is an extra to mirror the CP
// handler's documented contract (locationsvc/handler.go:51).
func TestHTTPLocationClient_403_ReturnsError(t *testing.T) {
	ts, _, _, callCnt := newTestServer(t, http.StatusForbidden, map[string]string{"error": "edge capability required"})

	c := NewHTTPLocationClient(ts.URL, func() string { return "valid-but-no-edge-jwt" })

	got, err := c.GetBlobLocations(context.Background(), "hash-403")
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if got != nil {
		t.Errorf("got non-nil slice on 403: %v", got)
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("error %q does not contain status code 403", err.Error())
	}
	if atomic.LoadInt32(callCnt) != 1 {
		t.Errorf("call count = %d, want 1 (no retry on 403)", *callCnt)
	}
}

// TestHTTPLocationClient_JWTProviderInvokedPerCall verifies the jwtProvider
// closure is called on every GetBlobLocations — so a freshly refreshed JWT
// (managed by internal/node/jwt's refresh loop) is always used, never a
// stale cached value held by the client.
func TestHTTPLocationClient_JWTProviderInvokedPerCall(t *testing.T) {
	ts, _, _, callCnt := newTestServer(t, http.StatusOK, locationsResponse{Locations: []types.BlobLocation{
		{BlobHash: "h", BackendID: "115:a", FileID: "f"},
	}})

	var providerCalls int32
	c := NewHTTPLocationClient(ts.URL, func() string {
		atomic.AddInt32(&providerCalls, 1)
		return "jwt-from-closure"
	})

	for i := 0; i < 3; i++ {
		if _, err := c.GetBlobLocations(context.Background(), "h"); err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&providerCalls); got != 3 {
		t.Errorf("jwtProvider called %d times, want 3 (one per GetBlobLocations)", got)
	}
	if got := atomic.LoadInt32(callCnt); got != 3 {
		t.Errorf("server called %d times, want 3", got)
	}
}

// TestHTTPLocationClient_EmptyJWTStillSendsBearerHeader verifies that an
// empty JWT (before the first successful RequestJWT) surfaces as a 401 error
// rather than a client-side panic or short-circuit.
func TestHTTPLocationClient_EmptyJWTStillSendsBearerHeader(t *testing.T) {
	var seenAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
	}))
	defer ts.Close()

	c := NewHTTPLocationClient(ts.URL, func() string { return "" })

	_, err := c.GetBlobLocations(context.Background(), "h")
	if err == nil {
		t.Fatal("expected error with empty JWT (CP returns 401)")
	}
	if !strings.HasPrefix(seenAuth, "Bearer") {
		t.Errorf("Authorization = %q, want Bearer scheme prefix", seenAuth)
	}
}

// TestHTTPLocationClient_NilJWTProviderErrorsClosed verifies that a
// misconfigured client (struct-literal with nil jwtProvider, bypassing the
// constructor) fails closed with a clear error instead of nil-dereferencing.
// Production code uses NewHTTPLocationClient, which always sets the field.
func TestHTTPLocationClient_NilJWTProviderErrorsClosed(t *testing.T) {
	c := &HTTPLocationClient{endpoint: "http://example.test", httpClient: http.DefaultClient}

	_, err := c.GetBlobLocations(context.Background(), "h")
	if err == nil {
		t.Fatal("expected error with nil jwtProvider, got nil")
	}
	if !strings.Contains(err.Error(), "jwtProvider is nil") {
		t.Errorf("error %q does not mention jwtProvider is nil", err.Error())
	}
}

// TestHTTPLocationClient_DefaultTimeoutConfigurable verifies that the
// per-request timeout is applied (request longer than timeout is aborted
// with a context-deadline-style error) and that
// NewHTTPLocationClientWithTimeout overrides the default.
func TestHTTPLocationClient_DefaultTimeoutConfigurable(t *testing.T) {
	// Server that sleeps 200ms before responding.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(200 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(locationsResponse{})
	}))
	defer ts.Close()

	// 50ms timeout → request must be aborted.
	c := NewHTTPLocationClientWithTimeout(ts.URL, func() string { return "jwt" }, 50*time.Millisecond)
	start := time.Now()
	_, err := c.GetBlobLocations(context.Background(), "h")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("timeout not respected: elapsed = %v, want < 300ms", elapsed)
	}
	// Don't assert on the exact error string (net/http wraps it differently
	// across Go versions) — just confirm it failed fast.
}

// TestHTTPLocationClient_ContextCancellationPropagates verifies the caller's
// context is respected: a cancelled context aborts the request before it
// hits the wire (or at least before the response is returned).
func TestHTTPLocationClient_ContextCancellationPropagates(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wait for the request context to be cancelled, then return.
		<-r.Context().Done()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(locationsResponse{})
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before issuing the request

	c := NewHTTPLocationClient(ts.URL, func() string { return "jwt" })
	_, err := c.GetBlobLocations(ctx, "h")
	if err == nil {
		t.Fatal("expected error with pre-cancelled context, got nil")
	}
	// The error should either be the context error directly or wrap it.
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error %q does not wrap context.Canceled", err.Error())
	}
}

// locationsResponse mirrors the CP-side response shape (locationsvc/handler.go:26-28)
// — duplicated here to avoid importing the controlplane package from dataplane
// (which would create an import cycle).
type locationsResponse struct {
	Locations []types.BlobLocation `json:"locations"`
}
