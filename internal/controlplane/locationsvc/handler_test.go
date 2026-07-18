package locationsvc_test

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/locationsvc"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// getLocsFunc is the narrow function signature fakeClient cares about; we
// wrap it to fully satisfy metadata.BlobStoreClient (the Write* methods panic
// since locationsvc never calls them).
type getLocsFunc func(ctx context.Context, blobHash string) ([]types.BlobLocation, error)

type fakeClient struct{ get getLocsFunc }

func (f *fakeClient) GetBlobLocations(ctx context.Context, h string) ([]types.BlobLocation, error) {
	return f.get(ctx, h)
}

// fakeClientWrapper fully satisfies metadata.BlobStoreClient. The Write*
// methods panic because locationsvc.Handler only calls GetBlobLocations.
type fakeClientWrapper struct{ inner *fakeClient }

func (w *fakeClientWrapper) GetBlobLocations(ctx context.Context, h string) ([]types.BlobLocation, error) {
	return w.inner.GetBlobLocations(ctx, h)
}

func (w *fakeClientWrapper) WriteBlob(_ context.Context, _ *sql.Tx, _ []types.BlobDescriptor) error {
	panic("WriteBlob not used by locationsvc")
}

func (w *fakeClientWrapper) WriteBlobLocations(_ context.Context, _ *sql.Tx, _ []types.BlobLocation) error {
	panic("WriteBlobLocations not used by locationsvc")
}

var _ metadata.BlobStoreClient = (*fakeClientWrapper)(nil)

func newFakeClient(get getLocsFunc) metadata.BlobStoreClient {
	return &fakeClientWrapper{inner: &fakeClient{get: get}}
}

// signTestJWT builds a real Ed25519-signed JWT with the given capabilities and
// expiry. Uses a freshly generated CP keypair by default; tests that need a
// specific key can call signTestJWTWithKey.
func signTestJWT(t *testing.T, cpPriv ed25519.PrivateKey, caps types.NodeCapabilities, exp int64) types.CapabilityJWT {
	t.Helper()
	payload := types.NodeJWTPayload{
		NodeID:         "test-node",
		PeerID:         "12D3KooWTestPeerIDxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		Capabilities:   caps,
		BandwidthQuota: 50_000_000,
		Iat:            time.Now().Unix(),
		Exp:            exp,
	}
	jwtStr, err := sjwt.SignJWT(payload, cpPriv)
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}
	return jwtStr
}

// makeRequest builds an authenticated GET request for the given hash.
func makeRequest(t *testing.T, jwt string, hash string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/blob-locations/"+hash, nil)
	req.SetPathValue("hash", hash)
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+string(jwt))
	}
	return req
}

// runHandler invokes the handler directly and returns the recorder + decoded body.
func runHandler(t *testing.T, h http.Handler, jwt string, hash string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, jwt, hash))
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec, body
}

// ---------------------------------------------------------------------------
// Tests: 5 required branches (200 / 404 / 401 / 403 / 503)
// ---------------------------------------------------------------------------

func TestHandler_ValidJWT_LocationsExist_Returns200(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	store := map[string][]types.BlobLocation{
		"abc123": {
			{BlobHash: "abc123", BackendID: "115:acct_03", FileID: "file_001"},
			{BlobHash: "abc123", BackendID: "baidu:acct_01", FileID: "file_002"},
		},
	}
	mc := newFakeClient(func(_ context.Context, h string) ([]types.BlobLocation, error) {
		return store[h], nil
	})

	h := locationsvc.NewHandler(cpPub, mc)
	jwt := signTestJWT(t, cpPriv, types.NodeCapabilities{Edge: true}, time.Now().Add(1*time.Hour).Unix())

	rec, body := runHandler(t, h, string(jwt), "abc123")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
	}
	locs, ok := body["locations"].([]any)
	if !ok {
		t.Fatalf("expected locations array in body, got %v", body)
	}
	if len(locs) != 2 {
		t.Fatalf("expected 2 locations, got %d", len(locs))
	}
	first := locs[0].(map[string]any)
	if first["backend_id"] != "115:acct_03" {
		t.Errorf("first backend_id = %q, want 115:acct_03", first["backend_id"])
	}
	if first["file_id"] != "file_001" {
		t.Errorf("first file_id = %q, want file_001", first["file_id"])
	}
	second := locs[1].(map[string]any)
	if second["backend_id"] != "baidu:acct_01" {
		t.Errorf("second backend_id = %q, want baidu:acct_01", second["backend_id"])
	}
}

func TestHandler_ValidJWT_NoLocations_Returns404(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	mc := newFakeClient(func(_ context.Context, _ string) ([]types.BlobLocation, error) {
		return nil, nil // hash not in store → empty result
	})

	h := locationsvc.NewHandler(cpPub, mc)
	jwt := signTestJWT(t, cpPriv, types.NodeCapabilities{Edge: true}, time.Now().Add(1*time.Hour).Unix())

	rec, body := runHandler(t, h, string(jwt), "nonexistent-hash")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	if body["error"] == nil {
		t.Errorf("expected error field in body, got %v", body)
	}
}

func TestHandler_MissingJWT_Returns401(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	mc := newFakeClient(func(_ context.Context, _ string) ([]types.BlobLocation, error) {
		t.Fatal("GetBlobLocations should not be called on auth failure")
		return nil, nil
	})

	h := locationsvc.NewHandler(cpPub, mc)

	// No Authorization header at all.
	rec, body := runHandler(t, h, "", "abc123")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	if body["error"] == nil {
		t.Errorf("expected error field, got %v", body)
	}
}

func TestHandler_ExpiredJWT_Returns401(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	mc := newFakeClient(func(_ context.Context, _ string) ([]types.BlobLocation, error) {
		t.Fatal("GetBlobLocations should not be called on auth failure")
		return nil, nil
	})

	h := locationsvc.NewHandler(cpPub, mc)
	// Expired 1 hour ago.
	expiredJWT := signTestJWT(t, cpPriv, types.NodeCapabilities{Edge: true}, time.Now().Add(-1*time.Hour).Unix())

	rec, _ := runHandler(t, h, string(expiredJWT), "abc123")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for expired JWT, got %d (body=%s)", rec.Code, rec.Body.String())
	}
}

func TestHandler_BadSignatureJWT_Returns401(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	// Sign with a DIFFERENT keypair — signature won't verify against cpPub.
	_, wrongPriv, _ := sjwt.GenerateEd25519Key()
	forgedJWT := signTestJWT(t, wrongPriv, types.NodeCapabilities{Edge: true}, time.Now().Add(1*time.Hour).Unix())

	mc := newFakeClient(func(_ context.Context, _ string) ([]types.BlobLocation, error) {
		t.Fatal("GetBlobLocations should not be called on auth failure")
		return nil, nil
	})

	h := locationsvc.NewHandler(cpPub, mc)
	rec, _ := runHandler(t, h, string(forgedJWT), "abc123")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad-signature JWT, got %d", rec.Code)
	}
}

func TestHandler_MalformedBearer_Returns401(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	mc := newFakeClient(func(_ context.Context, _ string) ([]types.BlobLocation, error) {
		t.Fatal("GetBlobLocations should not be called on auth failure")
		return nil, nil
	})

	h := locationsvc.NewHandler(cpPub, mc)

	// Wrong scheme (Basic instead of Bearer).
	req := httptest.NewRequest(http.MethodGet, "/v1/blob-locations/abc123", nil)
	req.SetPathValue("hash", "abc123")
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for non-Bearer scheme, got %d", rec.Code)
	}

	// Empty Bearer token.
	req2 := httptest.NewRequest(http.MethodGet, "/v1/blob-locations/abc123", nil)
	req2.SetPathValue("hash", "abc123")
	req2.Header.Set("Authorization", "Bearer ")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for empty bearer, got %d", rec2.Code)
	}

	// Garbage token (not 3-part JWT).
	req3 := httptest.NewRequest(http.MethodGet, "/v1/blob-locations/abc123", nil)
	req3.SetPathValue("hash", "abc123")
	req3.Header.Set("Authorization", "Bearer not.a.jwt")
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for malformed jwt, got %d", rec3.Code)
	}
}

func TestHandler_ValidJWT_NoEdgeCapability_Returns403(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	mc := newFakeClient(func(_ context.Context, _ string) ([]types.BlobLocation, error) {
		t.Fatal("GetBlobLocations should not be called when Edge cap is missing")
		return nil, nil
	})

	h := locationsvc.NewHandler(cpPub, mc)
	// Valid JWT but only RelayProvider capability, no Edge.
	jwt := signTestJWT(t, cpPriv, types.NodeCapabilities{RelayProvider: true, Edge: false}, time.Now().Add(1*time.Hour).Unix())

	rec, body := runHandler(t, h, string(jwt), "abc123")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for missing Edge capability, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "edge capability") {
		t.Errorf("expected 'edge capability' in error message, got %v", body)
	}
}

func TestHandler_NilMetadataClient_Returns503(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	// Construct with a nil metadata.BlobStoreClient — simulates PG unavailable.
	h := locationsvc.NewHandler(cpPub, nil)
	jwt := signTestJWT(t, cpPriv, types.NodeCapabilities{Edge: true}, time.Now().Add(1*time.Hour).Unix())

	rec, body := runHandler(t, h, string(jwt), "abc123")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for nil metadata client, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	if body["error"] == nil {
		t.Errorf("expected error field, got %v", body)
	}
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

func TestHandler_MetadataError_Returns500(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	mc := newFakeClient(func(_ context.Context, _ string) ([]types.BlobLocation, error) {
		return nil, errors.New("simulated PG outage")
	})

	h := locationsvc.NewHandler(cpPub, mc)
	jwt := signTestJWT(t, cpPriv, types.NodeCapabilities{Edge: true}, time.Now().Add(1*time.Hour).Unix())

	rec, _ := runHandler(t, h, string(jwt), "abc123")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for metadata error, got %d", rec.Code)
	}
}

func TestHandler_EmptyHashPathValue_Returns400(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	h := locationsvc.NewHandler(cpPub, nil)
	// PathValue("hash") absent — simulates a misconfigured route.
	req := httptest.NewRequest(http.MethodGet, "/v1/blob-locations/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing path value, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Integration: end-to-end through httptest.Server with the real mux pattern
// ---------------------------------------------------------------------------

func TestHandler_EndToEnd_MuxPattern_RoutesByHash(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp key: %v", err)
	}
	cpPub := cpPriv.Public().(ed25519.PublicKey)

	store := map[string][]types.BlobLocation{
		"deadbeef": {{BlobHash: "deadbeef", BackendID: "115:acct_03", FileID: "f1"}},
	}
	mc := newFakeClient(func(_ context.Context, h string) ([]types.BlobLocation, error) {
		return store[h], nil
	})

	mux := http.NewServeMux()
	mux.Handle("GET /v1/blob-locations/{hash}", locationsvc.NewHandler(cpPub, mc))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	jwt := signTestJWT(t, cpPriv, types.NodeCapabilities{Edge: true}, time.Now().Add(1*time.Hour).Unix())

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/blob-locations/deadbeef", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+string(jwt))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body struct {
		Locations []types.BlobLocation `json:"locations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Locations) != 1 || body.Locations[0].BackendID != "115:acct_03" {
		t.Fatalf("unexpected locations: %+v", body.Locations)
	}
}
