package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/accountregistry"
	"github.com/shlande/mediaworker/internal/controlplane/accounttester"
	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// accountTestRoundTripper rewrites requests for known vendor hosts to their
// mock servers (pattern: test/integration/storage_distribution_test.go
// 231-307). Unregistered hosts panic so stray network attempts fail loudly.
type accountTestRoundTripper struct {
	mu               sync.Mutex
	hosts            map[string]*httptest.Server
	defaultTransport http.RoundTripper
}

func newAccountTestRoundTripper() *accountTestRoundTripper {
	return &accountTestRoundTripper{
		hosts:            make(map[string]*httptest.Server),
		defaultTransport: http.DefaultTransport,
	}
}

func (m *accountTestRoundTripper) registerHost(originalHost string, mockSrv *httptest.Server) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hosts[originalHost] = mockSrv
}

func (m *accountTestRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	mockSrv, ok := m.hosts[req.URL.Host]
	m.mu.Unlock()
	if !ok {
		panic("accountTestRoundTripper: no mock registered for host " + req.URL.Host + " path " + req.URL.Path)
	}
	newURL := *req.URL
	newURL.Scheme = "http"
	newURL.Host = mockSrv.Listener.Addr().String()
	newReq, err := http.NewRequestWithContext(req.Context(), req.Method, newURL.String(), req.Body)
	if err != nil {
		return nil, err
	}
	newReq.Header = req.Header.Clone()
	newReq.ContentLength = req.ContentLength
	return m.defaultTransport.RoundTrip(newReq)
}

// accountTestSecretReader is the stored-mode SecretReader fake.
type accountTestSecretReader struct {
	cred      types.Credential
	cc        types.ClientConfig
	err       error
	calls     int
	gotVendor types.Vendor
	gotID     string
}

func (f *accountTestSecretReader) GetAccountSecret(_ context.Context, vendor types.Vendor, accountID string) (types.Credential, types.ClientConfig, error) {
	f.calls++
	f.gotVendor = vendor
	f.gotID = accountID
	return f.cred, f.cc, f.err
}

// ---------------------------------------------------------------------------
// Fixture wiring
// ---------------------------------------------------------------------------

var accountTestSecret = []byte("test-secret-key-for-admin-tokens")

// newBaiduAccountTestServer mounts the endpoint with a Tester whose HTTP
// client routes the Baidu token + PCS hosts to mocks. The uinfo endpoint
// sleeps 3ms so latency_ms is observably non-zero.
func newBaiduAccountTestServer(t *testing.T, tokenHandler http.HandlerFunc, reader accounttester.SecretReader) *Server {
	t.Helper()
	tokenSrv := httptest.NewServer(tokenHandler)
	t.Cleanup(tokenSrv.Close)
	pcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rest/2.0/xpan/nas" && r.URL.Query().Get("method") == "uinfo" {
			time.Sleep(3 * time.Millisecond) // fixture payload delay, not synchronization
			_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(pcsSrv.Close)
	rt := newAccountTestRoundTripper()
	rt.registerHost("openapi.baidu.com", tokenSrv)
	rt.registerHost("pan.baidu.com", pcsSrv)
	tester := accounttester.NewTester(reader, ValidateAuth, &http.Client{Transport: rt, Timeout: 30 * time.Second})
	srv := NewServer(accountTestSecret)
	RegisterAccountTestRoutes(srv, tester)
	return srv
}

func okAccountTestTokenHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token":  "mock-access-token",
		"refresh_token": "new-refresh-token",
		"expires_in":    3600,
	})
}

func postAccountTest(t *testing.T, srv *Server, body string, withAuth bool) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/accounts/test", strings.NewReader(body))
	if withAuth {
		req.Header.Set("Authorization", "Bearer "+signedToken(t, accountTestSecret, []string{"admin"}))
	}
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	var parsed map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
			t.Fatalf("response not JSON: %v body=%q", err, rec.Body.String())
		}
	}
	return rec.Code, parsed
}

func baiduDraftBody() string {
	return `{"vendor":"baidu","auth":{"client_id":"app-key","client_secret":"secret-key","refresh_token":"refresh-token"}}`
}

// ---------------------------------------------------------------------------
// Draft mode
// ---------------------------------------------------------------------------

func TestAccountTestHandlerDraftBaiduHealthy(t *testing.T) {
	srv := newBaiduAccountTestServer(t, okAccountTestTokenHandler, &accountTestSecretReader{})

	code, body := postAccountTest(t, srv, baiduDraftBody(), true)
	if code != http.StatusOK {
		t.Fatalf("status = %d body=%v, want 200", code, body)
	}
	if body["state"] != "healthy" {
		t.Fatalf("state = %v, want healthy", body["state"])
	}
	latency, ok := body["latency_ms"].(float64)
	if !ok || latency < 1 {
		t.Fatalf("latency_ms = %v, want >= 1", body["latency_ms"])
	}
	if _, leaked := body["error_msg"]; leaked {
		t.Fatalf("healthy response must not carry error_msg: %v", body)
	}
}

func TestAccountTestHandlerDraftInvalidGrant(t *testing.T) {
	srv := newBaiduAccountTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_grant",
			"error_description": "refresh token expired",
		})
	}, &accountTestSecretReader{})

	code, body := postAccountTest(t, srv, baiduDraftBody(), true)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d body=%v, want 422", code, body)
	}
	if body["state"] != "degraded" {
		t.Fatalf("state = %v, want degraded", body["state"])
	}
	// Verbatim driver error_msg — the B3 core diagnostic.
	want := "token: auth: token error for baidu:draft: invalid_grant (refresh token expired)"
	if body["error_msg"] != want {
		t.Fatalf("error_msg = %q, want verbatim %q", body["error_msg"], want)
	}
}

func TestAccountTestHandlerDraftValidation400(t *testing.T) {
	srv := newBaiduAccountTestServer(t, okAccountTestTokenHandler, &accountTestSecretReader{})

	code, body := postAccountTest(t, srv, `{"vendor":"baidu","auth":{"client_id":"app-key"}}`, true)
	if code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%v, want 400", code, body)
	}
	fields, ok := body["field_errors"].(map[string]any)
	if !ok {
		t.Fatalf("field_errors missing: %v", body)
	}
	if fields["client_secret"] == nil || fields["refresh_token"] == nil {
		t.Fatalf("field_errors = %v, want client_secret+refresh_token", fields)
	}
}

func TestAccountTestHandlerQuark501(t *testing.T) {
	// No vendor hosts registered: any network attempt panics, proving no
	// driver was constructed for the mock vendor.
	tester := accounttester.NewTester(&accountTestSecretReader{}, ValidateAuth, &http.Client{Transport: newAccountTestRoundTripper()})
	srv := NewServer(accountTestSecret)
	RegisterAccountTestRoutes(srv, tester)

	code, body := postAccountTest(t, srv, `{"vendor":"quark","auth":{"cookies":{"k":"v"}}}`, true)
	if code != http.StatusNotImplemented {
		t.Fatalf("status = %d body=%v, want 501", code, body)
	}
	if body["error"] != "driver not implemented" {
		t.Fatalf("error = %v, want driver not implemented", body["error"])
	}
	if body["vendor"] != "quark" {
		t.Fatalf("vendor = %v, want quark", body["vendor"])
	}
}

func TestAccountTestHandlerBothModesMissing400(t *testing.T) {
	srv := newBaiduAccountTestServer(t, okAccountTestTokenHandler, &accountTestSecretReader{})

	code, body := postAccountTest(t, srv, `{"vendor":"baidu"}`, true)
	if code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%v, want 400", code, body)
	}
	if body["error"] == nil {
		t.Fatalf("error message missing: %v", body)
	}
}

// ---------------------------------------------------------------------------
// Stored mode
// ---------------------------------------------------------------------------

func TestAccountTestHandlerStoredBaiduHealthy(t *testing.T) {
	reader := &accountTestSecretReader{
		cred: types.Credential{RefreshToken: "stored-refresh-token"},
		cc:   types.ClientConfig{ClientID: "stored-app", ClientSecret: "stored-secret"},
	}
	srv := newBaiduAccountTestServer(t, okAccountTestTokenHandler, reader)

	code, body := postAccountTest(t, srv, `{"vendor":"baidu","account_id":"mw_01"}`, true)
	if code != http.StatusOK {
		t.Fatalf("status = %d body=%v, want 200", code, body)
	}
	if body["state"] != "healthy" {
		t.Fatalf("state = %v, want healthy", body["state"])
	}
	if reader.calls != 1 || reader.gotVendor != types.VendorBaidu || reader.gotID != "mw_01" {
		t.Fatalf("GetAccountSecret calls=%d vendor=%q id=%q, want 1/baidu/mw_01", reader.calls, reader.gotVendor, reader.gotID)
	}
}

func TestAccountTestHandlerStoredNotFound404(t *testing.T) {
	reader := &accountTestSecretReader{
		err: fmt.Errorf("%w: baidu/nope", accountregistry.ErrAccountNotFound),
	}
	srv := newBaiduAccountTestServer(t, okAccountTestTokenHandler, reader)

	code, body := postAccountTest(t, srv, `{"vendor":"baidu","account_id":"nope"}`, true)
	if code != http.StatusNotFound {
		t.Fatalf("status = %d body=%v, want 404", code, body)
	}
}

// ---------------------------------------------------------------------------
// Cross-cutting
// ---------------------------------------------------------------------------

func TestAccountTestHandlerUnauthorized(t *testing.T) {
	srv := newBaiduAccountTestServer(t, okAccountTestTokenHandler, &accountTestSecretReader{})

	code, _ := postAccountTest(t, srv, baiduDraftBody(), false)
	if code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", code)
	}
}

// TestAccountTestHandlerNoSecretLeak pins the B3 secret-hygiene rule across
// every response shape: sentinel secret VALUES and credential-material keys
// must never appear in any body. (The 400 field_errors body legitimately
// NAMES fields like "client_secret" — that is the B4 contract shape shared
// with POST /v1/admin/accounts, not a value leak.)
func TestAccountTestHandlerNoSecretLeak(t *testing.T) {
	sentinel := "sentinel-client-secret-XYZ"
	reader := &accountTestSecretReader{
		cred: types.Credential{RefreshToken: "sentinel-refresh-token-XYZ"},
		cc:   types.ClientConfig{ClientID: "stored-app", ClientSecret: sentinel},
	}
	srv := newBaiduAccountTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid_grant", "error_description": "expired"})
	}, reader)

	bodies := []string{
		fmt.Sprintf(`{"vendor":"baidu","auth":{"client_id":"a","client_secret":%q,"refresh_token":"sentinel-refresh-token-XYZ"}}`, sentinel),
		`{"vendor":"baidu","account_id":"mw_01"}`,
		`{"vendor":"quark","auth":{"cookies":{"k":"v"}}}`,
		`{"vendor":"baidu","auth":{"client_id":"a"}}`,
		`{"vendor":"baidu"}`,
	}
	for _, body := range bodies {
		req := httptest.NewRequest(http.MethodPost, "/v1/admin/accounts/test", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+signedToken(t, accountTestSecret, []string{"admin"}))
		rec := httptest.NewRecorder()
		srv.mux.ServeHTTP(rec, req)
		out := rec.Body.String()
		if strings.Contains(out, sentinel) || strings.Contains(out, "sentinel-refresh-token-XYZ") {
			t.Fatalf("secret sentinel leaked in response to %s: %s", body, out)
		}
		if strings.Contains(out, `"credential":`) {
			t.Fatalf("credential material echoed in response to %s: %s", body, out)
		}
	}
}

// TestAccountTestHandlerRegisteredShape proves RegisterAccountTestRoutes
// mounted the exact B3 pattern on the mux (405 for the wrong method).
func TestAccountTestHandlerRegisteredShape(t *testing.T) {
	srv := newBaiduAccountTestServer(t, okAccountTestTokenHandler, &accountTestSecretReader{})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/accounts/test", bytes.NewReader(nil))
	req.Header.Set("Authorization", "Bearer "+signedToken(t, accountTestSecret, []string{"admin"}))
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405 (route is POST-only)", rec.Code)
	}
}
