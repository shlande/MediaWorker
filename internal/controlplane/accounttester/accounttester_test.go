package accounttester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/accountregistry"
	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// mockRoundTripper rewrites requests for known vendor hosts to their mock
// servers (pattern: test/integration/storage_distribution_test.go:231-307).
// Any unregistered host panics — a connection test that touches an
// unexpected network endpoint fails loudly.
type mockRoundTripper struct {
	mu               sync.Mutex
	hosts            map[string]*httptest.Server
	defaultTransport http.RoundTripper
}

func newMockRoundTripper() *mockRoundTripper {
	return &mockRoundTripper{
		hosts:            make(map[string]*httptest.Server),
		defaultTransport: http.DefaultTransport,
	}
}

func (m *mockRoundTripper) registerHost(originalHost string, mockSrv *httptest.Server) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hosts[originalHost] = mockSrv
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	mockSrv, ok := m.hosts[req.URL.Host]
	m.mu.Unlock()
	if !ok {
		panic("mockRoundTripper: no mock registered for host " + req.URL.Host + " path " + req.URL.Path)
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

func newMockHTTPClient(rt *mockRoundTripper) *http.Client {
	return &http.Client{Transport: rt, Timeout: 30 * time.Second}
}

// stubValidate mirrors the ValidateFunc contract (split + required checks)
// without importing adminapi (import cycle). The real adminapi.ValidateAuth
// is exercised end to end by the adminapi handler tests.
var stubRequired = map[types.Vendor][]string{
	types.VendorBaidu:    {"client_id", "client_secret", "refresh_token"},
	types.VendorOneDrive: {"client_id", "client_secret", "refresh_token", "redirect_uri", "region"},
	types.VendorQuark:    {"cookies"},
}

func stubValidate(vendor types.Vendor, auth map[string]any) (types.Credential, types.ClientConfig, map[string]string, []string) {
	get := func(k string) string {
		s, _ := auth[k].(string)
		return s
	}
	cred := types.Credential{RefreshToken: get("refresh_token")}
	if raw, ok := auth["cookies"].(map[string]any); ok {
		cookies := map[string]string{}
		for k, v := range raw {
			if s, isStr := v.(string); isStr {
				cookies[k] = s
			}
		}
		if len(cookies) > 0 {
			cred.Cookies = cookies
		}
	}
	cc := types.ClientConfig{
		ClientID:     get("client_id"),
		ClientSecret: get("client_secret"),
		RedirectURI:  get("redirect_uri"),
		Region:       get("region"),
	}
	fieldErrors := map[string]string{}
	for _, k := range stubRequired[vendor] {
		present := get(k) != ""
		if k == "cookies" {
			present = len(cred.Cookies) > 0
		}
		if !present {
			fieldErrors[k] = "required"
		}
	}
	return cred, cc, fieldErrors, nil
}

type fakeSecretReader struct {
	cred      types.Credential
	cc        types.ClientConfig
	err       error
	calls     int
	gotVendor types.Vendor
	gotID     string
}

func (f *fakeSecretReader) GetAccountSecret(_ context.Context, vendor types.Vendor, accountID string) (types.Credential, types.ClientConfig, error) {
	f.calls++
	f.gotVendor = vendor
	f.gotID = accountID
	return f.cred, f.cc, f.err
}

// ---------------------------------------------------------------------------
// Mock vendor endpoints
// ---------------------------------------------------------------------------

// newBaiduMocks returns a round tripper routing the Baidu token host and PCS
// host. tokenHandler serves POST /oauth/2.0/token; the uinfo endpoint answers
// errno=0 after a small delay so latency_ms is observably non-zero.
func newBaiduMocks(t *testing.T, tokenHandler http.HandlerFunc) *mockRoundTripper {
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
	rt := newMockRoundTripper()
	rt.registerHost("openapi.baidu.com", tokenSrv)
	rt.registerHost("pan.baidu.com", pcsSrv)
	return rt
}

func okTokenHandler(accessToken string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  accessToken,
			"refresh_token": "new-refresh-token",
			"expires_in":    3600,
		})
	}
}

func baiduDraftAuth() map[string]any {
	return map[string]any{
		"client_id":     "app-key",
		"client_secret": "secret-key",
		"refresh_token": "refresh-token",
	}
}

// ---------------------------------------------------------------------------
// Draft mode
// ---------------------------------------------------------------------------

func TestAccountTestDraftBaiduHealthy(t *testing.T) {
	rt := newBaiduMocks(t, okTokenHandler("mock-access-token"))
	tester := NewTester(&fakeSecretReader{}, stubValidate, newMockHTTPClient(rt))

	state, err := tester.TestDraft(context.Background(), types.VendorBaidu, baiduDraftAuth())
	if err != nil {
		t.Fatalf("TestDraft: %v", err)
	}
	if state.State != "healthy" {
		t.Fatalf("state = %q (error_msg=%q), want healthy", state.State, state.ErrorMsg)
	}
	if state.Latency <= 0 {
		t.Fatalf("latency = %v, want > 0", state.Latency)
	}
}

func TestAccountTestDraftOneDriveHealthy(t *testing.T) {
	tokenSrv := httptest.NewServer(okTokenHandler("mock-graph-token"))
	t.Cleanup(tokenSrv.Close)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1.0/me/drive" {
			time.Sleep(3 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "drive-id"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(graphSrv.Close)
	rt := newMockRoundTripper()
	rt.registerHost("login.microsoftonline.com", tokenSrv)
	rt.registerHost("graph.microsoft.com", graphSrv)
	tester := NewTester(&fakeSecretReader{}, stubValidate, newMockHTTPClient(rt))

	authMap := map[string]any{
		"client_id":     "azure-app",
		"client_secret": "azure-secret",
		"refresh_token": "refresh-token",
		"redirect_uri":  "https://example.com/callback",
		"region":        "global",
	}
	state, err := tester.TestDraft(context.Background(), types.VendorOneDrive, authMap)
	if err != nil {
		t.Fatalf("TestDraft: %v", err)
	}
	if state.State != "healthy" {
		t.Fatalf("state = %q (error_msg=%q), want healthy", state.State, state.ErrorMsg)
	}
}

func TestAccountTestDraftInvalidGrantVerbatimError(t *testing.T) {
	rt := newBaiduMocks(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_grant",
			"error_description": "refresh token expired",
		})
	})
	tester := NewTester(&fakeSecretReader{}, stubValidate, newMockHTTPClient(rt))

	state, err := tester.TestDraft(context.Background(), types.VendorBaidu, baiduDraftAuth())
	if err != nil {
		t.Fatalf("TestDraft: %v", err)
	}
	if state.State != "degraded" {
		t.Fatalf("state = %q, want degraded", state.State)
	}
	// The driver error_msg must pass through verbatim — it is the B3 core
	// diagnostic (wrong client_secret vs expired refresh_token).
	want := "token: auth: token error for baidu:draft: invalid_grant (refresh token expired)"
	if state.ErrorMsg != want {
		t.Fatalf("error_msg = %q, want verbatim %q", state.ErrorMsg, want)
	}
}

func TestAccountTestDraftQuarkDriverNotImplemented(t *testing.T) {
	// No hosts registered: any network attempt panics, proving no driver was
	// constructed for the mock vendor.
	tester := NewTester(&fakeSecretReader{}, stubValidate, newMockHTTPClient(newMockRoundTripper()))

	_, err := tester.TestDraft(context.Background(), types.VendorQuark, map[string]any{
		"cookies": map[string]any{"k": "v"},
	})
	if !errors.Is(err, ErrDriverNotImplemented) {
		t.Fatalf("err = %v, want ErrDriverNotImplemented", err)
	}
}

func TestAccountTestDraftValidationError(t *testing.T) {
	tester := NewTester(&fakeSecretReader{}, stubValidate, newMockHTTPClient(newMockRoundTripper()))

	incomplete := map[string]any{"client_id": "app-key"} // missing client_secret + refresh_token
	_, err := tester.TestDraft(context.Background(), types.VendorBaidu, incomplete)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *ValidationError", err)
	}
	if ve.FieldErrors["client_secret"] != "required" || ve.FieldErrors["refresh_token"] != "required" {
		t.Fatalf("fieldErrors = %v, want client_secret+refresh_token required", ve.FieldErrors)
	}
}

// ---------------------------------------------------------------------------
// Stored mode
// ---------------------------------------------------------------------------

func TestAccountTestStoredBaiduHealthy(t *testing.T) {
	rt := newBaiduMocks(t, okTokenHandler("mock-access-token"))
	reader := &fakeSecretReader{
		cred: types.Credential{RefreshToken: "stored-refresh-token"},
		cc:   types.ClientConfig{ClientID: "stored-app", ClientSecret: "stored-secret"},
	}
	tester := NewTester(reader, stubValidate, newMockHTTPClient(rt))

	state, err := tester.TestStored(context.Background(), types.VendorBaidu, "mw_01")
	if err != nil {
		t.Fatalf("TestStored: %v", err)
	}
	if state.State != "healthy" {
		t.Fatalf("state = %q (error_msg=%q), want healthy", state.State, state.ErrorMsg)
	}
	if reader.calls != 1 || reader.gotVendor != types.VendorBaidu || reader.gotID != "mw_01" {
		t.Fatalf("GetAccountSecret calls=%d vendor=%q id=%q, want 1/baidu/mw_01", reader.calls, reader.gotVendor, reader.gotID)
	}
}

func TestAccountTestStoredNotFoundPassthrough(t *testing.T) {
	reader := &fakeSecretReader{
		err: fmt.Errorf("%w: baidu/nope", accountregistry.ErrAccountNotFound),
	}
	tester := NewTester(reader, stubValidate, newMockHTTPClient(newMockRoundTripper()))

	_, err := tester.TestStored(context.Background(), types.VendorBaidu, "nope")
	if !errors.Is(err, accountregistry.ErrAccountNotFound) {
		t.Fatalf("err = %v, want wrapped ErrAccountNotFound passthrough", err)
	}
}

func TestAccountTestStoredQuarkDriverNotImplemented(t *testing.T) {
	reader := &fakeSecretReader{
		cred: types.Credential{Cookies: map[string]string{"k": "v"}},
	}
	// No hosts registered: any network attempt panics.
	tester := NewTester(reader, stubValidate, newMockHTTPClient(newMockRoundTripper()))

	_, err := tester.TestStored(context.Background(), types.VendorQuark, "quark_01")
	if !errors.Is(err, ErrDriverNotImplemented) {
		t.Fatalf("err = %v, want ErrDriverNotImplemented", err)
	}
	if reader.calls != 1 {
		t.Fatalf("GetAccountSecret calls = %d, want 1 (secret read precedes vendor gate)", reader.calls)
	}
}
