package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/accountregistry"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mock AdminAccountsReader ─────────────────────────────────────────────

type mockAccountsReader struct {
	views []metadata.AdminAccountView
	err   error

	// Spy fields: captured filter parameters from the last call
	lastVendor string
	lastState  string
}

func (m *mockAccountsReader) ListAccounts(ctx context.Context, vendorFilter, stateFilter string) ([]metadata.AdminAccountView, error) {
	m.lastVendor = vendorFilter
	m.lastState = stateFilter
	if m.err != nil {
		return nil, m.err
	}
	return m.views, nil
}

// ─── Test helpers ─────────────────────────────────────────────────────────

// tasty is a convenient time for deterministic test data.
var tasty = time.Date(2026, 1, 15, 8, 30, 0, 0, time.UTC)

func credMeta(authType string, hasSecret, hasRefresh bool, region string, cookieKeys ...string) metadata.CredentialMeta {
	sort.Strings(cookieKeys)
	return metadata.CredentialMeta{
		AuthType:        authType,
		HasClientSecret: hasSecret,
		HasRefreshToken: hasRefresh,
		Region:          region,
		CookieKeys:      cookieKeys,
	}
}

func healthView(state string, latency int, errMsg string, banUntil *time.Time, lastCheck time.Time) *metadata.HealthView {
	return &metadata.HealthView{
		State:     state,
		LatencyMs: latency,
		ErrorMsg:  errMsg,
		BanUntil:  banUntil,
		LastCheck: lastCheck,
	}
}

func makeServer(mc AdminAccountsReader, w AdminAccountsWriter) *Server {
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterAccountsRoutes(srv, mc, nil, w, nil, nil)
	return srv
}

func signAdminToken(t *testing.T, secret []byte) string {
	t.Helper()
	token, err := SignUserToken(UserTokenPayload{
		UserID:   "user-1",
		Username: "root",
		Roles:    []string{"admin"},
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(time.Hour).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}
	return token
}

func getAccounts(t *testing.T, ts *httptest.Server, token string, query string) (*http.Response, accountsResponse) {
	t.Helper()
	url := ts.URL + "/v1/admin/accounts"
	if query != "" {
		url += "?" + query
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	var body accountsResponse
	if resp.StatusCode == http.StatusOK && resp.Body != nil {
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
	}
	return resp, body
}

// ─── Tests ────────────────────────────────────────────────────────────────

// Given three accounts (one without health), when GET /v1/admin/accounts
// is called with a valid admin token, then all three are returned with
// null health for the no-row account and summary.by_state is correct.
func TestListAccounts_Happy(t *testing.T) {
	mock := &mockAccountsReader{
		views: []metadata.AdminAccountView{
			{
				Vendor:    "baidu",
				AccountID: "acct-01",
				Enabled:   true,
				RateLimitCfg: types.RateLimitConfig{
					QPS: 5, Burst: 10, ConcurrentLimit: 3,
				},
				VendorProfile:  types.VendorProfile{Vendor: "baidu", Weight: 3.0, BaseLatencyMs: 120, BandwidthMbps: 80},
				Health:         healthView("healthy", 45, "", nil, tasty),
				CredentialMeta: credMeta("cookie", false, false, "cn", "BAIDUID", "BDUSS"),
			},
			{
				Vendor:    "onedrive",
				AccountID: "acct-02",
				Enabled:   true,
				RateLimitCfg: types.RateLimitConfig{
					QPS: 2, Burst: 3, ConcurrentLimit: 1,
				},
				VendorProfile:  types.VendorProfile{Vendor: "onedrive", Weight: 2.5, BaseLatencyMs: 200},
				Health:         healthView("degraded", 350, "throttled", nil, tasty.Add(-time.Minute)),
				CredentialMeta: credMeta("oauth2", true, true, "global"),
			},
			{
				Vendor:    "quark",
				AccountID: "acct-03",
				Enabled:   true,
				RateLimitCfg: types.RateLimitConfig{
					QPS: 10, Burst: 20, ConcurrentLimit: 5,
				},
				VendorProfile:  types.VendorProfile{Vendor: "quark", Weight: 4.0, BaseLatencyMs: 90, BandwidthMbps: 120},
				Health:         nil, // no health row → awaiting first probe
				CredentialMeta: credMeta("cookie", false, false, "", "quark_token"),
			},
		},
	}

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := makeServer(mock, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	resp, body := getAccounts(t, ts, token, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	if len(body.Accounts) != 3 {
		t.Fatalf("len(accounts) = %d, want 3", len(body.Accounts))
	}

	// Account 1: baidu with health
	a1 := body.Accounts[0]
	if a1.Vendor != "baidu" || a1.AccountID != "acct-01" {
		t.Errorf("account[0] = %s/%s, want baidu/acct-01", a1.Vendor, a1.AccountID)
	}
	if a1.Health == nil {
		t.Error("account[0].health = nil, want non-nil (has health row)")
	} else {
		if a1.Health.State != "healthy" {
			t.Errorf("account[0].health.state = %q, want healthy", a1.Health.State)
		}
	}
	if a1.RateLimit.QPS != 5 || a1.RateLimit.Burst != 10 || a1.RateLimit.Concurrent != 3 {
		t.Errorf("account[0].rate_limit = %+v, want {5,10,3}", a1.RateLimit)
	}

	// Account 2: onedrive degraded
	a2 := body.Accounts[1]
	if a2.Vendor != "onedrive" {
		t.Errorf("account[1].vendor = %q, want onedrive", a2.Vendor)
	}
	if a2.Health == nil || a2.Health.State != "degraded" {
		t.Errorf("account[1].health = %v, want degraded", a2.Health)
	}

	// Account 3: quark, no health row → null
	a3 := body.Accounts[2]
	if a3.Vendor != "quark" {
		t.Errorf("account[2].vendor = %q, want quark", a3.Vendor)
	}
	if a3.Health != nil {
		t.Errorf("account[2].health = %+v, want nil (no health row)", a3.Health)
	}
	if a3.CredentialMeta.AuthType != "cookie" {
		t.Errorf("account[2].credential_meta.auth_type = %q, want cookie", a3.CredentialMeta.AuthType)
	}

	// Summary: 1 healthy, 1 degraded, 1 "healthy" (no-row = awaiting probe)
	if body.Summary.Total != 3 {
		t.Errorf("summary.total = %d, want 3", body.Summary.Total)
	}
	if body.Summary.ByState["healthy"] != 2 { // baidu + quark (no-row)
		t.Errorf("summary.by_state.healthy = %d, want 2", body.Summary.ByState["healthy"])
	}
	if body.Summary.ByState["degraded"] != 1 {
		t.Errorf("summary.by_state.degraded = %d, want 1", body.Summary.ByState["degraded"])
	}
	if body.Summary.ByState["banned"] != 0 {
		t.Errorf("summary.by_state.banned = %d, want 0", body.Summary.ByState["banned"])
	}
}

// Given a vendor filter, when GET /v1/admin/accounts?vendor=baidu is called,
// then the vendor parameter is passed through to ListAccounts.
func TestListAccounts_VendorFilter(t *testing.T) {
	mock := &mockAccountsReader{
		views: []metadata.AdminAccountView{
			{
				Vendor:    "baidu",
				AccountID: "acct-01",
				Enabled:   true,
				RateLimitCfg: types.RateLimitConfig{
					QPS: 1, Burst: 2, ConcurrentLimit: 1,
				},
				VendorProfile:  types.VendorProfile{Vendor: "baidu", Weight: 1.0},
				CredentialMeta: credMeta("cookie", false, false, ""),
			},
		},
	}

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := makeServer(mock, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	resp, body := getAccounts(t, ts, token, "vendor=baidu")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body.Accounts) != 1 {
		t.Fatalf("len(accounts) = %d, want 1", len(body.Accounts))
	}

	// Verify filter was passed through exactly
	if mock.lastVendor != "baidu" {
		t.Errorf("ListAccounts vendor filter = %q, want baidu", mock.lastVendor)
	}
	if mock.lastState != "" {
		t.Errorf("ListAccounts state filter = %q, want empty", mock.lastState)
	}
}

// Given a state filter, when GET /v1/admin/accounts?state=banned is called,
// then the state parameter is passed through to ListAccounts.
func TestListAccounts_StateFilter(t *testing.T) {
	mock := &mockAccountsReader{
		views: []metadata.AdminAccountView{},
	}

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := makeServer(mock, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	resp, body := getAccounts(t, ts, token, "state=banned")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body.Accounts) != 0 {
		t.Fatalf("len(accounts) = %d, want 0", len(body.Accounts))
	}
	if mock.lastState != "banned" {
		t.Errorf("ListAccounts state filter = %q, want banned", mock.lastState)
	}
}

// Given that the marshaled response body is inspected, then it must NOT
// contain the literal "credential": substring (only credential_meta is allowed).
func TestListAccounts_NoCredentialLeak(t *testing.T) {
	mock := &mockAccountsReader{
		views: []metadata.AdminAccountView{
			{
				Vendor:    "onedrive",
				AccountID: "acct-02",
				Enabled:   true,
				RateLimitCfg: types.RateLimitConfig{
					QPS: 2, Burst: 3, ConcurrentLimit: 1,
				},
				VendorProfile: types.VendorProfile{Vendor: "onedrive", Weight: 2.5, BaseLatencyMs: 200},
				Health:        healthView("healthy", 45, "", nil, tasty),
				// This CredentialMeta says has_client_secret=true and
				// has_refresh_token=true — but the name "client_secret"
				// without JSON context MUST NOT appear as a key.
				CredentialMeta: credMeta("oauth2", true, true, "global"),
			},
		},
	}

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := makeServer(mock, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/accounts", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	body := string(raw)

	// "credential_meta" is fine; '"credential"' (quoted as a JSON key) is not.
	if strings.Contains(body, `"credential"`) {
		t.Errorf("response contains '\"credential\"' — credential material leaked")
	}
	if !strings.Contains(body, `"credential_meta"`) {
		t.Error("response missing 'credential_meta' — expected credential metadata")
	}
}

// Given mock returns an error, when GET /v1/admin/accounts is called,
// then 500 is returned without panic.
func TestListAccounts_MetadataError(t *testing.T) {
	mock := &mockAccountsReader{
		err: errors.New("db connection refused"),
	}

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := makeServer(mock, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	resp, _ := getAccounts(t, ts, token, "")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}

// Given no authorization header, when GET /v1/admin/accounts is called,
// then 401 is returned (via bearer middleware).
func TestListAccounts_NoToken(t *testing.T) {
	mock := &mockAccountsReader{
		views: []metadata.AdminAccountView{
			{
				Vendor:        "baidu",
				AccountID:     "acct-01",
				Enabled:       true,
				VendorProfile: types.VendorProfile{Vendor: "baidu"},
			},
		},
	}

	srv := makeServer(mock, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	resp, _ := getAccounts(t, ts, "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// Given an empty result set, when GET /v1/admin/accounts is called,
// then response has empty accounts array and zero summary.
func TestListAccounts_Empty(t *testing.T) {
	mock := &mockAccountsReader{
		views: []metadata.AdminAccountView{},
	}

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := makeServer(mock, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	resp, body := getAccounts(t, ts, token, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body.Accounts) != 0 {
		t.Errorf("len(accounts) = %d, want 0", len(body.Accounts))
	}
	if body.Summary.Total != 0 {
		t.Errorf("summary.total = %d, want 0", body.Summary.Total)
	}
}

// Given a banned account (with ban_until set), when marshaled, then
// ban_until appears as RFC3339 in the response.
func TestListAccounts_BannedAccount(t *testing.T) {
	banTime := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	mock := &mockAccountsReader{
		views: []metadata.AdminAccountView{
			{
				Vendor:    "baidu",
				AccountID: "acct-banned",
				Enabled:   true,
				RateLimitCfg: types.RateLimitConfig{
					QPS: 1, Burst: 1, ConcurrentLimit: 1,
				},
				VendorProfile:  types.VendorProfile{Vendor: "baidu", Weight: 1.0},
				Health:         healthView("banned", 0, "account banned by operator", &banTime, tasty),
				CredentialMeta: credMeta("cookie", false, false, "cn"),
			},
		},
	}

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := makeServer(mock, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	resp, body := getAccounts(t, ts, token, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body.Accounts) != 1 {
		t.Fatalf("len(accounts) = %d, want 1", len(body.Accounts))
	}

	a := body.Accounts[0]
	if a.Health == nil {
		t.Fatal("health = nil, want banned with ban_until")
	}
	if a.Health.State != "banned" {
		t.Errorf("health.state = %q, want banned", a.Health.State)
	}
	if a.Health.BanUntil != "2026-12-31T23:59:59Z" {
		t.Errorf("health.ban_until = %q, want 2026-12-31T23:59:59Z", a.Health.BanUntil)
	}
	if body.Summary.ByState["banned"] != 1 {
		t.Errorf("summary.by_state.banned = %d, want 1", body.Summary.ByState["banned"])
	}
}

// ─── Write side (todo 26, B2 structured CRUD) ─────────────────────────────

// Sentinel secret values: every response body is grepped for these to prove
// zero secret leakage.
const (
	sentinelRefreshToken = "SENTINEL_RT_9f8e7d6c5b"
	sentinelClientSecret = "SENTINEL_CS_1a2b3c4d5e"
	sentinelCookieValue  = "SENTINEL_COOKIE_z9y8x7"
)

// sqlStateError mimics lib/pq.Error's SQLState() for conflict injection.
type sqlStateError struct{ code, msg string }

func (e sqlStateError) Error() string    { return e.msg }
func (e sqlStateError) SQLState() string { return e.code }

// fakeAccountRegistry is a stateful in-memory fake AdminAccountsWriter. It
// also implements AdminAccountsReader so POST→GET full-link tests observe
// what was actually written.
type fakeAccountRegistry struct {
	accounts map[string]accountregistry.AccountInfo

	createErr error // injected CreateAccount failure
	banErr    error // injected Ban failure

	setEnabledCalls       []bool
	setRateLimitCalls     []types.RateLimitConfig
	setVendorProfileCalls []types.VendorProfile
	credentialUpdates     int
	clientConfigUpdates   int
	broadcasts            int
	lastCredPayload       types.CredentialChangePayload
	banCalls              []banCall
	unbanCalls            []string
}

type banCall struct {
	vendor    types.Vendor
	accountID string
	reason    string
	banUntil  time.Time
}

func newFakeAccountRegistry() *fakeAccountRegistry {
	return &fakeAccountRegistry{accounts: map[string]accountregistry.AccountInfo{}}
}

func fakeKey(vendor types.Vendor, id string) string { return string(vendor) + "/" + id }

func (f *fakeAccountRegistry) CreateAccount(_ context.Context, info accountregistry.AccountInfo) error {
	if f.createErr != nil {
		return f.createErr
	}
	k := fakeKey(info.Vendor, info.AccountID)
	if _, exists := f.accounts[k]; exists {
		return fmt.Errorf("insert account: %w", sqlStateError{code: "23505", msg: "duplicate key value violates unique constraint"})
	}
	f.accounts[k] = info
	return nil
}

func (f *fakeAccountRegistry) GetAccountSecret(_ context.Context, vendor types.Vendor, accountID string) (types.Credential, types.ClientConfig, error) {
	info, ok := f.accounts[fakeKey(vendor, accountID)]
	if !ok {
		return types.Credential{}, types.ClientConfig{}, fmt.Errorf("%w: %s/%s", accountregistry.ErrAccountNotFound, vendor, accountID)
	}
	return info.Credential, info.ClientConfig, nil
}

func (f *fakeAccountRegistry) UpdateCredential(_ context.Context, vendor types.Vendor, accountID string, cred types.Credential) error {
	k := fakeKey(vendor, accountID)
	info, ok := f.accounts[k]
	if !ok {
		return fmt.Errorf("%w: %s", accountregistry.ErrAccountNotFound, k)
	}
	info.Credential = cred
	f.accounts[k] = info
	f.credentialUpdates++
	return nil
}

func (f *fakeAccountRegistry) UpdateClientConfig(_ context.Context, vendor types.Vendor, accountID string, cc types.ClientConfig) error {
	k := fakeKey(vendor, accountID)
	info, ok := f.accounts[k]
	if !ok {
		return fmt.Errorf("%w: %s", accountregistry.ErrAccountNotFound, k)
	}
	info.ClientConfig = cc
	f.accounts[k] = info
	f.clientConfigUpdates++
	return nil
}

func (f *fakeAccountRegistry) OnCredentialChange(_ context.Context, vendor types.Vendor, accountID string) {
	f.broadcasts++
	// Mirror production OnCredentialChange: re-read the stored material so the
	// captured payload is exactly what would be broadcast.
	cred, cc, err := f.GetAccountSecret(context.Background(), vendor, accountID)
	if err != nil {
		return
	}
	f.lastCredPayload = types.CredentialChangePayload{
		Vendor: vendor, AccountID: accountID, Credential: cred, ClientConfig: cc,
	}
}

func (f *fakeAccountRegistry) Ban(_ context.Context, vendor types.Vendor, accountID, reason string, banUntil time.Time) error {
	if f.banErr != nil {
		return f.banErr
	}
	f.banCalls = append(f.banCalls, banCall{vendor: vendor, accountID: accountID, reason: reason, banUntil: banUntil})
	return nil
}

func (f *fakeAccountRegistry) Unban(_ context.Context, vendor types.Vendor, accountID string) error {
	f.unbanCalls = append(f.unbanCalls, fakeKey(vendor, accountID))
	return nil
}

// fakeBroadcaster captures Broadcast calls for the circuit handler tests.
type fakeBroadcaster struct {
	eventTypes []string
	payloads   []any
	err        error // injected Broadcast failure
}

func (f *fakeBroadcaster) Broadcast(eventType string, payload any) error {
	if f.err != nil {
		return f.err
	}
	f.eventTypes = append(f.eventTypes, eventType)
	f.payloads = append(f.payloads, payload)
	return nil
}

func (f *fakeAccountRegistry) SetEnabled(_ context.Context, vendor types.Vendor, accountID string, enabled bool) error {
	k := fakeKey(vendor, accountID)
	info, ok := f.accounts[k]
	if !ok {
		return fmt.Errorf("%w: %s", accountregistry.ErrAccountNotFound, k)
	}
	info.Enabled = enabled
	f.accounts[k] = info
	f.setEnabledCalls = append(f.setEnabledCalls, enabled)
	return nil
}

func (f *fakeAccountRegistry) SetRateLimit(_ context.Context, vendor types.Vendor, accountID string, cfg types.RateLimitConfig) error {
	k := fakeKey(vendor, accountID)
	info, ok := f.accounts[k]
	if !ok {
		return fmt.Errorf("%w: %s", accountregistry.ErrAccountNotFound, k)
	}
	info.RateLimitCfg = cfg
	f.accounts[k] = info
	f.setRateLimitCalls = append(f.setRateLimitCalls, cfg)
	return nil
}

func (f *fakeAccountRegistry) SetVendorProfile(_ context.Context, vendor types.Vendor, accountID string, vp types.VendorProfile) error {
	k := fakeKey(vendor, accountID)
	info, ok := f.accounts[k]
	if !ok {
		return fmt.Errorf("%w: %s", accountregistry.ErrAccountNotFound, k)
	}
	info.VendorProfile = vp
	f.accounts[k] = info
	f.setVendorProfileCalls = append(f.setVendorProfileCalls, vp)
	return nil
}

// ListAccounts adapts the fake to the read surface so tests observe the
// post-write state end to end.
func (f *fakeAccountRegistry) ListAccounts(_ context.Context, vendorFilter, _ string) ([]metadata.AdminAccountView, error) {
	views := []metadata.AdminAccountView{}
	for _, info := range f.accounts {
		if vendorFilter != "" && string(info.Vendor) != vendorFilter {
			continue
		}
		cookieKeys := []string{}
		for k := range info.Credential.Cookies {
			cookieKeys = append(cookieKeys, k)
		}
		sort.Strings(cookieKeys)
		views = append(views, metadata.AdminAccountView{
			Vendor:        string(info.Vendor),
			AccountID:     info.AccountID,
			Enabled:       info.Enabled,
			RateLimitCfg:  info.RateLimitCfg,
			VendorProfile: info.VendorProfile,
			CredentialMeta: metadata.CredentialMeta{
				AuthType:        VendorRules[info.Vendor].AuthType,
				HasClientSecret: info.ClientConfig.ClientSecret != "",
				HasRefreshToken: info.Credential.RefreshToken != "",
				Region:          info.ClientConfig.Region,
				CookieKeys:      cookieKeys,
			},
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].AccountID < views[j].AccountID })
	return views, nil
}

func seedAccount(t *testing.T, f *fakeAccountRegistry, info accountregistry.AccountInfo) {
	t.Helper()
	if err := f.CreateAccount(context.Background(), info); err != nil {
		t.Fatalf("seed CreateAccount: %v", err)
	}
}

// doRaw performs an HTTP call with an optional pre-encoded body and returns
// status + raw body for leak grepping.
func doRaw(t *testing.T, ts *httptest.Server, method, path, token string, rawBody *string) (int, string) {
	t.Helper()
	var body io.Reader
	if rawBody != nil {
		body = strings.NewReader(*rawBody)
	}
	req, err := http.NewRequest(method, ts.URL+path, body)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return resp.StatusCode, string(raw)
}

func jsonBody(t *testing.T, v any) *string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	return &s
}

func assertNoSecretLeak(t *testing.T, body string) {
	t.Helper()
	for _, s := range []string{sentinelRefreshToken, sentinelClientSecret, sentinelCookieValue} {
		if strings.Contains(body, s) {
			t.Errorf("response body leaks sentinel secret %q: %s", s, body)
		}
	}
	if strings.Contains(body, `"credential"`) {
		t.Errorf("response body contains \"credential\" key: %s", body)
	}
}

func decodeFieldErrors(t *testing.T, body string) map[string]string {
	t.Helper()
	var parsed struct {
		Error       string            `json:"error"`
		FieldErrors map[string]string `json:"field_errors"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("decode error body: %v (body=%s)", err, body)
	}
	if parsed.Error != "validation failed" {
		t.Errorf("error = %q, want validation failed (body=%s)", parsed.Error, body)
	}
	return parsed.FieldErrors
}

func makeWriteServer(t *testing.T) (*fakeAccountRegistry, *fakeBroadcaster, *httptest.Server, string) {
	t.Helper()
	reg := newFakeAccountRegistry()
	bc := &fakeBroadcaster{}
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterAccountsRoutes(srv, reg, nil, reg, bc, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	return reg, bc, ts, signAdminToken(t, secret)
}

// Given valid baidu input, when POST then GET, then 201 and the read model
// shows credential_meta.has_refresh_token=true (full link through the fake).
func TestAccountsWrite_PostThenGetShowsCredentialMeta(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)

	status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", token, jsonBody(t, map[string]any{
		"vendor":     "baidu",
		"account_id": "mw_bak_01",
		"auth": map[string]any{
			"client_id":     "appkey-1",
			"client_secret": sentinelClientSecret,
			"refresh_token": sentinelRefreshToken,
			"redirect_uri":  "https://example.com/callback",
		},
	}))
	if status != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201 (body=%s)", status, body)
	}
	assertNoSecretLeak(t, body)
	var created createAccountResponse
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("decode 201 body: %v", err)
	}
	if created.Vendor != "baidu" || created.AccountID != "mw_bak_01" {
		t.Errorf("201 body = %+v, want baidu/mw_bak_01", created)
	}

	// Stored material carries BOTH credential + client_config (B1 split).
	stored := reg.accounts["baidu/mw_bak_01"]
	if stored.Credential.RefreshToken != sentinelRefreshToken {
		t.Errorf("stored credential.refresh_token missing")
	}
	if stored.ClientConfig.ClientID != "appkey-1" || stored.ClientConfig.ClientSecret != sentinelClientSecret {
		t.Errorf("stored client_config = %+v, want appkey-1/<sentinel>", stored.ClientConfig)
	}
	// Default rate limit from the vendor rule table (baidu 2/4/8).
	if stored.RateLimitCfg != (types.RateLimitConfig{QPS: 2, Burst: 4, ConcurrentLimit: 8}) {
		t.Errorf("stored rate_limit = %+v, want baidu default 2/4/8", stored.RateLimitCfg)
	}

	resp, list := getAccounts(t, ts, token, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	if len(list.Accounts) != 1 {
		t.Fatalf("GET accounts = %d, want 1", len(list.Accounts))
	}
	meta := list.Accounts[0].CredentialMeta
	if !meta.HasRefreshToken {
		t.Error("credential_meta.has_refresh_token = false, want true")
	}
	if !meta.HasClientSecret {
		t.Error("credential_meta.has_client_secret = false, want true")
	}
	if meta.AuthType != "oauth2" {
		t.Errorf("credential_meta.auth_type = %q, want oauth2", meta.AuthType)
	}
}

// Given B4 violations, when POST /v1/admin/accounts, then 400 with the
// documented field_errors entry.
func TestAccountsWrite_PostValidationErrors(t *testing.T) {
	_, _, ts, token := makeWriteServer(t)
	cases := []struct {
		name      string
		payload   map[string]any
		wantField string
		wantMsg   string
	}{
		{
			name: "baidu missing refresh_token",
			payload: map[string]any{
				"vendor": "baidu", "account_id": "mw_01",
				"auth": map[string]any{"client_id": "x", "client_secret": "y"},
			},
			wantField: "refresh_token",
			wantMsg:   "required",
		},
		{
			name: "onedrive missing region gets enum hint",
			payload: map[string]any{
				"vendor": "onedrive", "account_id": "mw_02",
				"auth": map[string]any{
					"client_id": "x", "client_secret": "y", "refresh_token": "z",
					"redirect_uri": "https://example.com/cb",
				},
			},
			wantField: "region",
			wantMsg:   "must be one of global|cn|us|de",
		},
		{
			name: "bad redirect_uri URL",
			payload: map[string]any{
				"vendor": "baidu", "account_id": "mw_03",
				"auth": map[string]any{
					"client_id": "x", "client_secret": "y", "refresh_token": "z",
					"redirect_uri": "not-a-url",
				},
			},
			wantField: "redirect_uri",
			wantMsg:   "must be a valid URL",
		},
		{
			name: "bad cookie key name",
			payload: map[string]any{
				"vendor": "quark", "account_id": "mw_04",
				"auth": map[string]any{"cookies": map[string]any{"token-x": "v"}},
			},
			wantField: "cookies",
			wantMsg:   "invalid cookie key",
		},
		{
			name: "bad account_id",
			payload: map[string]any{
				"vendor": "baidu", "account_id": "x",
				"auth": map[string]any{"client_id": "x", "client_secret": "y", "refresh_token": "z"},
			},
			wantField: "account_id",
			wantMsg:   "must match",
		},
		{
			name: "qps out of range",
			payload: map[string]any{
				"vendor": "baidu", "account_id": "mw_05",
				"rate_limit": map[string]any{"qps": 200, "burst": 4, "concurrent_limit": 8},
				"auth":       map[string]any{"client_id": "x", "client_secret": "y", "refresh_token": "z"},
			},
			wantField: "rate_limit.qps",
			wantMsg:   "between 0.1 and 100",
		},
		{
			name: "unknown vendor",
			payload: map[string]any{
				"vendor": "gdrive", "account_id": "mw_06",
				"auth": map[string]any{"refresh_token": "z"},
			},
			wantField: "vendor",
			wantMsg:   "must be one of",
		},
		{
			name: "missing auth",
			payload: map[string]any{
				"vendor": "baidu", "account_id": "mw_07",
			},
			wantField: "auth",
			wantMsg:   "required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", token, jsonBody(t, tc.payload))
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body=%s)", status, body)
			}
			assertNoSecretLeak(t, body)
			fe := decodeFieldErrors(t, body)
			msg, ok := fe[tc.wantField]
			if !ok {
				t.Fatalf("field_errors = %v, want key %q", fe, tc.wantField)
			}
			if !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("field_errors[%q] = %q, want substring %q", tc.wantField, msg, tc.wantMsg)
			}
		})
	}
}

// Given an existing account, when POST repeats (vendor, account_id), then
// 409 {"error":"account exists"}.
func TestAccountsWrite_PostConflict(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor:    types.VendorBaidu,
		AccountID: "mw_bak_01",
		Enabled:   true,
	})
	payload := jsonBody(t, map[string]any{
		"vendor": "baidu", "account_id": "mw_bak_01",
		"auth": map[string]any{"client_id": "x", "client_secret": "y", "refresh_token": "z"},
	})
	status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", token, payload)
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body=%s)", status, body)
	}
	assertNoSecretLeak(t, body)
	if !strings.Contains(body, `"account exists"`) {
		t.Errorf("body = %s, want error account exists", body)
	}
}

// Given a stored account, when PUT carries only {enabled:false}, then
// credentials stay byte-identical and SetEnabled is called — no broadcast.
func TestAccountsWrite_PutEnabledOnlyCredentialsUntouched(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor:       types.VendorBaidu,
		AccountID:    "mw_bak_01",
		Credential:   types.Credential{RefreshToken: sentinelRefreshToken},
		ClientConfig: types.ClientConfig{ClientID: "cid", ClientSecret: sentinelClientSecret},
		Enabled:      true,
	})
	beforeCred, beforeCC, err := reg.GetAccountSecret(context.Background(), types.VendorBaidu, "mw_bak_01")
	if err != nil {
		t.Fatalf("GetAccountSecret: %v", err)
	}

	status, body := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_bak_01", token,
		jsonBody(t, map[string]any{"enabled": false}))
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", status, body)
	}
	assertNoSecretLeak(t, body)
	if !strings.Contains(body, `"effective":"propagating"`) {
		t.Errorf("body = %s, want effective propagating", body)
	}

	afterCred, afterCC, err := reg.GetAccountSecret(context.Background(), types.VendorBaidu, "mw_bak_01")
	if err != nil {
		t.Fatalf("GetAccountSecret after: %v", err)
	}
	if afterCred.RefreshToken != beforeCred.RefreshToken || afterCC != beforeCC {
		t.Errorf("credentials changed: before %q/%+v after %q/%+v", beforeCred.RefreshToken, beforeCC, afterCred.RefreshToken, afterCC)
	}
	if len(reg.setEnabledCalls) != 1 || reg.setEnabledCalls[0] != false {
		t.Errorf("setEnabledCalls = %v, want [false]", reg.setEnabledCalls)
	}
	if reg.broadcasts != 0 {
		t.Errorf("broadcasts = %d, want 0 (no auth-material change)", reg.broadcasts)
	}
}

// Given a stored baidu account, when PUT auth carries only a new
// refresh_token, then the credential rotates, client_secret stays, and ONE
// CREDENTIAL_UPDATE broadcast fires.
func TestAccountsWrite_PutAuthRotatesRefreshTokenOnly(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor:       types.VendorBaidu,
		AccountID:    "mw_bak_01",
		Credential:   types.Credential{RefreshToken: "old-rt"},
		ClientConfig: types.ClientConfig{ClientID: "cid", ClientSecret: sentinelClientSecret},
		Enabled:      true,
	})

	status, body := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_bak_01", token,
		jsonBody(t, map[string]any{"auth": map[string]any{"refresh_token": sentinelRefreshToken}}))
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", status, body)
	}
	assertNoSecretLeak(t, body)

	cred, cc, err := reg.GetAccountSecret(context.Background(), types.VendorBaidu, "mw_bak_01")
	if err != nil {
		t.Fatalf("GetAccountSecret: %v", err)
	}
	if cred.RefreshToken != sentinelRefreshToken {
		t.Errorf("refresh_token not rotated, got %q", cred.RefreshToken)
	}
	if cc.ClientSecret != sentinelClientSecret || cc.ClientID != "cid" {
		t.Errorf("client_config = %+v, want unchanged cid/<sentinel>", cc)
	}
	if reg.credentialUpdates != 1 {
		t.Errorf("credentialUpdates = %d, want 1", reg.credentialUpdates)
	}
	if reg.clientConfigUpdates != 0 {
		t.Errorf("clientConfigUpdates = %d, want 0 (no client_config change)", reg.clientConfigUpdates)
	}
	if reg.broadcasts != 1 {
		t.Errorf("broadcasts = %d, want exactly 1 (caller-fires-once)", reg.broadcasts)
	}
}

// Given a stored 115 account, when PUT auth carries cookies, then the cookie
// map is replaced wholesale (old keys gone) with one broadcast.
func TestAccountsWrite_PutCookiesWholesaleReplacement(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor:    types.Vendor115,
		AccountID: "mw_115_01",
		Credential: types.Credential{Cookies: map[string]string{
			"UID": "old-uid", "CID": "old-cid", "SEID": "old-seid",
		}},
		Enabled: true,
	})

	status, body := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/115/mw_115_01", token,
		jsonBody(t, map[string]any{"auth": map[string]any{
			"cookies": map[string]any{"UID": sentinelCookieValue},
		}}))
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", status, body)
	}
	assertNoSecretLeak(t, body)
	if !strings.Contains(body, "CID") || !strings.Contains(body, "SEID") {
		t.Errorf("body = %s, want warning naming missing CID/SEID", body)
	}

	cred, _, err := reg.GetAccountSecret(context.Background(), types.Vendor115, "mw_115_01")
	if err != nil {
		t.Fatalf("GetAccountSecret: %v", err)
	}
	if len(cred.Cookies) != 1 || cred.Cookies["UID"] != sentinelCookieValue {
		t.Errorf("cookies = %v, want wholesale {UID:<sentinel>}", cred.Cookies)
	}
	if _, ok := cred.Cookies["CID"]; ok {
		t.Error("old CID key survived wholesale replacement")
	}
	if _, ok := cred.Cookies["SEID"]; ok {
		t.Error("old SEID key survived wholesale replacement")
	}
	if reg.broadcasts != 1 {
		t.Errorf("broadcasts = %d, want 1", reg.broadcasts)
	}
}

// Given PUT misuse, when the body is empty/absent or the path is bad or the
// account is missing, then the documented 400/404 results hold.
func TestAccountsWrite_PutEdgeCases(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor:     types.VendorBaidu,
		AccountID:  "mw_bak_01",
		Credential: types.Credential{RefreshToken: sentinelRefreshToken},
		ClientConfig: types.ClientConfig{
			ClientID: "cid", ClientSecret: sentinelClientSecret,
		},
		Enabled: true,
	})

	t.Run("empty JSON object", func(t *testing.T) {
		status, body := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_bak_01", token, jsonBody(t, map[string]any{}))
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body=%s)", status, body)
		}
		if !strings.Contains(body, "no fields to update") {
			t.Errorf("body = %s, want no fields to update", body)
		}
	})
	t.Run("no body at all", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_bak_01", token, nil)
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", status)
		}
	})
	t.Run("invalid JSON", func(t *testing.T) {
		raw := "{not json"
		status, _ := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_bak_01", token, &raw)
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", status)
		}
	})
	t.Run("primary key mismatch in body", func(t *testing.T) {
		status, body := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_bak_01", token,
			jsonBody(t, map[string]any{"vendor": "onedrive", "enabled": false}))
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body=%s)", status, body)
		}
	})
	t.Run("missing account via enabled", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/ghost_01", token,
			jsonBody(t, map[string]any{"enabled": true}))
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", status)
		}
	})
	t.Run("missing account via auth", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/ghost_01", token,
			jsonBody(t, map[string]any{"auth": map[string]any{"refresh_token": "x"}}))
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", status)
		}
	})
	t.Run("bad path vendor", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/gdrive/mw_01", token,
			jsonBody(t, map[string]any{"enabled": true}))
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", status)
		}
	})
	t.Run("bad path account_id", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/x", token,
			jsonBody(t, map[string]any{"enabled": true}))
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", status)
		}
	})
	t.Run("sensitive empty string leaves material unchanged", func(t *testing.T) {
		status, body := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_bak_01", token,
			jsonBody(t, map[string]any{"auth": map[string]any{"client_secret": ""}}))
		if status != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 (body=%s)", status, body)
		}
		_, cc, err := reg.GetAccountSecret(context.Background(), types.VendorBaidu, "mw_bak_01")
		if err != nil {
			t.Fatalf("GetAccountSecret: %v", err)
		}
		if cc.ClientSecret != sentinelClientSecret {
			t.Errorf("client_secret = %q, want unchanged sentinel", cc.ClientSecret)
		}
		if reg.broadcasts != 0 {
			t.Errorf("broadcasts = %d, want 0 (no effective change)", reg.broadcasts)
		}
	})
}

// Given a stored account, when PUT carries vendor_profile, then the PG-record
// value is written (Vendor pinned) and the response keeps the B2 note.
func TestAccountsWrite_PutVendorProfileWritesRecordWithNote(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor:    types.VendorBaidu,
		AccountID: "mw_bak_01",
		Enabled:   true,
	})
	status, body := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_bak_01", token,
		jsonBody(t, map[string]any{"vendor_profile": map[string]any{
			"weight": 3.0, "base_latency_ms": 150, "bandwidth_mbps": 90,
		}}))
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", status, body)
	}
	assertNoSecretLeak(t, body)
	if !strings.Contains(body, "节点以本地 YAML 为准") {
		t.Errorf("body = %s, want note 节点以本地 YAML 为准", body)
	}
	if len(reg.setVendorProfileCalls) != 1 {
		t.Fatalf("setVendorProfileCalls = %v, want 1 call", reg.setVendorProfileCalls)
	}
	vp := reg.setVendorProfileCalls[0]
	if vp.Vendor != types.VendorBaidu || vp.Weight != 3.0 || vp.BaseLatencyMs != 150 || vp.BandwidthMbps != 90 {
		t.Errorf("SetVendorProfile arg = %+v, want baidu/3.0/150/90", vp)
	}
}

// Given no bearer token, when POST or PUT is attempted, then 401.
func TestAccountsWrite_NoToken(t *testing.T) {
	_, _, ts, _ := makeWriteServer(t)
	status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", "", jsonBody(t, map[string]any{}))
	if status != http.StatusUnauthorized {
		t.Errorf("POST status = %d, want 401", status)
	}
	status, _ = doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/mw_01", "", jsonBody(t, map[string]any{"enabled": true}))
	if status != http.StatusUnauthorized {
		t.Errorf("PUT status = %d, want 401", status)
	}
}

// Given the full CRUD journey, when every response body is grepped, then no
// sentinel secret value appears anywhere.
func TestAccountsWrite_ZeroSecretLeakAcrossResponses(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	var bodies []string
	collect := func(status int, body string) int {
		bodies = append(bodies, body)
		return status
	}

	// 201 happy
	collect(doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", token, jsonBody(t, map[string]any{
		"vendor": "onedrive", "account_id": "mw_od_01",
		"auth": map[string]any{
			"client_id": "cid", "client_secret": sentinelClientSecret,
			"refresh_token": sentinelRefreshToken,
			"redirect_uri":  "https://example.com/cb", "region": "cn",
		},
	})))
	// 400 validation
	collect(doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", token, jsonBody(t, map[string]any{
		"vendor": "baidu", "account_id": "mw_02",
		"auth": map[string]any{"client_id": "x", "client_secret": sentinelClientSecret},
	})))
	// 409 conflict
	collect(doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", token, jsonBody(t, map[string]any{
		"vendor": "onedrive", "account_id": "mw_od_01",
		"auth": map[string]any{
			"client_id": "cid", "client_secret": sentinelClientSecret,
			"refresh_token": sentinelRefreshToken,
			"redirect_uri":  "https://example.com/cb", "region": "cn",
		},
	})))
	// 202 auth rotate
	collect(doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/onedrive/mw_od_01", token,
		jsonBody(t, map[string]any{"auth": map[string]any{"refresh_token": "rotated-" + sentinelRefreshToken}})))
	// 400 put validation
	collect(doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/onedrive/mw_od_01", token,
		jsonBody(t, map[string]any{"rate_limit": map[string]any{"qps": 0, "burst": 1, "concurrent_limit": 1}})))
	// 404 put missing
	collect(doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/onedrive/ghost_01", token,
		jsonBody(t, map[string]any{"auth": map[string]any{"refresh_token": sentinelRefreshToken}})))
	// 200 list read
	status, listBody := doRaw(t, ts, http.MethodGet, "/v1/admin/accounts", token, nil)
	if status != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", status)
	}
	bodies = append(bodies, listBody)

	if len(bodies) != 7 {
		t.Fatalf("collected %d bodies, want 7", len(bodies))
	}
	for i, body := range bodies {
		assertNoSecretLeak(t, body)
		if strings.Contains(body, "rotated-") {
			t.Errorf("body[%d] leaks rotated refresh token: %s", i, body)
		}
	}
	_ = reg
}

// ─── Operations (todo 27: rotate / ban / unban / circuit) ─────────────────

// Given a stored baidu account, when POST .../rotate carries a new auth field
// set, then the credential rotates through the SAME ApplyAuthPatch path as
// PUT and exactly ONE CREDENTIAL_UPDATE fires with the new material in the
// payload.
func TestAccountOps_RotateBroadcastsNewCredential(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor:       types.VendorBaidu,
		AccountID:    "mw_bak_01",
		Credential:   types.Credential{RefreshToken: "old-rt"},
		ClientConfig: types.ClientConfig{ClientID: "cid", ClientSecret: sentinelClientSecret},
		Enabled:      true,
	})

	status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/rotate", token,
		jsonBody(t, map[string]any{"refresh_token": sentinelRefreshToken, "client_id": "cid-2"}))
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", status, body)
	}
	assertNoSecretLeak(t, body)
	if !strings.Contains(body, `"effective":"propagating"`) {
		t.Errorf("body = %s, want effective propagating", body)
	}
	if reg.broadcasts != 1 {
		t.Fatalf("broadcasts = %d, want exactly 1 CREDENTIAL_UPDATE", reg.broadcasts)
	}
	p := reg.lastCredPayload
	if p.Vendor != types.VendorBaidu || p.AccountID != "mw_bak_01" {
		t.Errorf("payload id = %s/%s, want baidu/mw_bak_01", p.Vendor, p.AccountID)
	}
	if p.Credential.RefreshToken != sentinelRefreshToken {
		t.Errorf("payload credential.refresh_token not rotated")
	}
	if p.ClientConfig.ClientID != "cid-2" || p.ClientConfig.ClientSecret != sentinelClientSecret {
		t.Errorf("payload client_config = %+v, want cid-2/<sentinel>", p.ClientConfig)
	}
	// Stored material agrees (ApplyAuthPatch wrote through the same path).
	cred, cc, err := reg.GetAccountSecret(context.Background(), types.VendorBaidu, "mw_bak_01")
	if err != nil {
		t.Fatalf("GetAccountSecret: %v", err)
	}
	if cred.RefreshToken != sentinelRefreshToken || cc.ClientID != "cid-2" {
		t.Errorf("stored material = %q/%+v, want rotated", cred.RefreshToken, cc)
	}
}

// Given rotate misuse, when the body is empty/absent or fails B4 validation
// or the account is missing, then the documented 400/404 results hold.
func TestAccountOps_RotateEdgeCases(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor:       types.VendorBaidu,
		AccountID:    "mw_bak_01",
		Credential:   types.Credential{RefreshToken: sentinelRefreshToken},
		ClientConfig: types.ClientConfig{ClientID: "cid", ClientSecret: sentinelClientSecret},
		Enabled:      true,
	})

	t.Run("empty JSON object", func(t *testing.T) {
		status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/rotate", token, jsonBody(t, map[string]any{}))
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body=%s)", status, body)
		}
		fe := decodeFieldErrors(t, body)
		if fe["auth"] != "required" {
			t.Errorf("field_errors = %v, want auth required", fe)
		}
	})
	t.Run("no body at all", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/rotate", token, nil)
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", status)
		}
	})
	t.Run("B4 validation failure", func(t *testing.T) {
		status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/rotate", token,
			jsonBody(t, map[string]any{"redirect_uri": "not-a-url"}))
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body=%s)", status, body)
		}
		assertNoSecretLeak(t, body)
		fe := decodeFieldErrors(t, body)
		if !strings.Contains(fe["redirect_uri"], "valid URL") {
			t.Errorf("field_errors = %v, want redirect_uri URL error", fe)
		}
		if reg.broadcasts != 0 {
			t.Errorf("broadcasts = %d, want 0 (validation failed before write)", reg.broadcasts)
		}
	})
	t.Run("missing account", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/ghost_01/rotate", token,
			jsonBody(t, map[string]any{"refresh_token": "x"}))
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", status)
		}
	})
}

// Given a stored account, when POST .../ban carries only {reason}, then
// registry.Ban fires with ban_until defaulting to +24h and the response is
// 202 propagating (no second confirmation — that is the UI's job).
func TestAccountOps_BanDefaultsTo24h(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor: types.VendorBaidu, AccountID: "mw_bak_01", Enabled: true,
	})
	before := time.Now()
	status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/ban", token,
		jsonBody(t, map[string]any{"reason": "vendor throttling"}))
	after := time.Now()
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", status, body)
	}
	if !strings.Contains(body, `"effective":"propagating"`) {
		t.Errorf("body = %s, want effective propagating", body)
	}
	if len(reg.banCalls) != 1 {
		t.Fatalf("banCalls = %d, want 1", len(reg.banCalls))
	}
	call := reg.banCalls[0]
	if call.vendor != types.VendorBaidu || call.accountID != "mw_bak_01" {
		t.Errorf("ban target = %s/%s, want baidu/mw_bak_01", call.vendor, call.accountID)
	}
	if call.reason != "vendor throttling" {
		t.Errorf("ban reason = %q, want vendor throttling", call.reason)
	}
	lo := before.Add(defaultBanDuration)
	hi := after.Add(defaultBanDuration)
	if call.banUntil.Before(lo) || call.banUntil.After(hi) {
		t.Errorf("banUntil = %v, want within [%v, %v] (default +24h)", call.banUntil, lo, hi)
	}
}

// Given explicit ban inputs, when POST .../ban carries ban_until or an
// invalid value or an empty body, then the documented parse/default/400
// behaviors hold.
func TestAccountOps_BanExplicitAndEdgeCases(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor: types.VendorBaidu, AccountID: "mw_bak_01", Enabled: true,
	})

	t.Run("explicit ban_until honored", func(t *testing.T) {
		want := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/ban", token,
			jsonBody(t, map[string]any{"reason": "abuse", "ban_until": want.Format(time.RFC3339)}))
		if status != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 (body=%s)", status, body)
		}
		call := reg.banCalls[len(reg.banCalls)-1]
		if !call.banUntil.Equal(want) {
			t.Errorf("banUntil = %v, want %v", call.banUntil, want)
		}
	})
	t.Run("empty body uses all defaults", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/ban", token, nil)
		if status != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", status)
		}
		call := reg.banCalls[len(reg.banCalls)-1]
		if call.reason != "" {
			t.Errorf("reason = %q, want empty", call.reason)
		}
	})
	t.Run("bad ban_until is 400", func(t *testing.T) {
		status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/ban", token,
			jsonBody(t, map[string]any{"ban_until": "tomorrow"}))
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body=%s)", status, body)
		}
		fe := decodeFieldErrors(t, body)
		if !strings.Contains(fe["ban_until"], "RFC3339") {
			t.Errorf("field_errors = %v, want ban_until RFC3339 hint", fe)
		}
	})
	t.Run("registry error is 500", func(t *testing.T) {
		reg.banErr = errors.New("pg down")
		defer func() { reg.banErr = nil }()
		status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/ban", token,
			jsonBody(t, map[string]any{}))
		if status != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", status)
		}
	})
}

// Given a stored account, when POST .../unban fires, then registry.Unban is
// called with the exact vendor/account_id and the response is 202.
func TestAccountOps_Unban(t *testing.T) {
	reg, _, ts, token := makeWriteServer(t)
	seedAccount(t, reg, accountregistry.AccountInfo{
		Vendor: types.Vendor115, AccountID: "mw_115_01", Enabled: true,
	})
	status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/115/mw_115_01/unban", token, nil)
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", status, body)
	}
	if !strings.Contains(body, `"effective":"propagating"`) {
		t.Errorf("body = %s, want effective propagating", body)
	}
	if len(reg.unbanCalls) != 1 || reg.unbanCalls[0] != "115/mw_115_01" {
		t.Errorf("unbanCalls = %v, want [115/mw_115_01]", reg.unbanCalls)
	}
}

// Given a wired broadcaster, when POST .../circuit carries force_open or
// force_close, then exactly one matching event with the correct CircuitPayload
// is broadcast and the response is 202 (event emitted; node applies it).
func TestAccountOps_CircuitBroadcastsEvent(t *testing.T) {
	_, bc, ts, token := makeWriteServer(t)
	cases := []struct {
		action    string
		wantEvent string
	}{
		{"force_open", types.EventCircuitForceOpen},
		{"force_close", types.EventCircuitForceClose},
	}
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/quark/mw_qk_01/circuit", token,
				jsonBody(t, map[string]any{"action": tc.action}))
			if status != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 (body=%s)", status, body)
			}
			if !strings.Contains(body, `"effective":"propagating"`) {
				t.Errorf("body = %s, want effective propagating", body)
			}
			if len(bc.eventTypes) != 1 {
				t.Fatalf("broadcasts = %d, want 1", len(bc.eventTypes))
			}
			if bc.eventTypes[0] != tc.wantEvent {
				t.Errorf("eventType = %q, want %q", bc.eventTypes[0], tc.wantEvent)
			}
			payload, ok := bc.payloads[0].(types.CircuitPayload)
			if !ok {
				t.Fatalf("payload type = %T, want types.CircuitPayload", bc.payloads[0])
			}
			if payload.Vendor != types.VendorQuark || payload.AccountID != "mw_qk_01" {
				t.Errorf("payload = %+v, want quark/mw_qk_01", payload)
			}
			bc.eventTypes, bc.payloads = nil, nil
		})
	}
}

// Given circuit misuse, when the action is unknown or the broadcaster fails,
// then 400/500 hold; circuit never touches account_health (no Ban/Unban call).
func TestAccountOps_CircuitEdgeCases(t *testing.T) {
	reg, bc, ts, token := makeWriteServer(t)

	t.Run("unknown action is 400", func(t *testing.T) {
		status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/quark/mw_qk_01/circuit", token,
			jsonBody(t, map[string]any{"action": "trip"}))
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body=%s)", status, body)
		}
		fe := decodeFieldErrors(t, body)
		if !strings.Contains(fe["action"], "force_open|force_close") {
			t.Errorf("field_errors = %v, want action enum hint", fe)
		}
		if len(bc.eventTypes) != 0 {
			t.Errorf("broadcasts = %d, want 0 for bad action", len(bc.eventTypes))
		}
	})
	t.Run("broadcast failure is 500", func(t *testing.T) {
		bc.err = errors.New("no peers")
		defer func() { bc.err = nil }()
		status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/quark/mw_qk_01/circuit", token,
			jsonBody(t, map[string]any{"action": "force_open"}))
		if status != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", status)
		}
	})
	t.Run("no account_health write", func(t *testing.T) {
		status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/quark/mw_qk_01/circuit", token,
			jsonBody(t, map[string]any{"action": "force_close"}))
		if status != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", status)
		}
		if len(reg.banCalls) != 0 || len(reg.unbanCalls) != 0 {
			t.Errorf("circuit touched registry health: bans=%v unbans=%v", reg.banCalls, reg.unbanCalls)
		}
	})
}

// Given a server built WITHOUT a broadcaster (nil), when POST .../circuit is
// attempted, then 500 is returned without panic.
func TestAccountOps_CircuitNilBroadcasterIs500(t *testing.T) {
	reg := newFakeAccountRegistry()
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterAccountsRoutes(srv, reg, nil, reg, nil, nil)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	status, body := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/quark/mw_qk_01/circuit", token,
		jsonBody(t, map[string]any{"action": "force_open"}))
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body=%s)", status, body)
	}
}

// ─── Vendor profiles (todo 37: read-only) ──────────────────────────────────

// mockVendorProfilesReader is a mock VendorProfilesReader.
type mockVendorProfilesReader struct {
	rows []metadata.VendorProfileRow
	err  error
}

func (m *mockVendorProfilesReader) ListVendorProfiles(_ context.Context) ([]metadata.VendorProfileRow, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.rows, nil
}

func makeVendorProfilesServer(vpr VendorProfilesReader) *Server {
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterAccountsRoutes(srv, nil, vpr, nil, nil, nil)
	return srv
}

func getVendorProfiles(t *testing.T, ts *httptest.Server, token string) (*http.Response, *vendorProfilesResponse) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/vendor-profiles", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/admin/vendor-profiles: %v", err)
	}
	defer resp.Body.Close()

	var body vendorProfilesResponse
	if resp.StatusCode == http.StatusOK && resp.Body != nil {
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode vendor profiles response: %v", err)
		}
	}
	return resp, &body
}

// Given three vendor profiles, when GET /v1/admin/vendor-profiles is called
// with a valid token, then all profiles are returned with read_only=true and
// the expected note.
func TestVendorProfiles_Happy(t *testing.T) {
	mock := &mockVendorProfilesReader{
		rows: []metadata.VendorProfileRow{
			{Vendor: "baidu", VendorProfile: types.VendorProfile{Vendor: "baidu", Weight: 3.0, BaseLatencyMs: 120, BandwidthMbps: 80}},
			{Vendor: "115", VendorProfile: types.VendorProfile{Vendor: "115", Weight: 5.0, BaseLatencyMs: 50, BandwidthMbps: 200}},
			{Vendor: "quark", VendorProfile: types.VendorProfile{Vendor: "quark", Weight: 4.0, BaseLatencyMs: 90, BandwidthMbps: 120}},
		},
	}

	srv := makeVendorProfilesServer(mock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, []byte("test-secret-key-for-admin-tokens"))

	resp, body := getVendorProfiles(t, ts, token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body.Profiles) != 3 {
		t.Fatalf("len(profiles) = %d, want 3", len(body.Profiles))
	}
	if !body.ReadOnly {
		t.Error("read_only = false, want true")
	}
	if body.Note != "节点以本地 YAML 为准；CP 改动不传播（v1 只读）" {
		t.Errorf("note = %q, want 节点以本地 YAML 为准；CP 改动不传播（v1 只读）", body.Note)
	}

	// Verify first profile
	p0 := body.Profiles[0]
	if p0.Vendor != "baidu" || p0.Weight != 3.0 || p0.BaseLatencyMs != 120 || p0.BandwidthMbps != 80 {
		t.Errorf("profile[0] = %+v, want baidu/3.0/120/80", p0)
	}
}

// Given an empty result set, when GET /v1/admin/vendor-profiles, then
// profiles is [] not null.
func TestVendorProfiles_Empty(t *testing.T) {
	mock := &mockVendorProfilesReader{rows: []metadata.VendorProfileRow{}}

	srv := makeVendorProfilesServer(mock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, []byte("test-secret-key-for-admin-tokens"))

	resp, body := getVendorProfiles(t, ts, token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if body.Profiles == nil {
		t.Error("profiles = nil, want [] (not null)")
	}
	if len(body.Profiles) != 0 {
		t.Errorf("len(profiles) = %d, want 0", len(body.Profiles))
	}
	if !body.ReadOnly {
		t.Error("read_only = false, want true")
	}
}

// Given no bearer token, when GET /v1/admin/vendor-profiles, then 401.
func TestVendorProfiles_NoToken(t *testing.T) {
	mock := &mockVendorProfilesReader{rows: []metadata.VendorProfileRow{}}
	srv := makeVendorProfilesServer(mock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	resp, _ := getVendorProfiles(t, ts, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// Given a PUT request to /v1/admin/vendor-profiles, then 405 is returned
// (Go 1.22+ mux default for unregistered methods).
func TestVendorProfiles_PutReturns405(t *testing.T) {
	mock := &mockVendorProfilesReader{rows: []metadata.VendorProfileRow{}}
	srv := makeVendorProfilesServer(mock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, []byte("test-secret-key-for-admin-tokens"))

	status, _ := doRaw(t, ts, http.MethodPut, "/v1/admin/vendor-profiles", token, nil)
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", status)
	}
}

// Given a mock error, when GET /v1/admin/vendor-profiles, then 500.
func TestVendorProfiles_StoreError(t *testing.T) {
	mock := &mockVendorProfilesReader{err: errors.New("pg down")}
	srv := makeVendorProfilesServer(mock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, []byte("test-secret-key-for-admin-tokens"))

	resp, _ := getVendorProfiles(t, ts, token)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}

// Given the response JSON, verify it contains the required fields.
func TestVendorProfiles_ResponseShape(t *testing.T) {
	mock := &mockVendorProfilesReader{
		rows: []metadata.VendorProfileRow{
			{Vendor: "baidu", VendorProfile: types.VendorProfile{Vendor: "baidu", Weight: 3.0, BaseLatencyMs: 120, BandwidthMbps: 80}},
		},
	}
	srv := makeVendorProfilesServer(mock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, []byte("test-secret-key-for-admin-tokens"))

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/vendor-profiles", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	body := string(raw)

	// Must contain the key fields
	for _, key := range []string{`"profiles"`, `"read_only"`, `"note"`, `"vendor"`, `"weight"`, `"base_latency_ms"`, `"bandwidth_mbps"`} {
		if !strings.Contains(body, key) {
			t.Errorf("response missing key %q: %s", key, body)
		}
	}
	// Must NOT contain credential material
	if strings.Contains(body, `"credential"`) {
		t.Errorf("response contains '\"credential\"' — material leaked")
	}
}
