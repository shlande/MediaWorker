package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

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

func makeServer(mc AdminAccountsReader) *Server {
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterAccountsRoutes(srv, mc)
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
	srv := makeServer(mock)
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
	srv := makeServer(mock)
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
	srv := makeServer(mock)
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
	srv := makeServer(mock)
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
	srv := makeServer(mock)
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

	srv := makeServer(mock)
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
	srv := makeServer(mock)
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
	srv := makeServer(mock)
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
