package accountpool

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/types"
)

// tokenCall records one token-endpoint request observed by stubTransport.
type tokenCall struct {
	URL  string
	Form url.Values
}

// stubTransport intercepts TokenManager refresh POSTs. refreshBody is the
// canned JSON returned to every request.
type stubTransport struct {
	mu          sync.Mutex
	calls       []tokenCall
	refreshBody string
}

func (s *stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.calls = append(s.calls, tokenCall{URL: req.URL.String(), Form: form})
	s.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(s.refreshBody)),
	}, nil
}

func (s *stubTransport) callFor(host string) *tokenCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.calls {
		if strings.Contains(s.calls[i].URL, host) {
			return &s.calls[i]
		}
	}
	return nil
}

func snapshotFixture() []types.AccountSnapshotEntry {
	return []types.AccountSnapshotEntry{
		{
			Vendor:    types.VendorBaidu,
			AccountID: "bd_01",
			Credential: types.Credential{
				RefreshToken: "rt-baidu",
			},
			ClientConfig: types.ClientConfig{
				ClientID:     "cid-baidu",
				ClientSecret: "cs-baidu",
			},
			RateLimitCfg:  types.RateLimitConfig{QPS: 3, Burst: 6, ConcurrentLimit: 9},
			VendorProfile: types.VendorProfile{Vendor: types.VendorBaidu, Weight: 4.5},
			Enabled:       true,
		},
		{
			Vendor:    types.VendorOneDrive,
			AccountID: "od_01",
			Credential: types.Credential{
				RefreshToken: "rt-od",
			},
			ClientConfig: types.ClientConfig{
				ClientID:     "cid-od",
				ClientSecret: "cs-od",
				RedirectURI:  "https://login.example/od",
				Region:       "cn",
			},
			Enabled: true,
		},
		{
			Vendor:    types.Vendor115,
			AccountID: "ck_01",
			Credential: types.Credential{
				Cookies: map[string]string{"UID": "123"},
			},
			Enabled: true,
		},
	}
}

func accountByKey(pool *AccountPool, key string) *Account {
	for _, a := range pool.SnapshotAccounts() {
		if string(a.Vendor)+":"+a.AccountID == key {
			return a
		}
	}
	return nil
}

// Given a snapshot with baidu (full ClientConfig), onedrive (region=cn) and
// 115 (cookies), When BuildFromSnapshot runs, Then baidu+onedrive enter the
// pool with correct assembly and 115 is skipped with a Warn.
func TestBuildFromSnapshot(t *testing.T) {
	stub := &stubTransport{refreshBody: `{"access_token":"at-fresh","refresh_token":"rt-new","expires_in":3600}`}
	tokenMgr := auth.NewTokenManager(&http.Client{Transport: stub})

	var logBuf bytes.Buffer
	restore := swapSlogHandler(&logBuf)
	defer restore()

	pool := buildFromSnapshot(snapshotFixture(), nil, tokenMgr)

	// 115 skipped with Warn; baidu+onedrive added.
	if !strings.Contains(logBuf.String(), "ck_01") {
		t.Errorf("expected skip Warn for 115 account, logs: %s", logBuf.String())
	}
	if got := len(pool.SnapshotAccounts()); got != 2 {
		t.Fatalf("pool size = %d, want 2 (baidu+onedrive, 115 skipped)", got)
	}

	bd := accountByKey(pool, "baidu:bd_01")
	if bd == nil {
		t.Fatal("baidu:bd_01 not in pool")
	}
	if h := bd.Health.Load().(types.HealthState); h.State != "healthy" {
		t.Errorf("baidu initial health = %q, want healthy", h.State)
	}
	if bd.VendorWeight != 4.5 {
		t.Errorf("baidu VendorWeight = %v, want 4.5", bd.VendorWeight)
	}
	lim, ok := bd.Limiter.(*rate.Limiter)
	if !ok {
		t.Fatalf("baidu limiter type = %T, want *rate.Limiter", bd.Limiter)
	}
	if lim.Limit() != 3 || lim.Burst() != 6 {
		t.Errorf("baidu limiter = (%v, %v), want (3, 6) from entry.RateLimitCfg", lim.Limit(), lim.Burst())
	}
	if bd.CB == nil || bd.CB.State() != StateClosed {
		t.Errorf("baidu CB state = %v, want closed", bd.CB)
	}

	od := accountByKey(pool, "onedrive:od_01")
	if od == nil {
		t.Fatal("onedrive:od_01 not in pool")
	}
	// Zero RateLimitCfg falls back to the onedrive driver default (10/20).
	odLim, ok := od.Limiter.(*rate.Limiter)
	if !ok {
		t.Fatalf("onedrive limiter type = %T, want *rate.Limiter", od.Limiter)
	}
	if odLim.Limit() != 10 || odLim.Burst() != 20 {
		t.Errorf("onedrive limiter = (%v, %v), want driver default (10, 20)", odLim.Limit(), odLim.Burst())
	}
	// Zero VendorProfile.Weight falls back to 2.0.
	if od.VendorWeight != 2.0 {
		t.Errorf("onedrive VendorWeight = %v, want 2.0 fallback", od.VendorWeight)
	}

	// Forced first refresh carries the OAuth2Config four elements.
	if _, err := tokenMgr.GetAccessToken(types.VendorBaidu, "bd_01"); err != nil {
		t.Fatalf("baidu first refresh: %v", err)
	}
	bdCall := stub.callFor("openapi.baidu.com")
	if bdCall == nil {
		t.Fatal("no token request observed for baidu")
	}
	wantForm := map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": "rt-baidu",
		"client_id":     "cid-baidu",
		"client_secret": "cs-baidu",
	}
	for k, want := range wantForm {
		if got := bdCall.Form.Get(k); got != want {
			t.Errorf("baidu token form %s = %q, want %q", k, got, want)
		}
	}
	if _, has := bdCall.Form["redirect_uri"]; has {
		t.Errorf("baidu token form must not carry redirect_uri, got %v", bdCall.Form)
	}

	if _, err := tokenMgr.GetAccessToken(types.VendorOneDrive, "od_01"); err != nil {
		t.Fatalf("onedrive first refresh: %v", err)
	}
	odCall := stub.callFor("login.partner.microsoftonline.cn")
	if odCall == nil {
		t.Fatalf("no token request observed for onedrive cn host, calls: %+v", stub.calls)
	}
	wantFormOD := map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": "rt-od",
		"client_id":     "cid-od",
		"client_secret": "cs-od",
		"redirect_uri":  "https://login.example/od",
	}
	for k, want := range wantFormOD {
		if got := odCall.Form.Get(k); got != want {
			t.Errorf("onedrive token form %s = %q, want %q", k, got, want)
		}
	}
}

// Given a snapshot whose baidu account lacks client_secret, When
// BuildFromSnapshot runs, Then the account still enters the pool (Warn, no
// panic) and the first-refresh failure degrades health via the driver
// health-check path, excluding it from selection.
func TestBuildFromSnapshot_missingClientSecret_degradesOnFirstRefresh(t *testing.T) {
	stub := &stubTransport{refreshBody: `{"error":"invalid_client","error_description":"client_secret is empty"}`}
	tokenMgr := auth.NewTokenManager(&http.Client{Transport: stub})

	var logBuf bytes.Buffer
	restore := swapSlogHandler(&logBuf)
	defer restore()

	entries := []types.AccountSnapshotEntry{
		{
			Vendor:     types.VendorBaidu,
			AccountID:  "bd_bad",
			Credential: types.Credential{RefreshToken: "rt-baidu"},
			ClientConfig: types.ClientConfig{
				ClientID: "cid-baidu",
				// ClientSecret missing
			},
			Enabled: true,
		},
	}
	pool := buildFromSnapshot(entries, nil, tokenMgr)

	bd := accountByKey(pool, "baidu:bd_bad")
	if bd == nil {
		t.Fatal("baidu:bd_bad must still enter the pool")
	}
	if h := bd.Health.Load().(types.HealthState); h.State != "healthy" {
		t.Errorf("initial health = %q, want healthy", h.State)
	}
	if !strings.Contains(logBuf.String(), "missing OAuth2 client material") {
		t.Errorf("expected Warn about missing client material, logs: %s", logBuf.String())
	}

	// First refresh fails (forced by zero ExpiresAt) → driver HealthCheck
	// reports degraded without panicking.
	h := bd.Driver.HealthCheck(context.Background())
	if h.State != "degraded" {
		t.Fatalf("HealthCheck after failed refresh = %q, want degraded (err %q)", h.State, h.ErrorMsg)
	}

	// Degraded is informational (no taint): SelectK still selects the account.
	pool.UpdateHealth("baidu:bd_bad", h)
	if _, err := pool.SelectK(context.Background(), 1); err != nil {
		t.Errorf("SelectK with degraded account must succeed: %v", err)
	}

	// Banned is a scheduling taint: SelectK excludes the account.
	pool.UpdateHealth("baidu:bd_bad", types.HealthState{State: "banned"})
	if _, err := pool.SelectK(context.Background(), 1); err == nil {
		t.Error("SelectK after ban must fail (all accounts tainted), got nil error")
	}
}

func swapSlogHandler(buf *bytes.Buffer) func() {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	return func() { slog.SetDefault(prev) }
}
