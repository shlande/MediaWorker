package auth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Helpers ──────────────────────────────────────────────────────────

// authHandler is an httptest.Server handler that returns a fixed token response
// and counts calls.
type authHandler struct {
	mu         sync.Mutex
	callCount  int
	statusCode int
	errBody    string
}

func (h *authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.callCount++
	h.mu.Unlock()

	if h.statusCode == 0 {
		h.statusCode = http.StatusOK
	}

	if h.errBody != "" {
		w.WriteHeader(h.statusCode)
		_, _ = w.Write([]byte(h.errBody))
		return
	}

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if r.FormValue("grant_type") != "refresh_token" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"unsupported_grant_type"}`))
		return
	}

	resp := map[string]any{
		"access_token":  fmt.Sprintf("new_token_%d", h.callCount),
		"refresh_token": fmt.Sprintf("new_refresh_%d", h.callCount),
		"expires_in":    3600,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *authHandler) Calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.callCount
}

// ─── Tests ────────────────────────────────────────────────────────────

func TestGetAccessToken_FirstCallRefreshes(t *testing.T) {
	h := &authHandler{}
	srv := httptest.NewServer(h)
	defer srv.Close()

	tm := NewTokenManager(nil)
	tm.Register(types.VendorBaidu, "acc1", OAuth2Config{
		ClientID:     "cid",
		ClientSecret: "cs",
		RefreshToken: "rt1",
		TokenURL:     srv.URL,
	})

	token, err := tm.GetAccessToken(types.VendorBaidu, "acc1")
	if err != nil {
		t.Fatalf("GetAccessToken failed: %v", err)
	}
	if token != "new_token_1" {
		t.Fatalf("expected new_token_1, got %s", token)
	}
	if h.Calls() != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", h.Calls())
	}
}

func TestGetAccessToken_ReturnsCached(t *testing.T) {
	h := &authHandler{}
	srv := httptest.NewServer(h)
	defer srv.Close()

	tm := NewTokenManager(srv.Client())

	// Force-register a non-expired token directly so no HTTP call needed.
	key := "baidu:acc1"
	tm.mu.Lock()
	tm.states[key] = &registeredEntry{
		Config: OAuth2Config{RefreshToken: "rt1"},
		State: &TokenState{
			AccessToken:  "cached_token",
			RefreshToken: "rt1",
			ExpiresAt:    time.Now().Add(10 * time.Minute),
		},
	}
	tm.mu.Unlock()

	token, err := tm.GetAccessToken(types.VendorBaidu, "acc1")
	if err != nil {
		t.Fatalf("GetAccessToken failed: %v", err)
	}
	if token != "cached_token" {
		t.Fatalf("expected cached_token, got %s", token)
	}
	if h.Calls() != 0 {
		t.Fatalf("expected 0 HTTP calls (cached), got %d", h.Calls())
	}
}

func TestGetAccessToken_RefreshesAfterExpiry(t *testing.T) {
	h := &authHandler{}
	srv := httptest.NewServer(h)
	defer srv.Close()

	tm := NewTokenManager(srv.Client())

	// Token expires in 1 minute (less than 5 minute buffer) → should refresh.
	key := "baidu:acc1"
	tm.mu.Lock()
	tm.states[key] = &registeredEntry{
		Config: OAuth2Config{
			ClientID:     "cid",
			ClientSecret: "cs",
			RefreshToken: "rt1",
			TokenURL:     srv.URL,
		},
		State: &TokenState{
			AccessToken:  "stale_token",
			RefreshToken: "rt1",
			ExpiresAt:    time.Now().Add(1 * time.Minute),
		},
	}
	tm.mu.Unlock()

	token, err := tm.GetAccessToken(types.VendorBaidu, "acc1")
	if err != nil {
		t.Fatalf("GetAccessToken failed: %v", err)
	}
	if token != "new_token_1" {
		t.Fatalf("expected new_token_1, got %s", token)
	}
	if h.Calls() != 1 {
		t.Fatalf("expected 1 HTTP call after expiry, got %d", h.Calls())
	}
}

func TestGetAccessToken_ConcurrentDedup(t *testing.T) {
	h := &authHandler{}
	srv := httptest.NewServer(h)
	defer srv.Close()

	tm := NewTokenManager(srv.Client())
	tm.Register(types.VendorBaidu, "acc1", OAuth2Config{
		ClientID:     "cid",
		ClientSecret: "cs",
		RefreshToken: "rt1",
		TokenURL:     srv.URL,
	})

	var wg sync.WaitGroup
	const concurrency = 10
	results := make([]string, concurrency)
	errs := make([]error, concurrency)

	for i := range concurrency {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tok, err := tm.GetAccessToken(types.VendorBaidu, "acc1")
			results[idx] = tok
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: GetAccessToken error: %v", i, err)
		}
		if results[i] != "new_token_1" {
			t.Errorf("goroutine %d: expected new_token_1, got %s", i, results[i])
		}
	}

	// Exactly 1 HTTP call despite 10 concurrent goroutines.
	if h.Calls() != 1 {
		t.Fatalf("expected 1 HTTP call (singleflight dedup), got %d", h.Calls())
	}
}

func TestGetAccessToken_RefreshTokenFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"token expired"}`))
	}))
	defer srv.Close()

	tm := NewTokenManager(srv.Client())
	tm.Register(types.VendorBaidu, "acc1", OAuth2Config{
		ClientID:     "cid",
		ClientSecret: "cs",
		RefreshToken: "bad_rt",
		TokenURL:     srv.URL,
	})

	_, err := tm.GetAccessToken(types.VendorBaidu, "acc1")
	if err == nil {
		t.Fatal("expected error from invalid refresh_token, got nil")
	}
}

func TestGetAccessToken_DifferentKeysDontDedup(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls := callCount.Add(1)
		resp := map[string]any{
			"access_token":  fmt.Sprintf("token_%d", calls),
			"refresh_token": fmt.Sprintf("rt_%d", calls),
			"expires_in":    3600,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	tm := NewTokenManager(srv.Client())
	tm.Register(types.VendorBaidu, "acc1", OAuth2Config{
		ClientID: "cid1", ClientSecret: "cs1", RefreshToken: "rt1", TokenURL: srv.URL,
	})
	tm.Register(types.VendorBaidu, "acc2", OAuth2Config{
		ClientID: "cid2", ClientSecret: "cs2", RefreshToken: "rt2", TokenURL: srv.URL,
	})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = tm.GetAccessToken(types.VendorBaidu, "acc1")
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = tm.GetAccessToken(types.VendorBaidu, "acc2")
		}()
	}
	wg.Wait()

	if n := callCount.Load(); n != 2 {
		t.Fatalf("expected 2 HTTP calls (one per key), got %d", n)
	}
}

func TestNewTokenManager_NilClient(t *testing.T) {
	tm := NewTokenManager(nil)
	if tm.httpc != http.DefaultClient {
		t.Fatal("expected http.DefaultClient when nil passed")
	}
}

func TestOneDriveTokenURL(t *testing.T) {
	tests := []struct {
		region string
		want   string
	}{
		{"global", "https://login.microsoftonline.com/common/oauth2/v2.0/token"},
		{"cn", "https://login.partner.microsoftonline.cn/common/oauth2/v2.0/token"},
		{"us", "https://login.microsoftonline.us/common/oauth2/v2.0/token"},
		{"de", "https://login.microsoftonline.de/common/oauth2/v2.0/token"},
		{"unknown", "https://login.microsoftonline.com/common/oauth2/v2.0/token"},
	}
	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			got := OneDriveTokenURL(tt.region)
			if got != tt.want {
				t.Fatalf("OneDriveTokenURL(%q) = %s, want %s", tt.region, got, tt.want)
			}
		})
	}
}

func TestRegister_Overwrites(t *testing.T) {
	tm := NewTokenManager(nil)
	tm.Register(types.VendorBaidu, "acc1", OAuth2Config{
		ClientID: "old", RefreshToken: "old_rt",
	})
	tm.Register(types.VendorBaidu, "acc1", OAuth2Config{
		ClientID: "new", RefreshToken: "new_rt",
	})

	key := "baidu:acc1"
	tm.mu.Lock()
	entry, ok := tm.states[key]
	tm.mu.Unlock()
	if !ok {
		t.Fatal("expected entry to exist after overwrite")
	}
	if entry.Config.ClientID != "new" {
		t.Fatalf("expected Config.ClientID=new, got %s", entry.Config.ClientID)
	}
	if entry.State.RefreshToken != "new_rt" {
		t.Fatalf("expected State.RefreshToken=new_rt, got %s", entry.State.RefreshToken)
	}
}

func TestGetAccessToken_NoRegistration(t *testing.T) {
	tm := NewTokenManager(nil)
	_, err := tm.GetAccessToken(types.VendorBaidu, "nonexistent")
	if err == nil {
		t.Fatal("expected error for unregistered key")
	}
}
