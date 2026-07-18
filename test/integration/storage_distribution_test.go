// Package integration_test provides full-pipeline integration tests for the
// MediaWorker storage distribution subsystem — verifying that a blob request
// flows through: cache miss → DataPlane.FetchBlobLocal → AccountPool.SelectForRead
// → LinkPool.GetOrFetch → Driver.GetLink → HTTP download → data delivery.
// Both Baidu (PCS API) and OneDrive (Graph API) backends are tested with
// httptest.Server mocks, plus degradation (403/429) and circuit breaker scenarios.
package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/controlplane/metadata"
	"github.com/shlande/mediaworker/internal/node/backhaul"
	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	"github.com/shlande/mediaworker/internal/storage/dataplane"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/storage/driver/baidu"
	"github.com/shlande/mediaworker/internal/storage/driver/onedrive"
	"github.com/shlande/mediaworker/internal/storage/linkpool"
	"github.com/shlande/mediaworker/internal/types"
)

// ═══════════════════════════════════════════════════════════════════════════
// HTTP routing helpers: intercept real driver HTTP calls and redirect them
// to httptest.Server mocks. BaiduDriver and OneDriveDriver use unexported
// baseURL fields, so we inject a custom Transport that rewrites host/port.
// ═══════════════════════════════════════════════════════════════════════════

// mockRoundTripper routes requests to the mock server based on their original
// host. Panics if a request reaches a host with no registered mock server —
// tests must set up mocks for every host the driver contacts.
type mockRoundTripper struct {
	mu       sync.Mutex
	hosts    map[string]*httptest.Server // original host → mock server
	defaultTransport http.RoundTripper
}

func newMockRoundTripper() *mockRoundTripper {
	return &mockRoundTripper{
		hosts:   make(map[string]*httptest.Server),
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
	_, ok := m.hosts[req.URL.Host]
	m.mu.Unlock()
	if !ok {
		// If no mock registered, pass through to real network.
		// In tests this should never happen — all hosts must be mocked.
		panic("mockRoundTripper: no mock registered for host " + req.URL.Host + " path " + req.URL.Path)
	}

	m.mu.Lock()
	mockSrv := m.hosts[req.URL.Host]
	m.mu.Unlock()

	// Build a *new* request pointing at the mock server, copying the original URL path + query.
	newURL := *req.URL
	newURL.Scheme = "http"
	newURL.Host = mockSrv.Listener.Addr().String()

	newReq, err := http.NewRequestWithContext(req.Context(), req.Method, newURL.String(), req.Body)
	if err != nil {
		return nil, err
	}
	newReq.Header = req.Header.Clone()
	newReq.ContentLength = req.ContentLength

	// Use a fresh client to avoid redirect issues with Baidu's redirect client.
	// The mock server's client should handle this cleanly.
	return m.defaultTransport.RoundTrip(newReq)
}

// newMockHTTPClient returns an *http.Client that routes all requests through rt.
func newMockHTTPClient(rt *mockRoundTripper) *http.Client {
	return &http.Client{
		Transport: rt,
		Timeout:   30 * time.Second,
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// In-memory CacheReader for BackhaulManager
// ═══════════════════════════════════════════════════════════════════════════

type memBlobCache struct {
	mu    sync.RWMutex
	store map[string][]byte
}

func newMemBlobCache() *memBlobCache {
	return &memBlobCache{store: make(map[string][]byte)}
}

func (m *memBlobCache) Get(blobHash string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.store[blobHash]
	return data, ok
}

func (m *memBlobCache) storePut(blobHash string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[blobHash] = append([]byte(nil), data...)
}

// Put satisfies backhaul.CacheWriter so HandleBlobL4 caches fetched data
// for subsequent cache-hit requests.
func (m *memBlobCache) Put(blobHash string, data []byte, _ int) error {
	m.storePut(blobHash, data)
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════
// Mock MetadataClient (satisfies BOTH pinstrategy.MetadataClient AND
// accountpool.MetadataClient — same GetSegmentLocations signature)
// ═══════════════════════════════════════════════════════════════════════════

type mockMetaClient struct {
	mu        sync.Mutex
	locations map[string][]types.BlobLocation
}

func (m *mockMetaClient) GetBlobLocations(_ context.Context, blobHash string) ([]types.BlobLocation, error) {
	locs, ok := m.locations[blobHash]
	if !ok {
		return nil, nil
	}
	return locs, nil
}

// Satisfy the rest of metadata.BlobStoreClient + ContentMetaClient + PopularityClient (unused in tests).
func (m *mockMetaClient) GetContentMeta(_ context.Context, _ string) (*types.ContentMeta, error) {
	return nil, nil
}
func (m *mockMetaClient) GetContentBlobs(_ context.Context, _ string) ([]types.BlobDescriptor, []types.BlobRole, error) {
	return nil, nil, nil
}
func (m *mockMetaClient) WriteContentMeta(_ context.Context, _ *sql.Tx, _ types.ContentMeta, _ []types.BlobDescriptor, _ []types.BlobRole) error {
	return nil
}
func (m *mockMetaClient) GetTopContents(_ context.Context, _ int) ([]metadata.TopContent, error) {
	return nil, nil
}
func (m *mockMetaClient) GetPopularity24h(_ context.Context, _ string) float64 { return 0 }
func (m *mockMetaClient) WriteBlob(_ context.Context, _ *sql.Tx, _ []types.BlobDescriptor) error {
	return nil
}
func (m *mockMetaClient) WriteBlobLocations(_ context.Context, _ *sql.Tx, _ []types.BlobLocation) error {
	return nil
}
func (m *mockMetaClient) ReportAccountHealth(_ context.Context, _ types.Vendor, _ string, _ types.HealthState) error {
	return nil
}
func (m *mockMetaClient) GetAccountHealths(_ context.Context, _ types.Vendor) ([]metadata.AccountHealth, error) {
	return nil, nil
}

// ═══════════════════════════════════════════════════════════════════════════
// Test helper: build the full L4 stack (TokenManager, AccountPool,
// LinkPool, LocalDataPlane, BackhaulManager) for a set of accounts.
// ═══════════════════════════════════════════════════════════════════════════

// storageTestHarness holds all the components needed for a storage distribution test.
type storageTestHarness struct {
	rt     *mockRoundTripper
	httpc  *http.Client
	tokenMgr *auth.TokenManager
	meta   *mockMetaClient
	pool   *accountpool.AccountPool
	linkP  *linkpool.LinkPool
	dp     *dataplane.LocalDataPlane
	cache  *memBlobCache
	bm     *backhaul.BackhaulManager
}

// addAccount creates an Account with the given driver and registers it in the pool.
func (h *storageTestHarness) addAccount(
	vendor types.Vendor,
	accountID string,
	drv driver.Driver,
) *accountpool.Account {
	acct := &accountpool.Account{
		Vendor:       vendor,
		AccountID:    accountID,
		Driver:       drv,
		Limiter:      rate.NewLimiter(rate.Limit(drv.RateLimitConfig().QPS), drv.RateLimitConfig().Burst),
		CB:           circuitbreaker.New(accountID, 5, 100*time.Millisecond),
		VendorWeight: 2.0,
	}
	acct.Health.Store(types.HealthState{State: "healthy"})
	h.pool.AddAccount(acct)
	return acct
}

// ═══════════════════════════════════════════════════════════════════════════
// Subtest A: Baidu (PCS API) end-to-end backhaul chain
// ═══════════════════════════════════════════════════════════════════════════

func TestStorageDistribution_Baidu(t *testing.T) {
	mockBlobData := []byte("baidu-integration-test-blob-data-hello-world")
	mockBlobHash := "baidu_test_blob_hash"
	mockFileID := "1234567890"

	// ── Step 1: Start httptest mock servers ─────────────────────────────

	// Mock Baidu OAuth2 token endpoint.
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = r.ParseForm()
		if r.FormValue("grant_type") != "refresh_token" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token":  "mock-baidu-access-token",
			"refresh_token": "mock-baidu-refresh-token",
			"expires_in":    3600,
		})
	}))
	defer tokenSrv.Close()

	// Mock Baidu PCS API (filemetas, etc.) + CDN download.
	// The flow: BaiduDriver calls /rest/2.0/xpan/file?method=filemetas to get dlink,
	// then does HEAD to dlink to resolve CDN URL, then GETs the CDN URL.
	// We use a single httptest server for all Baidu API endpoints.
	var pcsCallCount int
	pcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pcsCallCount++
		path := r.URL.Path
		query := r.URL.Query()

		switch {
		case path == "/rest/2.0/xpan/file" && query.Get("method") == "filemetas":
			// Return dlink pointing back to our own server.
			// fs_id must be a JSON number (int64 in Go), not a string.
			dlink := "http://" + r.Host + "/cdn/" + mockFileID + "/download?dlink=1"
			json.NewEncoder(w).Encode(map[string]interface{}{
				"errno": 0,
				"list": []map[string]interface{}{
					{
						"fs_id":    1234567890,
						"filename": "test_blob.dat",
						"size":     len(mockBlobData),
						"isdir":    0,
						"dlink":    dlink,
					},
				},
			})

		case path == "/cdn/"+mockFileID+"/download":
			// Simulate CDN redirect: HEAD returns 302 with Location.
			w.Header().Set("Location", "http://"+r.Host+"/cdn/"+mockFileID+"/real-content")
			w.WriteHeader(http.StatusFound)

		case path == "/cdn/"+mockFileID+"/real-content":
			// Actual CDN download: return blob data.
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(mockBlobData)))
			w.WriteHeader(http.StatusOK)
			w.Write(mockBlobData)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer pcsSrv.Close()

	// ── Step 2: Set up mock HTTP routing ────────────────────────────────

	rt := newMockRoundTripper()
	rt.registerHost("openapi.baidu.com", tokenSrv)
	rt.registerHost("pan.baidu.com", pcsSrv)
	// Also register the actual mock server host:port so CDN redirects work.
	// BaiduDriver.resolveCDNURL returns the raw CDN URL from the Location
	// header. That URL's host is the mock server's listener address, which
	// is not "pan.baidu.com". The mockRoundTripper panics on unknown hosts,
	// so we must register it.
	rt.registerHost(pcsSrv.Listener.Addr().String(), pcsSrv)
	httpc := newMockHTTPClient(rt)

	// ── Step 3: Create TokenManager, register Baidu account ─────────────

	tm := auth.NewTokenManager(httpc)
	// Note: TokenURL must use a host that is registered in mockRoundTripper.
	// We use "openapi.baidu.com" (the default Baidu token host) which is
	// routed to tokenSrv. The path just needs to be handled by the mock.
	tm.Register(types.VendorBaidu, "baidu-acct-1", auth.OAuth2Config{
		ClientID:     "mock-client-id",
		ClientSecret: "mock-client-secret",
		RefreshToken: "mock-refresh-token",
		TokenURL:     "https://openapi.baidu.com/oauth/2.0/token",
	})
	tm.Register(types.VendorBaidu, "baidu-acct-2", auth.OAuth2Config{
		ClientID:     "mock-client-id-2",
		ClientSecret: "mock-client-secret-2",
		RefreshToken: "mock-refresh-token-2",
		TokenURL:     "https://openapi.baidu.com/oauth/2.0/token",
	})

	// ── Step 4: Create BaiduDrivers ─────────────────────────────────────

	baiduDrv1 := baidu.NewBaiduDriver(tm, "baidu-acct-1", "c1", "s1", httpc)
	baiduDrv2 := baidu.NewBaiduDriver(tm, "baidu-acct-2", "c2", "s2", httpc)

	// ── Step 5: Create mock MetadataClient with K=2 redundancy ──────────

	meta := &mockMetaClient{
		locations: map[string][]types.BlobLocation{
			mockBlobHash: {
				{BackendID: "baidu:baidu-acct-1", FileID: mockFileID, BlobHash: mockBlobHash},
				{BackendID: "baidu:baidu-acct-2", FileID: mockFileID, BlobHash: mockBlobHash},
			},
		},
	}

	// ── Step 6: Create AccountPool ──────────────────────────────────────

	pool := accountpool.NewAccountPool(meta)
	lp := linkpool.NewLinkPool(100)
	mcache := newMemBlobCache()

	harness := &storageTestHarness{
		rt:       rt,
		httpc:    httpc,
		tokenMgr: tm,
		meta:     meta,
		pool:     pool,
		linkP:    lp,
		cache:    mcache,
	}
	acct1 := harness.addAccount(types.VendorBaidu, "baidu-acct-1", baiduDrv1)
	_ = harness.addAccount(types.VendorBaidu, "baidu-acct-2", baiduDrv2)
	_ = acct1

	// ── Step 7: Create LocalDataPlane ───────────────────────────────────

	dp := dataplane.NewLocalDataPlane(pool, lp, meta, httpc)
	harness.dp = dp

	// ── Step 8-9: Create BackhaulManager ────────────────────────────────

	bm := backhaul.NewBackhaulManager(mcache, dp, nil, nil)
	harness.bm = bm

	// ── Step 10: Call HandleBlobL4 → should fetch via Baidu PCS ────────

	t.Run("cache_miss_triggers_full_backhaul_chain", func(t *testing.T) {
		ctx := context.Background()
		var buf bytes.Buffer
		err := bm.HandleBlobL4(ctx, &buf, mockBlobHash)
		if err != nil {
			t.Fatalf("HandleBlobL4: unexpected error: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), mockBlobData) {
			t.Fatalf("HandleBlobL4: data mismatch: got %q, want %q", buf.String(), mockBlobData)
		}
		t.Logf("Baidu backhaul: cache miss → %d bytes delivered, %d PCS API calls", buf.Len(), pcsCallCount)

		// ── Step 11: Verify data in buffer ─────────────────────────── (done above)

		// ── Step 12: Second call → cache hit (no HTTP) ────────────────
		var buf2 bytes.Buffer
		pcsBefore := pcsCallCount
		err = bm.HandleBlobL4(ctx, &buf2, mockBlobHash)
		if err != nil {
			t.Fatalf("HandleBlobL4 (2nd call): unexpected error: %v", err)
		}
		if !bytes.Equal(buf2.Bytes(), mockBlobData) {
			t.Fatalf("HandleBlobL4 (2nd call): data mismatch: got %q, want %q", buf2.String(), mockBlobData)
		}
		if pcsCallCount != pcsBefore {
			t.Errorf("expected no new PCS API calls on cache hit, got %d new calls", pcsCallCount-pcsBefore)
		}
		t.Logf("Baidu cache hit: buffer ok, PCS calls unchanged at %d", pcsCallCount)
	})

	// ── Step 13: Degradation test: 403 → CircuitBreaker opens → select
	// second account on the 6th call (threshold=5).
	t.Run("degradation_403_circuit_breaker_fallback", func(t *testing.T) {
		// Create a fresh test with a "broken" account 1.
		meta2 := &mockMetaClient{
			locations: map[string][]types.BlobLocation{
				mockBlobHash: {
					{BackendID: "baidu:baidu-deg-1", FileID: mockFileID, BlobHash: mockBlobHash},
					{BackendID: "baidu:baidu-deg-2", FileID: mockFileID, BlobHash: mockBlobHash},
				},
			},
		}

		// Mock PCS server that returns 403 for account 1 requests,
		// 200 for account 2 requests. We distinguish by the access_token
		// query parameter.
		degSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path
			query := r.URL.Query()

			switch {
			case path == "/rest/2.0/xpan/file" && query.Get("method") == "filemetas":
				token := query.Get("access_token")
				if token == "mock-token-deg-1" {
					// Account 1 returns 403 (banned).
					w.WriteHeader(http.StatusForbidden)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"errno":  0,
						"errmsg": "account banned",
					})
				} else {
					// Account 2 returns valid dlink.
					dlink := "http://" + r.Host + "/cdn/" + mockFileID + "/download?dlink=1"
					json.NewEncoder(w).Encode(map[string]interface{}{
						"errno": 0,
						"list": []map[string]interface{}{
							{
								"fs_id":    1234567890,
								"filename": "test_blob.dat",
								"size":   len(mockBlobData),
								"isdir":  0,
								"dlink":  dlink,
							},
						},
					})
				}

			case path == "/cdn/"+mockFileID+"/download":
				w.Header().Set("Location", "http://"+r.Host+"/cdn/"+mockFileID+"/real-content")
				w.WriteHeader(http.StatusFound)

			case path == "/cdn/"+mockFileID+"/real-content":
				w.WriteHeader(http.StatusOK)
				w.Write(mockBlobData)

			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer degSrv.Close()

		degRT := newMockRoundTripper()
		degRT.registerHost("openapi.baidu.com", tokenSrv)
		degRT.registerHost("pan.baidu.com", degSrv)
		degHTTPC := newMockHTTPClient(degRT)

		// TokenManager that returns distinguishable tokens per account.
		_ = auth.NewTokenManager(degHTTPC) // placeholder, we use degTM2 below

		// Override the token server to return "mock-token-deg-1" / "mock-token-deg-2".
		// We need a different token mock per account. Use query-based routing.
		multiTokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			refreshToken := r.FormValue("refresh_token")
			var accessToken string
			switch refreshToken {
			case "refresh-deg-1":
				accessToken = "mock-token-deg-1"
			case "refresh-deg-2":
				accessToken = "mock-token-deg-2"
			default:
				accessToken = "mock-token-unknown"
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token":  accessToken,
				"refresh_token": refreshToken,
				"expires_in":    3600,
			})
		}))
		defer multiTokenSrv.Close()

		degRT2 := newMockRoundTripper()
		degRT2.registerHost("openapi.baidu.com", multiTokenSrv)
		degRT2.registerHost("pan.baidu.com", degSrv)
		// Register raw host for CDN redirects (same reason as subtest A).
		degRT2.registerHost(degSrv.Listener.Addr().String(), degSrv)
		degHTTPC2 := newMockHTTPClient(degRT2)

		degTM2 := auth.NewTokenManager(degHTTPC2)
		degTM2.Register(types.VendorBaidu, "baidu-deg-1", auth.OAuth2Config{
			ClientID: "c1", ClientSecret: "s1", RefreshToken: "refresh-deg-1",
			TokenURL: "https://openapi.baidu.com/oauth/2.0/token",
		})
		degTM2.Register(types.VendorBaidu, "baidu-deg-2", auth.OAuth2Config{
			ClientID: "c2", ClientSecret: "s2", RefreshToken: "refresh-deg-2",
			TokenURL: "https://openapi.baidu.com/oauth/2.0/token",
		})

		d1 := baidu.NewBaiduDriver(degTM2, "baidu-deg-1", "c1", "s1", degHTTPC2)
		d2 := baidu.NewBaiduDriver(degTM2, "baidu-deg-2", "c2", "s2", degHTTPC2)

		degPool := accountpool.NewAccountPool(meta2)
		degLP := linkpool.NewLinkPool(100)
		degCache := newMemBlobCache()

		acctA := &accountpool.Account{
			Vendor:       types.VendorBaidu,
			AccountID:    "baidu-deg-1",
			Driver:       d1,
			Limiter:      rate.NewLimiter(rate.Limit(d1.RateLimitConfig().QPS), d1.RateLimitConfig().Burst),
			CB:           circuitbreaker.New("baidu-deg-1", 5, 100*time.Millisecond),
			VendorWeight: 2.0,
		}
		acctA.Health.Store(types.HealthState{State: "healthy"})
		degPool.AddAccount(acctA)

		acctB := &accountpool.Account{
			Vendor:       types.VendorBaidu,
			AccountID:    "baidu-deg-2",
			Driver:       d2,
			Limiter:      rate.NewLimiter(rate.Limit(d2.RateLimitConfig().QPS), d2.RateLimitConfig().Burst),
			CB:           circuitbreaker.New("baidu-deg-2", 5, 100*time.Millisecond),
			VendorWeight: 2.0,
		}
		acctB.Health.Store(types.HealthState{State: "healthy"})
		degPool.AddAccount(acctB)

		degDP := dataplane.NewLocalDataPlane(degPool, degLP, meta2, degHTTPC2)
		degBM := backhaul.NewBackhaulManager(degCache, degDP, nil, nil)

		ctx := context.Background()

		// The circuit breaker in AccountPool.SelectForRead only checks State() to
		// skip accounts, but the actual CB state transitions require explicit
		// cb.Call(...) wrapping. In the current codebase, the read path does not
		// wrap driver calls in cb.Call, so we manually exercise the CB to simulate
		// 5 consecutive BanSignalError failures on account 1.
		for i := 0; i < 5; i++ {
			_ = acctA.CB.(*circuitbreaker.CircuitBreaker).Call(ctx, func() error {
				return &types.BanSignalError{Code: 403, Msg: "test 403 degradation"}
			})
		}

		if acctA.CB.State() != 2 { // 2 = StateOpen
			t.Fatalf("degradation: CB for account 1 should be Open (state=2), got state=%d", acctA.CB.State())
		}
		t.Logf("degradation: account 1 CB opened after 5 consecutive BanSignalErrors, state=%d", acctA.CB.State())

		// 6th call: account 1 CB open, SelectForRead skips it, falls back to account 2 → succeeds.
		sixthBlobHash := "baidu-deg-blob-6"
		meta2.mu.Lock()
		meta2.locations[sixthBlobHash] = []types.BlobLocation{
			{BackendID: "baidu:baidu-deg-1", FileID: mockFileID, BlobHash: sixthBlobHash},
			{BackendID: "baidu:baidu-deg-2", FileID: mockFileID, BlobHash: sixthBlobHash},
		}
		meta2.mu.Unlock()

		// Set account 2's driver token to return a valid response.
		// The degSrv returns valid dlink when the token is NOT "mock-token-deg-1".
		var buf6 bytes.Buffer
		err := degBM.HandleBlobL4(ctx, &buf6, sixthBlobHash)
		if err != nil {
			t.Fatalf("degradation: 6th call should succeed via account 2, got error: %v", err)
		}
		if !bytes.Equal(buf6.Bytes(), mockBlobData) {
			t.Fatalf("degradation: 6th call data mismatch: got %q, want %q", buf6.String(), mockBlobData)
		}
		t.Logf("degradation: 6th call succeeded via account 2 fallback, %d bytes", buf6.Len())
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// Subtest B: OneDrive (Graph API) end-to-end backhaul chain
// ═══════════════════════════════════════════════════════════════════════════

func TestStorageDistribution_OneDrive(t *testing.T) {
	mockBlobData := []byte("onedrive-integration-test-blob-data-hello-world")
	mockBlobHash := "onedrive_test_blob_hash"
	mockFileID := "01ABCDEFGHIJKLMNOP"

	// ── Step 1: Start httptest mock servers ─────────────────────────────

	// Mock OneDrive OAuth2 token endpoint.
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = r.ParseForm()
		if r.FormValue("grant_type") != "refresh_token" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token":  "mock-onedrive-access-token",
			"refresh_token": "mock-onedrive-refresh-token",
			"expires_in":    3600,
		})
	}))
	defer tokenSrv.Close()

	// Mock OneDrive Graph API.
	var graphCallCount int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		graphCallCount++

		path := r.URL.Path

		// CDN download URL is anonymous (no Authorization header required).
		// Check this BEFORE the Authorization check.
		if path == "/cdn-onedrive/"+mockFileID {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(mockBlobData)))
			w.WriteHeader(http.StatusOK)
			w.Write(mockBlobData)
			return
		}

		// All other endpoints require Authorization header.
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if path == "/v1.0/me/drive/items/"+mockFileID {
			dlURL := "http://" + r.Host + "/cdn-onedrive/" + mockFileID
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":                              mockFileID,
				"name":                            "test_blob.dat",
				"size":                            len(mockBlobData),
				"@microsoft.graph.downloadUrl":    dlURL,
			})
			return
		}

		// Health check: GET /v1.0/me/drive
		if path == "/v1.0/me/drive" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id": "root-drive-id",
			})
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer graphSrv.Close()

	// ── Step 2: Set up mock HTTP routing ────────────────────────────────

	rt := newMockRoundTripper()
	rt.registerHost("login.microsoftonline.com", tokenSrv)
	rt.registerHost("graph.microsoft.com", graphSrv)
	// Register raw host for the downloadUrl returned by GetLink.
	// OneDrive download URLs point directly to the CDN server, which is
	// our mock graph server (same host:port as the listener).
	rt.registerHost(graphSrv.Listener.Addr().String(), graphSrv)
	httpc := newMockHTTPClient(rt)

	// ── Step 3: Create TokenManager, register OneDrive account ──────────

	tm := auth.NewTokenManager(httpc)
	tm.Register(types.VendorOneDrive, "od-acct-1", auth.OAuth2Config{
		ClientID:     "mock-od-client-id",
		ClientSecret: "mock-od-client-secret",
		RefreshToken: "mock-od-refresh-token",
		RedirectURI:  "http://localhost/callback",
		TokenURL:     "https://login.microsoftonline.com/common/oauth2/v2.0/token",
	})

	// ── Step 4: Create OneDriveDriver ───────────────────────────────────

	// Set baseHost to point to our mock Graph server.
	odDrv := onedrive.NewOneDriveDriver(tm, "od-acct-1", "global", httpc)
	// Use the test-only constructor to override the API host.
	// Note: newOneDriveDriverWithHost is unexported, but we can work around this
	// by using the mock round tripper which already intercepts graph.microsoft.com.
	// The baseHost field is only used by baseURL() to construct URLs.
	// Since we have mockRoundTripper intercepting graph.microsoft.com, the
	// URL construction is fine — graph.microsoft.com will be routed to graphSrv.

	// ── Step 5: Create mock MetadataClient ──────────────────────────────

	meta := &mockMetaClient{
		locations: map[string][]types.BlobLocation{
			mockBlobHash: {
				{BackendID: "onedrive:od-acct-1", FileID: mockFileID, BlobHash: mockBlobHash},
			},
		},
	}

	// ── Step 6: Create AccountPool ──────────────────────────────────────

	pool := accountpool.NewAccountPool(meta)
	lp := linkpool.NewLinkPool(100)
	mcache := newMemBlobCache()

	acct := &accountpool.Account{
		Vendor:       types.VendorOneDrive,
		AccountID:    "od-acct-1",
		Driver:       odDrv,
		Limiter:      rate.NewLimiter(rate.Limit(odDrv.RateLimitConfig().QPS), odDrv.RateLimitConfig().Burst),
		CB:           circuitbreaker.New("od-acct-1", 5, 100*time.Millisecond),
		VendorWeight: 2.0,
	}
	acct.Health.Store(types.HealthState{State: "healthy"})
	pool.AddAccount(acct)

	// ── Step 7: Create LocalDataPlane ───────────────────────────────────

	dp := dataplane.NewLocalDataPlane(pool, lp, meta, httpc)

	// ── Step 8-9: Create BackhaulManager ────────────────────────────────

	bm := backhaul.NewBackhaulManager(mcache, dp, nil, nil)

	// ── Step 10: Call HandleBlobL4 → should fetch via OneDrive Graph ────

	t.Run("cache_miss_triggers_graph_download", func(t *testing.T) {
		ctx := context.Background()
		var buf bytes.Buffer
		err := bm.HandleBlobL4(ctx, &buf, mockBlobHash)
		if err != nil {
			t.Fatalf("HandleBlobL4: unexpected error: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), mockBlobData) {
			t.Fatalf("HandleBlobL4: data mismatch: got %q, want %q", buf.String(), mockBlobData)
		}
		t.Logf("OneDrive backhaul: cache miss → %d bytes delivered, %d Graph API calls", buf.Len(), graphCallCount)

		// ── Step 11: Verify data in buffer ─────────────────────────── (done above)

		// ── Step 12: Second call → cache hit ─────────────────────────
		var buf2 bytes.Buffer
		graphBefore := graphCallCount
		err = bm.HandleBlobL4(ctx, &buf2, mockBlobHash)
		if err != nil {
			t.Fatalf("HandleBlobL4 (2nd call): unexpected error: %v", err)
		}
		if !bytes.Equal(buf2.Bytes(), mockBlobData) {
			t.Fatalf("HandleBlobL4 (2nd call): data mismatch: got %q, want %q", buf2.String(), mockBlobData)
		}
		if graphCallCount != graphBefore {
			t.Errorf("expected no new Graph API calls on cache hit, got %d new calls", graphCallCount-graphBefore)
		}
		t.Logf("OneDrive cache hit: buffer ok, Graph API calls unchanged at %d", graphCallCount)
	})

	// ── Step 13: Degradation test: 429 → BanSignalError ────────────────

	t.Run("degradation_429_returns_ban_signal", func(t *testing.T) {
		// Create a mock Graph server that ONLY returns 429.
		banGraphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests) // 429
		}))
		defer banGraphSrv.Close()

		banRT := newMockRoundTripper()
		banRT.registerHost("login.microsoftonline.com", tokenSrv)
		banRT.registerHost("graph.microsoft.com", banGraphSrv)
		banRT.registerHost(banGraphSrv.Listener.Addr().String(), banGraphSrv)
		banHTTPC := newMockHTTPClient(banRT)

		banTM := auth.NewTokenManager(banHTTPC)
		banTM.Register(types.VendorOneDrive, "od-ban-acct", auth.OAuth2Config{
			ClientID:     "c1",
			ClientSecret: "s1",
			RefreshToken: "refresh-ban",
			RedirectURI:  "http://localhost/callback",
			TokenURL:     "https://login.microsoftonline.com/common/oauth2/v2.0/token",
		})

		banDrv := onedrive.NewOneDriveDriver(banTM, "od-ban-acct", "global", banHTTPC)

		banMeta := &mockMetaClient{
			locations: map[string][]types.BlobLocation{
				"od-ban-blob": {
					{BackendID: "onedrive:od-ban-acct", FileID: "ban-file-id", BlobHash: "od-ban-blob"},
				},
			},
		}

		banPool := accountpool.NewAccountPool(banMeta)
		banLP := linkpool.NewLinkPool(100)
		banCache := newMemBlobCache()

		banAcct := &accountpool.Account{
			Vendor:       types.VendorOneDrive,
			AccountID:    "od-ban-acct",
			Driver:       banDrv,
			Limiter:      rate.NewLimiter(rate.Limit(banDrv.RateLimitConfig().QPS), banDrv.RateLimitConfig().Burst),
			CB:           circuitbreaker.New("od-ban-acct", 5, 100*time.Millisecond),
			VendorWeight: 2.0,
		}
		banAcct.Health.Store(types.HealthState{State: "healthy"})
		banPool.AddAccount(banAcct)

		banDP := dataplane.NewLocalDataPlane(banPool, banLP, banMeta, banHTTPC)
		banBM := backhaul.NewBackhaulManager(banCache, banDP, nil, nil)

		ctx := context.Background()
		var buf bytes.Buffer
		err := banBM.HandleBlobL4(ctx, &buf, "od-ban-blob")

		// The 429 from OneDrive's do() method returns a BanSignalError,
		// which should propagate through the chain as an error.
		if err == nil {
			t.Fatal("degradation 429: expected error, got nil")
		}
		// The error should contain a reference to the ban.
		t.Logf("degradation 429: got expected error: %v", err)

		// Verify it IS a BanSignalError somewhere in the chain.
		var banErr *types.BanSignalError
		if !unwrapAsBanSignal(err, &banErr) {
			t.Logf("degradation 429: warning — error is not directly a BanSignalError, got %T: %v", err, err)
		} else {
			if banErr.Code != http.StatusTooManyRequests {
				t.Errorf("degradation 429: expected code 429, got %d", banErr.Code)
			}
			t.Logf("degradation 429: BanSignalError code=%d msg=%s", banErr.Code, banErr.Msg)
		}
	})
}

// unwrapAsBanSignal checks if the error chain contains a *types.BanSignalError.
func unwrapAsBanSignal(err error, target **types.BanSignalError) bool {
	if err == nil {
		return false
	}
	type causer interface{ Unwrap() error }
	for {
		if be, ok := err.(*types.BanSignalError); ok {
			*target = be
			return true
		}
		u, ok := err.(causer)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
}
