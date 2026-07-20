package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shlande/mediaworker/internal/types"
)

// fakePinStore implements PinSpaceQuerier for tests.
type fakePinStore struct {
	available   int64
	pinnedCount int32
	totalPinned int64
}

func (f *fakePinStore) QuerySpace() types.PinSpaceInfo {
	return types.PinSpaceInfo{
		AvailableBytes:  f.available,
		PinnedCount:     f.pinnedCount,
		TotalPinnedSize: f.totalPinned,
	}
}

// fakeWarmCache implements WarmCacheReader for tests.
type fakeWarmCache struct {
	used  int64
	total int64
	count int
	ev1h  int
}

func (f *fakeWarmCache) Usage() (used, total int64) { return f.used, f.total }
func (f *fakeWarmCache) Count() int                 { return f.count }
func (f *fakeWarmCache) Evictions1h() int           { return f.ev1h }

func doCacheGet(t *testing.T, s *Server, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/cache", nil)
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)
	return rr
}

func decodeCacheBody(t *testing.T, rr *httptest.ResponseRecorder) cacheResponse {
	t.Helper()
	var resp cacheResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode cache response: %v", err)
	}
	return resp
}

// TestCacheHandler_FullComponents verifies all partitions are populated and
// eviction counters returned when both pinStore and warmCache are non-nil.
func TestCacheHandler_FullComponents(t *testing.T) {
	s := NewServer(testToken)
	RegisterCacheRoutes(s, &fakePinStore{
		available:   500,
		pinnedCount: 2,
		totalPinned: 200,
	}, &fakeWarmCache{
		used:  10,
		total: 100,
		count: 3,
		ev1h:  5,
	})

	rr := doCacheGet(t, s, testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	resp := decodeCacheBody(t, rr)

	if resp.Prefix == nil {
		t.Fatal("prefix partition is nil, want non-nil")
	}
	if resp.Prefix.Total != 700 { // 200 + 500
		t.Fatalf("prefix.total = %d, want 700", resp.Prefix.Total)
	}
	if resp.Prefix.Used != 200 {
		t.Fatalf("prefix.used = %d, want 200", resp.Prefix.Used)
	}
	if resp.Prefix.BlobCount != 2 {
		t.Fatalf("prefix.blob_count = %d, want 2", resp.Prefix.BlobCount)
	}

	if resp.Warm == nil {
		t.Fatal("warm partition is nil, want non-nil")
	}
	if resp.Warm.Total != 100 {
		t.Fatalf("warm.total = %d, want 100", resp.Warm.Total)
	}
	if resp.Warm.Used != 10 {
		t.Fatalf("warm.used = %d, want 10", resp.Warm.Used)
	}
	if resp.Warm.BlobCount != 3 {
		t.Fatalf("warm.blob_count = %d, want 3", resp.Warm.BlobCount)
	}

	if resp.EvictionCounter.Warm1h != 5 {
		t.Fatalf("eviction_counters.warm_1h = %d, want 5", resp.EvictionCounter.Warm1h)
	}
	if resp.EvictionCounter.Cold1h != 0 {
		t.Fatalf("eviction_counters.cold_1h = %d, want 0", resp.EvictionCounter.Cold1h)
	}
}

// TestCacheHandler_PinStoreNil verifies prefix is null when pinStore is nil.
func TestCacheHandler_PinStoreNil(t *testing.T) {
	s := NewServer(testToken)
	RegisterCacheRoutes(s, nil, &fakeWarmCache{
		used:  10,
		total: 100,
		count: 3,
		ev1h:  5,
	})

	rr := doCacheGet(t, s, testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	resp := decodeCacheBody(t, rr)

	if resp.Prefix != nil {
		t.Fatal("prefix partition is non-nil, want nil")
	}
	if resp.Warm == nil {
		t.Fatal("warm partition is nil, want non-nil")
	}
}

// TestCacheHandler_WarmCacheNil verifies warm is null when warmCache is nil.
func TestCacheHandler_WarmCacheNil(t *testing.T) {
	s := NewServer(testToken)
	RegisterCacheRoutes(s, &fakePinStore{
		available:   500,
		pinnedCount: 2,
		totalPinned: 200,
	}, nil)

	rr := doCacheGet(t, s, testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	resp := decodeCacheBody(t, rr)

	if resp.Prefix == nil {
		t.Fatal("prefix partition is nil, want non-nil")
	}
	if resp.Warm != nil {
		t.Fatal("warm partition is non-nil, want nil")
	}
	if resp.EvictionCounter.Warm1h != 0 {
		t.Fatalf("warm_1h = %d, want 0 when warmCache is nil", resp.EvictionCounter.Warm1h)
	}
}

// TestCacheHandler_DoubleNil verifies both partitions are null without panic.
func TestCacheHandler_DoubleNil(t *testing.T) {
	s := NewServer(testToken)
	RegisterCacheRoutes(s, nil, nil)

	rr := doCacheGet(t, s, testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	resp := decodeCacheBody(t, rr)

	if resp.Prefix != nil {
		t.Fatal("prefix partition is non-nil, want nil")
	}
	if resp.Warm != nil {
		t.Fatal("warm partition is non-nil, want nil")
	}
	if resp.EvictionCounter.Warm1h != 0 {
		t.Fatalf("warm_1h = %d, want 0", resp.EvictionCounter.Warm1h)
	}
}

// TestCacheHandler_BadToken_401 verifies the cache handler requires auth.
func TestCacheHandler_BadToken_401(t *testing.T) {
	s := NewServer(testToken)
	RegisterCacheRoutes(s, &fakePinStore{}, &fakeWarmCache{})

	rr := doCacheGet(t, s, "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d, want 401", rr.Code)
	}

	rr = doCacheGet(t, s, "wrong-token")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d, want 401", rr.Code)
	}
}
