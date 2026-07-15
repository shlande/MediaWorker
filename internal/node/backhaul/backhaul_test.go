package backhaul

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

// ─── Mocks ───────────────────────────────────────────────────────────────────

// mockDataPlane simulates an L4 data plane fetching from local storage.
type mockDataPlane struct {
	data  []byte
	err   error
	calls atomic.Int32
}

func (m *mockDataPlane) FetchBlobLocal(_ interface{}, _ string) (io.ReadCloser, error) {
	m.calls.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	return io.NopCloser(bytes.NewReader(m.data)), nil
}

// mockL4Fetcher simulates fetching from an L4 node for non-L4 nodes.
type mockL4Fetcher struct {
	data  []byte
	err   error
	calls atomic.Int32
}

func (m *mockL4Fetcher) FetchFromL4Node(_ context.Context, _ string) (interface{}, error) {
	m.calls.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	return io.NopCloser(bytes.NewReader(m.data)), nil
}

// mockCache is an in-memory cache that implements both CacheReader and CacheWriter.
type mockCache struct {
	mu    sync.RWMutex
	store map[string][]byte
}

func newMockCache() *mockCache {
	return &mockCache{store: make(map[string][]byte)}
}

func (m *mockCache) Get(blobHash string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.store[blobHash]
	return data, ok
}

func (m *mockCache) Put(blobHash string, data []byte, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[blobHash] = append([]byte(nil), data...)
	return nil
}

// ─── Tests ───────────────────────────────────────────────────────────────────

const testBlobHash = "abc123"
const testBlobData = "hello world, this is blob data for backhaul testing"

func TestHandleBlobL4_CacheHit(t *testing.T) {
	cache := newMockCache()
	_ = cache.Put(testBlobHash, []byte(testBlobData), 0)

	dp := &mockDataPlane{data: []byte(testBlobData)}
	bm := NewBackhaulManager(cache, dp, nil, nil)

	var buf bytes.Buffer
	err := bm.HandleBlobL4(context.Background(), &buf, testBlobHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != testBlobData {
		t.Errorf("expected %q, got %q", testBlobData, buf.String())
	}
	if dp.calls.Load() != 0 {
		t.Errorf("DataPlane called %d times for cache hit, expected 0", dp.calls.Load())
	}
}

func TestHandleBlobL4_BackhaulMiss(t *testing.T) {
	cache := newMockCache()
	dp := &mockDataPlane{data: []byte(testBlobData)}
	bm := NewBackhaulManager(cache, dp, nil, nil)

	var buf bytes.Buffer
	err := bm.HandleBlobL4(context.Background(), &buf, testBlobHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != testBlobData {
		t.Errorf("expected %q, got %q", testBlobData, buf.String())
	}
	if dp.calls.Load() != 1 {
		t.Errorf("DataPlane called %d times for miss, expected 1", dp.calls.Load())
	}

	// Verify cache was populated.
	cached, ok := cache.Get(testBlobHash)
	if !ok {
		t.Fatal("blob was not written to cache")
	}
	if string(cached) != testBlobData {
		t.Errorf("cached data mismatch: expected %q, got %q", testBlobData, string(cached))
	}
}

func TestHandleBlobL4_SingleflightDedup(t *testing.T) {
	cache := newMockCache()
	dp := &mockDataPlane{data: []byte(testBlobData)}
	bm := NewBackhaulManager(cache, dp, nil, nil)

	const numConcurrent = 5
	var wg sync.WaitGroup
	results := make([]string, numConcurrent)
	errs := make([]error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var buf bytes.Buffer
			errs[idx] = bm.HandleBlobL4(context.Background(), &buf, testBlobHash)
			results[idx] = buf.String()
		}(i)
	}
	wg.Wait()

	// All should succeed.
	for i := 0; i < numConcurrent; i++ {
		if errs[i] != nil {
			t.Errorf("request %d failed: %v", i, errs[i])
		}
		if results[i] != testBlobData {
			t.Errorf("request %d: expected %q, got %q", i, testBlobData, results[i])
		}
	}

	// DataPlane should be called exactly ONCE.
	if dp.calls.Load() != 1 {
		t.Errorf("DataPlane called %d times, expected 1 (singleflight dedup)", dp.calls.Load())
	}
}

func TestHandleBlobL4_SingleflightWaiters(t *testing.T) {
	cache := newMockCache()
	dp := &mockDataPlane{data: []byte(testBlobData)}
	bm := NewBackhaulManager(cache, dp, nil, nil)

	// First request triggers backhaul (caches data via singleflight).
	var buf1 bytes.Buffer
	err := bm.HandleBlobL4(context.Background(), &buf1, testBlobHash)
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	if buf1.String() != testBlobData {
		t.Errorf("first request: expected %q, got %q", testBlobData, buf1.String())
	}

	// Second sequential request hits cache (data cached by first request).
	var buf2 bytes.Buffer
	err = bm.HandleBlobL4(context.Background(), &buf2, testBlobHash)
	if err != nil {
		t.Fatalf("waiter request failed: %v", err)
	}
	if buf2.String() != testBlobData {
		t.Errorf("waiter request: expected %q, got %q", testBlobData, buf2.String())
	}

	// DataPlane should be called exactly ONCE.
	if dp.calls.Load() != 1 {
		t.Errorf("DataPlane called %d times, expected 1", dp.calls.Load())
	}
}

func TestHandleBlobL4_BackhaulFail(t *testing.T) {
	cache := newMockCache()
	dp := &mockDataPlane{err: errors.New("data plane down")}
	bm := NewBackhaulManager(cache, dp, nil, nil)

	var buf bytes.Buffer
	err := bm.HandleBlobL4(context.Background(), &buf, testBlobHash)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if buf.Len() > 0 {
		t.Errorf("expected empty buffer on error, got %q", buf.String())
	}
}

func TestHandleBlobNoL4_L4Fetch(t *testing.T) {
	cache := newMockCache()
	fetcher := &mockL4Fetcher{data: []byte(testBlobData)}
	bm := NewBackhaulManager(cache, nil, nil, fetcher)

	var buf bytes.Buffer
	err := bm.HandleBlobNoL4(context.Background(), &buf, testBlobHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != testBlobData {
		t.Errorf("expected %q, got %q", testBlobData, buf.String())
	}
	if fetcher.calls.Load() != 1 {
		t.Errorf("L4Fetcher called %d times, expected 1", fetcher.calls.Load())
	}

	// Verify cache was populated.
	cached, ok := cache.Get(testBlobHash)
	if !ok {
		t.Fatal("blob was not written to cache")
	}
	if string(cached) != testBlobData {
		t.Errorf("cached data mismatch: expected %q, got %q", testBlobData, string(cached))
	}
}

func TestHandleBlobNoL4_AllL4Unreachable(t *testing.T) {
	cache := newMockCache()
	fetcher := &mockL4Fetcher{err: errors.New("no L4 nodes reachable")}
	bm := NewBackhaulManager(cache, nil, nil, fetcher)

	var buf bytes.Buffer
	err := bm.HandleBlobNoL4(context.Background(), &buf, testBlobHash)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	expectedSubstr := "L4 unavailable"
	if !bytes.Contains([]byte(err.Error()), []byte(expectedSubstr)) {
		t.Errorf("expected error containing %q, got %q", expectedSubstr, err.Error())
	}
	if buf.Len() > 0 {
		t.Errorf("expected empty buffer on error, got %q", buf.String())
	}
}

func TestBackhaulUtilization(t *testing.T) {
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	if bm.BackhaulUtilization() != 0 {
		t.Errorf("expected 0, got %f", bm.BackhaulUtilization())
	}
}
