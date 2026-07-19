package backhaul

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"
)

// ─── WarmCacheHitRate (todo 42) ─────────────────────────────────────────────

// Given zero traffic, When WarmCacheHitRate is read, Then it is exactly 0,
// not NaN (division guard).
func TestWarmCacheHitRate_ZeroTraffic(t *testing.T) {
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	if got := bm.WarmCacheHitRate(); got != 0 || math.IsNaN(got) {
		t.Errorf("WarmCacheHitRate() = %f, want 0 (not NaN)", got)
	}
}

// Given one cache hit and one miss through HandleBlobL4, When the rate is
// read, Then it is hits/requests = 0.5 — counters advance even with nil
// metrics (they are independent of the prometheus gate).
func TestWarmCacheHitRate_HitAndMiss(t *testing.T) {
	cache := newMockCache()
	_ = cache.Put(testBlobHash, []byte(testBlobData), 0)
	bm := NewBackhaulManager(cache, &mockDataPlane{data: []byte("x")}, nil, nil)
	// no SetMetrics — atomics must advance regardless

	var buf bytes.Buffer
	if err := bm.HandleBlobL4(context.Background(), &buf, testBlobHash); err != nil {
		t.Fatalf("hit: %v", err)
	}
	buf.Reset()
	if err := bm.HandleBlobL4(context.Background(), &buf, "missing-blob"); err != nil {
		t.Fatalf("miss: %v", err)
	}

	if got := bm.WarmCacheHitRate(); math.Abs(got-0.5) > 1e-9 {
		t.Errorf("WarmCacheHitRate() = %f, want 0.5", got)
	}
}

// ─── TTFBP95Ms (todo 42) ────────────────────────────────────────────────────

// Given no ttfb samples, When TTFBP95Ms is read, Then it is 0.
func TestTTFBP95Ms_ZeroSamples(t *testing.T) {
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	if got := bm.TTFBP95Ms(); got != 0 {
		t.Errorf("TTFBP95Ms() = %d, want 0", got)
	}
}

// Given samples 1..100 ms, When TTFBP95Ms is read, Then it is the
// nearest-rank P95 (= 95).
func TestTTFBP95Ms_NearestRank(t *testing.T) {
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	for i := 1; i <= 100; i++ {
		bm.recordTTFB(time.Duration(i) * time.Millisecond)
	}
	if got := bm.TTFBP95Ms(); got != 95 {
		t.Errorf("TTFBP95Ms() = %d, want 95 (nearest-rank of 1..100)", got)
	}
}

// Given more samples than the ring capacity, When they are recorded, Then
// the ring stays bounded (oldest half evicted).
func TestRecordTTFB_RingBoundedAtCapacity(t *testing.T) {
	bm := NewBackhaulManager(newMockCache(), &mockDataPlane{}, nil, nil)
	for i := 0; i < ttfbObservationCapacity+2000; i++ {
		bm.recordTTFB(time.Millisecond)
	}
	bm.obsMu.Lock()
	n := len(bm.ttfbMs)
	bm.obsMu.Unlock()
	if n > ttfbObservationCapacity {
		t.Errorf("ttfb ring length = %d, want <= %d", n, ttfbObservationCapacity)
	}
}

// Given a successful L4 data-plane fetch, When it completes, Then exactly
// one ttfb sample exists (cache hits and failures produce none).
func TestHandleBlobL4_RecordsTTFBOnSuccess(t *testing.T) {
	cache := newMockCache()
	bm := NewBackhaulManager(cache, &mockDataPlane{data: []byte("payload")}, nil, nil)

	var buf bytes.Buffer
	if err := bm.HandleBlobL4(context.Background(), &buf, testBlobHash); err != nil {
		t.Fatalf("HandleBlobL4: %v", err)
	}

	bm.obsMu.Lock()
	n := len(bm.ttfbMs)
	bm.obsMu.Unlock()
	if n != 1 {
		t.Errorf("ttfb samples = %d, want 1 after one successful fetch", n)
	}
}
