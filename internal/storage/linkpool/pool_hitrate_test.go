package linkpool_test

import (
	"context"
	"math"
	"testing"

	"github.com/shlande/mediaworker/internal/types"
)

func TestHitRate_ZeroRequests(t *testing.T) {
	// Given a fresh pool with no GetOrFetch calls
	pool := newTestPool(t)

	// When reading HitRate
	got := pool.HitRate()

	// Then it is exactly 0, never NaN (JSON-marshalled downstream)
	if got != 0 || math.IsNaN(got) {
		t.Fatalf("HitRate on zero requests = %v, want 0", got)
	}
}

func TestHitRate_MissThenHit(t *testing.T) {
	// Given a pool and driver with one file
	ctx := context.Background()
	pool := newTestPool(t)
	d := newTestDriver(t)

	// When the first call misses (sync fetch) and the second hits
	if _, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01"); err != nil {
		t.Fatalf("first GetOrFetch: %v", err)
	}
	if _, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01"); err != nil {
		t.Fatalf("second GetOrFetch: %v", err)
	}

	// Then hit rate is 1/2
	if got := pool.HitRate(); got != 0.5 {
		t.Fatalf("HitRate = %v, want 0.5", got)
	}
}

func TestHitRate_AllHits(t *testing.T) {
	// Given a warmed pool entry
	ctx := context.Background()
	pool := newTestPool(t)
	d := newTestDriver(t)
	if _, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01"); err != nil {
		t.Fatalf("warm GetOrFetch: %v", err)
	}

	// When two more calls are served from cache
	for i := 0; i < 2; i++ {
		if _, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01"); err != nil {
			t.Fatalf("hit GetOrFetch %d: %v", i, err)
		}
	}

	// Then hit rate is 2/3 (warm call was a miss)
	want := 2.0 / 3.0
	if got := pool.HitRate(); got != want {
		t.Fatalf("HitRate = %v, want %v", got, want)
	}
}

func TestHitRate_ErrorNotCountedAsHit(t *testing.T) {
	// Given a pool whose only request fails (driver error, nothing cached)
	ctx := context.Background()
	pool := newTestPool(t)
	d := newTestDriver(t)

	if _, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent file")
	}

	// Then the failed request counts as a miss
	if got := pool.HitRate(); got != 0 {
		t.Fatalf("HitRate = %v, want 0 after failed fetch", got)
	}
}
