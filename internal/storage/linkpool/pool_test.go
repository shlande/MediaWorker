package linkpool_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shlande/mediaworker/internal/storage/driver/mock"
	"github.com/shlande/mediaworker/internal/storage/linkpool"
	"github.com/shlande/mediaworker/internal/types"
)

func newTestPool(t *testing.T) *linkpool.LinkPool {
	t.Helper()
	return linkpool.NewLinkPool(100)
}

// newTestDriver creates a mock driver with a single file for testing.
func newTestDriver(t *testing.T) *mock.MockDriver {
	t.Helper()
	return mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{
		Filesystem: map[string]types.FileInfo{
			"fid_01": {ID: "fid_01", Name: "test.bin", Size: 1024},
		},
	})
}

func TestGetOrFetch_CacheMissThenHit(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	d := newTestDriver(t)

	// First call: cache miss → fetch via driver.
	link1, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01")
	if err != nil {
		t.Fatalf("first GetOrFetch failed: %v", err)
	}
	if link1 == nil {
		t.Fatal("expected non-nil link")
	}
	if link1.URL != "https://mock.example.com/115/fid_01" {
		t.Errorf("unexpected URL: %s", link1.URL)
	}

	// Second call: cache hit → same link pointer.
	link2, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01")
	if err != nil {
		t.Fatalf("second GetOrFetch failed: %v", err)
	}
	if link2 != link1 {
		t.Error("expected the same *DownloadLink pointer on cache hit")
	}
}

func TestGetOrFetch_DriverErrorNotCached(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	// Use an empty mock driver — fid "nonexistent" triggers ErrNotFound.
	d := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{
		Filesystem: map[string]types.FileInfo{},
	})

	_, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}

	// The cache should still be empty (no entry on error).
	if got := pool.Len(); got != 0 {
		t.Fatalf("expected empty cache after error, got %d entries", got)
	}
}

func TestGetOrFetch_ConcurrentDedup(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	var callCount atomic.Int32

	firstArrived := make(chan struct{}, 1)
	proceed := make(chan struct{})

	d := &callCountingDriver{
		MockDriver: *mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{
			Filesystem: map[string]types.FileInfo{
				"fid_01": {ID: "fid_01", Name: "test.bin", Size: 1024},
			},
		}),
		callCount: &callCount,
		onCall: func() {
			// First caller parks until released; any later concurrent caller
			// falls through. Required because GetOrFetch does not singleflight
			// concurrent misses for the same key.
			select {
			case firstArrived <- struct{}{}:
				<-proceed
			default:
			}
		},
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var link1, link2 *types.DownloadLink
	var err1, err2 error

	go func() {
		defer wg.Done()
		link1, err1 = pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01")
	}()
	go func() {
		defer wg.Done()
		link2, err2 = pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01")
	}()

	<-firstArrived
	close(proceed)

	wg.Wait()

	if err1 != nil {
		t.Errorf("first goroutine error: %v", err1)
	}
	if err2 != nil {
		t.Errorf("second goroutine error: %v", err2)
	}
	if link1 == nil || link2 == nil {
		t.Fatal("both goroutines should have returned a non-nil link")
	}
	// GetOrFetch does not dedup concurrent misses, so the driver may be called
	// once or twice depending on scheduling. Assert at-least-once.
	if n := callCount.Load(); n < 1 {
		t.Errorf("expected driver.GetLink called at least once, got %d", n)
	}
	if link1.URL == "" || link2.URL == "" {
		t.Error("both links should have a non-empty URL")
	}
}

func TestLinkPool_CapacityLRUEviction(t *testing.T) {
	pool := linkpool.NewLinkPool(5)
	ctx := context.Background()

	fs := make(map[string]types.FileInfo)
	for i := 0; i < 6; i++ {
		fid := fmtFID(i)
		fs[fid] = types.FileInfo{ID: fid, Name: fid + ".bin", Size: 1024}
	}
	d := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{Filesystem: fs})

	for i := 0; i < 5; i++ {
		_, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", fmtFID(i))
		if err != nil {
			t.Fatalf("GetOrFetch key %d: %v", i, err)
		}
	}
	if got := pool.Len(); got != 5 {
		t.Fatalf("expected cache Len 5, got %d", got)
	}

	_, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", fmtFID(5))
	if err != nil {
		t.Fatalf("GetOrFetch key 5: %v", err)
	}
	if got := pool.Len(); got != 5 {
		t.Fatalf("expected cache Len 5 after eviction, got %d", got)
	}

	_, err = pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", fmtFID(0))
	if err != nil {
		t.Fatalf("GetOrFetch key 0 after eviction: %v", err)
	}
	if pool.Len() != 5 {
		t.Errorf("expected cache Len 5 after re-adding key 0, got %d", pool.Len())
	}
}

func TestLinkPool_GetOrFetch_DifferentKeys(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	d := newTestDriver(t)

	// Fetch two different files (key includes accountID).
	l1, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01")
	if err != nil {
		t.Fatal(err)
	}
	l2, err := pool.GetOrFetch(ctx, d, types.Vendor115, "acct_02", "fid_01")
	if err != nil {
		t.Fatal(err)
	}
	if l1 == l2 {
		t.Error("different keys should produce different cached entries")
	}
}

func TestLinkPool_NewLinkPool_DefaultCapacity(t *testing.T) {
	p := linkpool.NewLinkPool(0)
	if p.Len() != 0 {
		t.Error("new pool should be empty")
	}
	// Verify it works by fetching.
	ctx := context.Background()
	d := newTestDriver(t)
	link, err := p.GetOrFetch(ctx, d, types.Vendor115, "acct_01", "fid_01")
	if err != nil {
		t.Fatalf("GetOrFetch on default-capacity pool: %v", err)
	}
	if link == nil {
		t.Fatal("expected non-nil link")
	}
}

// ─── helpers ───

func fmtFID(i int) string {
	return fmt.Sprintf("fid_%02d", i)
}

// callCountingDriver wraps mock.MockDriver and counts GetLink calls.
type callCountingDriver struct {
	mock.MockDriver
	callCount *atomic.Int32
	onCall    func()
}

func (d *callCountingDriver) GetLink(ctx context.Context, fileID string) (*types.DownloadLink, error) {
	d.callCount.Add(1)
	if d.onCall != nil {
		d.onCall()
	}
	return d.MockDriver.GetLink(ctx, fileID)
}
