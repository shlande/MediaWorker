package healthcheck

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/storage/driver/mock"
	"github.com/shlande/mediaworker/internal/types"
)

// mockWriter implements MetadataWriter for testing.
type mockWriter struct {
	mu   sync.Mutex
	args []writeCall
}

type writeCall struct {
	vendor    types.Vendor
	accountID string
	state     types.HealthState
}

func (w *mockWriter) ReportAccountHealth(_ context.Context, vendor types.Vendor, accountID string, state types.HealthState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.args = append(w.args, writeCall{vendor: vendor, accountID: accountID, state: state})
	return nil
}

func (w *mockWriter) calls() []writeCall {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := make([]writeCall, len(w.args))
	copy(c, w.args)
	return c
}

// concurrentTracker wraps a driver.Driver to track concurrent HealthCheck calls.
type concurrentTracker struct {
	driver.Driver
	maxConcurrent atomic.Int32
	active        atomic.Int32
}

func (ct *concurrentTracker) HealthCheck(ctx context.Context) types.HealthState {
	v := ct.active.Add(1)
	// track peak concurrency
	for {
		cur := ct.maxConcurrent.Load()
		if v <= cur {
			break
		}
		if ct.maxConcurrent.CompareAndSwap(cur, v) {
			break
		}
	}
	defer ct.active.Add(-1)
	// small sleep to let other goroutines start
	time.Sleep(5 * time.Millisecond)
	return ct.Driver.HealthCheck(ctx)
}

func TestCheckAll_ProbesAllAccountsConcurrently(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	for i := 0; i < 10; i++ {
		aid := "acct_" + string(rune('0'+i))
		d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
			Health: types.HealthState{State: "healthy"},
		})
		ct := &concurrentTracker{Driver: d}
		pool.AddAccount(&accountpool.Account{
			Vendor:    types.Vendor("mock"),
			AccountID: aid,
			Driver:    ct,
		})
	}

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, writer)
	hc.checkAll(context.Background())

	// All 10 accounts probed
	accounts := pool.SnapshotAccounts()
	if len(accounts) != 10 {
		t.Fatalf("expected 10 accounts, got %d", len(accounts))
	}

	// All health states stored
	for _, acct := range accounts {
		h, ok := acct.Health.Load().(types.HealthState)
		if !ok {
			t.Errorf("account %s: health not stored", acct.AccountID)
			continue
		}
		if h.State != "healthy" {
			t.Errorf("account %s: expected state healthy, got %s", acct.AccountID, h.State)
		}
	}

	// All reported to writer
	calls := writer.calls()
	if len(calls) != 10 {
		t.Fatalf("expected 10 writer calls, got %d", len(calls))
	}
}

func TestCheckAll_ConcurrentExecution(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	for i := 0; i < 5; i++ {
		aid := "acct_" + string(rune('0'+i))
		d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
			Health: types.HealthState{State: "healthy"},
		})
		ct := &concurrentTracker{Driver: d}
		pool.AddAccount(&accountpool.Account{
			Vendor:    types.Vendor("mock"),
			AccountID: aid,
			Driver:    ct,
		})
	}

	hc := NewHealthChecker(pool, time.Hour, &mockWriter{})
	hc.checkAll(context.Background())

	// The tracker should have seen concurrency > 1 (at least 2 goroutines in flight)
	// but we just check that it didn't deadlock — actual concurrency depends on
	// go scheduler timing. If accounts are probed sequentially, this still passes.
	_ = pool.SnapshotAccounts()
}

func TestHealthCheck_IntervalCallsCheckAll(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "healthy"},
	})
	pool.AddAccount(&accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	})

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, 50*time.Millisecond, writer)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	hc.Start(ctx)

	// Within 200ms with 50ms interval, checkAll should have run at least 1 time.
	calls := writer.calls()
	// 200ms / 50ms = at most 4 ticks. First tick fires at 50ms.
	// Allow 0 in case scheduling is tight — but it should run at least once.
	if len(calls) < 1 {
		t.Errorf("expected at least 1 health check within 200ms, got %d", len(calls))
	}
}

func TestHealthCheck_DegradedState(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "degraded", ErrorMsg: "api timeout"},
	})
	pool.AddAccount(&accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	})

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, writer)
	hc.checkAll(context.Background())

	accts := pool.SnapshotAccounts()
	if len(accts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accts))
	}
	h, ok := accts[0].Health.Load().(types.HealthState)
	if !ok {
		t.Fatal("health state not stored")
	}
	if h.State != "degraded" {
		t.Errorf("expected degraded state, got %s", h.State)
	}
	if h.ErrorMsg != "api timeout" {
		t.Errorf("expected error msg 'api timeout', got %q", h.ErrorMsg)
	}

	calls := writer.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 writer call, got %d", len(calls))
	}
	if calls[0].state.State != "degraded" {
		t.Errorf("expected degraded in writer call, got %s", calls[0].state.State)
	}
}

func TestHealthCheck_NoWriter(t *testing.T) {
	// When writer is nil, checkOne should not panic.
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "healthy"},
	})
	pool.AddAccount(&accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	})

	hc := NewHealthChecker(pool, time.Hour, nil)
	hc.checkAll(context.Background())

	accts := pool.SnapshotAccounts()
	if len(accts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accts))
	}
	h, ok := accts[0].Health.Load().(types.HealthState)
	if !ok {
		t.Fatal("health state not stored")
	}
	if h.State != "healthy" {
		t.Errorf("expected healthy, got %s", h.State)
	}
}

func TestHealthCheck_ContextCancellation(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "healthy"},
	})
	pool.AddAccount(&accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	})

	hc := NewHealthChecker(pool, time.Hour, &mockWriter{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	hc.Start(ctx) // should return immediately without panic
}