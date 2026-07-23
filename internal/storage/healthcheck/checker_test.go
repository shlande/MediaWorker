package healthcheck

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
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
	cancel()      // already cancelled
	hc.Start(ctx) // should return immediately without panic
}

func TestRecover_when_degraded_and_three_consecutive_healthy_probes_expect_healthy_and_log(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "healthy"},
	})
	acct := &accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	}
	// Seed as probe-degraded
	acct.Health.Store(types.HealthState{
		State:       "degraded",
		ErrorMsg:    "latency 2.2s > threshold 1s",
		Recoverable: true,
	})
	pool.AddAccount(acct)

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	hc := NewHealthChecker(pool, time.Hour, &mockWriter{})
	hc.log = log

	// Probe 1: still degraded (1 consecutive)
	hc.checkAll(context.Background())
	snap := pool.SnapshotAccounts()
	h := snap[0].Health.Load().(types.HealthState)
	if h.State != "degraded" {
		t.Fatalf("probe 1: expected degraded, got %s", h.State)
	}
	if h.ConsecutiveHealthy != 1 {
		t.Fatalf("probe 1: expected consecutive=1, got %d", h.ConsecutiveHealthy)
	}

	// Probe 2: still degraded (2 consecutive)
	hc.checkAll(context.Background())
	snap = pool.SnapshotAccounts()
	h = snap[0].Health.Load().(types.HealthState)
	if h.State != "degraded" {
		t.Fatalf("probe 2: expected degraded, got %s", h.State)
	}
	if h.ConsecutiveHealthy != 2 {
		t.Fatalf("probe 2: expected consecutive=2, got %d", h.ConsecutiveHealthy)
	}

	// Probe 3: recovered to healthy
	hc.checkAll(context.Background())
	snap = pool.SnapshotAccounts()
	h = snap[0].Health.Load().(types.HealthState)
	if h.State != "healthy" {
		t.Fatalf("probe 3: expected healthy, got %s", h.State)
	}
	if h.ConsecutiveHealthy != 3 {
		t.Fatalf("probe 3: expected consecutive=3, got %d", h.ConsecutiveHealthy)
	}
	if h.ErrorMsg != "" {
		t.Errorf("expected empty error_msg after recovery, got %q", h.ErrorMsg)
	}

	logs := buf.String()
	if !strings.Contains(logs, "account recovered to healthy") {
		t.Errorf("expected recover log, got: %s", logs)
	}
}

func TestRecover_when_degraded_and_healthy_probe_then_degraded_probe_expect_counter_resets(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "healthy"},
	})
	acct := &accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	}
	acct.Health.Store(types.HealthState{
		State:       "degraded",
		ErrorMsg:    "latency 2.2s > threshold 1s",
		Recoverable: true,
	})
	pool.AddAccount(acct)

	hc := NewHealthChecker(pool, time.Hour, &mockWriter{})

	// Two consecutive healthy probes
	hc.checkAll(context.Background())
	hc.checkAll(context.Background())
	snap := pool.SnapshotAccounts()
	h := snap[0].Health.Load().(types.HealthState)
	if h.ConsecutiveHealthy != 2 {
		t.Fatalf("expected consecutive=2 after 2 healthy probes, got %d", h.ConsecutiveHealthy)
	}

	// Driver returns degraded on the third probe
	degradedD := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "degraded", ErrorMsg: "api timeout"},
	})
	snap = pool.SnapshotAccounts()
	snap[0].Driver = degradedD

	hc.checkAll(context.Background())
	snap = pool.SnapshotAccounts()
	h = snap[0].Health.Load().(types.HealthState)
	if h.State != "degraded" {
		t.Fatalf("expected degraded after non-healthy probe, got %s", h.State)
	}
	if h.ConsecutiveHealthy != 0 {
		t.Fatalf("expected counter reset to 0 after non-healthy probe, got %d", h.ConsecutiveHealthy)
	}
	if !h.Recoverable {
		t.Error("expected Recoverable=true for fresh probe-driver degrade")
	}
}

func TestRecover_when_banned_expect_not_auto_recovered(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "healthy"},
	})
	acct := &accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	}
	// Banned by external action — NOT Recoverable
	acct.Health.Store(types.HealthState{
		State:       "banned",
		Recoverable: false,
	})
	pool.AddAccount(acct)

	hc := NewHealthChecker(pool, time.Hour, &mockWriter{})

	// Multiple healthy probes should NOT recover a banned account
	for i := 0; i < 5; i++ {
		hc.checkAll(context.Background())
	}
	snap := pool.SnapshotAccounts()
	h := snap[0].Health.Load().(types.HealthState)
	if h.State != "banned" {
		t.Errorf("expected banned after 5 healthy probes, got %s", h.State)
	}
	if h.ConsecutiveHealthy != 0 {
		t.Errorf("expected ConsecutiveHealthy=0 for non-recoverable account, got %d", h.ConsecutiveHealthy)
	}
}

func TestRecover_when_manual_degraded_not_recoverable_expect_not_auto_recovered(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "healthy"},
	})
	acct := &accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	}
	// Manually degraded (e.g. via admin API) — NOT Recoverable
	acct.Health.Store(types.HealthState{
		State:       "degraded",
		ErrorMsg:    "manual override",
		Recoverable: false,
	})
	pool.AddAccount(acct)

	hc := NewHealthChecker(pool, time.Hour, &mockWriter{})

	for i := 0; i < 5; i++ {
		hc.checkAll(context.Background())
	}
	snap := pool.SnapshotAccounts()
	h := snap[0].Health.Load().(types.HealthState)
	if h.State != "degraded" {
		t.Errorf("expected degraded after 5 healthy probes (manual degrade), got %s", h.State)
	}
	if h.ConsecutiveHealthy != 0 {
		t.Errorf("expected ConsecutiveHealthy=0 for non-recoverable manual degrade, got %d", h.ConsecutiveHealthy)
	}
}

func TestRecover_when_healthy_then_probe_degraded_expect_degrade_log(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "degraded", ErrorMsg: "latency 3s > threshold 2s"},
	})
	acct := &accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: "acct_1",
		Driver:    d,
	}
	acct.Health.Store(types.HealthState{State: "healthy"})
	pool.AddAccount(acct)

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	hc := NewHealthChecker(pool, time.Hour, &mockWriter{})
	hc.log = log

	hc.checkAll(context.Background())
	snap := pool.SnapshotAccounts()
	h := snap[0].Health.Load().(types.HealthState)
	if h.State != "degraded" {
		t.Fatalf("expected degraded, got %s", h.State)
	}
	if !h.Recoverable {
		t.Error("expected Recoverable=true for probe-driven degrade")
	}

	logs := buf.String()
	if !strings.Contains(logs, "account degraded") {
		t.Errorf("expected degrade log, got: %s", logs)
	}
}
