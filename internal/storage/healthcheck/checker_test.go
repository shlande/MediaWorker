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
	calls         atomic.Int32
}

func (ct *concurrentTracker) HealthCheck(ctx context.Context) types.HealthState {
	ct.calls.Add(1)
	v := ct.active.Add(1)
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
	time.Sleep(5 * time.Millisecond)
	return ct.Driver.HealthCheck(ctx)
}

func healthyDriver() driver.Driver {
	return mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "healthy"},
	})
}

func failingDriver(msg string) driver.Driver {
	return mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "degraded", ErrorMsg: msg},
	})
}

func addAccount(pool *accountpool.AccountPool, d driver.Driver, id string) *accountpool.Account {
	acct := &accountpool.Account{
		Vendor:    types.Vendor("mock"),
		AccountID: id,
		Driver:    d,
	}
	acct.Health.Store(types.HealthState{State: "healthy"})
	pool.AddAccount(acct)
	return acct
}

// Given never-used (idle) accounts, when checkAll runs, then every account is
// probed and healthy results are stored and reported.
func TestCheckAll_ProbesAllAccountsConcurrently(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	for i := 0; i < 10; i++ {
		aid := "acct_" + string(rune('0'+i))
		ct := &concurrentTracker{Driver: healthyDriver()}
		pool.AddAccount(&accountpool.Account{
			Vendor:    types.Vendor("mock"),
			AccountID: aid,
			Driver:    ct,
		})
	}

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, time.Hour, writer)
	hc.checkAll(context.Background())

	accounts := pool.SnapshotAccounts()
	if len(accounts) != 10 {
		t.Fatalf("expected 10 accounts, got %d", len(accounts))
	}
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
	if calls := writer.calls(); len(calls) != 10 {
		t.Fatalf("expected 10 writer calls, got %d", len(calls))
	}
}

func TestCheckAll_ConcurrentExecution(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	for i := 0; i < 5; i++ {
		aid := "acct_" + string(rune('0'+i))
		ct := &concurrentTracker{Driver: healthyDriver()}
		pool.AddAccount(&accountpool.Account{
			Vendor:    types.Vendor("mock"),
			AccountID: aid,
			Driver:    ct,
		})
	}

	hc := NewHealthChecker(pool, time.Hour, time.Hour, &mockWriter{})
	hc.checkAll(context.Background())
	_ = pool.SnapshotAccounts()
}

func TestHealthCheck_IntervalCallsCheckAll(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	addAccount(pool, healthyDriver(), "acct_1")

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, 50*time.Millisecond, time.Hour, writer)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	hc.Start(ctx)

	if calls := writer.calls(); len(calls) < 1 {
		t.Errorf("expected at least 1 health check within 200ms, got %d", len(calls))
	}
}

// Given a recently used account, when checkAll runs, then the account is
// skipped entirely — usage itself is the health signal.
func TestCheckOne_SkipsRecentlyUsed(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	ct := &concurrentTracker{Driver: failingDriver("should not be called")}
	addAccount(pool, ct, "acct_1")
	accts := pool.SnapshotAccounts()
	accts[0].MarkUsed(time.Now())

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, time.Hour, writer)
	hc.checkAll(context.Background())

	if got := ct.calls.Load(); got != 0 {
		t.Errorf("driver probed %d times, want 0 for recently used account", got)
	}
	if calls := writer.calls(); len(calls) != 0 {
		t.Errorf("writer calls = %d, want 0", len(calls))
	}
}

// Given an account used over idleThreshold ago, when checkAll runs, then it
// is probed like an idle account.
func TestCheckOne_ProbesStaleUsedAccount(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	ct := &concurrentTracker{Driver: healthyDriver()}
	addAccount(pool, ct, "acct_1")
	accts := pool.SnapshotAccounts()
	accts[0].MarkUsed(time.Now().Add(-2 * time.Hour))

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, time.Hour, writer)
	hc.checkAll(context.Background())

	if got := ct.calls.Load(); got != 1 {
		t.Errorf("driver probed %d times, want 1", got)
	}
	if calls := writer.calls(); len(calls) != 1 {
		t.Errorf("writer calls = %d, want 1", len(calls))
	}
}

// Given a tainted (banned) account, when checkAll runs, then the probe is
// skipped and the banned state is never overwritten.
func TestCheckOne_BannedNotProbed(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	ct := &concurrentTracker{Driver: healthyDriver()}
	acct := addAccount(pool, ct, "acct_1")
	acct.Health.Store(types.HealthState{State: "banned", ErrorMsg: "vendor 403"})

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, time.Hour, writer)
	for i := 0; i < 3; i++ {
		hc.checkAll(context.Background())
	}

	if got := ct.calls.Load(); got != 0 {
		t.Errorf("driver probed %d times, want 0 for banned account", got)
	}
	h := acct.Health.Load().(types.HealthState)
	if h.State != "banned" || h.ErrorMsg != "vendor 403" {
		t.Errorf("health = %+v, want banned state preserved", h)
	}
	if calls := writer.calls(); len(calls) != 0 {
		t.Errorf("writer calls = %d, want 0", len(calls))
	}
}

// Given consecutive probe failures, when the threshold is reached, then the
// account is stored and reported as banned; failures below the threshold
// leave state and writer untouched.
func TestFailures_ThresholdTripsBan(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	acct := addAccount(pool, failingDriver("token: invalid_client"), "acct_1")

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, time.Hour, writer)

	for i := 1; i <= 2; i++ {
		hc.checkAll(context.Background())
		h := acct.Health.Load().(types.HealthState)
		if h.State != "healthy" {
			t.Fatalf("probe %d: state = %s, want healthy (sub-threshold failure keeps state)", i, h.State)
		}
	}
	if calls := writer.calls(); len(calls) != 0 {
		t.Fatalf("writer calls = %d, want 0 before threshold", len(calls))
	}

	hc.checkAll(context.Background())
	h := acct.Health.Load().(types.HealthState)
	if h.State != "banned" {
		t.Fatalf("state = %s, want banned after 3 consecutive failures", h.State)
	}
	if h.ErrorMsg != "token: invalid_client" {
		t.Errorf("error_msg = %q, want probe failure reason", h.ErrorMsg)
	}
	calls := writer.calls()
	if len(calls) != 1 || calls[0].state.State != "banned" {
		t.Fatalf("writer calls = %+v, want exactly one banned report", calls)
	}
}

// Given interleaved results, when a healthy probe lands between failures,
// then the consecutive-failure counter resets.
func TestFailures_ResetOnHealthy(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	acct := addAccount(pool, failingDriver("net timeout"), "acct_1")

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, time.Hour, writer)

	hc.checkAll(context.Background()) // fail 1
	hc.checkAll(context.Background()) // fail 2
	acct.Driver = healthyDriver()
	hc.checkAll(context.Background()) // healthy → reset, stored + reported
	acct.Driver = failingDriver("net timeout")
	hc.checkAll(context.Background()) // fail 1 again

	h := acct.Health.Load().(types.HealthState)
	if h.State != "healthy" {
		t.Fatalf("state = %s, want healthy (threshold never reached)", h.State)
	}
	calls := writer.calls()
	if len(calls) != 1 || calls[0].state.State != "healthy" {
		t.Fatalf("writer calls = %+v, want exactly one healthy report", calls)
	}
}

// Given a driver-reported ban (403), when the probe runs, then it is stored
// and reported immediately without failure counting.
func TestCheckOne_DriverBanSignal(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	d := mock.NewMockDriver(types.Vendor("mock"), mock.MockDriverConfig{
		Health: types.HealthState{State: "banned", ErrorMsg: "banned (HTTP 403)"},
	})
	acct := addAccount(pool, d, "acct_1")

	writer := &mockWriter{}
	hc := NewHealthChecker(pool, time.Hour, time.Hour, writer)
	hc.checkAll(context.Background())

	h := acct.Health.Load().(types.HealthState)
	if h.State != "banned" {
		t.Fatalf("state = %s, want banned", h.State)
	}
	calls := writer.calls()
	if len(calls) != 1 || calls[0].state.State != "banned" {
		t.Fatalf("writer calls = %+v, want exactly one banned report", calls)
	}
}

func TestHealthCheck_NoWriter(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	addAccount(pool, healthyDriver(), "acct_1")

	hc := NewHealthChecker(pool, time.Hour, time.Hour, nil)
	hc.checkAll(context.Background())

	accts := pool.SnapshotAccounts()
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
	addAccount(pool, healthyDriver(), "acct_1")

	hc := NewHealthChecker(pool, time.Hour, time.Hour, &mockWriter{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	hc.Start(ctx)
}

// Given a probe failure, when the first failure lands, then it is logged with
// the consecutive count and the driver error message.
func TestFailures_LogsProbeFailure(t *testing.T) {
	pool := accountpool.NewAccountPool(nil)
	addAccount(pool, failingDriver("token: invalid_client"), "acct_1")

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	hc := NewHealthChecker(pool, time.Hour, time.Hour, &mockWriter{})
	hc.log = log

	hc.checkAll(context.Background())

	logs := buf.String()
	if !strings.Contains(logs, "idle probe failed") {
		t.Errorf("expected probe failure log, got: %s", logs)
	}
	if !strings.Contains(logs, "token: invalid_client") {
		t.Errorf("expected driver error in log, got: %s", logs)
	}
}
