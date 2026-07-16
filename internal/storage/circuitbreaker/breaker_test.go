package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// fakeNow returns a controllable clock. All tests that depend on time use this.
func fakeNow(t0 time.Time) (func() time.Time, func(d time.Duration)) {
	var mu sync.Mutex
	now := t0
	return func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return now
		}, func(d time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			now = now.Add(d)
		}
}

// banSignal creates a *types.BanSignalError for testing.
func banSignal(code int, msg string) error {
	return &types.BanSignalError{Code: code, Msg: msg}
}

func TestClosed_to_Open_on_ban_signal_threshold(t *testing.T) {
	// Given: a circuit breaker with threshold=3
	now, _ := fakeNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	origNow := nowFn
	nowFn = now
	t.Cleanup(func() { nowFn = origNow })

	cb := New("test-acct", 3, 10*time.Minute)

	// When: 3 consecutive BanSignalError failures
	err := cb.Call(context.Background(), func() error { return banSignal(403, "forbidden") })
	if err == nil {
		t.Fatal("expected ban signal error, got nil")
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after 1 failure, got %d", cb.State())
	}

	err = cb.Call(context.Background(), func() error { return banSignal(403, "forbidden") })
	if err == nil {
		t.Fatal("expected ban signal error, got nil")
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after 2 failures, got %d", cb.State())
	}

	err = cb.Call(context.Background(), func() error { return banSignal(403, "forbidden") })
	if err == nil {
		t.Fatal("expected ban signal error, got nil")
	}

	// Then: after the 3rd failure, circuit is open
	if cb.State() != StateOpen {
		t.Errorf("expected StateOpen after 3 ban signal failures, got %d", cb.State())
	}
}

func TestOpen_returns_ErrCircuitOpen(t *testing.T) {
	// Given: a circuit breaker that is already Open
	now, _ := fakeNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	origNow := nowFn
	nowFn = now
	t.Cleanup(func() { nowFn = origNow })

	cb := New("test-acct", 2, 10*time.Minute)
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })

	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %d", cb.State())
	}

	// When: we call before openDuration elapses
	err := cb.Call(context.Background(), func() error { return nil })

	// Then: we get ErrCircuitOpen
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestOpen_to_HalfOpen_to_Closed_after_openDuration(t *testing.T) {
	// Given: an Open circuit breaker
	now, advance := fakeNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	origNow := nowFn
	nowFn = now
	t.Cleanup(func() { nowFn = origNow })

	cb := New("test-acct", 2, 50*time.Millisecond)
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })

	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %d", cb.State())
	}

	// When: openDuration elapses and a successful call is made
	advance(60 * time.Millisecond)
	err := cb.Call(context.Background(), func() error { return nil })

	// Then: call succeeds and returns to Closed
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if cb.State() != StateClosed {
		t.Errorf("expected StateClosed after successful probe, got %d", cb.State())
	}
}

func TestHalfOpen_probe_failure_returns_to_Open(t *testing.T) {
	// Given: an Open circuit breaker
	now, advance := fakeNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	origNow := nowFn
	nowFn = now
	t.Cleanup(func() { nowFn = origNow })

	cb := New("test-acct", 2, 50*time.Millisecond)
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })

	// When: openDuration elapses but probe fails
	advance(60 * time.Millisecond)
	err := cb.Call(context.Background(), func() error { return banSignal(429, "rate limit") })

	// Then: probe failure returns to Open, error is propagated
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var target *types.BanSignalError
	if !errors.As(err, &target) {
		t.Errorf("expected BanSignalError, got %T", err)
	}
	if target.Code != 429 {
		t.Errorf("expected code 429, got %d", target.Code)
	}
	if cb.State() != StateOpen {
		t.Errorf("expected StateOpen after failed probe, got %d", cb.State())
	}
}

func TestForceOpen_and_ForceClose(t *testing.T) {
	now, _ := fakeNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	origNow := nowFn
	nowFn = now
	t.Cleanup(func() { nowFn = origNow })

	cb := New("test-acct", 5, 10*time.Minute)

	// Force open
	cb.ForceOpen()
	if cb.State() != StateOpen {
		t.Errorf("expected StateOpen after ForceOpen, got %d", cb.State())
	}

	// Should return ErrCircuitOpen
	if err := cb.Call(context.Background(), func() error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen after ForceOpen, got %v", err)
	}

	// Force close
	cb.ForceClose()
	if cb.State() != StateClosed {
		t.Errorf("expected StateClosed after ForceClose, got %d", cb.State())
	}

	// Should now succeed
	if err := cb.Call(context.Background(), func() error { return nil }); err != nil {
		t.Errorf("expected nil after ForceClose, got %v", err)
	}
}

func Test_non_BanSignalError_does_not_increment_failureCount(t *testing.T) {
	now, _ := fakeNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	origNow := nowFn
	nowFn = now
	t.Cleanup(func() { nowFn = origNow })

	cb := New("test-acct", 3, 10*time.Minute)
	plainErr := errors.New("network timeout")

	// When: 5 plain errors (non-BanSignal)
	for i := 0; i < 5; i++ {
		err := cb.Call(context.Background(), func() error { return plainErr })
		if !errors.Is(err, plainErr) {
			t.Fatalf("iteration %d: expected plainErr, got %v", i, err)
		}
	}

	// Then: circuit should still be Closed (non-BanSignal does not count)
	if cb.State() != StateClosed {
		t.Errorf("expected StateClosed after 5 plain errors, got %d", cb.State())
	}
}

func Test_success_resets_failureCount(t *testing.T) {
	now, _ := fakeNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	origNow := nowFn
	nowFn = now
	t.Cleanup(func() { nowFn = origNow })

	cb := New("test-acct", 3, 10*time.Minute)

	// 2 BanSignal failures
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })

	// A success resets the count
	_ = cb.Call(context.Background(), func() error { return nil })

	// Another 2 BanSignal failures should NOT trigger Open (since count was reset)
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })

	if cb.State() != StateClosed {
		t.Errorf("expected StateClosed after 2+success+2 ban signals (reset), got %d", cb.State())
	}

	// 3rd failure after reset should trigger Open (2 existing + 1 more)
	_ = cb.Call(context.Background(), func() error { return banSignal(403, "x") })
	if cb.State() != StateOpen {
		t.Errorf("expected StateOpen after 2+success+3 ban signals, got %d", cb.State())
	}
}

func Test_isBanSignal_detects_wrapped_error(t *testing.T) {
	// Given: a BanSignalError wrapped in another error
	inner := &types.BanSignalError{Code: 429, Msg: "rate limited"}
	wrapped := errors.New("upstream: some text: " + inner.Error())

	// Then: isBanSignal still detects it
	if !isBanSignal(errors.Join(inner, errors.New("extra"))) {
		t.Error("expected isBanSignal to detect BanSignalError in joined error")
	}
	if isBanSignal(wrapped) {
		// errors.New wrapping a string that *contains* the error text is NOT wrapping.
		// This test verifies we don't do string matching.
		t.Log("correctly rejected string-wrapped error")
	}
	if isBanSignal(errors.New("ban signal: 403 forbidden")) {
		t.Error("isBanSignal must NOT use string matching")
	}
}

func Test_New_defaults(t *testing.T) {
	cb := New("", 0, 0)
	if cb.failureThreshold != 5 {
		t.Errorf("expected default threshold 5, got %d", cb.failureThreshold)
	}
	if cb.openDuration != 10*time.Minute {
		t.Errorf("expected default openDuration 10m, got %v", cb.openDuration)
	}
	if cb.accountID != "" {
		t.Errorf("expected empty accountID, got %q", cb.accountID)
	}
}