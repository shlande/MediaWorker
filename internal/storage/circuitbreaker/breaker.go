// Package circuitbreaker implements a state-machine circuit breaker for cloud drive
// account-level fault isolation. It tracks consecutive BanSignalErrors and transitions
// through Closed → Open → HalfOpen → Closed to protect banned or throttled accounts.
//
// State machine:
//
//	Closed (normal)
//	  → on failureThreshold consecutive BanSignalError → Open
//	Open (rejecting all calls)
//	  → after openDuration → HalfOpen (probe)
//	HalfOpen (single probe)
//	  → probe succeeds → Closed (reset)
//	  → probe fails → Open (new openDuration)
//
// The CircuitBreaker satisfies accountpool.CircuitBreaker (State() int, ForceOpen(), ForceClose()).
package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// State constants match accountpool.CircuitBreakerState values.
const (
	StateClosed   = 0
	StateHalfOpen = 1
	StateOpen     = 2
)

// ErrCircuitOpen is returned when the circuit breaker is in the Open state and
// the open duration has not yet elapsed.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// nowFn returns time.Now. Exposed as a variable so tests can replace it.
var nowFn = time.Now

// CircuitBreaker implements a per-account circuit breaker with three states:
// Closed, HalfOpen, and Open. It tracks consecutive BanSignalError failures and
// transitions between states accordingly.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            int
	failureCount     int
	lastFailTime     time.Time
	openUntil        time.Time
	failureThreshold int
	openDuration     time.Duration
	accountID        string
}

// New creates a CircuitBreaker for the given account. threshold is the number
// of consecutive BanSignalError failures that trigger the Open state (default 5).
// openDuration is how long the circuit stays Open before transitioning to HalfOpen
// (default 10 minutes).
func New(accountID string, threshold int, openDuration time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		threshold = 5
	}
	if openDuration <= 0 {
		openDuration = 10 * time.Minute
	}
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: threshold,
		openDuration:     openDuration,
		accountID:        accountID,
	}
}

// Call executes fn under the circuit breaker state machine. It returns
// ErrCircuitOpen if the breaker is Open and the open duration has not elapsed.
// Only BanSignalError failures increment the failure count; other errors are
// returned without affecting state. A successful call resets the failure count.
func (cb *CircuitBreaker) Call(ctx context.Context, fn func() error) error {
	cb.mu.Lock()

	switch cb.state {
	case StateOpen:
		if nowFn().Before(cb.openUntil) {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		// Open duration elapsed → transition to HalfOpen for probe
		cb.state = StateHalfOpen
		cb.failureCount = 0
		cb.mu.Unlock()

		err := fn()
		cb.mu.Lock()
		if err != nil {
			cb.state = StateOpen
			cb.openUntil = nowFn().Add(cb.openDuration)
			cb.mu.Unlock()
			return err
		}
		cb.state = StateClosed
		cb.mu.Unlock()
		return nil

	case StateClosed, StateHalfOpen:
		cb.mu.Unlock()
		err := fn()
		cb.mu.Lock()
		if err != nil {
			if isBanSignal(err) {
				cb.failureCount++
				cb.lastFailTime = nowFn()
				if cb.failureCount >= cb.failureThreshold {
					cb.state = StateOpen
					cb.openUntil = nowFn().Add(cb.openDuration)
				}
			}
		} else {
			cb.failureCount = 0
		}
		cb.mu.Unlock()
		return err
	}

	return nil
}

// isBanSignal reports whether err is (or wraps) a types.BanSignalError.
// It uses errors.As with the pointer receiver, NOT string matching.
func isBanSignal(err error) bool {
	var target *types.BanSignalError
	return errors.As(err, &target)
}

// State returns the current circuit breaker state (0=Closed, 1=HalfOpen, 2=Open).
// Satisfies accountpool.CircuitBreaker.
func (cb *CircuitBreaker) State() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// ForceOpen transitions the circuit breaker to the Open state unconditionally.
// Satisfies accountpool.CircuitBreaker.
func (cb *CircuitBreaker) ForceOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateOpen
	cb.openUntil = nowFn().Add(cb.openDuration)
}

// ForceClose transitions the circuit breaker to the Closed state unconditionally
// and resets the failure count. Satisfies accountpool.CircuitBreaker.
func (cb *CircuitBreaker) ForceClose() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failureCount = 0
}
