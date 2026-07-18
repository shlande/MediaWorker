// Package quota implements distributed quota allocation with a borrowable rate limiter.
package quota

import (
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
)

// BorrowableLimiter wraps a *rate.Limiter with the ability to borrow extra tokens
// beyond the base allocation. Tokens are borrowed from the "global reserve" — the
// 20% safety margin (§8.3 in docs/storage/README.md).
//
// When base tokens are exhausted, the limiter checks whether borrowed tokens are
// available (non-zero borrowed count and within validity period). Borrowed tokens
// are granted by the control plane via Grant() and revoked via Revoke().
type BorrowableLimiter struct {
	base        *rate.Limiter
	borrowed    atomic.Int64
	borrowUntil atomic.Int64
	maxBorrow   int64
}

// NewBorrowableLimiter creates a BorrowableLimiter wrapping the given base limiter.
// maxBorrow is set to 30% of the base limiter's burst capacity (minimum 1).
func NewBorrowableLimiter(base *rate.Limiter) *BorrowableLimiter {
	maxBorrow := int64(base.Burst()) * 3 / 10
	if maxBorrow < 1 {
		maxBorrow = 1
	}
	return &BorrowableLimiter{
		base:      base,
		maxBorrow: maxBorrow,
	}
}

// Allow checks whether a token can be consumed. It first tries the base rate
// limiter; if tokens are exhausted it falls back to borrowed tokens.
func (bl *BorrowableLimiter) Allow() bool {
	if bl.base.Allow() {
		return true
	}
	// Base exhausted — try borrowed tokens.
	if time.Now().UnixNano() < bl.borrowUntil.Load() && bl.borrowed.Load() > 0 {
		bl.borrowed.Add(-1)
		return true
	}
	return false
}

// SetLimit delegates to the underlying base rate limiter.
func (bl *BorrowableLimiter) SetLimit(limit rate.Limit) {
	bl.base.SetLimit(limit)
}

// Grant adds extra borrowed tokens with a validity deadline. It caps the
// borrowed count at maxBorrow. The until parameter specifies the wall-clock
// time at which borrowed tokens expire.
func (bl *BorrowableLimiter) Grant(extra int64, until time.Time) {
	borrowed := extra
	if borrowed > bl.maxBorrow {
		borrowed = bl.maxBorrow
	}
	bl.borrowed.Store(borrowed)
	bl.borrowUntil.Store(until.UnixNano())
}

// Revoke clears all borrowed tokens immediately. After Revoke, Allow() will
// only succeed if the base limiter has tokens.
func (bl *BorrowableLimiter) Revoke() {
	bl.borrowed.Store(0)
}

// compile-time assertion that BorrowableLimiter satisfies accountpool.Limiter
var _ accountpool.Limiter = (*BorrowableLimiter)(nil)
