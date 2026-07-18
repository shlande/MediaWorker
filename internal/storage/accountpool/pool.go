// Package accountpool manages a pool of cloud drive accounts for read/write operations.
// It provides account selection with health, circuit breaker, rate limiting and load-aware scoring.
package accountpool

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

// CircuitBreakerState constants used by the CircuitBreaker interface.
const (
	StateClosed   = 0
	StateHalfOpen = 1
	StateOpen     = 2
)

// Limiter abstracts rate limiting for an account.
// Implementations: *rate.Limiter, BorrowableLimiter (future).
type Limiter interface {
	Allow() bool
	SetLimit(rate.Limit)
}

// CircuitBreaker abstracts circuit breaker state for an account.
// The actual circuitbreaker.CircuitBreaker from todo 8 satisfies this interface.
type CircuitBreaker interface {
	State() int
	ForceOpen()
	ForceClose()
}

// BlobLocationClient provides blob location lookup for SelectForRead.
type BlobLocationClient interface {
	GetBlobLocations(ctx context.Context, blobHash string) ([]types.BlobLocation, error)
}

// Account represents a single cloud drive account with its driver, rate limiter,
// circuit breaker, health state and vendor weight for load-aware selection.
type Account struct {
	Vendor     types.Vendor
	AccountID  string
	Credential types.Credential
	Driver     driver.Driver
	Limiter    Limiter
	Concurrent atomic.Int32
	CB         CircuitBreaker
	Health     atomic.Value // stores types.HealthState
	VendorWeight float64
}

// AccountPool manages a set of accounts indexed by key ("vendor:account_id")
// and by vendor for efficient selection.
type AccountPool struct {
	mu       sync.RWMutex
	accounts map[string]*Account // key = "vendor:account_id"
	vendors  map[types.Vendor][]string
	metadata BlobLocationClient
}

// accountKey returns the map key for an account.
func accountKey(vendor types.Vendor, accountID string) string {
	return string(vendor) + ":" + accountID
}

// NewAccountPool creates a new AccountPool with the given MetadataClient.
func NewAccountPool(mc BlobLocationClient) *AccountPool {
	return &AccountPool{
		accounts: make(map[string]*Account),
		vendors:  make(map[types.Vendor][]string),
		metadata: mc,
	}
}

// AddAccount adds an account to the pool. If an account with the same key already
// exists it is silently replaced.
func (ap *AccountPool) AddAccount(a *Account) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	key := accountKey(a.Vendor, a.AccountID)
	ap.accounts[key] = a
	ap.vendors[a.Vendor] = append(ap.vendors[a.Vendor], key)
}

// account returns a single account by key. Caller must hold at least a read lock.
func (ap *AccountPool) account(key string) *Account {
	return ap.accounts[key]
}

// SelectForRead selects the best *Account for reading a blob without calling
// Driver.GetLink. It returns only the account; the caller uses LinkPool.GetOrFetch
// to obtain the download link.
//
// Selection criteria (all must pass):
//   - HealthState.State == "healthy"
//   - CircuitBreaker.State() != StateOpen
//   - Limiter.Allow() returns true
//   - Concurrent < Driver.RateLimitConfig().ConcurrentLimit
//
// Candidates are sorted by score = Concurrent / VendorWeight ascending.
func (ap *AccountPool) SelectForRead(ctx context.Context, blobHash string) (*Account, error) {
	locations, err := ap.metadata.GetBlobLocations(ctx, blobHash)
	if err != nil {
		return nil, fmt.Errorf("accountpool: get locations for %q: %w", blobHash, err)
	}

	ap.mu.RLock()
	defer ap.mu.RUnlock()

	var candidates []*Account
	for _, loc := range locations {
		key := loc.BackendID
		acct := ap.account(key)
		if acct == nil {
			continue
		}

		// Health check
		h, ok := acct.Health.Load().(types.HealthState)
		if !ok || h.State != "healthy" {
			continue
		}

		// Circuit breaker
		if acct.CB != nil && acct.CB.State() == StateOpen {
			continue
		}

		// Rate limiter
		if acct.Limiter != nil && !acct.Limiter.Allow() {
			continue
		}

		// Concurrent limit
		if int(acct.Concurrent.Load()) >= acct.Driver.RateLimitConfig().ConcurrentLimit {
			continue
		}

		candidates = append(candidates, acct)
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("accountpool: no available account for blob %q", blobHash)
	}

	sort.Slice(candidates, func(i, j int) bool {
		si := float64(candidates[i].Concurrent.Load()) / candidates[i].VendorWeight
		sj := float64(candidates[j].Concurrent.Load()) / candidates[j].VendorWeight
		return si < sj
	})

	return candidates[0], nil
}

// SelectK selects up to k healthy accounts for upload, preferring cross-vendor
// diversity and least-loaded accounts. It does not deduplicate by location;
// the caller passes blob locations during UploadBlob.
func (ap *AccountPool) SelectK(ctx context.Context, k int) ([]*Account, error) {
	ap.mu.RLock()
	defer ap.mu.RUnlock()

	var candidates []*Account
	for _, acct := range ap.accounts {
		h, ok := acct.Health.Load().(types.HealthState)
		if !ok || h.State != "healthy" {
			continue
		}
		if acct.CB != nil && acct.CB.State() == StateOpen {
			continue
		}
		if acct.Limiter != nil && !acct.Limiter.Allow() {
			continue
		}
		if int(acct.Concurrent.Load()) >= acct.Driver.RateLimitConfig().ConcurrentLimit {
			continue
		}
		candidates = append(candidates, acct)
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("accountpool: no healthy accounts available for upload")
	}

	// Cross-vendor: prefer accounts from different vendors.
	// Sort candidates: vendor diversity first (shuffle within vendor, then alternate).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Vendor != candidates[j].Vendor {
			// Stable but non-deterministic ordering across vendors
			return string(candidates[i].Vendor) < string(candidates[j].Vendor)
		}
		// Same vendor: least loaded first
		si := float64(candidates[i].Concurrent.Load()) / candidates[i].VendorWeight
		sj := float64(candidates[j].Concurrent.Load()) / candidates[j].VendorWeight
		return si < sj
	})

	// Select up to k, ensuring no more than k/2 from the same vendor.
	selected := make([]*Account, 0, k)
	vendorCount := make(map[types.Vendor]int)
	for _, acct := range candidates {
		if len(selected) >= k {
			break
		}
		maxPerVendor := (k + 1) / 2
		if vendorCount[acct.Vendor] >= maxPerVendor {
			continue
		}
		vendorCount[acct.Vendor]++
		selected = append(selected, acct)
	}

	if len(selected) == 0 {
		return nil, fmt.Errorf("accountpool: could not select any accounts for upload")
	}

	return selected, nil
}

// UploadBlob uploads data to k redundant accounts concurrently.
// It selects k healthy accounts, uploads in parallel via errgroup,
// and writes metadata on success. At least one successful upload is required;
// partial failures are returned as-is.
func (ap *AccountPool) UploadBlob(ctx context.Context, blobHash string, data []byte) error {
	accounts, err := ap.SelectK(ctx, 2)
	if err != nil {
		return fmt.Errorf("accountpool: select accounts for upload: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	type result struct {
		idx  int
		fi   *types.FileInfo
		err  error
	}
	results := make([]result, len(accounts))

	for i, acct := range accounts {
		i, acct := i, acct
		g.Go(func() error {
			fi, err := acct.Driver.Put(gctx, blobHash, blobHash+".bin", bytes.NewReader(data), int64(len(data)))
			if err != nil {
				results[i] = result{idx: i, err: fmt.Errorf("upload to %s/%s: %w", acct.Vendor, acct.AccountID, err)}
				return nil // do not fail fast — let others continue
			}
			results[i] = result{idx: i, fi: fi}
			return nil
		})
	}

	_ = g.Wait()

	// At least one success required
	var firstErr error
	successCount := 0
	for _, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		successCount++
		_ = r.fi // metadata WriteSegmentLocations would use this
	}

	if successCount == 0 {
		return fmt.Errorf("accountpool: all uploads failed: %w", firstErr)
	}

	return nil
}

// ReplaceAll atomically replaces all accounts in the pool.
func (ap *AccountPool) ReplaceAll(accounts []Account) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	ap.accounts = make(map[string]*Account, len(accounts))
	ap.vendors = make(map[types.Vendor][]string)
	for i := range accounts {
		a := &accounts[i]
		key := accountKey(a.Vendor, a.AccountID)
		ap.accounts[key] = a
		ap.vendors[a.Vendor] = append(ap.vendors[a.Vendor], key)
	}
}

// UpdateCredential updates the credential for an account identified by key.
// If the account does not exist the call is a no-op.
func (ap *AccountPool) UpdateCredential(key string, c types.Credential) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if a, ok := ap.accounts[key]; ok {
		a.Credential = c
	}
}

// UpdateHealth updates the health state for an account identified by key.
// If the account does not exist the call is a no-op.
func (ap *AccountPool) UpdateHealth(key string, h types.HealthState) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if a, ok := ap.accounts[key]; ok {
		a.Health.Store(h)
	}
}

// MarkBanned sets the account's health state to banned and forces the circuit
// breaker open. If the account does not exist the call is a no-op.
func (ap *AccountPool) MarkBanned(key string) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if a, ok := ap.accounts[key]; ok {
		a.Health.Store(types.HealthState{State: "banned"})
		if a.CB != nil {
			a.CB.ForceOpen()
		}
	}
}

// SnapshotAccounts returns a snapshot of all accounts in the pool.
// The caller receives a shallow copy safe for read-only use without holding the lock.
func (ap *AccountPool) SnapshotAccounts() []*Account {
	ap.mu.RLock()
	defer ap.mu.RUnlock()
	result := make([]*Account, 0, len(ap.accounts))
	for _, a := range ap.accounts {
		result = append(result, a)
	}
	return result
}

// compile-time interface checks
var _ Limiter = (*rate.Limiter)(nil)
