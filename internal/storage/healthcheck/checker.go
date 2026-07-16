// Package healthcheck periodically probes all accounts in the pool for health status.
package healthcheck

import (
	"context"
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/types"
)

// MetadataWriter is the interface for persisting health states to the metadata service.
// Implementations: PGMetadataClient (todo 13).
type MetadataWriter interface {
	ReportAccountHealth(ctx context.Context, vendor types.Vendor, accountID string, state types.HealthState) error
}

// HealthChecker periodically checks health of all accounts in the pool.
type HealthChecker struct {
	pool     *accountpool.AccountPool
	interval time.Duration
	writer   MetadataWriter
}

// NewHealthChecker creates a new HealthChecker.
func NewHealthChecker(pool *accountpool.AccountPool, interval time.Duration, writer MetadataWriter) *HealthChecker {
	return &HealthChecker{
		pool:     pool,
		interval: interval,
		writer:   writer,
	}
}

// Start begins the periodic health check loop. Blocks until ctx is cancelled.
func (hc *HealthChecker) Start(ctx context.Context) {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hc.checkAll(ctx)
		}
	}
}

// checkAll probes all accounts concurrently and records health states.
func (hc *HealthChecker) checkAll(ctx context.Context) {
	accounts := hc.pool.SnapshotAccounts()
	var wg sync.WaitGroup
	for _, acct := range accounts {
		wg.Add(1)
		go func(a *accountpool.Account) {
			defer wg.Done()
			hc.checkOne(ctx, a)
		}(acct)
	}
	wg.Wait()
}

// checkOne probes a single account and records its health state.
func (hc *HealthChecker) checkOne(ctx context.Context, acct *accountpool.Account) {
	start := time.Now()
	state := acct.Driver.HealthCheck(ctx)
	state.LastCheck = time.Now()
	state.Latency = time.Since(start)
	acct.Health.Store(state)

	if hc.writer != nil {
		_ = hc.writer.ReportAccountHealth(ctx, acct.Vendor, acct.AccountID, state)
	}
}