// Package healthcheck periodically probes all accounts in the pool for health status.
package healthcheck

import (
	"context"
	"log/slog"
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
	pool             *accountpool.AccountPool
	interval         time.Duration
	writer           MetadataWriter
	log              *slog.Logger
	recoverThreshold int // consecutive healthy probes required to auto-recover from degraded
}

const defaultRecoverThreshold = 3

// NewHealthChecker creates a new HealthChecker.
func NewHealthChecker(pool *accountpool.AccountPool, interval time.Duration, writer MetadataWriter) *HealthChecker {
	return &HealthChecker{
		pool:             pool,
		interval:         interval,
		writer:           writer,
		recoverThreshold: defaultRecoverThreshold,
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
	log := hc.log
	if log == nil {
		log = slog.Default()
	}

	start := time.Now()
	newState := acct.Driver.HealthCheck(ctx)
	newState.LastCheck = time.Now()
	newState.Latency = time.Since(start)

	prev, _ := acct.Health.Load().(types.HealthState)

	if prev.State == "degraded" && prev.Recoverable && newState.State == "healthy" {
		newState.ConsecutiveHealthy = prev.ConsecutiveHealthy + 1
		newState.Recoverable = true

		if newState.ConsecutiveHealthy >= hc.recoverThreshold {
			log.Info("health check: account recovered to healthy after consecutive good probes",
				"vendor", acct.Vendor, "account", acct.AccountID,
				"consecutive_healthy", newState.ConsecutiveHealthy)
			newState.ErrorMsg = ""
		} else {
			newState.State = "degraded"
			newState.ErrorMsg = prev.ErrorMsg
		}
	} else if newState.State == "degraded" {
		newState.Recoverable = true

		log.Info("health check: account degraded",
			"vendor", acct.Vendor, "account", acct.AccountID,
			"error_msg", newState.ErrorMsg)
	} else if prev.State == "banned" || (prev.State == "degraded" && !prev.Recoverable) {
		return
	}

	acct.Health.Store(newState)

	if hc.writer != nil {
		_ = hc.writer.ReportAccountHealth(ctx, acct.Vendor, acct.AccountID, newState)
	}
}
