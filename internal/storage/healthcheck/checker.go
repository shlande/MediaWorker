// Package healthcheck probes IDLE accounts to catch silent credential expiry.
//
// Usage is the primary health signal: an account with recent successful
// traffic is never probed. Only accounts idle longer than idleThreshold (or
// never used) get an active probe per tick. Probe failures (credential or
// network errors, surfaced by drivers as "degraded") are counted in-memory;
// reaching failureThreshold consecutive failures marks the account banned
// (a scheduling taint). Latency alone never affects state.
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

// HealthChecker probes idle accounts on a fixed cadence.
type HealthChecker struct {
	pool             *accountpool.AccountPool
	interval         time.Duration
	idleThreshold    time.Duration
	failureThreshold int
	writer           MetadataWriter
	log              *slog.Logger

	mu       sync.Mutex
	failures map[string]int // consecutive probe failures per account key
}

const defaultFailureThreshold = 3

// NewHealthChecker creates a checker that ticks every interval and only
// probes accounts idle longer than idleThreshold (never-used counts as idle).
func NewHealthChecker(pool *accountpool.AccountPool, interval, idleThreshold time.Duration, writer MetadataWriter) *HealthChecker {
	return &HealthChecker{
		pool:             pool,
		interval:         interval,
		idleThreshold:    idleThreshold,
		failureThreshold: defaultFailureThreshold,
		writer:           writer,
		failures:         map[string]int{},
	}
}

// Start begins the probe loop. Blocks until ctx is cancelled.
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

// checkAll probes all eligible (idle, untainted) accounts concurrently.
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

// checkOne probes a single account when it is idle and untainted, then applies
// the result: healthy/banned are stored and reported; failures only increment
// the consecutive-failure counter until the threshold trips a ban.
func (hc *HealthChecker) checkOne(ctx context.Context, acct *accountpool.Account) {
	log := hc.log
	if log == nil {
		log = slog.Default()
	}

	prev, _ := acct.Health.Load().(types.HealthState)
	if prev.Taint() != nil {
		return // tainted accounts need manual unban, probes must not overwrite
	}
	if lastUsed := acct.LastUsedAt(); !lastUsed.IsZero() && time.Since(lastUsed) < hc.idleThreshold {
		return // recent traffic is the health signal
	}

	start := time.Now()
	res := acct.Driver.HealthCheck(ctx)
	res.LastCheck = time.Now()
	res.Latency = time.Since(start)
	key := string(acct.Vendor) + ":" + acct.AccountID

	switch res.State {
	case "healthy", "banned":
		hc.resetFailures(key)
		acct.Health.Store(res)
		hc.report(ctx, acct, res)
	default: // probe failure (driver reports "degraded" on credential/network errors)
		n := hc.incFailures(key)
		log.Info("health check: idle probe failed",
			"vendor", acct.Vendor, "account", acct.AccountID,
			"consecutive_failures", n, "error_msg", res.ErrorMsg)
		if n < hc.failureThreshold {
			return
		}
		hc.resetFailures(key)
		banned := types.HealthState{
			State:     "banned",
			LastCheck: res.LastCheck,
			Latency:   res.Latency,
			ErrorMsg:  res.ErrorMsg,
		}
		acct.Health.Store(banned)
		hc.report(ctx, acct, banned)
		log.Info("health check: account banned after consecutive probe failures",
			"vendor", acct.Vendor, "account", acct.AccountID,
			"consecutive_failures", n)
	}
}

func (hc *HealthChecker) report(ctx context.Context, acct *accountpool.Account, state types.HealthState) {
	if hc.writer != nil {
		_ = hc.writer.ReportAccountHealth(ctx, acct.Vendor, acct.AccountID, state)
	}
}

func (hc *HealthChecker) incFailures(key string) int {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.failures[key]++
	return hc.failures[key]
}

func (hc *HealthChecker) resetFailures(key string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	delete(hc.failures, key)
}
