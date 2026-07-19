// Janitor: standalone two-phase garbage collector for orphaned
// content-addressed blobs. NOT merged into control-plane or ingest-worker
// (plan line 221 — independent service).
//
// Two run modes:
//   - Once (`-once`): run one GC cycle (MarkOrphans + Sweep) then exit.
//     Exit 0 on success, 1 on error.
//   - Interval (default): run a cycle every gc.interval, until SIGINT/SIGTERM.
//
// DryRun defaults to TRUE (config-level safety guard, plan line 221).
// In dry-run mode Sweep only logs `would delete blob=... locations=N
// backends=[...]` — Driver.Remove and PG DELETE are never called. An explicit
// `gc.dry_run: false` in YAML or `-dry-run=false` on the CLI is required to
// actually delete.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/storage/gc"
	"github.com/shlande/mediaworker/internal/storage/metadata"
)

func main() {
	configPath := flag.String("config", "configs/janitor.yaml", "path to janitor YAML config")
	onceFlag := flag.Bool("once", false, "run one GC cycle and exit (exit 0 success / 1 error); overrides config gc.once")
	dryRunFlag := flag.Bool("dry-run", true, "dry-run mode: log what would be deleted without actually deleting; overrides config gc.dry_run")
	flag.Parse()

	if err := run(*configPath, *onceFlag, *dryRunFlag); err != nil {
		slog.Error("janitor fatal", "err", err)
		os.Exit(1)
	}
}

func run(configPath string, onceFlag, dryRunFlag bool) error {
	cfg, err := config.LoadJanitorConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// CLI flags override config. The `-dry-run` flag defaults to true on the
	// CLI too — an operator MUST explicitly pass `-dry-run=false` to delete.
	// This is the second layer of the "default dry-run" guarantee (plan line
	// 221): even if a YAML config silently sets dry_run: false, the CLI flag
	// still defaults true unless the operator explicitly opts out.
	dryRun := dryRunFlag
	once := onceFlag || cfg.GC.EffectiveOnce()

	slog.Info("janitor starting",
		"once", once,
		"dry_run", dryRun,
		"interval", cfg.GC.ParsedInterval.String(),
		"min_age", cfg.GC.ParsedMinAge.String(),
		"grace", cfg.GC.ParsedGrace.String(),
		"batch_limit", cfg.GC.BatchLimit,
	)

	mc, err := metadata.NewPGMetadataClient(cfg.Metadata.PGDSN)
	if err != nil {
		return fmt.Errorf("metadata client: %w", err)
	}
	defer func() { _ = mc.Close() }()

	pool := accountpool.BuildFromConfig(cfg.Storage.ToAccountPoolConfig(), nil)
	if len(pool.SnapshotAccounts()) == 0 {
		return fmt.Errorf("no enabled cloud accounts in config — at least one is required")
	}
	resolver := newPoolResolver(pool)

	collector := gc.NewCollector(mc.DB(), resolver, nil)

	if once {
		return runOnce(collector, cfg, dryRun)
	}
	return runInterval(collector, cfg, dryRun)
}

// runOnce executes one full GC cycle (phase 1 + phase 2) and exits.
func runOnce(collector *gc.Collector, cfg *config.JanitorConfig, dryRun bool) error {
	ctx := context.Background()
	if err := runCycle(ctx, collector, cfg, dryRun); err != nil {
		slog.Error("janitor once: cycle failed", "err", err)
		return err
	}
	slog.Info("janitor once: cycle complete")
	return nil
}

// runInterval runs GC cycles on a ticker until SIGINT/SIGTERM.
func runInterval(collector *gc.Collector, cfg *config.JanitorConfig, dryRun bool) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ticker := time.NewTicker(cfg.GC.ParsedInterval)
	defer ticker.Stop()

	slog.Info("janitor interval mode: waiting for first tick", "interval", cfg.GC.ParsedInterval.String())

	// Run an immediate first cycle on startup, then on each tick.
	runCycleIfNotCancelled(ctx, collector, cfg, dryRun)

	for {
		select {
		case <-ctx.Done():
			slog.Info("janitor shutting down", "reason", ctx.Err())
			return nil
		case <-ticker.C:
			runCycleIfNotCancelled(ctx, collector, cfg, dryRun)
		}
	}
}

func runCycleIfNotCancelled(ctx context.Context, collector *gc.Collector, cfg *config.JanitorConfig, dryRun bool) {
	if err := ctx.Err(); err != nil {
		return
	}
	if err := runCycle(ctx, collector, cfg, dryRun); err != nil {
		slog.Error("janitor: cycle failed", "err", err)
	}
}

// runCycle executes one MarkOrphans + Sweep cycle. DryRun is passed through
// to SweepWithDryRun — there is NO code path that calls Sweep (the live
// deleter) when DryRun=true. Plan line 221: "DryRun 语义不得被任何代码路径绕过".
func runCycle(ctx context.Context, collector *gc.Collector, cfg *config.JanitorConfig, dryRun bool) error {
	cycleStart := time.Now()

	marked, err := collector.MarkOrphans(ctx, cfg.GC.ParsedMinAge)
	if err != nil {
		return fmt.Errorf("phase 1 mark: %w", err)
	}
	slog.Info("janitor cycle: phase 1 complete", "marked", marked)

	res, err := collector.SweepWithDryRun(ctx, cfg.GC.ParsedGrace, cfg.GC.BatchLimit, dryRun)
	if err != nil {
		return fmt.Errorf("phase 2 sweep: %w", err)
	}
	slog.Info("janitor cycle: phase 2 complete",
		"rescued", res.Rescued,
		"deleted", res.Deleted,
		"failed", res.Failed,
		"dry_run", dryRun,
		"duration", time.Since(cycleStart).String(),
	)
	return nil
}

// poolResolver adapts *accountpool.AccountPool to gc.AccountResolver.
// Each call to Resolve looks up the account by backend_id (vendor:account_id)
// and returns its Driver + CircuitBreaker.
type poolResolver struct {
	pool *accountpool.AccountPool
}

func newPoolResolver(pool *accountpool.AccountPool) *poolResolver {
	return &poolResolver{pool: pool}
}

func (r *poolResolver) Resolve(backendID string) (driver.Driver, *circuitbreaker.CircuitBreaker, bool) {
	if r.pool == nil {
		return nil, nil, false
	}
	for _, acct := range r.pool.SnapshotAccounts() {
		key := string(acct.Vendor) + ":" + acct.AccountID
		if key == backendID {
			// accountpool.Account.CB is the interface type; the concrete
			// *circuitbreaker.CircuitBreaker is what gc needs. The pool
			// always stores concrete CBs (BuildFromConfig wires them), so
			// the type assertion is safe in production. A nil CB is handled
			// gracefully by gc (cb == nil guard).
			var cb *circuitbreaker.CircuitBreaker
			if acct.CB != nil {
				if concrete, ok := acct.CB.(*circuitbreaker.CircuitBreaker); ok {
					cb = concrete
				}
			}
			return acct.Driver, cb, true
		}
	}
	return nil, nil, false
}

// Ensure poolResolver satisfies gc.AccountResolver at compile time.
var _ gc.AccountResolver = (*poolResolver)(nil)
