// Package gc implements the janitor's two-phase soft-delete garbage collector
// for orphaned content-addressed blobs.
//
// Two phases (plan line 211):
//
//  1. MarkOrphans (soft-mark): a single UPDATE marks blob rows whose
//     deleted_at IS NULL, that have no content_blob reference, and whose
//     created_at is older than minAge (default 24h — protects the in-flight
//     ingest transaction window).
//
//  2. Sweep (hard-delete): processes rows whose deleted_at is older than
//     grace (default 24h). For each blob_hash it first re-checks content_blob
//     (TOCTOU protection, Metis F3): if a reference appeared during the grace
//     window (new ingest hit dedup), deleted_at is reset to NULL and the blob
//     is rescued. Otherwise all blob_location copies are deleted via the
//     matching account's Driver.Remove; on full success a single transaction
//     DELETEs blob_location + blob. A single-account delete failure
//     circuit-breaks that account for this run, logs, and continues.
//
// Class B (drive reconciliation) is intentionally NOT implemented: Driver.List
// is non-recursive and the per-hash directory upload layout (cmd/ingest-worker
// Put's dirID=blobHash) makes full enumeration infeasible. See TODO below.
//
// This package does NOT host an HTTP server (plan line 212).
package gc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

// TODO(gc): Class B drive reconciliation requires flat upload layout or drive
// index. Driver.List is non-recursive + per-hash directory upload layout
// (cmd/ingest-worker/main.go:203, Put's dirID=blobHash) makes full enumeration
// infeasible. Documented in T21.

// DefaultMinAge is the default minimum age a blob must reach before MarkOrphans
// will consider it (guards the in-flight ingest transaction window).
const DefaultMinAge = 24 * time.Hour

// DefaultGrace is the default grace window between soft-mark and hard-delete.
// A blob rescued (content_blob reference appeared) during this window has its
// deleted_at reset to NULL and is not deleted.
const DefaultGrace = 24 * time.Hour

// DefaultBatchLimit is the default maximum number of blob_hashes Sweep
// processes in a single invocation.
const DefaultBatchLimit = 500

// AccountResolver looks up the storage Account for a backend_id of the form
// "vendor:account_id". The gc package defines this narrow interface so tests
// can inject a fake without pulling in the full accountpool package.
//
// In production this is satisfied by a thin adapter around
// *accountpool.AccountPool — see AccountResolverFromPool in this file.
type AccountResolver interface {
	// Resolve returns the Driver and circuit breaker for the given backend_id.
	// Returns ok=false if no account matches (e.g. credential removed).
	Resolve(backendID string) (d driver.Driver, cb *circuitbreaker.CircuitBreaker, ok bool)
}

// ResolverFunc is a function adapter for AccountResolver.
type ResolverFunc func(backendID string) (driver.Driver, *circuitbreaker.CircuitBreaker, bool)

// Resolve implements AccountResolver.
func (f ResolverFunc) Resolve(backendID string) (driver.Driver, *circuitbreaker.CircuitBreaker, bool) {
	return f(backendID)
}

// Collector runs the two-phase soft-delete GC over the blob table.
type Collector struct {
	db       *sql.DB
	resolver AccountResolver
	logger   *slog.Logger
}

// NewCollector constructs a Collector. db must be a live PostgreSQL handle
// (PGMetadataClient.DB() in production). resolver maps backend_id → driver+CB.
// logger may be nil — a default slog.Default() is used.
func NewCollector(db *sql.DB, resolver AccountResolver, logger *slog.Logger) *Collector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Collector{db: db, resolver: resolver, logger: logger}
}

// MarkOrphans runs phase 1: soft-marks orphaned blobs (no content_blob
// reference, deleted_at IS NULL, created_at older than minAge) by setting
// deleted_at = now(). Returns the number of rows newly marked.
//
// minAge <= 0 falls back to DefaultMinAge. Idempotent: re-running on already
// marked rows marks 0 (the WHERE deleted_at IS NULL predicate excludes them).
func (c *Collector) MarkOrphans(ctx context.Context, minAge time.Duration) (int, error) {
	if minAge <= 0 {
		minAge = DefaultMinAge
	}
	res, err := c.db.ExecContext(ctx, `
UPDATE blob
   SET deleted_at = now()
 WHERE deleted_at IS NULL
   AND NOT EXISTS (SELECT 1 FROM content_blob cb WHERE cb.blob_hash = blob.blob_hash)
   AND created_at < now() - $1::interval
`, minAge.String())
	if err != nil {
		return 0, fmt.Errorf("gc: mark orphans: %w", err)
	}
	n, _ := res.RowsAffected()
	c.logger.Info("gc phase 1 (mark)", "marked", n, "min_age", minAge.String())
	return int(n), nil
}

// SweepResult is the structured outcome of a Sweep run.
type SweepResult struct {
	Rescued int // content_blob reference appeared during grace → deleted_at reset
	Deleted int // all copies removed + blob_location + blob row deleted
	Failed  int // at least one copy delete failed (account circuit-broken, row preserved)
}

// Sweep runs phase 2: for each blob whose deleted_at is older than grace,
// re-check content_blob (TOCTOU), then delete all blob_location copies via
// Driver.Remove, then single-tx DELETE blob_location + blob.
//
// grace <= 0 falls back to DefaultGrace. batchLimit <= 0 falls back to
// DefaultBatchLimit.
//
// A single-account delete failure circuit-breaks that account for the rest of
// this run (its ForceOpen is called); the blob is counted as Failed and will
// be retried on the next Sweep. After a failed copy, remaining copies of the
// same blob are NOT attempted in this run (the blob is left intact for retry).
func (c *Collector) Sweep(ctx context.Context, grace time.Duration, batchLimit int) (SweepResult, error) {
	return c.sweep(ctx, grace, batchLimit, false)
}

// SweepWithDryRun is the dry-run-aware variant of Sweep. When dryRun=true,
// for each candidate blob the collector:
//   - still performs the TOCTOU re-check (rescue path is real — a rescued
//     blob's deleted_at is reset to NULL so it stops being a candidate);
//   - logs `would delete blob=<hash> locations=N backends=[...]` with the
//     resolved backend_ids (one log line per blob);
//   - does NOT call Driver.Remove on any copy;
//   - does NOT issue the single-tx DELETE blob_location + blob.
//
// The returned SweepResult counts "Deleted" as the number of blobs that
// WOULD have been deleted (i.e. had ≥0 locations and passed TOCTOU). This
// lets operators see how many blobs a real run would reclaim.
//
// Plan line 221: "DryRun 语义不得被任何代码路径绕过". This method is the ONLY
// dry-run entrypoint — Sweep() above ALWAYS deletes (dryRun=false hard-coded).
// The janitor service MUST route through SweepWithDryRun when DryRun=true and
// through Sweep when DryRun=false. There is no other code path.
func (c *Collector) SweepWithDryRun(ctx context.Context, grace time.Duration, batchLimit int, dryRun bool) (SweepResult, error) {
	return c.sweep(ctx, grace, batchLimit, dryRun)
}

// sweep is the unified implementation shared by Sweep and SweepWithDryRun.
func (c *Collector) sweep(ctx context.Context, grace time.Duration, batchLimit int, dryRun bool) (SweepResult, error) {
	if grace <= 0 {
		grace = DefaultGrace
	}
	if batchLimit <= 0 {
		batchLimit = DefaultBatchLimit
	}

	// Circuit-broken accounts for THIS run. ForceOpen is called on the CB so
	// concurrent code (read path, ingest) also sees the open state, but we
	// also track them locally so we skip without re-attempting the CB Call.
	brokenAccounts := make(map[string]struct{})

	rows, err := c.db.QueryContext(ctx, `
SELECT blob_hash
  FROM blob
 WHERE deleted_at IS NOT NULL
   AND deleted_at < now() - $1::interval
 ORDER BY deleted_at ASC
 LIMIT $2
`, grace.String(), batchLimit)
	if err != nil {
		return SweepResult{}, fmt.Errorf("gc: sweep query: %w", err)
	}
	defer rows.Close()

	var blobHashes []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return SweepResult{}, fmt.Errorf("gc: sweep scan: %w", err)
		}
		blobHashes = append(blobHashes, h)
	}
	if err := rows.Err(); err != nil {
		return SweepResult{}, fmt.Errorf("gc: sweep iterate: %w", err)
	}

	var result SweepResult
	for _, hash := range blobHashes {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		rescued, err := c.processOne(ctx, hash, brokenAccounts, dryRun)
		if err != nil {
			result.Failed++
			c.logger.Error("gc phase 2: process blob failed", "blob_hash", hash, "err", err, "dry_run", dryRun)
			continue
		}
		if rescued {
			result.Rescued++
			continue
		}
		result.Deleted++
	}

	mode := "live"
	if dryRun {
		mode = "dry-run"
	}
	c.logger.Info("gc phase 2 (sweep)",
		"rescued", result.Rescued,
		"deleted", result.Deleted,
		"failed", result.Failed,
		"grace", grace.String(),
		"batch_limit", batchLimit,
		"candidates", len(blobHashes),
		"mode", mode,
	)
	return result, nil
}

// processOne handles a single blob_hash. Returns rescued=true if the blob was
// rescued (content_blob reference appeared), or an error if processing failed
// (account circuit-broken or DB error). On success returns (false, nil).
//
// When dryRun=true the rescue path still executes for real (a rescued blob's
// deleted_at must be reset to NULL so it stops being a candidate), but the
// delete path only logs `would delete blob=... locations=N backends=[...]`
// and returns without calling Driver.Remove or issuing the DELETE tx.
func (c *Collector) processOne(ctx context.Context, hash string, broken map[string]struct{}, dryRun bool) (bool, error) {
	// TOCTOU re-check: did a content_blob reference appear during the grace
	// window? If so, rescue the blob.
	referenced, err := c.isReferenced(ctx, hash)
	if err != nil {
		return false, fmt.Errorf("re-check content_blob: %w", err)
	}
	if referenced {
		if _, err := c.db.ExecContext(ctx,
			`UPDATE blob SET deleted_at = NULL WHERE blob_hash = $1`, hash,
		); err != nil {
			return false, fmt.Errorf("rescue update: %w", err)
		}
		c.logger.Info("gc phase 2: rescued (content_blob reference appeared)", "blob_hash", hash, "dry_run", dryRun)
		return true, nil
	}

	locations, err := c.fetchLocations(ctx, hash)
	if err != nil {
		return false, fmt.Errorf("fetch locations: %w", err)
	}

	if dryRun {
		backends := make([]string, 0, len(locations))
		for _, loc := range locations {
			backends = append(backends, loc.BackendID)
		}
		c.logger.Info("gc phase 2: would delete (dry-run)",
			"blob", hash,
			"locations", len(locations),
			"backends", backends,
		)
		return false, nil
	}

	if len(locations) == 0 {
		if err := c.deleteBlobTx(ctx, hash); err != nil {
			return false, fmt.Errorf("delete blob (no locations): %w", err)
		}
		return false, nil
	}

	for _, loc := range locations {
		if _, broken := broken[loc.BackendID]; broken {
			return false, fmt.Errorf("account %s already circuit-broken this run", loc.BackendID)
		}
		vendor, accountID, ok := parseBackendID(loc.BackendID)
		if !ok {
			c.logger.Warn("gc: malformed backend_id, skipping copy",
				"blob_hash", hash, "backend_id", loc.BackendID)
			return false, fmt.Errorf("malformed backend_id %q", loc.BackendID)
		}
		_ = vendor
		d, cb, ok := c.resolver.Resolve(loc.BackendID)
		if !ok {
			c.logger.Warn("gc: no account for backend_id, skipping copy",
				"blob_hash", hash, "backend_id", loc.BackendID, "vendor", vendor, "account_id", accountID)
			return false, fmt.Errorf("no account for backend_id %q", loc.BackendID)
		}
		if err := d.Remove(ctx, loc.FileID); err != nil {
			if cb != nil {
				cb.ForceOpen()
			}
			broken[loc.BackendID] = struct{}{}
			c.logger.Error("gc: driver.Remove failed, circuit-breaking account",
				"blob_hash", hash,
				"backend_id", loc.BackendID,
				"vendor", vendor,
				"account_id", accountID,
				"file_id", loc.FileID,
				"err", err,
			)
			return false, fmt.Errorf("remove on %s: %w", loc.BackendID, err)
		}
	}

	if err := c.deleteBlobTx(ctx, hash); err != nil {
		return false, fmt.Errorf("delete blob tx: %w", err)
	}
	return false, nil
}

// isReferenced returns true if a content_blob row exists for the given hash.
func (c *Collector) isReferenced(ctx context.Context, hash string) (bool, error) {
	var one int
	err := c.db.QueryRowContext(ctx,
		`SELECT 1 FROM content_blob WHERE blob_hash = $1 LIMIT 1`, hash,
	).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

// fetchLocations returns all blob_location rows for the given hash.
func (c *Collector) fetchLocations(ctx context.Context, hash string) ([]types.BlobLocation, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = $1`,
		hash,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.BlobLocation
	for rows.Next() {
		var loc types.BlobLocation
		if err := rows.Scan(&loc.BlobHash, &loc.BackendID, &loc.FileID); err != nil {
			return nil, err
		}
		out = append(out, loc)
	}
	return out, rows.Err()
}

// deleteBlobTx DELETEs blob_location + blob in a single transaction.
func (c *Collector) deleteBlobTx(ctx context.Context, hash string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx,
		`DELETE FROM blob_location WHERE blob_hash = $1`, hash,
	); err != nil {
		return fmt.Errorf("delete blob_location: %w", err)
	}
	if _, err = tx.ExecContext(ctx,
		`DELETE FROM blob WHERE blob_hash = $1`, hash,
	); err != nil {
		return fmt.Errorf("delete blob: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// parseBackendID splits "vendor:account_id" into its parts.
// Returns ok=false if the format is wrong (len != 2 after split).
func parseBackendID(backendID string) (vendor string, accountID string, ok bool) {
	parts := strings.SplitN(backendID, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}
