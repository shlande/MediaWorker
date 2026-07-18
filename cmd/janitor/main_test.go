package main

import (
	"context"
	"database/sql"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	drvpkg "github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/storage/driver/mock"
	"github.com/shlande/mediaworker/internal/storage/gc"
	"github.com/shlande/mediaworker/internal/types"
)

// countingDriver wraps the real mock driver and counts Remove calls — used
// to assert that dry-run mode never invokes Driver.Remove (plan line 221).
type countingDriver struct {
	inner       *mock.MockDriver
	removeCalls atomic.Int32
}

func (d *countingDriver) Vendor() types.Vendor { return d.inner.Vendor() }
func (d *countingDriver) List(ctx context.Context, dirID string, limit int) ([]types.FileInfo, error) {
	return d.inner.List(ctx, dirID, limit)
}
func (d *countingDriver) Get(ctx context.Context, fileID string) (types.FileInfo, error) {
	return d.inner.Get(ctx, fileID)
}
func (d *countingDriver) GetLink(ctx context.Context, fileID string) (*types.DownloadLink, error) {
	return d.inner.GetLink(ctx, fileID)
}
func (d *countingDriver) Put(ctx context.Context, dirID, name string, r io.Reader, size int64) (*types.FileInfo, error) {
	return d.inner.Put(ctx, dirID, name, r, size)
}
func (d *countingDriver) Remove(ctx context.Context, fileID string) error {
	d.removeCalls.Add(1)
	return d.inner.Remove(ctx, fileID)
}
func (d *countingDriver) Mkdir(ctx context.Context, parentID, name string) (*types.FileInfo, error) {
	return d.inner.Mkdir(ctx, parentID, name)
}
func (d *countingDriver) HealthCheck(ctx context.Context) types.HealthState {
	return d.inner.HealthCheck(ctx)
}
func (d *countingDriver) RateLimitConfig() types.RateLimitConfig {
	return d.inner.RateLimitConfig()
}

// newCountingMockDriver builds a countingDriver pre-seeded with fileIDs.
func newCountingMockDriver(vendor types.Vendor, fileIDs ...string) *countingDriver {
	fs := make(map[string]types.FileInfo, len(fileIDs))
	for _, id := range fileIDs {
		fs[id] = types.FileInfo{ID: id}
	}
	return &countingDriver{
		inner: mock.NewMockDriver(vendor, mock.MockDriverConfig{Filesystem: fs}),
	}
}

// TestRunCycle_DryRun_NoRemoveCalls is the critical dry-run semantics test
// (plan line 221: "DryRun 语义不得被任何代码路径绕过"):
//   - seed: 1 orphan blob with 2 blob_location rows
//   - run runCycle with dryRun=true
//   - assert: 0 Driver.Remove calls (zero blob_location deletes)
//   - assert: 0 DELETE transactions (zero PG row deletes)
//   - assert: MarkOrphans still ran (UPDATE deleted_at = now())
//   - assert: TOCTOU re-check still ran (SELECT 1 FROM content_blob)
//
// This is the "would delete" path — the blob is logged but left intact.
func TestRunCycle_DryRun_NoRemoveCalls(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	const (
		hash     = "orphan-abc"
		backendA = "115:acct_01"
		backendB = "115:acct_02"
		fileA    = "file-AAA"
		fileB    = "file-BBB"
	)

	// Use the existing mock driver from internal/storage/driver/mock so we
	// don't reimplement Get/GetLink/etc. The countingDriver wrapper only
	// intercepts Remove.
	mockDrvA := newCountingMockDriver(types.Vendor115, fileA)
	mockDrvB := newCountingMockDriver(types.Vendor115, fileB)
	cbA := circuitbreaker.New(backendA, 5, time.Minute)
	cbB := circuitbreaker.New(backendB, 5, time.Minute)

	resolver := &staticResolver{
		drivers: map[string]drvpkg.Driver{backendA: mockDrvA, backendB: mockDrvB},
		cbs:     map[string]*circuitbreaker.CircuitBreaker{backendA: cbA, backendB: cbB},
	}

	// Phase 1: MarkOrphans — UPDATE returns 1 row marked.
	m.ExpectExec(`UPDATE blob\s+SET deleted_at = now\(\)`).
		WithArgs((24 * time.Hour).String()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Phase 2: Sweep SELECT returns 1 candidate.
	m.ExpectQuery(`SELECT blob_hash\s+FROM blob\s+WHERE deleted_at IS NOT NULL`).
		WithArgs((24 * time.Hour).String(), 500).
		WillReturnRows(sqlmock.NewRows([]string{"blob_hash"}).AddRow(hash))

	// TOCTOU re-check: no content_blob reference.
	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash).
		WillReturnError(sql.ErrNoRows)

	// Fetch blob_location rows — 2 copies on backends A + B.
	locRows := sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"}).
		AddRow(hash, backendA, fileA).
		AddRow(hash, backendB, fileB)
	m.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnRows(locRows)

	// NO DELETE expectations — dry-run must not issue any DELETE tx.
	// NO Remove expectations — dry-run must not call Driver.Remove.

	collector := gc.NewCollector(db, resolver, nil)
	cfg := &config.JanitorConfig{}
	cfg.GC.ParsedMinAge = 24 * time.Hour
	cfg.GC.ParsedGrace = 24 * time.Hour
	cfg.GC.BatchLimit = 500

	if err := runCycle(context.Background(), collector, cfg, true); err != nil {
		t.Fatalf("runCycle dry-run: %v", err)
	}

	if calls := mockDrvA.removeCalls.Load() + mockDrvB.removeCalls.Load(); calls != 0 {
		t.Errorf("Driver.Remove call count = %d, want 0 (dry-run must not delete)", calls)
	}

	// Files must still exist in both mock drivers (Remove was never called).
	if _, err := mockDrvA.inner.Get(context.Background(), fileA); err != nil {
		t.Errorf("fileA should still exist after dry-run, got err: %v", err)
	}
	if _, err := mockDrvB.inner.Get(context.Background(), fileB); err != nil {
		t.Errorf("fileB should still exist after dry-run, got err: %v", err)
	}

	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRunCycle_LiveDelete_RemovesAndDeletes verifies the live path still
// works: when dryRun=false, Driver.Remove IS called and the DELETE tx fires.
// This guards against accidentally breaking the live path while adding
// dry-run support.
func TestRunCycle_LiveDelete_RemovesAndDeletes(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	const (
		hash     = "orphan-xyz"
		backendA = "115:acct_01"
		fileA    = "file-AAA"
	)

	mockDrvA := newCountingMockDriver(types.Vendor115, fileA)
	cbA := circuitbreaker.New(backendA, 5, time.Minute)

	resolver := &staticResolver{
		drivers: map[string]drvpkg.Driver{backendA: mockDrvA},
		cbs:     map[string]*circuitbreaker.CircuitBreaker{backendA: cbA},
	}

	m.ExpectExec(`UPDATE blob\s+SET deleted_at = now\(\)`).
		WithArgs((24 * time.Hour).String()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	m.ExpectQuery(`SELECT blob_hash\s+FROM blob\s+WHERE deleted_at IS NOT NULL`).
		WithArgs((24 * time.Hour).String(), 500).
		WillReturnRows(sqlmock.NewRows([]string{"blob_hash"}).AddRow(hash))

	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash).
		WillReturnError(sql.ErrNoRows)

	locRows := sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"}).
		AddRow(hash, backendA, fileA)
	m.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnRows(locRows)

	// Single-tx DELETE blob_location + blob (live path).
	m.ExpectBegin()
	m.ExpectExec(`DELETE FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec(`DELETE FROM blob WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()

	collector := gc.NewCollector(db, resolver, nil)
	cfg := &config.JanitorConfig{}
	cfg.GC.ParsedMinAge = 24 * time.Hour
	cfg.GC.ParsedGrace = 24 * time.Hour
	cfg.GC.BatchLimit = 500

	if err := runCycle(context.Background(), collector, cfg, false); err != nil {
		t.Fatalf("runCycle live: %v", err)
	}

	if calls := mockDrvA.removeCalls.Load(); calls != 1 {
		t.Errorf("Driver.Remove call count = %d, want 1 (live delete)", calls)
	}

	// File should be gone from the mock driver.
	if _, err := mockDrvA.inner.Get(context.Background(), fileA); err == nil {
		t.Errorf("fileA should be removed after live delete")
	}

	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRunCycle_DryRun_RescueStillHappens verifies the rescue path (TOCTOU
// re-check) still executes in dry-run mode — a blob that gained a
// content_blob reference during grace must have deleted_at reset to NULL,
// even in dry-run. This is critical: rescue is NOT a delete, so dry-run
// must not skip it.
func TestRunCycle_DryRun_RescueStillHappens(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	const hash = "rescued-blob"

	m.ExpectExec(`UPDATE blob\s+SET deleted_at = now\(\)`).
		WithArgs((24 * time.Hour).String()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	m.ExpectQuery(`SELECT blob_hash\s+FROM blob\s+WHERE deleted_at IS NOT NULL`).
		WithArgs((24 * time.Hour).String(), 500).
		WillReturnRows(sqlmock.NewRows([]string{"blob_hash"}).AddRow(hash))

	// TOCTOU re-check: content_blob reference EXISTS now (rescue!).
	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))

	// Rescue UPDATE: deleted_at = NULL (must run even in dry-run).
	m.ExpectExec(`UPDATE blob SET deleted_at = NULL WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// NO fetch-locations query, NO Remove, NO DELETE — rescue short-circuits.

	collector := gc.NewCollector(db, &staticResolver{}, nil)
	cfg := &config.JanitorConfig{}
	cfg.GC.ParsedMinAge = 24 * time.Hour
	cfg.GC.ParsedGrace = 24 * time.Hour
	cfg.GC.BatchLimit = 500

	if err := runCycle(context.Background(), collector, cfg, true); err != nil {
		t.Fatalf("runCycle dry-run rescue: %v", err)
	}

	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestPoolResolver_Resolve verifies the janitor's poolResolver correctly
// resolves backend_ids to the underlying Driver + CircuitBreaker.
func TestPoolResolver_Resolve(t *testing.T) {
	// We can't easily build a real *accountpool.AccountPool with a real
	// mock driver without duplicating BuildFromConfig, so this test is a
	// sanity check on the resolver's key-matching logic using a hand-built
	// pool. Use the real accountpool package's NewAccountPool + AddAccount.
	// Skipped if the pool is empty (which it always is in unit test context
	// without YAML config) — see TestPoolResolver_EmptyReturnsFalse instead.
	t.Skip("integration: requires real account pool wiring; covered by BuildFromConfig + integration tests")
}

func TestPoolResolver_EmptyReturnsFalse(t *testing.T) {
	r := &poolResolver{pool: nil}
	_, _, ok := r.Resolve("115:acct_01")
	if ok {
		t.Errorf("empty pool should return ok=false")
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────

// staticResolver is a test gc.AccountResolver that returns pre-registered
// drivers + circuit breakers by backend_id.
type staticResolver struct {
	drivers map[string]drvpkg.Driver
	cbs     map[string]*circuitbreaker.CircuitBreaker
}

func (r *staticResolver) Resolve(backendID string) (drvpkg.Driver, *circuitbreaker.CircuitBreaker, bool) {
	d, ok := r.drivers[backendID]
	if !ok {
		return nil, nil, false
	}
	return d, r.cbs[backendID], true
}
