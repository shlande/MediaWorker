package gc

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	drvpkg "github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/storage/driver/mock"
	"github.com/shlande/mediaworker/internal/types"
)

// fakeResolver is a test AccountResolver mapping backend_id → (Driver, CircuitBreaker).
type fakeResolver struct {
	drivers map[string]drvpkg.Driver
	cbs     map[string]*circuitbreaker.CircuitBreaker
}

func (r *fakeResolver) Resolve(backendID string) (drvpkg.Driver, *circuitbreaker.CircuitBreaker, bool) {
	d, ok := r.drivers[backendID]
	if !ok {
		return nil, nil, false
	}
	return d, r.cbs[backendID], true
}

// newMockCB creates a fresh circuit breaker for tests (threshold=1 → open on first failure).
func newMockCB() *circuitbreaker.CircuitBreaker {
	return circuitbreaker.New("test-acct", 1, 10*time.Minute)
}

// ─── Phase 1: MarkOrphans ────────────────────────────────────────────────────

// TestMarkOrphans_HappyPath verifies phase 1 marks orphaned blobs: issues a
// single UPDATE with the expected predicates and returns the affected count.
func TestMarkOrphans_HappyPath(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	m.ExpectExec(`UPDATE blob\s+SET deleted_at = now\(\)\s+WHERE deleted_at IS NULL`).
		WithArgs((24 * time.Hour).String()).
		WillReturnResult(sqlmock.NewResult(0, 3))

	c := NewCollector(db, &fakeResolver{}, nil)

	n, err := c.MarkOrphans(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("MarkOrphans: %v", err)
	}
	if n != 3 {
		t.Errorf("marked = %d, want 3", n)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestMarkOrphans_Idempotent verifies re-running phase 1 on already-marked
// rows returns 0 newly marked (the WHERE deleted_at IS NULL predicate excludes them).
func TestMarkOrphans_Idempotent(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	m.ExpectExec(`UPDATE blob\s+SET deleted_at = now\(\)`).
		WithArgs((24 * time.Hour).String()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	c := NewCollector(db, &fakeResolver{}, nil)

	n, err := c.MarkOrphans(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("MarkOrphans: %v", err)
	}
	if n != 0 {
		t.Errorf("marked = %d, want 0 on idempotent re-run", n)
	}
}

// TestMarkOrphans_DefaultMinAge verifies minAge<=0 falls back to DefaultMinAge.
func TestMarkOrphans_DefaultMinAge(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	m.ExpectExec(`UPDATE blob\s+SET deleted_at = now\(\)`).
		WithArgs(DefaultMinAge.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	c := NewCollector(db, &fakeResolver{}, nil)

	if _, err := c.MarkOrphans(context.Background(), 0); err != nil {
		t.Fatalf("MarkOrphans(0): %v", err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// ─── Phase 2: Sweep — 4 paths ────────────────────────────────────────────────

// expectSweepQuery sets up the canonical Sweep SELECT expectations.
func expectSweepQuery(m sqlmock.Sqlmock, hashes ...string) {
	rows := sqlmock.NewRows([]string{"blob_hash"})
	for _, h := range hashes {
		rows.AddRow(h)
	}
	m.ExpectQuery(`SELECT blob_hash\s+FROM blob\s+WHERE deleted_at IS NOT NULL`).
		WithArgs((24 * time.Hour).String(), 500).
		WillReturnRows(rows)
}

// TestSweep_HappyPath: orphan blob (no content_blob ref) → Driver.Remove
// called on each location → single-tx DELETE blob_location + blob.
func TestSweep_HappyPath(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	const (
		hash     = "abc123"
		backendA = "115:acct_01"
		backendB = "115:acct_02"
		fileA    = "file-AAA"
		fileB    = "file-BBB"
	)

	drvA := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{
		Filesystem: map[string]types.FileInfo{fileA: {ID: fileA}},
	})
	drvB := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{
		Filesystem: map[string]types.FileInfo{fileB: {ID: fileB}},
	})
	cbs := map[string]*circuitbreaker.CircuitBreaker{
		backendA: newMockCB(),
		backendB: newMockCB(),
	}

	expectSweepQuery(m, hash)

	// TOCTOU re-check: no content_blob reference.
	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash).
		WillReturnError(sql.ErrNoRows)

	// Fetch blob_location rows.
	locRows := sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"}).
		AddRow(hash, backendA, fileA).
		AddRow(hash, backendB, fileB)
	m.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnRows(locRows)

	// Single-tx DELETE blob_location + blob.
	m.ExpectBegin()
	m.ExpectExec(`DELETE FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnResult(sqlmock.NewResult(0, 2))
	m.ExpectExec(`DELETE FROM blob WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()

	c := NewCollector(db, &fakeResolver{
		drivers: map[string]drvpkg.Driver{backendA: drvA, backendB: drvB},
		cbs:     cbs,
	}, nil)

	res, err := c.Sweep(context.Background(), 24*time.Hour, 500)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Deleted != 1 {
		t.Errorf("Deleted = %d, want 1", res.Deleted)
	}
	if res.Rescued != 0 {
		t.Errorf("Rescued = %d, want 0", res.Rescued)
	}
	if res.Failed != 0 {
		t.Errorf("Failed = %d, want 0", res.Failed)
	}
	// Verify both files were removed from the mock filesystem.
	if _, err := drvA.Get(context.Background(), fileA); err == nil {
		t.Errorf("fileA should be removed from drvA")
	}
	if _, err := drvB.Get(context.Background(), fileB); err == nil {
		t.Errorf("fileB should be removed from drvB")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestSweep_Rescue: phase 2 finds a content_blob reference appeared during
// grace → deleted_at set NULL, driver NOT called.
func TestSweep_Rescue(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	const hash = "rescued-blob"
	const file = "file-XYZ"

	drv := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{
		Filesystem: map[string]types.FileInfo{file: {ID: file}},
	})

	expectSweepQuery(m, hash)

	// TOCTOU re-check: reference EXISTS now (rescue).
	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))

	// Rescue UPDATE: deleted_at = NULL.
	m.ExpectExec(`UPDATE blob SET deleted_at = NULL WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnResult(sqlmock.NewResult(0, 1))

	c := NewCollector(db, &fakeResolver{
		drivers: map[string]drvpkg.Driver{"115:acct_01": drv},
		cbs:     map[string]*circuitbreaker.CircuitBreaker{"115:acct_01": newMockCB()},
	}, nil)

	res, err := c.Sweep(context.Background(), 24*time.Hour, 500)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Rescued != 1 {
		t.Errorf("Rescued = %d, want 1", res.Rescued)
	}
	if res.Deleted != 0 {
		t.Errorf("Deleted = %d, want 0", res.Deleted)
	}
	if res.Failed != 0 {
		t.Errorf("Failed = %d, want 0", res.Failed)
	}
	// Driver.Remove should NOT have been called — file must still exist.
	if _, err := drv.Get(context.Background(), file); err != nil {
		t.Errorf("file should still exist (driver.Remove not called): %v", err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestSweep_DeleteFail_CircuitBreak: driver.Remove errors → account
// circuit-broken (ForceOpen'd), blob row preserved, Failed=1.
func TestSweep_DeleteFail_CircuitBreak(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	const (
		hash     = "fail-blob"
		backendA = "115:acct_01"
		fileA    = "file-AAA"
	)

	// Empty filesystem → Remove returns ErrNotFound.
	drvA := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{})
	cbA := newMockCB()

	expectSweepQuery(m, hash)

	// TOCTOU re-check: no reference.
	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash).
		WillReturnError(sql.ErrNoRows)

	// Single blob_location row.
	locRows := sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"}).
		AddRow(hash, backendA, fileA)
	m.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnRows(locRows)

	// NO DELETE expectations — blob row must be preserved.

	c := NewCollector(db, &fakeResolver{
		drivers: map[string]drvpkg.Driver{backendA: drvA},
		cbs:     map[string]*circuitbreaker.CircuitBreaker{backendA: cbA},
	}, nil)

	res, err := c.Sweep(context.Background(), 24*time.Hour, 500)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Failed != 1 {
		t.Errorf("Failed = %d, want 1", res.Failed)
	}
	if res.Deleted != 0 {
		t.Errorf("Deleted = %d, want 0 (blob row preserved on failure)", res.Deleted)
	}
	if res.Rescued != 0 {
		t.Errorf("Rescued = %d, want 0", res.Rescued)
	}
	if cbA.State() != circuitbreaker.StateOpen {
		t.Errorf("cbA.State = %d, want StateOpen (%d)", cbA.State(), circuitbreaker.StateOpen)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestSweep_AlreadyBrokenSkipped: after a single failure, a subsequent blob
// using the SAME backend_id is skipped without calling Driver.Remove again.
func TestSweep_AlreadyBrokenSkipped(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	const (
		hash1   = "fail-blob-1"
		hash2   = "fail-blob-2"
		backend = "115:acct_01"
		file    = "file-AAA"
	)

	drv := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}) // empty → Remove fails
	cb := newMockCB()

	expectSweepQuery(m, hash1, hash2)

	// Blob 1: TOCTOU no ref → fetch locations → Remove fails (circuit-break).
	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash1).
		WillReturnError(sql.ErrNoRows)
	m.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash1).
		WillReturnRows(sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"}).AddRow(hash1, backend, file))

	// Blob 2: TOCTOU no ref → fetch locations → backend already broken → fail.
	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash2).
		WillReturnError(sql.ErrNoRows)
	m.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash2).
		WillReturnRows(sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"}).AddRow(hash2, backend, file))

	// NO DELETE expectations — both blobs preserved.

	c := NewCollector(db, &fakeResolver{
		drivers: map[string]drvpkg.Driver{backend: drv},
		cbs:     map[string]*circuitbreaker.CircuitBreaker{backend: cb},
	}, nil)

	res, err := c.Sweep(context.Background(), 24*time.Hour, 500)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Failed != 2 {
		t.Errorf("Failed = %d, want 2", res.Failed)
	}
	if res.Deleted != 0 || res.Rescued != 0 {
		t.Errorf("Deleted=%d Rescued=%d, want 0/0", res.Deleted, res.Rescued)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestSweep_NoLocations: blob has no blob_location rows → directly DELETE blob row.
func TestSweep_NoLocations(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	const hash = "lonely-blob"

	expectSweepQuery(m, hash)
	m.ExpectQuery(`SELECT 1 FROM content_blob WHERE blob_hash = \$1 LIMIT 1`).
		WithArgs(hash).
		WillReturnError(sql.ErrNoRows)
	m.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnRows(sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"})) // empty

	// Single-tx DELETE (only blob, no locations).
	m.ExpectBegin()
	m.ExpectExec(`DELETE FROM blob_location WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnResult(sqlmock.NewResult(0, 0))
	m.ExpectExec(`DELETE FROM blob WHERE blob_hash = \$1`).
		WithArgs(hash).
		WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()

	c := NewCollector(db, &fakeResolver{}, nil)

	res, err := c.Sweep(context.Background(), 24*time.Hour, 500)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Deleted != 1 {
		t.Errorf("Deleted = %d, want 1", res.Deleted)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// TestParseBackendID covers the vendor:account_id parser.
func TestParseBackendID(t *testing.T) {
	cases := []struct {
		in      string
		vendor  string
		account string
		ok      bool
	}{
		{"115:acct_01", "115", "acct_01", true},
		{"baidu:foo", "baidu", "foo", true},
		{"onedrive:user:with:colons", "onedrive", "user:with:colons", true}, // SplitN 2
		{"noprefix", "", "", false},
		{":noVendor", "", "", false},
		{"noAccount:", "", "", false},
		{"", "", "", false},
	}
	for _, tc := range cases {
		v, a, ok := parseBackendID(tc.in)
		if v != tc.vendor || a != tc.account || ok != tc.ok {
			t.Errorf("parseBackendID(%q) = (%q,%q,%t), want (%q,%q,%t)",
				tc.in, v, a, ok, tc.vendor, tc.account, tc.ok)
		}
	}
}
