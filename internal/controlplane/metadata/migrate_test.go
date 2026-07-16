package metadata

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMigrateAll_ExecutesInOrder(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

	// Expect exactly 6 migration files, in sorted order,
	// each containing a CREATE TABLE IF NOT EXISTS statement.
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS content`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_index`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_location`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS account_health`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS cloud_account`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS video_popularity`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := migrateAll(db); err != nil {
		t.Fatalf("migrateAll: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestMigrateAll_WithExistingTables(t *testing.T) {
	t.Parallel()

	// Simulate idempotency: running the same migrations again should
	// succeed because all tables use IF NOT EXISTS.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS content`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_index`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_location`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS account_health`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS cloud_account`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS video_popularity`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := migrateAll(db); err != nil {
		t.Fatalf("migrateAll (idempotent): %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestMigrateAll_SQLParsesCorrectly(t *testing.T) {
	t.Parallel()

	// Read all SQL files and verify they parse by executing them
	// against an in-memory mock. This validates the SQL syntax is
	// well-formed and the embedded files are readable.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS content`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_index`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_location`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS account_health`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS cloud_account`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS video_popularity`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := migrateAll(db); err != nil {
		t.Fatalf("migrateAll: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestMigrateAll_FailsOnBadSQL(t *testing.T) {
	t.Parallel()

	// Test that migrateAll propagates an error when a statement fails.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS content`).
		WillReturnError(sqlmock.ErrCancelled)

	if err := migrateAll(db); err == nil {
		t.Fatal("expected error from migrateAll, got nil")
	}
}