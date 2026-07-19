package metadata

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

// expectAllMigrations registers sqlmock expectations for every embedded
// migration file in sorted order (001-015). Called once per MigrateAll run.
func expectAllMigrations(mock sqlmock.Sqlmock) {
	for _, pattern := range []string{
		`CREATE TABLE IF NOT EXISTS content`,
		`CREATE TABLE IF NOT EXISTS blob_index`,
		`CREATE TABLE IF NOT EXISTS blob_location`,
		`CREATE TABLE IF NOT EXISTS account_health`,
		`CREATE TABLE IF NOT EXISTS cloud_account`,
		`CREATE TABLE IF NOT EXISTS video_popularity`,
		`CREATE TABLE IF NOT EXISTS blob \(`,
		`CREATE TABLE IF NOT EXISTS content_blob`,
		`DO \$\$`,
		`CREATE TABLE IF NOT EXISTS blob_location_v2`,
		`DO \$\$`,
		`ALTER TABLE blob_location_v2`,
		`DROP TABLE IF EXISTS blob_index`,
		`ALTER TABLE blob ADD COLUMN IF NOT EXISTS deleted_at`,
		`CREATE TABLE IF NOT EXISTS app_user`,
		`CREATE TABLE IF NOT EXISTS node_status_history`,
		`CREATE TABLE IF NOT EXISTS alert_events`,
	} {
		mock.ExpectExec(pattern).WillReturnResult(sqlmock.NewResult(0, 0))
	}
}

// ─── MigrateAll idempotency (015 app_user) ─────────────────────────────────────

func TestMigrateAll_RunTwiceIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Simulate a restart: MigrateAll runs again over an already-migrated DB.
	expectAllMigrations(mock)
	expectAllMigrations(mock)

	if err := MigrateAll(db); err != nil {
		t.Fatalf("first MigrateAll: %v", err)
	}
	if err := MigrateAll(db); err != nil {
		t.Fatalf("second MigrateAll (restart idempotency): %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── CreateUser / GetUserByUsername ───────────────────────────────────────────

func TestAppUser_CreateThenGetRoundtrip(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)
	ctx := context.Background()

	mock.ExpectExec(`INSERT INTO app_user \(user_id, username, password_hash, roles\)`).
		WithArgs(sqlmock.AnyArg(), "alice", "$2a$10$hash", pq.Array([]string{"admin", "operator"})).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := client.CreateUser(ctx, "alice", "$2a$10$hash", []string{"admin", "operator"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	rows := sqlmock.NewRows([]string{"user_id", "password_hash", "roles", "disabled"}).
		AddRow("11111111-1111-1111-1111-111111111111", "$2a$10$hash", "{admin,operator}", false)
	mock.ExpectQuery(`SELECT user_id, password_hash, roles, disabled FROM app_user WHERE username = \$1`).
		WithArgs("alice").
		WillReturnRows(rows)

	userID, passwordHash, roles, disabled, err := client.GetUserByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("GetUserByUsername: %v", err)
	}
	if userID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("userID = %q, want fixed uuid", userID)
	}
	if passwordHash != "$2a$10$hash" {
		t.Errorf("passwordHash = %q, want bcrypt hash passthrough", passwordHash)
	}
	if len(roles) != 2 || roles[0] != "admin" || roles[1] != "operator" {
		t.Errorf("roles = %v, want [admin operator]", roles)
	}
	if disabled {
		t.Error("disabled = true, want false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestAppUser_CreateDuplicateUsernameFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectExec(`INSERT INTO app_user \(user_id, username, password_hash, roles\)`).
		WithArgs(sqlmock.AnyArg(), "alice", "$2a$10$hash", pq.Array([]string{"admin"})).
		WillReturnError(errors.New(`pq: duplicate key value violates unique constraint "app_user_username_key"`))

	err = client.CreateUser(context.Background(), "alice", "$2a$10$hash", []string{"admin"})
	if err == nil {
		t.Fatal("expected duplicate-username error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestAppUser_GetByUsernameNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT user_id, password_hash, roles, disabled FROM app_user WHERE username = \$1`).
		WithArgs("ghost").
		WillReturnError(sql.ErrNoRows)

	_, _, _, _, err = client.GetUserByUsername(context.Background(), "ghost")
	if err == nil {
		t.Fatal("expected not-found error, got nil")
	}
	if !errors.Is(err, ErrUserNotFound) {
		t.Errorf("expected ErrUserNotFound in chain, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── CountUsers ────────────────────────────────────────────────────────────────

func TestAppUser_CountUsers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT count\(\*\) FROM app_user`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	n, err := client.CountUsers(context.Background())
	if err != nil {
		t.Fatalf("CountUsers: %v", err)
	}
	if n != 3 {
		t.Errorf("n = %d, want 3", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestAppUser_CountUsersEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT count\(\*\) FROM app_user`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	n, err := client.CountUsers(context.Background())
	if err != nil {
		t.Fatalf("CountUsers: %v", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
