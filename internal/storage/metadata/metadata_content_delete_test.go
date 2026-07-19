package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Given a live content, When SoftDeleteContent runs, Then it updates
// deleted_at and unlinks content_blob in one committed transaction.
func TestSoftDeleteContent_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE content SET deleted_at = now\(\) WHERE content_id = \$1 AND deleted_at IS NULL`).
		WithArgs("vid-001").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM content_blob WHERE content_id = \$1`).
		WithArgs("vid-001").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()

	if err := client.SoftDeleteContent(context.Background(), "vid-001"); err != nil {
		t.Fatalf("SoftDeleteContent: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given an already-deleted content, When SoftDeleteContent runs again, Then
// both statements affect zero rows and no error is returned (idempotent).
func TestSoftDeleteContent_RepeatIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE content SET deleted_at`).
		WithArgs("vid-001").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM content_blob WHERE content_id = \$1`).
		WithArgs("vid-001").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	if err := client.SoftDeleteContent(context.Background(), "vid-001"); err != nil {
		t.Fatalf("repeat SoftDeleteContent must not error, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given a failing UPDATE, When SoftDeleteContent runs, Then the transaction
// rolls back and the error propagates (no content_blob unlink attempted).
func TestSoftDeleteContent_UpdateError_RollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE content SET deleted_at`).
		WithArgs("vid-001").
		WillReturnError(errors.New("connection lost"))
	mock.ExpectRollback()

	if err := client.SoftDeleteContent(context.Background(), "vid-001"); err == nil {
		t.Fatal("expected error from failing UPDATE, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given a failing content_blob unlink, When SoftDeleteContent runs, Then the
// transaction rolls back and the error propagates.
func TestSoftDeleteContent_UnlinkError_RollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE content SET deleted_at`).
		WithArgs("vid-001").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM content_blob`).
		WithArgs("vid-001").
		WillReturnError(errors.New("deadlock detected"))
	mock.ExpectRollback()

	if err := client.SoftDeleteContent(context.Background(), "vid-001"); err == nil {
		t.Fatal("expected error from failing DELETE, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
