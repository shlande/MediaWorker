package metadata

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// newPGMetadataClientWithDB is the test-only constructor that accepts a
// pre-configured *sql.DB (typically a sqlmock). The public constructor
// NewPGMetadataClient handles real PostgreSQL connections.
func newPGMetadataClientWithDB(db *sql.DB) *PGMetadataClient {
	return &PGMetadataClient{db: db}
}

// ─── GetContentMeta ──────────────────────────────────────────────────────────

func TestGetContentMeta_ReturnsContentMeta(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"content_id", "content_type", "type_metadata"}).
		AddRow("vid-001", "video", []byte(`{"codec":"h264"}`))
	mock.ExpectQuery(`SELECT content_id, content_type, type_metadata FROM content WHERE content_id = \$1`).
		WithArgs("vid-001").
		WillReturnRows(rows)

	got, err := client.GetContentMeta("vid-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ContentID != "vid-001" {
		t.Errorf("ContentID = %q, want %q", got.ContentID, "vid-001")
	}
	if got.ContentType != "video" {
		t.Errorf("ContentType = %q, want %q", got.ContentType, "video")
	}
	if string(got.TypeMetadata) != `{"codec":"h264"}` {
		t.Errorf("TypeMetadata = %q, want %q", got.TypeMetadata, `{"codec":"h264"}`)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetContentMeta_NotFoundWrapsErrNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT content_id, content_type, type_metadata FROM content WHERE content_id = \$1`).
		WithArgs("missing-id").
		WillReturnError(sql.ErrNoRows)

	_, err = client.GetContentMeta("missing-id")
	if err == nil {
		t.Fatal("expected error for not-found, got nil")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("expected sql.ErrNoRows in chain, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── GetTopContents ──────────────────────────────────────────────────────────

func TestGetTopContents_ReturnsSortedByPopularity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"content_id", "content_type", "type_metadata", "window_24h"}).
		AddRow("vid-001", "video", []byte(`{"codec":"h264"}`), int64(9500)).
		AddRow("vid-002", "image", []byte(`{"format":"webp"}`), int64(7200))
	mock.ExpectQuery(`SELECT c\.content_id, c\.content_type, c\.type_metadata, v\.window_24h`).
		WithArgs(10).
		WillReturnRows(rows)

	got, err := client.GetTopContents(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].ContentMeta.ContentID != "vid-001" {
		t.Errorf("first ContentID = %q, want vid-001", got[0].ContentMeta.ContentID)
	}
	if got[0].Popularity != 9500 {
		t.Errorf("first Popularity = %d, want 9500", got[0].Popularity)
	}
	if got[1].ContentMeta.ContentID != "vid-002" {
		t.Errorf("second ContentID = %q, want vid-002", got[1].ContentMeta.ContentID)
	}
	if got[1].Popularity != 7200 {
		t.Errorf("second Popularity = %d, want 7200", got[1].Popularity)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetTopContents_QueryErrorReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT c\.content_id`).
		WithArgs(5).
		WillReturnError(sql.ErrConnDone)

	_, err = client.GetTopContents(context.Background(), 5)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── GetSegmentLocations ─────────────────────────────────────────────────────

func TestGetSegmentLocations_ReturnsLocations(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"blob_hash", "vendor", "account_id", "file_id"}).
		AddRow("abc123", "s3", "acct-1", "path/to/seg1").
		AddRow("abc123", "gcs", "acct-2", "path/to/seg1")
	mock.ExpectQuery(`SELECT blob_hash, vendor, account_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs("abc123").
		WillReturnRows(rows)

	got, err := client.GetSegmentLocations("abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Vendor != "s3" {
		t.Errorf("first Vendor = %q, want s3", got[0].Vendor)
	}
	if got[1].Vendor != "gcs" {
		t.Errorf("second Vendor = %q, want gcs", got[1].Vendor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetSegmentLocations_EmptyWhenNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"blob_hash", "vendor", "account_id", "file_id"})
	mock.ExpectQuery(`SELECT blob_hash, vendor, account_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs("unknown-blob").
		WillReturnRows(rows)

	got, err := client.GetSegmentLocations("unknown-blob")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── GetPopularity24h ────────────────────────────────────────────────────────

func TestGetPopularity24h_ReturnsPopularity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"window_24h"}).AddRow(int64(8712))
	mock.ExpectQuery(`SELECT window_24h FROM video_popularity WHERE content_id = \$1`).
		WithArgs("vid-001").
		WillReturnRows(rows)

	got := client.GetPopularity24h("vid-001")
	if got != 8712.0 {
		t.Errorf("got %f, want 8712.0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetPopularity24h_NotFoundReturnsZero(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT window_24h FROM video_popularity WHERE content_id = \$1`).
		WithArgs("missing-id").
		WillReturnError(sql.ErrNoRows)

	got := client.GetPopularity24h("missing-id")
	if got != 0.0 {
		t.Errorf("got %f, want 0.0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetPopularity24h_DBErrorReturnsZero(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT window_24h FROM video_popularity WHERE content_id = \$1`).
		WithArgs("any-id").
		WillReturnError(sql.ErrConnDone)

	got := client.GetPopularity24h("any-id")
	if got != 0.0 {
		t.Errorf("got %f, want 0.0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
