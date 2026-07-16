package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/shlande/mediaworker/internal/types"
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

	rows := sqlmock.NewRows([]string{"content_id", "blob_hash", "vendor", "account_id", "file_id"}).
		AddRow("cnt-001", "abc123", "s3", "acct-1", "path/to/seg1").
		AddRow("cnt-001", "abc123", "gcs", "acct-2", "path/to/seg1")
	mock.ExpectQuery(`SELECT content_id, blob_hash, vendor, account_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs("abc123").
		WillReturnRows(rows)

	got, err := client.GetSegmentLocations("abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].ContentID != "cnt-001" {
		t.Errorf("first ContentID = %q, want cnt-001", got[0].ContentID)
	}
	if got[0].Vendor != "s3" {
		t.Errorf("first Vendor = %q, want s3", got[0].Vendor)
	}
	if got[1].ContentID != "cnt-001" {
		t.Errorf("second ContentID = %q, want cnt-001", got[1].ContentID)
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

	rows := sqlmock.NewRows([]string{"content_id", "blob_hash", "vendor", "account_id", "file_id"})
	mock.ExpectQuery(`SELECT content_id, blob_hash, vendor, account_id, file_id FROM blob_location WHERE blob_hash = \$1`).
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

// ─── WriteContentMeta ───────────────────────────────────────────────────────

func TestWriteContentMeta_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	content := types.ContentMeta{
		ContentID:    "abc-123",
		ContentType:  "dash_video",
		TypeMetadata: []byte(`{"codec":"h264"}`),
	}
	blobs := []types.BlobDescriptor{
		{BlobHash: "init_720p", BlobType: "init", Size: 1024, SortOrder: 0},
		{BlobHash: "seg_720p_1", BlobType: "media", Size: 2048, SortOrder: 1},
	}
	locations := []types.BlobLocation{
		{ContentID: "abc-123", BlobHash: "init_720p", Vendor: "115", AccountID: "acct-1", FileID: "fid_init"},
		{ContentID: "abc-123", BlobHash: "init_720p", Vendor: "baidu", AccountID: "acct-2", FileID: "fid_init_baidu"},
		{ContentID: "abc-123", BlobHash: "seg_720p_1", Vendor: "115", AccountID: "acct-1", FileID: "fid_seg1"},
		{ContentID: "abc-123", BlobHash: "seg_720p_1", Vendor: "baidu", AccountID: "acct-2", FileID: "fid_seg1_baidu"},
	}

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO content \(content_id, content_type, type_metadata\) VALUES \(\$1, \$2, \$3\)`).
		WithArgs("abc-123", "dash_video", []byte(`{"codec":"h264"}`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_index \(content_id, blob_hash, role, sort_order, size_bytes, checksum\) VALUES \(\$1, \$2, \$3, \$4, \$5, \$6\)`).
		WithArgs("abc-123", "init_720p", "init", 0, int64(1024), "init_720p").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_index \(content_id, blob_hash, role, sort_order, size_bytes, checksum\) VALUES \(\$1, \$2, \$3, \$4, \$5, \$6\)`).
		WithArgs("abc-123", "seg_720p_1", "media", 1, int64(2048), "seg_720p_1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_location \(content_id, blob_hash, vendor, account_id, file_id\) VALUES \(\$1, \$2, \$3, \$4, \$5\)`).
		WithArgs("abc-123", "init_720p", "115", "acct-1", "fid_init").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_location \(content_id, blob_hash, vendor, account_id, file_id\) VALUES \(\$1, \$2, \$3, \$4, \$5\)`).
		WithArgs("abc-123", "init_720p", "baidu", "acct-2", "fid_init_baidu").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_location \(content_id, blob_hash, vendor, account_id, file_id\) VALUES \(\$1, \$2, \$3, \$4, \$5\)`).
		WithArgs("abc-123", "seg_720p_1", "115", "acct-1", "fid_seg1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_location \(content_id, blob_hash, vendor, account_id, file_id\) VALUES \(\$1, \$2, \$3, \$4, \$5\)`).
		WithArgs("abc-123", "seg_720p_1", "baidu", "acct-2", "fid_seg1_baidu").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = client.WriteContentMeta(context.Background(), content, blobs, locations)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestWriteContentMeta_ROLLBACKOnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	content := types.ContentMeta{ContentID: "abc-123", ContentType: "video", TypeMetadata: nil}
	blobs := []types.BlobDescriptor{
		{BlobHash: "seg_1", BlobType: "media", Size: 512, SortOrder: 0},
	}
	locations := []types.BlobLocation{
		{ContentID: "abc-123", BlobHash: "seg_1", Vendor: "115", AccountID: "acct-1", FileID: "fid_1"},
	}

	mock.ExpectBegin()
	// content insert succeeds
	mock.ExpectExec(`INSERT INTO content`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// blob_index insert succeeds
	mock.ExpectExec(`INSERT INTO blob_index`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// blob_location insert fails → triggers ROLLBACK
	mock.ExpectExec(`INSERT INTO blob_location`).
		WillReturnError(fmt.Errorf("duplicate key value"))
	mock.ExpectRollback()

	err = client.WriteContentMeta(context.Background(), content, blobs, locations)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── ReportAccountHealth ──────────────────────────────────────────────────────

func TestReportAccountHealth_Upserts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	now := time.Now()
	state := types.HealthState{
		State:     "healthy",
		LastCheck: now,
		Latency:   150 * time.Millisecond,
		ErrorMsg:  "",
	}

	mock.ExpectExec(`INSERT INTO account_health \(vendor, account_id, state, last_check, latency_ms, error_msg, ban_until\)`).
		WithArgs("115", "acct-1", "healthy", now, int64(150), "", nil).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = client.ReportAccountHealth(context.Background(), types.Vendor("115"), "acct-1", state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestReportAccountHealth_BannedSetsBanUntil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	now := time.Now()
	state := types.HealthState{
		State:     "banned",
		LastCheck: now,
		Latency:   0,
		ErrorMsg:  "rate limited",
	}

	mock.ExpectExec(`INSERT INTO account_health \(vendor, account_id, state, last_check, latency_ms, error_msg, ban_until\)`).
		WithArgs("115", "acct-1", "banned", now, int64(0), "rate limited", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = client.ReportAccountHealth(context.Background(), types.Vendor("115"), "acct-1", state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── GetAccountHealths ──────────────────────────────────────────────────────

func TestGetAccountHealths_ReturnsHealthRecords(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	now := time.Now()
	rows := sqlmock.NewRows([]string{"vendor", "account_id", "state", "last_check", "latency_ms", "error_msg", "ban_until"}).
		AddRow("115", "acct-1", "healthy", now, int64(120), "", nil).
		AddRow("115", "acct-2", "banned", now, int64(0), "403 forbidden", &now)

	mock.ExpectQuery(`SELECT vendor, account_id, state, last_check, latency_ms, error_msg, ban_until FROM account_health WHERE vendor = \$1`).
		WithArgs("115").
		WillReturnRows(rows)

	got, err := client.GetAccountHealths(context.Background(), "115")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Vendor != "115" || got[0].AccountID != "acct-1" || got[0].State != "healthy" {
		t.Errorf("first record: got %+v, want healthy acct-1", got[0])
	}
	if got[1].Vendor != "115" || got[1].AccountID != "acct-2" || got[1].State != "banned" {
		t.Errorf("second record: got %+v, want banned acct-2", got[1])
	}
	if got[1].BanUntil == nil {
		t.Error("expected BanUntil to be set for banned account")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetAccountHealths_EmptyWhenNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"vendor", "account_id", "state", "last_check", "latency_ms", "error_msg", "ban_until"})
	mock.ExpectQuery(`SELECT vendor, account_id, state, last_check, latency_ms, error_msg, ban_until FROM account_health WHERE vendor = \$1`).
		WithArgs("quark").
		WillReturnRows(rows)

	got, err := client.GetAccountHealths(context.Background(), "quark")
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
