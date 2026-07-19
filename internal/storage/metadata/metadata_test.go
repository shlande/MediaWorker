package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
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

// ─── Compile-time interface checks ─────────────────────────────────────────────

func TestPGMetadataClient_SatisfiesBlobStoreClient(t *testing.T) {
	var _ BlobStoreClient = newPGMetadataClientWithDB(nil)
}

func TestPGMetadataClient_SatisfiesContentMetaClient(t *testing.T) {
	var _ ContentMetaClient = newPGMetadataClientWithDB(nil)
}

func TestPGMetadataClient_SatisfiesPopularityClient(t *testing.T) {
	var _ PopularityClient = newPGMetadataClientWithDB(nil)
}

func TestPGMetadataClient_SatisfiesMetadataWriter(t *testing.T) {
	var _ MetadataWriter = newPGMetadataClientWithDB(nil)
}

// ─── GetContentMeta ────────────────────────────────────────────────────────────

func TestGetContentMeta_ReturnsContentMeta(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"content_id", "content_type", "type_metadata", "title", "deleted_at"}).
		AddRow("vid-001", "video", []byte(`{"codec":"h264"}`), "Sample Title", nil)
	mock.ExpectQuery(`SELECT content_id, content_type, type_metadata, title, deleted_at FROM content WHERE content_id = \$1`).
		WithArgs("vid-001").
		WillReturnRows(rows)

	got, err := client.GetContentMeta(context.Background(), "vid-001")
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
	if got.Title != "Sample Title" {
		t.Errorf("Title = %q, want %q", got.Title, "Sample Title")
	}
	if got.DeletedAt != nil {
		t.Errorf("DeletedAt = %v, want nil (live content)", got.DeletedAt)
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
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT content_id, content_type, type_metadata, title, deleted_at FROM content WHERE content_id = \$1`).
		WithArgs("missing-id").
		WillReturnError(sql.ErrNoRows)

	_, err = client.GetContentMeta(context.Background(), "missing-id")
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

// ─── GetTopContents ────────────────────────────────────────────────────────────

func TestGetTopContents_ReturnsSortedByPopularity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"content_id", "content_type", "type_metadata", "window_24h"}).
		AddRow("vid-001", "video", []byte(`{"codec":"h264"}`), int64(9500)).
		AddRow("vid-002", "image", []byte(`{"format":"webp"}`), int64(7200))
	mock.ExpectQuery(`SELECT c\.content_id, c\.content_type, c\.type_metadata, p\.window_24h`).
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
	defer func() { _ = db.Close() }()
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

// ─── GetBlobLocations (renamed from GetSegmentLocations) ───────────────────────

func TestGetBlobLocations_ReturnsLocations(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"}).
		AddRow("abc123", "115:acct-1", "path/to/seg1").
		AddRow("abc123", "baidu:acct-2", "path/to/seg1")
	mock.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs("abc123").
		WillReturnRows(rows)

	got, err := client.GetBlobLocations(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].BlobHash != "abc123" {
		t.Errorf("first BlobHash = %q, want abc123", got[0].BlobHash)
	}
	if got[0].BackendID != "115:acct-1" {
		t.Errorf("first BackendID = %q, want 115:acct-1", got[0].BackendID)
	}
	if got[1].BlobHash != "abc123" {
		t.Errorf("second BlobHash = %q, want abc123", got[1].BlobHash)
	}
	if got[1].BackendID != "baidu:acct-2" {
		t.Errorf("second BackendID = %q, want baidu:acct-2", got[1].BackendID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetBlobLocations_EmptyWhenNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"blob_hash", "backend_id", "file_id"})
	mock.ExpectQuery(`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = \$1`).
		WithArgs("unknown-blob").
		WillReturnRows(rows)

	got, err := client.GetBlobLocations(context.Background(), "unknown-blob")
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

// ─── GetPopularity24h ──────────────────────────────────────────────────────────

func TestGetPopularity24h_ReturnsPopularity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"window_24h"}).AddRow(int64(8712))
	mock.ExpectQuery(`SELECT window_24h FROM content_popularity WHERE content_id = \$1`).
		WithArgs("vid-001").
		WillReturnRows(rows)

	got := client.GetPopularity24h(context.Background(), "vid-001")
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
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT window_24h FROM content_popularity WHERE content_id = \$1`).
		WithArgs("missing-id").
		WillReturnError(sql.ErrNoRows)

	got := client.GetPopularity24h(context.Background(), "missing-id")
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
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT window_24h FROM content_popularity WHERE content_id = \$1`).
		WithArgs("any-id").
		WillReturnError(sql.ErrConnDone)

	got := client.GetPopularity24h(context.Background(), "any-id")
	if got != 0.0 {
		t.Errorf("got %f, want 0.0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── GetContentBlobs ───────────────────────────────────────────────────────────

func TestGetContentBlobs_ReturnsBlobsAndRoles(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	businessMeta := []byte(`{"representation_id":"720p","bitrate":1500000}`)
	rows := sqlmock.NewRows([]string{"blob_hash", "blob_type", "size_bytes", "role", "sort_order", "business_meta"}).
		AddRow("abc123", "mp4_init_segment", int64(1024), "init", 0, businessMeta).
		AddRow("def456", "m4s_media_segment", int64(2048), "media", 1, []byte(`{}`))
	mock.ExpectQuery(`SELECT b\.blob_hash, b\.blob_type, b\.size_bytes, cb\.role, cb\.sort_order, cb\.business_meta`).
		WithArgs("vid-001").
		WillReturnRows(rows)

	blobs, roles, err := client.GetContentBlobs(context.Background(), "vid-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("blobs len = %d, want 2", len(blobs))
	}
	if len(roles) != 2 {
		t.Fatalf("roles len = %d, want 2", len(roles))
	}
	// First row
	if blobs[0].BlobHash != "abc123" {
		t.Errorf("blobs[0].BlobHash = %q, want abc123", blobs[0].BlobHash)
	}
	if blobs[0].BlobType != "mp4_init_segment" {
		t.Errorf("blobs[0].BlobType = %q, want mp4_init_segment", blobs[0].BlobType)
	}
	if roles[0].Role != "init" {
		t.Errorf("roles[0].Role = %q, want init", roles[0].Role)
	}
	if roles[0].SortOrder != 0 {
		t.Errorf("roles[0].SortOrder = %d, want 0", roles[0].SortOrder)
	}
	if v, ok := roles[0].BusinessMeta["representation_id"]; !ok || v != "720p" {
		t.Errorf("roles[0].BusinessMeta = %v, want representation_id=720p", roles[0].BusinessMeta)
	}
	// Second row
	if blobs[1].BlobHash != "def456" {
		t.Errorf("blobs[1].BlobHash = %q, want def456", blobs[1].BlobHash)
	}
	if roles[1].Role != "media" {
		t.Errorf("roles[1].Role = %q, want media", roles[1].Role)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetContentBlobs_EmptyWhenNoContent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"blob_hash", "blob_type", "size_bytes", "role", "sort_order", "business_meta"})
	mock.ExpectQuery(`SELECT b\.blob_hash, b\.blob_type, b\.size_bytes, cb\.role, cb\.sort_order, cb\.business_meta`).
		WithArgs("unknown-id").
		WillReturnRows(rows)

	blobs, roles, err := client.GetContentBlobs(context.Background(), "unknown-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("blobs len = %d, want 0", len(blobs))
	}
	if len(roles) != 0 {
		t.Errorf("roles len = %d, want 0", len(roles))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── WriteIngestTransaction ────────────────────────────────────────────────────

func TestWriteIngestTransaction_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	content := types.ContentMeta{
		ContentID:    "abc-123",
		ContentType:  "dash_video",
		TypeMetadata: []byte(`{"codec":"h264"}`),
	}
	blobs := []types.BlobDescriptor{
		{BlobHash: "init_720p", BlobType: "mp4_init_segment", Size: 1024},
		{BlobHash: "seg_720p_1", BlobType: "m4s_media_segment", Size: 2048},
	}
	roles := []types.BlobRole{
		{BlobHash: "init_720p", Role: "init", SortOrder: 0, BusinessMeta: map[string]any{"representation_id": "720p"}},
		{BlobHash: "seg_720p_1", Role: "media", SortOrder: 1, BusinessMeta: map[string]any{"bitrate": float64(1500000)}},
	}
	locations := []types.BlobLocation{
		{BlobHash: "init_720p", BackendID: "115:acct-1", FileID: "fid_init"},
		{BlobHash: "init_720p", BackendID: "baidu:acct-2", FileID: "fid_init_baidu"},
		{BlobHash: "seg_720p_1", BackendID: "115:acct-1", FileID: "fid_seg1"},
		{BlobHash: "seg_720p_1", BackendID: "baidu:acct-2", FileID: "fid_seg1_baidu"},
	}

	mock.ExpectBegin()

	// Step 1: WriteBlob — 2 blobs
	mock.ExpectExec(`INSERT INTO blob \(blob_hash, blob_type, size_bytes\) VALUES \(\$1, \$2, \$3\) ON CONFLICT \(blob_hash\) DO NOTHING`).
		WithArgs("init_720p", "mp4_init_segment", int64(1024)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob \(blob_hash, blob_type, size_bytes\) VALUES \(\$1, \$2, \$3\) ON CONFLICT \(blob_hash\) DO NOTHING`).
		WithArgs("seg_720p_1", "m4s_media_segment", int64(2048)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Step 2: WriteBlobLocations — 4 locations
	mock.ExpectExec(`INSERT INTO blob_location \(blob_hash, backend_id, file_id\) VALUES \(\$1, \$2, \$3\) ON CONFLICT \(blob_hash, backend_id\) DO NOTHING`).
		WithArgs("init_720p", "115:acct-1", "fid_init").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_location \(blob_hash, backend_id, file_id\) VALUES \(\$1, \$2, \$3\) ON CONFLICT \(blob_hash, backend_id\) DO NOTHING`).
		WithArgs("init_720p", "baidu:acct-2", "fid_init_baidu").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_location \(blob_hash, backend_id, file_id\) VALUES \(\$1, \$2, \$3\) ON CONFLICT \(blob_hash, backend_id\) DO NOTHING`).
		WithArgs("seg_720p_1", "115:acct-1", "fid_seg1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_location \(blob_hash, backend_id, file_id\) VALUES \(\$1, \$2, \$3\) ON CONFLICT \(blob_hash, backend_id\) DO NOTHING`).
		WithArgs("seg_720p_1", "baidu:acct-2", "fid_seg1_baidu").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Step 3: WriteContentMeta — content insert + 2 content_blob inserts
	// content insert
	mock.ExpectExec(`INSERT INTO content \(content_id, content_type, type_metadata, title\) VALUES \(\$1, \$2, \$3, \$4\)`).
		WithArgs("abc-123", "dash_video", []byte(`{"codec":"h264"}`), "Test Title").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// content_blob for init_720p
	initMeta, _ := json.Marshal(roles[0].BusinessMeta)
	mock.ExpectExec(`INSERT INTO content_blob \(content_id, blob_hash, role, sort_order, business_meta\) VALUES \(\$1, \$2, \$3, \$4, \$5\)`).
		WithArgs("abc-123", "init_720p", "init", 0, initMeta).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// content_blob for seg_720p_1
	segMeta, _ := json.Marshal(roles[1].BusinessMeta)
	mock.ExpectExec(`INSERT INTO content_blob \(content_id, blob_hash, role, sort_order, business_meta\) VALUES \(\$1, \$2, \$3, \$4, \$5\)`).
		WithArgs("abc-123", "seg_720p_1", "media", 1, segMeta).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err = client.WriteIngestTransaction(context.Background(), content, "Test Title", blobs, roles, locations)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestWriteIngestTransaction_RollbackOnBlobError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	content := types.ContentMeta{ContentID: "abc-123", ContentType: "video", TypeMetadata: nil}
	blobs := []types.BlobDescriptor{
		{BlobHash: "seg_1", BlobType: "m4s_media_segment", Size: 512},
	}
	roles := []types.BlobRole{
		{BlobHash: "seg_1", Role: "media", SortOrder: 0},
	}
	locations := []types.BlobLocation{
		{BlobHash: "seg_1", BackendID: "115:acct-1", FileID: "fid_1"},
	}

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO blob`).
		WillReturnError(fmt.Errorf("duplicate key value"))
	mock.ExpectRollback()

	err = client.WriteIngestTransaction(context.Background(), content, "Test Title", blobs, roles, locations)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestWriteIngestTransaction_RollbackOnLocationError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	content := types.ContentMeta{ContentID: "abc-123", ContentType: "video", TypeMetadata: nil}
	blobs := []types.BlobDescriptor{
		{BlobHash: "seg_1", BlobType: "m4s_media_segment", Size: 512},
	}
	roles := []types.BlobRole{
		{BlobHash: "seg_1", Role: "media", SortOrder: 0},
	}
	locations := []types.BlobLocation{
		{BlobHash: "seg_1", BackendID: "115:acct-1", FileID: "fid_1"},
	}

	mock.ExpectBegin()
	// blob insert succeeds
	mock.ExpectExec(`INSERT INTO blob`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// blob_location insert fails -> rollback
	mock.ExpectExec(`INSERT INTO blob_location`).
		WillReturnError(fmt.Errorf("duplicate key value"))
	mock.ExpectRollback()

	err = client.WriteIngestTransaction(context.Background(), content, "Test Title", blobs, roles, locations)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestWriteIngestTransaction_RollbackOnContentError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	content := types.ContentMeta{ContentID: "abc-123", ContentType: "video", TypeMetadata: nil}
	blobs := []types.BlobDescriptor{
		{BlobHash: "seg_1", BlobType: "m4s_media_segment", Size: 512},
	}
	roles := []types.BlobRole{
		{BlobHash: "seg_1", Role: "media", SortOrder: 0},
	}
	locations := []types.BlobLocation{
		{BlobHash: "seg_1", BackendID: "115:acct-1", FileID: "fid_1"},
	}

	mock.ExpectBegin()
	// blob insert succeeds
	mock.ExpectExec(`INSERT INTO blob`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// blob_location insert succeeds
	mock.ExpectExec(`INSERT INTO blob_location`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// content insert fails -> rollback
	mock.ExpectExec(`INSERT INTO content`).
		WillReturnError(fmt.Errorf("duplicate key"))
	mock.ExpectRollback()

	err = client.WriteIngestTransaction(context.Background(), content, "Test Title", blobs, roles, locations)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── ReportAccountHealth ────────────────────────────────────────────────────────

func TestReportAccountHealth_Upserts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
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
	defer func() { _ = db.Close() }()
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

// ─── GetAccountHealths ──────────────────────────────────────────────────────────

func TestGetAccountHealths_ReturnsHealthRecords(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
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
	defer func() { _ = db.Close() }()
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

// Given a soft-deleted content row, When GetContentMeta reads it, Then
// DeletedAt is populated and Title decodes from NULL to empty string.
func TestGetContentMeta_DeletedContent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	deletedAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"content_id", "content_type", "type_metadata", "title", "deleted_at"}).
		AddRow("vid-del", "video", []byte(`{"codec":"h264"}`), nil, deletedAt)
	mock.ExpectQuery(`SELECT content_id, content_type, type_metadata, title, deleted_at FROM content WHERE content_id = \$1`).
		WithArgs("vid-del").
		WillReturnRows(rows)

	got, err := client.GetContentMeta(context.Background(), "vid-del")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Title != "" {
		t.Errorf("Title = %q, want empty for NULL column", got.Title)
	}
	if got.DeletedAt == nil || !got.DeletedAt.Equal(deletedAt) {
		t.Errorf("DeletedAt = %v, want %v", got.DeletedAt, deletedAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given an empty title, When WriteIngestTransaction inserts the content row,
// Then the title column is bound as SQL NULL (not an empty string).
func TestWriteIngestTransaction_EmptyTitleStoresNULL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	content := types.ContentMeta{ContentID: "abc-null", ContentType: "video", TypeMetadata: []byte(`{}`)}
	blobs := []types.BlobDescriptor{{BlobHash: "seg_1", BlobType: "m4s_media_segment", Size: 512}}
	roles := []types.BlobRole{{BlobHash: "seg_1", Role: "media", SortOrder: 0}}
	locations := []types.BlobLocation{{BlobHash: "seg_1", BackendID: "115:acct-1", FileID: "fid_1"}}

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO blob `).
		WithArgs("seg_1", "m4s_media_segment", int64(512)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO blob_location `).
		WithArgs("seg_1", "115:acct-1", "fid_1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO content \(content_id, content_type, type_metadata, title\) VALUES \(\$1, \$2, \$3, \$4\)`).
		WithArgs("abc-null", "video", []byte(`{}`), nil).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO content_blob `).
		WithArgs("abc-null", "seg_1", "media", 0, []byte(`null`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := client.WriteIngestTransaction(context.Background(), content, "", blobs, roles, locations); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
