package metadata

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// detailMetaColumns is the SELECT column order of GetContentMeta (todo 7 shape).
var detailMetaColumns = []string{"content_id", "content_type", "type_metadata", "title", "deleted_at"}

// detailBlobColumns is the SELECT column order of GetContentBlobs.
var detailBlobColumns = []string{"blob_hash", "blob_type", "size_bytes", "role", "sort_order", "business_meta"}

// detailLocationColumns is the SELECT column order of the locations query.
var detailLocationColumns = []string{"blob_hash", "backend_id", "file_id", "state"}

// Given a content with 2 blobs and 2 locations, When GetContentDetail runs,
// Then meta/blobs/locations are assembled and the account_health join lands
// per-location (healthy state propagated; missing health row -> nil).
func TestGetContentDetail_ReturnsFullDetail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT content_id, content_type, type_metadata, title, deleted_at FROM content WHERE content_id = \$1`).
		WithArgs("vid-001").
		WillReturnRows(sqlmock.NewRows(detailMetaColumns).
			AddRow("vid-001", "video", []byte(`{"codec":"h264"}`), "Sample Title", nil))

	mock.ExpectQuery(`SELECT b\.blob_hash, b\.blob_type, b\.size_bytes, cb\.role, cb\.sort_order, cb\.business_meta`).
		WithArgs("vid-001").
		WillReturnRows(sqlmock.NewRows(detailBlobColumns).
			AddRow("hash_init", "mp4_init_segment", int64(1024), "init", 0, []byte(`{"representation_id":"720p"}`)).
			AddRow("hash_seg1", "m4s_media_segment", int64(2048), "media", 1, []byte(`{}`)))

	// Locks the join shape: backend_id split into vendor:account_id feeds the
	// LEFT JOIN against account_health.
	mock.ExpectQuery(`(?s)JOIN blob_location bl ON bl\.blob_hash = cb\.blob_hash.*LEFT JOIN account_health ah.*split_part`).
		WithArgs("vid-001").
		WillReturnRows(sqlmock.NewRows(detailLocationColumns).
			AddRow("hash_init", "115:acct_01", "fid_init_115", "healthy").
			AddRow("hash_seg1", "baidu:acct_02", "fid_seg1_baidu", nil))

	got, err := client.GetContentDetail(context.Background(), "vid-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// (a) meta
	if got.Meta == nil {
		t.Fatal("Meta = nil, want content meta")
	}
	if got.Meta.ContentID != "vid-001" || got.Meta.Title != "Sample Title" {
		t.Errorf("Meta = %+v", got.Meta)
	}
	if got.Meta.DeletedAt != nil {
		t.Errorf("DeletedAt = %v, want nil (live content)", got.Meta.DeletedAt)
	}

	// (b) blobs: descriptor + role merged per entry
	if len(got.Blobs) != 2 {
		t.Fatalf("Blobs len = %d, want 2", len(got.Blobs))
	}
	b0 := got.Blobs[0]
	if b0.Hash != "hash_init" || b0.BlobType != "mp4_init_segment" || b0.Size != 1024 {
		t.Errorf("Blobs[0] = %+v", b0)
	}
	if b0.Role != "init" || b0.SortOrder != 0 {
		t.Errorf("Blobs[0] role/order = %q/%d", b0.Role, b0.SortOrder)
	}
	if v, ok := b0.BusinessMeta["representation_id"]; !ok || v != "720p" {
		t.Errorf("Blobs[0].BusinessMeta = %v, want representation_id=720p", b0.BusinessMeta)
	}
	b1 := got.Blobs[1]
	if b1.Hash != "hash_seg1" || b1.Role != "media" || b1.SortOrder != 1 || b1.Size != 2048 {
		t.Errorf("Blobs[1] = %+v", b1)
	}

	// (c) locations: account_health joined per location
	if len(got.Locations) != 2 {
		t.Fatalf("Locations len = %d, want 2", len(got.Locations))
	}
	l0 := got.Locations[0]
	if l0.BlobHash != "hash_init" || l0.BackendID != "115:acct_01" || l0.FileID != "fid_init_115" {
		t.Errorf("Locations[0] = %+v", l0)
	}
	if l0.AccountHealth == nil || *l0.AccountHealth != "healthy" {
		t.Errorf("Locations[0].AccountHealth = %v, want \"healthy\"", l0.AccountHealth)
	}
	l1 := got.Locations[1]
	if l1.BlobHash != "hash_seg1" || l1.BackendID != "baidu:acct_02" {
		t.Errorf("Locations[1] = %+v", l1)
	}
	if l1.AccountHealth != nil {
		t.Errorf("Locations[1].AccountHealth = %v, want nil (no health row)", *l1.AccountHealth)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given a missing content_id, When GetContentDetail runs, Then the error
// chain contains ErrContentNotFound (QA failure scenario).
func TestGetContentDetail_NotFoundReturnsSentinel(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT content_id, content_type, type_metadata, title, deleted_at FROM content WHERE content_id = \$1`).
		WithArgs("missing-id").
		WillReturnError(sql.ErrNoRows)

	_, err = client.GetContentDetail(context.Background(), "missing-id")
	if err == nil {
		t.Fatal("expected error for not-found, got nil")
	}
	if !errors.Is(err, ErrContentNotFound) {
		t.Errorf("expected ErrContentNotFound in chain, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given a soft-deleted content, When GetContentDetail runs, Then it is STILL
// returned (API layer marks pending_delete; the query must not 410).
func TestGetContentDetail_DeletedContentStillReturned(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	deletedAt := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT content_id, content_type, type_metadata, title, deleted_at FROM content WHERE content_id = \$1`).
		WithArgs("vid-del").
		WillReturnRows(sqlmock.NewRows(detailMetaColumns).
			AddRow("vid-del", "video", []byte(`{}`), "", deletedAt))

	mock.ExpectQuery(`SELECT b\.blob_hash, b\.blob_type, b\.size_bytes, cb\.role, cb\.sort_order, cb\.business_meta`).
		WithArgs("vid-del").
		WillReturnRows(sqlmock.NewRows(detailBlobColumns).
			AddRow("hash_gone", "m4s_media_segment", int64(512), "media", 0, []byte(`{}`)))

	mock.ExpectQuery(`(?s)JOIN blob_location bl ON bl\.blob_hash = cb\.blob_hash`).
		WithArgs("vid-del").
		WillReturnRows(sqlmock.NewRows(detailLocationColumns))

	got, err := client.GetContentDetail(context.Background(), "vid-del")
	if err != nil {
		t.Fatalf("deleted content must still be returned, got error: %v", err)
	}
	if got.Meta.DeletedAt == nil || !got.Meta.DeletedAt.Equal(deletedAt) {
		t.Errorf("DeletedAt = %v, want %v", got.Meta.DeletedAt, deletedAt)
	}
	if len(got.Blobs) != 1 {
		t.Errorf("Blobs len = %d, want 1", len(got.Blobs))
	}
	if len(got.Locations) != 0 {
		t.Errorf("Locations len = %d, want 0", len(got.Locations))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
