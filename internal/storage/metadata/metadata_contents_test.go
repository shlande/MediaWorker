package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// listContentColumns is the SELECT column order of ListContents.
var listContentColumns = []string{
	"content_id", "title", "content_type", "total_bytes",
	"blob_count", "replicas_have", "window_24h", "created_at",
}

var listContentCreatedAt = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

func addContentRow(rows *sqlmock.Rows, id, title, contentType string, totalBytes, blobCount, replicasHave, window24h int64) *sqlmock.Rows {
	return rows.AddRow(id, title, contentType, totalBytes, blobCount, replicasHave, window24h, listContentCreatedAt)
}

func expectContentCount(mock sqlmock.Sqlmock, total int64) {
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM content c WHERE`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(total))
}

// Given a content whose weakest blob sits on 1 backend (replication target
// K=2 lives at the API layer), When ListContents runs, Then the row reports
// ReplicasHave=1 — the MIN over per-blob location counts.
func TestListContents_OneReplicaContent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	expectContentCount(mock, 1)
	rows := addContentRow(sqlmock.NewRows(listContentColumns),
		"11111111-1111-1111-1111-111111111111", "My Title", "video", 1024, 2, 1, 7)
	// Locks the weakest-replica aggregation shape: per-blob counts from
	// blob_location JOIN content_blob, then MIN across the content's blobs.
	mock.ExpectQuery(`(?s)MIN.*blob_location.*GROUP BY cb\.blob_hash`).
		WithArgs(20, 0).
		WillReturnRows(rows)

	got, total, err := client.ListContents(context.Background(), ListContentsQuery{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 1 {
		t.Errorf("total = %d, want 1", total)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	r := got[0]
	if r.ReplicasHave != 1 {
		t.Errorf("ReplicasHave = %d, want 1 (K=2 scenario: 1-replica content)", r.ReplicasHave)
	}
	if r.BlobCount != 2 || r.TotalBytes != 1024 {
		t.Errorf("blob aggregation = %d blobs/%d bytes, want 2/1024", r.BlobCount, r.TotalBytes)
	}
	if r.Window24h != 7 {
		t.Errorf("Window24h = %d, want 7", r.Window24h)
	}
	if r.Title != "My Title" || r.ContentType != "video" {
		t.Errorf("row = %+v", r)
	}
	if !r.CreatedAt.Equal(listContentCreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", r.CreatedAt, listContentCreatedAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given soft-deleted contents exist, When ListContents runs, Then both the
// count and page queries carry the deleted_at IS NULL filter (rows filtered
// in SQL, not in Go).
func TestListContents_ExcludesSoftDeleted(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`deleted_at IS NULL`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`deleted_at IS NULL`).
		WillReturnRows(sqlmock.NewRows(listContentColumns))

	got, total, err := client.ListContents(context.Background(), ListContentsQuery{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 || total != 0 {
		t.Errorf("got len=%d total=%d, want 0/0 (deleted rows must not appear)", len(got), total)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given 5 rows and page size 2, When page 2 is requested, Then LIMIT/OFFSET
// are bound as (2, 2) and the page's rows are returned with the full total.
func TestListContents_PageTwo(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	expectContentCount(mock, 5)
	rows := sqlmock.NewRows(listContentColumns)
	rows = addContentRow(rows, "33333333-3333-3333-3333-333333333333", "", "video", 10, 1, 2, 0)
	rows = addContentRow(rows, "44444444-4444-4444-4444-444444444444", "", "video", 20, 1, 2, 0)
	mock.ExpectQuery(`SELECT c\.content_id`).
		WithArgs(2, 2).
		WillReturnRows(rows)

	got, total, err := client.ListContents(context.Background(), ListContentsQuery{Sort: "created_at", Page: 2, PageSize: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].ContentID != "33333333-3333-3333-3333-333333333333" {
		t.Errorf("page 2 first row = %q", got[0].ContentID)
	}
	if got[0].Title != "" {
		t.Errorf("Title = %q, want empty (SQL NULL coalesced)", got[0].Title)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given Sort="popularity", When ListContents runs, Then the ORDER BY is the
// 24h window descending (NULL popularity treated as 0).
func TestListContents_SortPopularity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	expectContentCount(mock, 2)
	rows := sqlmock.NewRows(listContentColumns)
	rows = addContentRow(rows, "55555555-5555-5555-5555-555555555555", "hot", "video", 100, 1, 2, 99)
	rows = addContentRow(rows, "66666666-6666-6666-6666-666666666666", "cold", "video", 100, 1, 2, 1)
	mock.ExpectQuery(`ORDER BY COALESCE\(p\.window_24h, 0\) DESC`).
		WillReturnRows(rows)

	got, _, err := client.ListContents(context.Background(), ListContentsQuery{Sort: "popularity"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0].Window24h != 99 || got[1].Window24h != 1 {
		t.Errorf("popularity order wrong: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given an empty database, When ListContents runs, Then the result is an
// empty slice and total 0 (QA failure scenario).
func TestListContents_EmptyDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	expectContentCount(mock, 0)
	mock.ExpectQuery(`SELECT c\.content_id`).
		WillReturnRows(sqlmock.NewRows(listContentColumns))

	got, total, err := client.ListContents(context.Background(), ListContentsQuery{Page: 1, PageSize: 20})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given a content_type filter, When ListContents runs, Then the type is bound
// as $1 in BOTH count and page queries, and pagination defaults apply.
func TestListContents_TypeFilter(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`content_type = \$1`).
		WithArgs("video").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	rows := addContentRow(sqlmock.NewRows(listContentColumns),
		"77777777-7777-7777-7777-777777777777", "t", "video", 5, 1, 2, 0)
	mock.ExpectQuery(`content_type = \$1`).
		WithArgs("video", 20, 0).
		WillReturnRows(rows)

	got, total, err := client.ListContents(context.Background(), ListContentsQuery{Type: "video"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 1 || len(got) != 1 || got[0].ContentType != "video" {
		t.Errorf("got total=%d rows=%+v", total, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given an unknown sort value, When ListContents runs, Then the fallback is
// created_at DESC (no error, no caller-injected SQL fragment).
func TestListContents_UnknownSortDefaultsToCreatedAt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	expectContentCount(mock, 1)
	rows := addContentRow(sqlmock.NewRows(listContentColumns),
		"88888888-8888-8888-8888-888888888888", "t", "video", 5, 1, 2, 0)
	mock.ExpectQuery(`ORDER BY c\.created_at DESC`).
		WillReturnRows(rows)

	got, _, err := client.ListContents(context.Background(), ListContentsQuery{Sort: "bogus"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len = %d, want 1", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
