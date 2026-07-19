package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ─── node_status_history accessors ─────────────────────────────────────────────

// TestNodeStatusHistory_InsertAndGetRecent inserts 3 reports for one peer and
// verifies GetNodeStatusHistory(limit=2) returns the newest 2, newest first,
// with nullable columns mapping to nil pointers.
func TestNodeStatusHistory_InsertAndGetRecent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	base := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	nodeID := "node-1"
	region := "cn-east"
	version := "v0.9.1"
	rows := []NodeStatusHistoryRow{
		{PeerID: "peer-1", NodeID: &nodeID, Healthy: true, Region: &region, Version: &version, ReportedAt: base},
		{PeerID: "peer-1", NodeID: &nodeID, Healthy: true, Region: &region, Version: &version, ReportedAt: base.Add(time.Minute)},
		// Newest report: nullable fields absent (nil -> NULL).
		{PeerID: "peer-1", NodeID: &nodeID, Healthy: false, ReportedAt: base.Add(2 * time.Minute)},
	}

	insertRe := `INSERT INTO node_status_history`
	for _, r := range rows {
		mock.ExpectExec(insertRe).
			WithArgs(r.PeerID, nodeID, r.Healthy,
				r.PrefixUsed, r.PrefixTotal, r.WarmUsed, r.WarmTotal,
				r.ConnCount, r.Region, r.Version, r.ReportedAt).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	for _, r := range rows {
		if err := client.InsertNodeStatusHistory(context.Background(), r); err != nil {
			t.Fatalf("InsertNodeStatusHistory: %v", err)
		}
	}

	t2 := base.Add(2 * time.Minute)
	t1 := base.Add(time.Minute)
	mock.ExpectQuery(`SELECT id, peer_id, node_id, healthy, prefix_used, prefix_total, warm_used, warm_total, conn_count, region, version, reported_at, received_at FROM node_status_history WHERE peer_id = \$1 ORDER BY received_at DESC LIMIT \$2`).
		WithArgs("peer-1", 2).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "peer_id", "node_id", "healthy", "prefix_used", "prefix_total",
			"warm_used", "warm_total", "conn_count", "region", "version",
			"reported_at", "received_at",
		}).
			AddRow(int64(3), "peer-1", nodeID, false, nil, nil, nil, nil, nil, nil, nil, t2, t2).
			AddRow(int64(2), "peer-1", nodeID, true, int64(100), int64(1000), int64(50), int64(500), int32(7), region, version, t1, t1))

	got, err := client.GetNodeStatusHistory(context.Background(), "peer-1", 2)
	if err != nil {
		t.Fatalf("GetNodeStatusHistory: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	// Newest first.
	if !got[0].ReportedAt.Equal(t2) {
		t.Errorf("first ReportedAt = %v, want %v (newest)", got[0].ReportedAt, t2)
	}
	if got[0].Healthy {
		t.Error("first Healthy = true, want false")
	}
	// NULL columns scan into nil pointers.
	if got[0].Region != nil || got[0].PrefixUsed != nil || got[0].ConnCount != nil {
		t.Errorf("expected nil pointers for NULL columns, got Region=%v PrefixUsed=%v ConnCount=%v",
			got[0].Region, got[0].PrefixUsed, got[0].ConnCount)
	}
	// Populated columns round-trip through pointers.
	if got[1].Region == nil || *got[1].Region != region {
		t.Errorf("second Region = %v, want %q", got[1].Region, region)
	}
	if got[1].PrefixUsed == nil || *got[1].PrefixUsed != 100 {
		t.Errorf("second PrefixUsed = %v, want 100", got[1].PrefixUsed)
	}
	if got[1].ConnCount == nil || *got[1].ConnCount != 7 {
		t.Errorf("second ConnCount = %v, want 7", got[1].ConnCount)
	}
	if got[1].ID != 2 || got[1].PeerID != "peer-1" {
		t.Errorf("second ID/PeerID = %d/%q, want 2/peer-1", got[1].ID, got[1].PeerID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestNodeStatusHistory_PruneKeepsTwo prunes a peer to keep=2 and verifies the
// delete runs with the keep parameter and exactly 2 rows remain afterwards.
func TestNodeStatusHistory_PruneKeepsTwo(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectExec(`DELETE FROM node_status_history WHERE peer_id = \$1 AND id NOT IN`).
		WithArgs("peer-1", 2).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := client.PruneNodeStatusHistory(context.Background(), "peer-1", 2); err != nil {
		t.Fatalf("PruneNodeStatusHistory: %v", err)
	}

	now := time.Date(2026, 7, 20, 10, 2, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT id, peer_id, node_id, healthy, prefix_used, prefix_total, warm_used, warm_total, conn_count, region, version, reported_at, received_at FROM node_status_history WHERE peer_id = \$1 ORDER BY received_at DESC LIMIT \$2`).
		WithArgs("peer-1", 50).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "peer_id", "node_id", "healthy", "prefix_used", "prefix_total",
			"warm_used", "warm_total", "conn_count", "region", "version",
			"reported_at", "received_at",
		}).
			AddRow(int64(3), "peer-1", nil, true, nil, nil, nil, nil, nil, nil, nil, now, now).
			AddRow(int64(2), "peer-1", nil, true, nil, nil, nil, nil, nil, nil, nil, now.Add(-time.Minute), now.Add(-time.Minute)))

	got, err := client.GetNodeStatusHistory(context.Background(), "peer-1", 50)
	if err != nil {
		t.Fatalf("GetNodeStatusHistory after prune: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len after prune = %d, want exactly 2", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestNodeStatusHistory_EmptyReturnsEmptySlice verifies an unknown peer yields
// an empty slice with a nil error, not an error.
func TestNodeStatusHistory_EmptyReturnsEmptySlice(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT id, peer_id, node_id, healthy, prefix_used, prefix_total, warm_used, warm_total, conn_count, region, version, reported_at, received_at FROM node_status_history WHERE peer_id = \$1 ORDER BY received_at DESC LIMIT \$2`).
		WithArgs("peer-unknown", 10).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "peer_id", "node_id", "healthy", "prefix_used", "prefix_total",
			"warm_used", "warm_total", "conn_count", "region", "version",
			"reported_at", "received_at",
		}))

	got, err := client.GetNodeStatusHistory(context.Background(), "peer-unknown", 10)
	if err != nil {
		t.Fatalf("GetNodeStatusHistory on empty table: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestNodeStatusHistory_MigrateAllTwiceIdempotent runs MigrateAll twice against
// a mock that accepts every migration statement both times; every statement is
// idempotent (IF NOT EXISTS), so the second run must not error either.
func TestNodeStatusHistory_MigrateAllTwiceIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	// One full pass of all 16 embedded migrations in lexical order.
	expectMigrationPass := func() {
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS content`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_index`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_location`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS account_health`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS cloud_account`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS video_popularity`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob \(`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS content_blob`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`DO \$\$`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS blob_location_v2`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`DO \$\$`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`ALTER TABLE blob_location_v2`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`DROP TABLE IF EXISTS blob_index`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`ALTER TABLE blob ADD COLUMN IF NOT EXISTS deleted_at`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS app_user`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS node_status_history`).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	expectMigrationPass()
	expectMigrationPass()

	if err := MigrateAll(db); err != nil {
		t.Fatalf("MigrateAll (first run): %v", err)
	}
	if err := MigrateAll(db); err != nil {
		t.Fatalf("MigrateAll (second run, idempotent): %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
