package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ─── alert_events accessors (ui-admin-apis todo 51) ───────────────────────────

// TestAlertEvents_InsertUpsertDedup verifies that inserting the same logical
// alert (same fingerprint+since) twice issues the upsert statement both
// times: ON CONFLICT (fingerprint, since) DO UPDATE refreshes status and
// received_at instead of creating a second row. This is the dedup contract
// for Alertmanager repeat_interval resends.
func TestAlertEvents_InsertUpsertDedup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	since := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	severity := "critical"
	target := "edge-node-7"
	detail := []byte(`{"labels":{"alertname":"CacheFillStall"},"annotations":{"summary":"fill stalled"}}`)

	upsertRe := `INSERT INTO alert_events.*ON CONFLICT \(fingerprint, since\) DO UPDATE SET status = EXCLUDED\.status, received_at = now\(\)`
	row := AlertEventRow{
		Fingerprint: "fp-abc123",
		Name:        "CacheFillStall",
		Severity:    &severity,
		Target:      &target,
		Detail:      detail,
		Status:      "firing",
		Since:       &since,
	}
	// First delivery: insert. Second delivery (resend): conflict path refreshes.
	for _, status := range []string{"firing", "firing"} {
		row.Status = status
		mock.ExpectExec(upsertRe).
			WithArgs(row.Fingerprint, row.Name, severity, target, string(detail), status, since).
			WillReturnResult(sqlmock.NewResult(1, 1))
		if err := client.InsertAlertEvent(context.Background(), row); err != nil {
			t.Fatalf("InsertAlertEvent (%s): %v", status, err)
		}
	}

	// Resolved resend carries NULL detail and no severity/target.
	row2 := AlertEventRow{Fingerprint: "fp-abc123", Name: "CacheFillStall", Status: "resolved", Since: &since}
	mock.ExpectExec(upsertRe).
		WithArgs("fp-abc123", "CacheFillStall", nil, nil, nil, "resolved", since).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := client.InsertAlertEvent(context.Background(), row2); err != nil {
		t.Fatalf("InsertAlertEvent (resolved): %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAlertEvents_ListMapsNullableColumns verifies ListAlertEvents("firing", n)
// issues the status-filtered query and maps nullable columns to nil pointers.
func TestAlertEvents_ListMapsNullableColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	since := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	recv := time.Date(2026, 7, 20, 8, 5, 0, 0, time.UTC)
	severity := "warning"
	target := "peer-12D3Koo"
	detail := []byte(`{"labels":{"alertname":"HighTTFB"}}`)

	mock.ExpectQuery(`SELECT id, fingerprint, name, severity, target, detail, status, since, received_at FROM alert_events WHERE status = \$1 ORDER BY received_at DESC LIMIT \$2`).
		WithArgs("firing", 100).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "fingerprint", "name", "severity", "target", "detail", "status", "since", "received_at",
		}).
			// Newest first: fully populated row.
			AddRow(int64(2), "fp-b", "HighTTFB", severity, target, detail, "firing", since, recv).
			// Older row: nullable columns absent (NULL -> nil).
			AddRow(int64(1), "fp-a", "CacheFillStall", nil, nil, nil, "firing", nil, recv.Add(-time.Minute)))

	got, err := client.ListAlertEvents(context.Background(), "firing", 100)
	if err != nil {
		t.Fatalf("ListAlertEvents: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}

	first := got[0]
	if first.ID != 2 || first.Fingerprint != "fp-b" || first.Name != "HighTTFB" || first.Status != "firing" {
		t.Errorf("first row identity = %+v", first)
	}
	if first.Severity == nil || *first.Severity != severity {
		t.Errorf("first Severity = %v, want %q", first.Severity, severity)
	}
	if first.Target == nil || *first.Target != target {
		t.Errorf("first Target = %v, want %q", first.Target, target)
	}
	if string(first.Detail) != string(detail) {
		t.Errorf("first Detail = %q, want %q", first.Detail, detail)
	}
	if first.Since == nil || !first.Since.Equal(since) {
		t.Errorf("first Since = %v, want %v", first.Since, since)
	}
	if !first.ReceivedAt.Equal(recv) {
		t.Errorf("first ReceivedAt = %v, want %v", first.ReceivedAt, recv)
	}

	second := got[1]
	if second.Severity != nil || second.Target != nil || second.Detail != nil || second.Since != nil {
		t.Errorf("expected nil pointers for NULL columns, got Severity=%v Target=%v Detail=%q Since=%v",
			second.Severity, second.Target, second.Detail, second.Since)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAlertEvents_ListAllStatuses verifies status="" drops the WHERE clause
// and passes only the limit argument.
func TestAlertEvents_ListAllStatuses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	now := time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT id, fingerprint, name, severity, target, detail, status, since, received_at FROM alert_events ORDER BY received_at DESC LIMIT \$1`).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "fingerprint", "name", "severity", "target", "detail", "status", "since", "received_at",
		}).
			AddRow(int64(3), "fp-c", "NodeOffline", nil, nil, nil, "resolved", nil, now))

	got, err := client.ListAlertEvents(context.Background(), "", 50)
	if err != nil {
		t.Fatalf("ListAlertEvents: %v", err)
	}
	if len(got) != 1 || got[0].Status != "resolved" {
		t.Errorf("got %+v, want 1 resolved row", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAlertEvents_ListEmpty verifies an empty table yields an empty slice
// with a nil error, not an error.
func TestAlertEvents_ListEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(`SELECT id, fingerprint, name, severity, target, detail, status, since, received_at FROM alert_events WHERE status = \$1 ORDER BY received_at DESC LIMIT \$2`).
		WithArgs("firing", 100).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "fingerprint", "name", "severity", "target", "detail", "status", "since", "received_at",
		}))

	got, err := client.ListAlertEvents(context.Background(), "firing", 100)
	if err != nil {
		t.Fatalf("ListAlertEvents on empty table: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAlertEvents_MigrateAllTwiceIdempotent runs MigrateAll twice against a
// mock that accepts every migration statement both times; every statement is
// idempotent (IF NOT EXISTS), so the second run must not error either. This
// also locks that 019 (alert_events, with the unquoted `since` column) is
// embedded and executed in lexical order after 016.
func TestAlertEvents_MigrateAllTwiceIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	// One full pass of all 17 embedded migrations in lexical order.
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
		mock.ExpectExec(`ALTER TABLE content ADD COLUMN IF NOT EXISTS title`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE TABLE IF NOT EXISTS alert_events`).WillReturnResult(sqlmock.NewResult(0, 0))
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
