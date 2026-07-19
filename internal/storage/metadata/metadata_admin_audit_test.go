package metadata

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ─── admin_audit accessors (ui-admin-apis todo 33) ──────────────────────────

func strptr(s string) *string { return &s }

// Given a fully-populated entry, when InsertAdminAudit runs, then all eight
// columns bind in order (TS present -> no COALESCE fallback to now()).
func TestAdminAudit_Insert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ts := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	entry := AdminAuditRow{
		TS:     ts,
		Kind:   "account",
		Actor:  "admin",
		Action: "create",
		Target: strptr("baidu:mw_bak_01"),
		IP:     strptr("127.0.0.1:54321"),
		Result: "ok",
		Detail: []byte(`{"warnings":["missing optional key"]}`),
	}

	mock.ExpectExec(`INSERT INTO admin_audit`).
		WithArgs(ts, "account", "admin", "create", "baidu:mw_bak_01", "127.0.0.1:54321", "ok", `{"warnings":["missing optional key"]}`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	c := &PGMetadataClient{db: db}
	if err := c.InsertAdminAudit(context.Background(), entry); err != nil {
		t.Fatalf("InsertAdminAudit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given an entry with zero TS and nil Target/IP/Detail, when InsertAdminAudit
// runs, then those columns bind as SQL NULL (TS falls back to the DB now()
// default via COALESCE).
func TestAdminAudit_InsertDefaults(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	entry := AdminAuditRow{
		Kind:   "auth",
		Actor:  "admin",
		Action: "login",
		Result: "fail",
	}

	mock.ExpectExec(`INSERT INTO admin_audit`).
		WithArgs(nil, "auth", "admin", "login", nil, nil, "fail", nil).
		WillReturnResult(sqlmock.NewResult(0, 1))

	c := &PGMetadataClient{db: db}
	if err := c.InsertAdminAudit(context.Background(), entry); err != nil {
		t.Fatalf("InsertAdminAudit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given the DB rejects the insert, when InsertAdminAudit runs, then the error
// is wrapped and returned (the adminapi recorder Warn-logs it; the metadata
// layer must not swallow).
func TestAdminAudit_InsertError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectExec(`INSERT INTO admin_audit`).
		WillReturnError(errors.New("db down"))

	c := &PGMetadataClient{db: db}
	err = c.InsertAdminAudit(context.Background(), AdminAuditRow{Kind: "pin", Actor: "admin", Action: "pin", Result: "ok"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given all filters (kind/from/to/q/page), when ListAdminAudit runs, then the
// WHERE fragment carries every condition, the count and page queries share it,
// and rows scan through (NULL target/ip tolerated).
func TestAdminAudit_ListAllFilters(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	ts := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM admin_audit WHERE kind = \$1 AND ts >= \$2 AND ts <= \$3 AND target ILIKE \$4`).
		WithArgs("account", from, to, "%baidu%").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT id, ts, kind, actor, action, target, ip, result, detail FROM admin_audit WHERE kind = \$1 AND ts >= \$2 AND ts <= \$3 AND target ILIKE \$4 ORDER BY ts DESC, id DESC LIMIT \$5 OFFSET \$6`).
		WithArgs("account", from, to, "%baidu%", 10, 20).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts", "kind", "actor", "action", "target", "ip", "result", "detail"}).
			AddRow(7, ts, "account", "admin", "ban", "baidu:mw_bak_01", "10.0.0.1:8000", "ok", []byte(`{"reason":"abuse"}`)))

	c := &PGMetadataClient{db: db}
	rows, total, err := c.ListAdminAudit(context.Background(), AdminAuditQuery{
		Kind: "account", From: &from, To: &to, Q: "baidu", Page: 3, PageSize: 10,
	})
	if err != nil {
		t.Fatalf("ListAdminAudit: %v", err)
	}
	if total != 1 {
		t.Errorf("total = %d, want 1", total)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.ID != 7 || r.Kind != "account" || r.Actor != "admin" || r.Action != "ban" || r.Result != "ok" {
		t.Errorf("unexpected row: %+v", r)
	}
	if r.Target == nil || *r.Target != "baidu:mw_bak_01" {
		t.Errorf("target = %v, want baidu:mw_bak_01", r.Target)
	}
	if r.IP == nil || *r.IP != "10.0.0.1:8000" {
		t.Errorf("ip = %v, want 10.0.0.1:8000", r.IP)
	}
	if string(r.Detail) != `{"reason":"abuse"}` {
		t.Errorf("detail = %s, want ban reason JSON", r.Detail)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given no filters, when ListAdminAudit runs, then WHERE TRUE matches all
// rows, defaults page 1 / size 20 apply, and NULL target/ip/detail scan as
// nil.
func TestAdminAudit_ListNoFilters(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ts := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM admin_audit WHERE TRUE`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT id, ts, kind, actor, action, target, ip, result, detail FROM admin_audit WHERE TRUE ORDER BY ts DESC, id DESC LIMIT \$1 OFFSET \$2`).
		WithArgs(20, 0).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts", "kind", "actor", "action", "target", "ip", "result", "detail"}).
			AddRow(3, ts, "auth", "admin", "login", nil, nil, "fail", nil))

	c := &PGMetadataClient{db: db}
	rows, total, err := c.ListAdminAudit(context.Background(), AdminAuditQuery{})
	if err != nil {
		t.Fatalf("ListAdminAudit: %v", err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("total=%d rows=%d, want 1/1", total, len(rows))
	}
	if rows[0].Target != nil || rows[0].IP != nil || rows[0].Detail != nil {
		t.Errorf("NULL columns should scan as nil: %+v", rows[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given only the q substring filter, when ListAdminAudit runs, then target
// ILIKE binds the %q% pattern (todo-34 admin-source query semantics).
func TestAdminAudit_ListTargetSubstring(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM admin_audit WHERE target ILIKE \$1`).
		WithArgs("%12D3Koo%").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT id, ts, kind, actor, action, target, ip, result, detail FROM admin_audit WHERE target ILIKE \$1 ORDER BY ts DESC, id DESC LIMIT \$2 OFFSET \$3`).
		WithArgs("%12D3Koo%", 20, 0).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts", "kind", "actor", "action", "target", "ip", "result", "detail"}))

	c := &PGMetadataClient{db: db}
	rows, total, err := c.ListAdminAudit(context.Background(), AdminAuditQuery{Q: "12D3Koo"})
	if err != nil {
		t.Fatalf("ListAdminAudit: %v", err)
	}
	if total != 0 || len(rows) != 0 {
		t.Errorf("total=%d rows=%d, want empty", total, len(rows))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given the count query fails, when ListAdminAudit runs, then the wrapped
// error propagates and no page query is issued.
func TestAdminAudit_ListCountError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM admin_audit`).
		WillReturnError(errors.New("db down"))

	c := &PGMetadataClient{db: db}
	if _, _, err := c.ListAdminAudit(context.Background(), AdminAuditQuery{}); err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
