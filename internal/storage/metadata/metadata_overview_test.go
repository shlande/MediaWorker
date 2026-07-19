package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// ─── overview aggregates (ui-admin-apis todo 52) ────────────────────────────

const accountHealthRateQueryRe = `SELECT COUNT\(\*\) FILTER \(WHERE state='healthy'\)::float/NULLIF\(COUNT\(\*\),0\) FROM account_health`

// TestAccountHealthRate_ReturnsRatio verifies the healthy share is scanned
// through as (rate, true, nil) when the aggregate yields a value.
func TestAccountHealthRate_ReturnsRatio(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(accountHealthRateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"rate"}).AddRow(0.75))

	rate, ok, err := client.AccountHealthRate(context.Background())
	if err != nil {
		t.Fatalf("AccountHealthRate: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for a non-NULL aggregate")
	}
	if rate != 0.75 {
		t.Fatalf("rate = %v, want 0.75", rate)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAccountHealthRate_NoRowsReturnsNotOk locks the division-by-zero guard:
// an empty account_health table yields SQL NULL (via NULLIF), surfaced as
// ok=false so the API layer renders null instead of 500-ing.
func TestAccountHealthRate_NoRowsReturnsNotOk(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery(accountHealthRateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"rate"}).AddRow(nil))

	rate, ok, err := client.AccountHealthRate(context.Background())
	if err != nil {
		t.Fatalf("AccountHealthRate: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for a NULL aggregate (empty table)")
	}
	if rate != 0 {
		t.Fatalf("rate = %v, want 0 when ok=false", rate)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAccountHealthRate_QueryErrorPropagates verifies real query failures
// surface as errors (the API layer degrades the field to null + partial).
func TestAccountHealthRate_QueryErrorPropagates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	boom := errors.New("connection reset")
	mock.ExpectQuery(accountHealthRateQueryRe).WillReturnError(boom)

	_, _, err = client.AccountHealthRate(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want wrapped %v", err, boom)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
