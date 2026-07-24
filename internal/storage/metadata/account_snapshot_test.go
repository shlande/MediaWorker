package metadata

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

var snapshotColumns = []string{
	"vendor", "account_id", "credential", "client_config",
	"rate_limit_config", "vendor_profile", "banned",
}

// Given enabled cloud_account rows (one banned via account_health), when
// LoadAccountSnapshot runs, then entries carry full credential material and
// per-entry Banned flags derived from the JOIN.
func TestLoadAccountSnapshot_ParsesEntriesAndBanned(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows(snapshotColumns).
		AddRow("baidu", "acct-1",
			[]byte(`{"refresh_token":"rt-1"}`), []byte(`{"client_id":"cid-1","client_secret":"cs-1"}`),
			[]byte(`{"qps":2,"burst":4,"concurrent_limit":8}`),
			[]byte(`{"vendor":"baidu","weight":2.0,"base_latency_ms":410,"bandwidth_mbps":60}`),
			false).
		AddRow("onedrive", "acct-2",
			[]byte(`{"refresh_token":"rt-2"}`), []byte(`{"client_id":"cid-2","client_secret":"cs-2","region":"global"}`),
			[]byte(`{"qps":1,"burst":2,"concurrent_limit":5}`),
			[]byte(`{"vendor":"onedrive","weight":2.0,"base_latency_ms":380,"bandwidth_mbps":70}`),
			true)
	mock.ExpectQuery("FROM cloud_account").
		WillReturnRows(rows)

	entries, err := client.LoadAccountSnapshot(context.Background())
	if err != nil {
		t.Fatalf("LoadAccountSnapshot: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}

	e1 := entries[0]
	if e1.Vendor != "baidu" || e1.AccountID != "acct-1" || !e1.Enabled || e1.Banned {
		t.Errorf("entry[0] = %+v, want baidu/acct-1 enabled unbanned", e1)
	}
	if e1.Credential.RefreshToken != "rt-1" || e1.ClientConfig.ClientSecret != "cs-1" {
		t.Errorf("entry[0] credential material = %+v / %+v", e1.Credential, e1.ClientConfig)
	}
	if e1.RateLimitCfg.ConcurrentLimit != 8 || e1.VendorProfile.Weight != 2.0 {
		t.Errorf("entry[0] rate/profile = %+v / %+v", e1.RateLimitCfg, e1.VendorProfile)
	}

	e2 := entries[1]
	if !e2.Banned {
		t.Errorf("entry[1].Banned = false, want true (account_health state=banned)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// Given no rows, when LoadAccountSnapshot runs, then the result is empty
// (not nil-error) so callers can distinguish "no accounts" from failure.
func TestLoadAccountSnapshot_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery("FROM cloud_account").
		WillReturnRows(sqlmock.NewRows(snapshotColumns))

	entries, err := client.LoadAccountSnapshot(context.Background())
	if err != nil {
		t.Fatalf("LoadAccountSnapshot: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("entries = %d, want 0", len(entries))
	}
}
