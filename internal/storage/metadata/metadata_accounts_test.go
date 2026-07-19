package metadata

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// listAccountColumns is the SELECT column order of ListAccounts.
var listAccountColumns = []string{
	"vendor", "account_id", "enabled", "rate_limit_config", "vendor_profile",
	"credential", "client_config",
	"state", "latency_ms", "error_msg", "ban_until", "last_check",
}

func addAccountRow(rows *sqlmock.Rows, vendor, accountID string, enabled bool, credJSON, ccJSON []byte, state any) *sqlmock.Rows {
	var latency, errMsg, banUntil, lastCheck any
	if state != nil {
		latency = 42
		errMsg = ""
		banUntil = nil
		lastCheck = time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	}
	return rows.AddRow(
		vendor, accountID, enabled,
		[]byte(`{"qps":5,"burst":10,"concurrent_limit":3}`),
		[]byte(`{"vendor":"`+vendor+`","weight":1.5,"base_latency_ms":120,"bandwidth_mbps":50}`),
		credJSON, ccJSON,
		state, latency, errMsg, banUntil, lastCheck,
	)
}

// Given an account with a joined account_health row, When ListAccounts runs,
// Then the view carries a non-nil Health with the row's fields.
func TestListAccounts_WithHealthRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := addAccountRow(sqlmock.NewRows(listAccountColumns),
		"115", "acct-1", true,
		[]byte(`{"cookies":{}}`), []byte(`{}`),
		"healthy")
	mock.ExpectQuery("FROM cloud_account").WillReturnRows(rows)

	got, err := client.ListAccounts(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	v := got[0]
	if v.Health == nil {
		t.Fatal("Health = nil, want non-nil")
	}
	if v.Health.State != "healthy" {
		t.Errorf("Health.State = %q, want %q", v.Health.State, "healthy")
	}
	if v.Health.LatencyMs != 42 {
		t.Errorf("Health.LatencyMs = %d, want 42", v.Health.LatencyMs)
	}
	if v.Health.BanUntil != nil {
		t.Errorf("Health.BanUntil = %v, want nil", v.Health.BanUntil)
	}
	if v.RateLimitCfg.QPS != 5 || v.VendorProfile.Weight != 1.5 {
		t.Errorf("decoded JSONB wrong: %+v %+v", v.RateLimitCfg, v.VendorProfile)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given an account with no account_health row (LEFT JOIN NULLs), When
// ListAccounts runs, Then Health stays nil (UI empty-state semantics).
func TestListAccounts_WithoutHealthRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := addAccountRow(sqlmock.NewRows(listAccountColumns),
		"baidu", "acct-2", false,
		[]byte(`{"access_token":"at","refresh_token":"rt","token_expire":"2030-01-01T00:00:00Z"}`),
		[]byte(`{"client_id":"cid","region":"cn"}`),
		nil)
	mock.ExpectQuery("FROM cloud_account").WillReturnRows(rows)

	got, err := client.ListAccounts(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Health != nil {
		t.Errorf("Health = %+v, want nil", got[0].Health)
	}
	if got[0].Enabled {
		t.Error("Enabled = true, want false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given a vendor filter, When ListAccounts runs, Then the query passes the
// vendor as the first bind argument.
func TestListAccounts_VendorFilter(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := addAccountRow(sqlmock.NewRows(listAccountColumns),
		"115", "acct-1", true, []byte(`{}`), []byte(`{}`), nil)
	mock.ExpectQuery("WHERE a.vendor").WithArgs("115").WillReturnRows(rows)

	got, err := client.ListAccounts(context.Background(), "115", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0].Vendor != "115" {
		t.Errorf("got %+v, want one 115 row", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given a state filter that matches no row, When ListAccounts runs, Then the
// result is an empty slice.
func TestListAccounts_StateFilterNoMatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	mock.ExpectQuery("h.state").
		WithArgs("banned").
		WillReturnRows(sqlmock.NewRows(listAccountColumns))

	got, err := client.ListAccounts(context.Background(), "", "banned")
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

// Given an account whose credential has cookies, When ListAccounts runs, Then
// credential_meta reports auth_type=cookie with sorted cookie key names.
func TestListAccounts_CredentialMetaCookie(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := addAccountRow(sqlmock.NewRows(listAccountColumns),
		"115", "acct-cookie", true,
		[]byte(`{"cookies":{"zeta":"1","alpha":"2","mid":"3"}}`),
		[]byte(`{}`),
		nil)
	mock.ExpectQuery("FROM cloud_account").WillReturnRows(rows)

	got, err := client.ListAccounts(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	meta := got[0].CredentialMeta
	if meta.AuthType != "cookie" {
		t.Errorf("AuthType = %q, want %q", meta.AuthType, "cookie")
	}
	wantKeys := []string{"alpha", "mid", "zeta"}
	if len(meta.CookieKeys) != len(wantKeys) {
		t.Fatalf("CookieKeys = %v, want %v", meta.CookieKeys, wantKeys)
	}
	for i, k := range wantKeys {
		if meta.CookieKeys[i] != k {
			t.Errorf("CookieKeys[%d] = %q, want %q", i, meta.CookieKeys[i], k)
		}
	}
	if meta.HasRefreshToken {
		t.Error("HasRefreshToken = true, want false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given an oauth2 account with client_secret and refresh_token, When
// ListAccounts runs, Then credential_meta reports the correct booleans/region.
func TestListAccounts_CredentialMetaOAuth2(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := addAccountRow(sqlmock.NewRows(listAccountColumns),
		"onedrive", "acct-oauth", true,
		[]byte(`{"access_token":"at","refresh_token":"secret_rt_123","token_expire":"2030-01-01T00:00:00Z"}`),
		[]byte(`{"client_id":"cid","client_secret":"supersecret","redirect_uri":"http://x/cb","region":"eu"}`),
		nil)
	mock.ExpectQuery("FROM cloud_account").WillReturnRows(rows)

	got, err := client.ListAccounts(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	meta := got[0].CredentialMeta
	if meta.AuthType != "oauth2" {
		t.Errorf("AuthType = %q, want %q", meta.AuthType, "oauth2")
	}
	if !meta.HasClientSecret {
		t.Error("HasClientSecret = false, want true")
	}
	if !meta.HasRefreshToken {
		t.Error("HasRefreshToken = false, want true")
	}
	if meta.Region != "eu" {
		t.Errorf("Region = %q, want %q", meta.Region, "eu")
	}
	if len(meta.CookieKeys) != 0 {
		t.Errorf("CookieKeys = %v, want empty", meta.CookieKeys)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given accounts with secret material in credential/client_config, When the
// returned views are JSON-marshalled, Then no secret value nor secret key
// name appears in the output (zero-leak contract).
func TestListAccounts_NoSecretLeak(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows(listAccountColumns)
	rows = addAccountRow(rows, "onedrive", "acct-oauth", true,
		[]byte(`{"access_token":"at_secret","refresh_token":"secret_rt_123","token_expire":"2030-01-01T00:00:00Z"}`),
		[]byte(`{"client_id":"cid","client_secret":"supersecret","region":"eu"}`),
		"healthy")
	rows = addAccountRow(rows, "115", "acct-cookie", true,
		[]byte(`{"cookies":{"session":"secret_rt_123"}}`),
		[]byte(`{}`),
		nil)
	mock.ExpectQuery("FROM cloud_account").WillReturnRows(rows)

	got, err := client.ListAccounts(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}

	js, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(js)
	for _, forbidden := range []string{"secret_rt_123", "at_secret", "supersecret", `"credential":`, `"client_secret"`, `"access_token"`, `"refresh_token"`, `"cookies"`} {
		if strings.Contains(s, forbidden) {
			t.Errorf("marshalled view leaks %q: %s", forbidden, s)
		}
	}
	if !strings.Contains(s, `"credential_meta"`) {
		t.Errorf("marshalled view missing credential_meta: %s", s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Given two vendors, When ListVendorProfiles runs, Then the latest
// vendor_profile per vendor is decoded.
func TestListVendorProfiles_ReturnsRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	client := newPGMetadataClientWithDB(db)

	rows := sqlmock.NewRows([]string{"vendor", "vendor_profile"}).
		AddRow("115", []byte(`{"vendor":"115","weight":1.5,"base_latency_ms":120,"bandwidth_mbps":50}`)).
		AddRow("baidu", []byte(`{"vendor":"baidu","weight":1.0,"base_latency_ms":200,"bandwidth_mbps":30}`))
	mock.ExpectQuery("DISTINCT ON").WillReturnRows(rows)

	got, err := client.ListVendorProfiles(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Vendor != "115" || got[0].VendorProfile.Weight != 1.5 {
		t.Errorf("row 0 = %+v", got[0])
	}
	if got[1].Vendor != "baidu" || got[1].VendorProfile.BaseLatencyMs != 200 {
		t.Errorf("row 1 = %+v", got[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
