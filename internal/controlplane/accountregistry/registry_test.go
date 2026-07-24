package accountregistry

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/shlande/mediaworker/internal/types"
)

// mockBroadcaster implements Broadcaster for testing with concurrency safety.
type mockBroadcaster struct {
	mu     sync.Mutex
	events []broadcastCall
	err    error // injected failure for eventual-consistency tests
}

type broadcastCall struct {
	eventType string
	payload   any
}

func (m *mockBroadcaster) Broadcast(eventType string, payload any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.events = append(m.events, broadcastCall{eventType: eventType, payload: payload})
	return nil
}

func (m *mockBroadcaster) getEvents() []broadcastCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]broadcastCall, len(m.events))
	copy(result, m.events)
	return result
}

func TestCreateAccountAndListByVendor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	info := AccountInfo{
		Vendor:    types.Vendor115,
		AccountID: "acct_01",
		Credential: types.Credential{
			Cookies:      map[string]string{"CID": "abc123"},
			AccessToken:  "tok_abc",
			RefreshToken: "ref_abc",
			TokenExpire:  time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC),
		},
		ClientConfig: types.ClientConfig{
			ClientID:     "cid_abc",
			ClientSecret: "cs_abc",
			RedirectURI:  "https://example.com/cb",
		},
		RateLimitCfg: types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5},
		VendorProfile: types.VendorProfile{
			Vendor: types.Vendor115, Weight: 3.0, BaseLatencyMs: 100, BandwidthMbps: 50,
		},
		Enabled: true,
	}

	credJSON, _ := json.Marshal(info.Credential)
	ccJSON, _ := json.Marshal(info.ClientConfig)
	rlJSON, _ := json.Marshal(info.RateLimitCfg)
	vpJSON, _ := json.Marshal(info.VendorProfile)

	mock.ExpectExec(`INSERT INTO cloud_account`).
		WithArgs(string(info.Vendor), info.AccountID, credJSON, ccJSON, rlJSON, vpJSON, info.Enabled).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := ar.CreateAccount(ctx, info); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("CreateAccount expectations: %v", err)
	}

	// Now ListByVendor and verify the fields round-trip.
	rows := sqlmock.NewRows([]string{"account_id", "credential", "client_config", "rate_limit_config", "vendor_profile", "enabled", "banned"}).
		AddRow("acct_01", credJSON, ccJSON, rlJSON, vpJSON, true, false)

	mock.ExpectQuery(`SELECT a.account_id, a.credential, a.client_config, a.rate_limit_config, a.vendor_profile, a.enabled, COALESCE\(h.state = 'banned', false\) FROM cloud_account a LEFT JOIN account_health h ON h.vendor = a.vendor AND h.account_id = a.account_id WHERE a.vendor = \$1 AND a.enabled = true ORDER BY a.account_id`).
		WithArgs(string(types.Vendor115)).
		WillReturnRows(rows)

	accounts, err := ar.ListByVendor(ctx, types.Vendor115)
	if err != nil {
		t.Fatalf("ListByVendor: %v", err)
	}

	if len(accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accounts))
	}

	got := accounts[0]
	if got.Vendor != info.Vendor {
		t.Errorf("Vendor: expected %s, got %s", info.Vendor, got.Vendor)
	}
	if got.AccountID != info.AccountID {
		t.Errorf("AccountID: expected %s, got %s", info.AccountID, got.AccountID)
	}
	if got.Credential.AccessToken != info.Credential.AccessToken {
		t.Errorf("AccessToken: expected %s, got %s", info.Credential.AccessToken, got.Credential.AccessToken)
	}
	if got.ClientConfig != info.ClientConfig {
		t.Errorf("ClientConfig: expected %+v, got %+v", info.ClientConfig, got.ClientConfig)
	}
	if got.RateLimitCfg.QPS != info.RateLimitCfg.QPS {
		t.Errorf("QPS: expected %f, got %f", info.RateLimitCfg.QPS, got.RateLimitCfg.QPS)
	}
	if got.VendorProfile.Weight != info.VendorProfile.Weight {
		t.Errorf("Weight: expected %f, got %f", info.VendorProfile.Weight, got.VendorProfile.Weight)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ListByVendor expectations: %v", err)
	}
}

// Given a row whose client_config column is NULL (predates migration 020),
// When ListByVendor scans it, Then ClientConfig decodes as zero value, no error.
func TestListByVendor_NullClientConfig(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})
	ctx := context.Background()

	credJSON, _ := json.Marshal(types.Credential{Cookies: map[string]string{"CID": "x"}})
	rlJSON, _ := json.Marshal(types.RateLimitConfig{})
	vpJSON, _ := json.Marshal(types.VendorProfile{})

	rows := sqlmock.NewRows([]string{"account_id", "credential", "client_config", "rate_limit_config", "vendor_profile", "enabled", "banned"}).
		AddRow("acct_old", credJSON, nil, rlJSON, vpJSON, true, false)
	mock.ExpectQuery(`SELECT a.account_id, a.credential, a.client_config, a.rate_limit_config, a.vendor_profile, a.enabled, COALESCE\(h.state = 'banned', false\) FROM cloud_account a LEFT JOIN account_health h ON h.vendor = a.vendor AND h.account_id = a.account_id`).
		WithArgs(string(types.VendorQuark)).
		WillReturnRows(rows)

	accounts, err := ar.ListByVendor(ctx, types.VendorQuark)
	if err != nil {
		t.Fatalf("ListByVendor: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accounts))
	}
	if accounts[0].ClientConfig != (types.ClientConfig{}) {
		t.Errorf("ClientConfig = %+v, want zero for NULL column", accounts[0].ClientConfig)
	}
}

// UpdateCredential writes through but never broadcasts by itself (B5:
// caller-fires-once for combined auth updates).
func TestUpdateCredential_NoBroadcast(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	newCred := types.Credential{
		Cookies:      map[string]string{"CID": "new123"},
		AccessToken:  "new_tok",
		RefreshToken: "new_ref",
	}

	credJSON, _ := json.Marshal(newCred)

	mock.ExpectExec(`UPDATE cloud_account SET credential = \$1, updated_at = now\(\) WHERE vendor = \$2 AND account_id = \$3`).
		WithArgs(credJSON, "115", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.UpdateCredential(ctx, types.Vendor115, "acct_01", newCred); err != nil {
		t.Fatalf("UpdateCredential: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("UpdateCredential expectations: %v", err)
	}

	if len(b.events) != 0 {
		t.Errorf("expected 0 broadcasts from UpdateCredential alone, got %d", len(b.events))
	}
}

func TestUpdateCredential_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	credJSON, _ := json.Marshal(types.Credential{Cookies: map[string]string{"CID": "x"}})

	mock.ExpectExec(`UPDATE cloud_account SET credential = \$1, updated_at = now\(\) WHERE vendor = \$2 AND account_id = \$3`).
		WithArgs(credJSON, "115", "nonexistent").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ar.UpdateCredential(ctx, types.Vendor115, "nonexistent", types.Credential{Cookies: map[string]string{"CID": "x"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "accountregistry: account not found: 115/nonexistent" {
		t.Errorf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}

	// No broadcast on not-found.
	if len(b.events) != 0 {
		t.Errorf("expected 0 broadcasts on not-found, got %d", len(b.events))
	}
}

func TestRevoke(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	mock.ExpectExec(`UPDATE cloud_account SET enabled = false, updated_at = now\(\) WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("115", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.Revoke(ctx, types.Vendor115, "acct_01"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("Revoke expectations: %v", err)
	}
}

// OnCredentialChange reads the stored secrets and broadcasts ONE
// CREDENTIAL_UPDATE carrying both credential and client_config.
func TestOnCredentialChange_BroadcastsStoredSecrets(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	credJSON, _ := json.Marshal(types.Credential{RefreshToken: "rt-live"})
	ccJSON, _ := json.Marshal(types.ClientConfig{ClientID: "cid-live", ClientSecret: "cs-live"})
	mock.ExpectQuery(`SELECT credential, client_config FROM cloud_account WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("baidu", "baidu_01").
		WillReturnRows(sqlmock.NewRows([]string{"credential", "client_config"}).AddRow(credJSON, ccJSON))

	ar.OnCredentialChange(ctx, types.VendorBaidu, "baidu_01")

	if len(b.events) != 1 {
		t.Fatalf("expected 1 broadcast, got %d", len(b.events))
	}
	if b.events[0].eventType != types.EventCredentialUpdate {
		t.Errorf("event type: expected %s, got %s", types.EventCredentialUpdate, b.events[0].eventType)
	}
	payload, ok := b.events[0].payload.(CredentialChangePayload)
	if !ok {
		t.Fatalf("payload type: expected CredentialChangePayload, got %T", b.events[0].payload)
	}
	if payload.Vendor != types.VendorBaidu {
		t.Errorf("payload.Vendor: expected baidu, got %s", payload.Vendor)
	}
	if payload.AccountID != "baidu_01" {
		t.Errorf("payload.AccountID: expected baidu_01, got %s", payload.AccountID)
	}
	if payload.Credential.RefreshToken != "rt-live" {
		t.Errorf("payload.Credential.RefreshToken: expected rt-live, got %q", payload.Credential.RefreshToken)
	}
	if payload.ClientConfig.ClientID != "cid-live" {
		t.Errorf("payload.ClientConfig.ClientID: expected cid-live, got %q", payload.ClientConfig.ClientID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// A combined credential + client_config update fires exactly ONE
// CREDENTIAL_UPDATE, with both bodies in the payload.
func TestCombinedAuthUpdate_SingleMergedBroadcast(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	cred := types.Credential{RefreshToken: "rt-new"}
	cc := types.ClientConfig{ClientID: "cid-new", ClientSecret: "cs-new", RedirectURI: "https://cb"}
	credJSON, _ := json.Marshal(cred)
	ccJSON, _ := json.Marshal(cc)

	mock.ExpectExec(`UPDATE cloud_account SET credential = \$1`).
		WithArgs(credJSON, "baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE cloud_account SET client_config = \$1`).
		WithArgs(ccJSON, "baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// OnCredentialChange reads back the live row.
	mock.ExpectQuery(`SELECT credential, client_config FROM cloud_account`).
		WithArgs("baidu", "acct_01").
		WillReturnRows(sqlmock.NewRows([]string{"credential", "client_config"}).AddRow(credJSON, ccJSON))

	if err := ar.UpdateCredential(ctx, types.VendorBaidu, "acct_01", cred); err != nil {
		t.Fatalf("UpdateCredential: %v", err)
	}
	if err := ar.UpdateClientConfig(ctx, types.VendorBaidu, "acct_01", cc); err != nil {
		t.Fatalf("UpdateClientConfig: %v", err)
	}
	ar.OnCredentialChange(ctx, types.VendorBaidu, "acct_01")

	if len(b.events) != 1 {
		t.Fatalf("expected exactly 1 CREDENTIAL_UPDATE for combined update, got %d", len(b.events))
	}
	payload := b.events[0].payload.(CredentialChangePayload)
	if payload.Credential.RefreshToken != "rt-new" || payload.ClientConfig.ClientID != "cid-new" {
		t.Errorf("payload = %+v, want credential(rt-new) + client_config(cid-new)", payload)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestStartSync_EmitsSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare mock rows. Only Vendor115 has accounts.
	// Use a signal channel to know when emitSnapshot has been called so we
	// can cancel deterministically without extra ticker calls.
	done := make(chan struct{})
	origEmit := ar.emitSnapshotFn
	ar.emitSnapshotFn = func(ctx context.Context) {
		origEmit(ctx)
		select {
		case done <- struct{}{}:
		default:
		}
	}

	credJSON, _ := json.Marshal(types.Credential{Cookies: map[string]string{"CID": "x"}})
	rlJSON, _ := json.Marshal(types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})
	vpJSON, _ := json.Marshal(types.VendorProfile{Vendor: types.Vendor115, Weight: 3.0, BaseLatencyMs: 100, BandwidthMbps: 50})

	for _, vendor := range []types.Vendor{types.Vendor115, types.VendorBaidu, types.VendorQuark, types.VendorOneDrive, types.VendorAliyundrive} {
		if vendor == types.Vendor115 {
			rows := sqlmock.NewRows([]string{"account_id", "credential", "client_config", "rate_limit_config", "vendor_profile", "enabled", "banned"}).
				AddRow("acct_01", credJSON, nil, rlJSON, vpJSON, true, false)
			mock.ExpectQuery(`SELECT a.account_id, a.credential, a.client_config, a.rate_limit_config, a.vendor_profile, a.enabled, COALESCE\(h.state = 'banned', false\) FROM cloud_account a LEFT JOIN account_health h ON h.vendor = a.vendor AND h.account_id = a.account_id WHERE a.vendor = \$1 AND a.enabled = true ORDER BY a.account_id`).
				WithArgs(string(vendor)).
				WillReturnRows(rows)
		} else {
			rows := sqlmock.NewRows([]string{"account_id", "credential", "client_config", "rate_limit_config", "vendor_profile", "enabled", "banned"})
			mock.ExpectQuery(`SELECT a.account_id, a.credential, a.client_config, a.rate_limit_config, a.vendor_profile, a.enabled, COALESCE\(h.state = 'banned', false\) FROM cloud_account a LEFT JOIN account_health h ON h.vendor = a.vendor AND h.account_id = a.account_id WHERE a.vendor = \$1 AND a.enabled = true ORDER BY a.account_id`).
				WithArgs(string(vendor)).
				WillReturnRows(rows)
		}
	}

	ar.StartSync(ctx, 50*time.Millisecond)

	// Wait for the immediate emitSnapshot to complete.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot")
	}
	cancel()
	// Give the goroutine a moment to observe cancellation.
	time.Sleep(10 * time.Millisecond)

	events := b.getEvents()

	if len(events) == 0 {
		t.Fatal("expected at least 1 snapshot broadcast, got 0")
	}

	found := false
	for _, evt := range events {
		if evt.eventType == "ACCOUNT_SNAPSHOT" {
			found = true
			accounts, ok := evt.payload.([]AccountInfo)
			if !ok {
				t.Fatalf("snapshot payload type: expected []AccountInfo, got %T", evt.payload)
			}
			if len(accounts) != 1 {
				t.Errorf("expected 1 account in snapshot, got %d", len(accounts))
			}
			if accounts[0].AccountID != "acct_01" {
				t.Errorf("account ID: expected acct_01, got %s", accounts[0].AccountID)
			}
		}
	}
	if !found {
		t.Error("no ACCOUNT_SNAPSHOT event found in broadcasts")
	}
}

func TestStartSync_NoAccounts_NoBroadcast(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// All vendors return empty results.
	done := make(chan struct{})
	origEmit := ar.emitSnapshotFn
	ar.emitSnapshotFn = func(ctx context.Context) {
		origEmit(ctx)
		select {
		case done <- struct{}{}:
		default:
		}
	}

	for _, vendor := range []types.Vendor{types.Vendor115, types.VendorBaidu, types.VendorQuark, types.VendorOneDrive, types.VendorAliyundrive} {
		rows := sqlmock.NewRows([]string{"account_id", "credential", "client_config", "rate_limit_config", "vendor_profile", "enabled", "banned"})
		mock.ExpectQuery(`SELECT a.account_id, a.credential, a.client_config, a.rate_limit_config, a.vendor_profile, a.enabled, COALESCE\(h.state = 'banned', false\) FROM cloud_account a LEFT JOIN account_health h ON h.vendor = a.vendor AND h.account_id = a.account_id WHERE a.vendor = \$1 AND a.enabled = true ORDER BY a.account_id`).
			WithArgs(string(vendor)).
			WillReturnRows(rows)
	}

	ar.StartSync(ctx, 50*time.Millisecond)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot")
	}
	cancel()
	time.Sleep(10 * time.Millisecond)

	for _, evt := range b.getEvents() {
		if evt.eventType == "ACCOUNT_SNAPSHOT" {
			t.Fatal("expected no ACCOUNT_SNAPSHOT when no accounts exist")
		}
	}
}

func TestListByVendor_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	rows := sqlmock.NewRows([]string{"account_id", "credential", "client_config", "rate_limit_config", "vendor_profile", "enabled", "banned"})
	mock.ExpectQuery(`SELECT a.account_id, a.credential, a.client_config, a.rate_limit_config, a.vendor_profile, a.enabled, COALESCE\(h.state = 'banned', false\) FROM cloud_account a LEFT JOIN account_health h ON h.vendor = a.vendor AND h.account_id = a.account_id WHERE a.vendor = \$1 AND a.enabled = true ORDER BY a.account_id`).
		WithArgs(string(types.Vendor115)).
		WillReturnRows(rows)

	accounts, err := ar.ListByVendor(ctx, types.Vendor115)
	if err != nil {
		t.Fatalf("ListByVendor: %v", err)
	}
	if len(accounts) != 0 {
		t.Errorf("expected 0 accounts, got %d", len(accounts))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestNewAccountRegistry(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	if ar == nil {
		t.Fatal("NewAccountRegistry returned nil")
	}
}

// ─── UpdateClientConfig ───

func TestUpdateClientConfig(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	cc := types.ClientConfig{ClientID: "cid", ClientSecret: "cs", RedirectURI: "https://cb", Region: "cn"}
	ccJSON, _ := json.Marshal(cc)

	mock.ExpectExec(`UPDATE cloud_account SET client_config = \$1, updated_at = now\(\) WHERE vendor = \$2 AND account_id = \$3`).
		WithArgs(ccJSON, "onedrive", "od_01").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.UpdateClientConfig(ctx, types.VendorOneDrive, "od_01", cc); err != nil {
		t.Fatalf("UpdateClientConfig: %v", err)
	}
	if len(b.events) != 0 {
		t.Errorf("expected 0 broadcasts from UpdateClientConfig alone, got %d", len(b.events))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestUpdateClientConfig_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})
	ccJSON, _ := json.Marshal(types.ClientConfig{ClientID: "cid"})

	mock.ExpectExec(`UPDATE cloud_account SET client_config = \$1`).
		WithArgs(ccJSON, "baidu", "ghost").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ar.UpdateClientConfig(context.Background(), types.VendorBaidu, "ghost", types.ClientConfig{ClientID: "cid"})
	if err == nil {
		t.Fatal("expected ErrAccountNotFound, got nil")
	}
}

// ─── GetAccountSecret ───

func TestGetAccountSecret(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})

	credJSON, _ := json.Marshal(types.Credential{RefreshToken: "rt-secret", Cookies: map[string]string{"k": "v"}})
	ccJSON, _ := json.Marshal(types.ClientConfig{ClientID: "cid", Region: "cn"})
	mock.ExpectQuery(`SELECT credential, client_config FROM cloud_account WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("onedrive", "od_01").
		WillReturnRows(sqlmock.NewRows([]string{"credential", "client_config"}).AddRow(credJSON, ccJSON))

	cred, cc, err := ar.GetAccountSecret(context.Background(), types.VendorOneDrive, "od_01")
	if err != nil {
		t.Fatalf("GetAccountSecret: %v", err)
	}
	if cred.RefreshToken != "rt-secret" || cred.Cookies["k"] != "v" {
		t.Errorf("cred = %+v, want RefreshToken rt-secret + cookie k=v", cred)
	}
	if cc.ClientID != "cid" || cc.Region != "cn" {
		t.Errorf("cc = %+v, want ClientID cid + Region cn", cc)
	}
}

func TestGetAccountSecret_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})

	mock.ExpectQuery(`SELECT credential, client_config FROM cloud_account`).
		WithArgs("baidu", "ghost").
		WillReturnError(sql.ErrNoRows)

	_, _, err = ar.GetAccountSecret(context.Background(), types.VendorBaidu, "ghost")
	if err == nil {
		t.Fatal("expected ErrAccountNotFound, got nil")
	}
	if !errors.Is(err, ErrAccountNotFound) {
		t.Errorf("expected ErrAccountNotFound in chain, got %v", err)
	}
}

// ─── SetEnabled ───

func TestSetEnabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})

	mock.ExpectExec(`UPDATE cloud_account SET enabled = \$1, updated_at = now\(\) WHERE vendor = \$2 AND account_id = \$3`).
		WithArgs(true, "baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.SetEnabled(context.Background(), types.VendorBaidu, "acct_01", true); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSetEnabled_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})

	mock.ExpectExec(`UPDATE cloud_account SET enabled = \$1`).
		WithArgs(false, "baidu", "ghost").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := ar.SetEnabled(context.Background(), types.VendorBaidu, "ghost", false); err == nil {
		t.Fatal("expected ErrAccountNotFound, got nil")
	}
}

// ─── Ban / Unban ───

// Given a healthy account, When Ban runs, Then account_health upserts to
// banned and a BAN event with reason + ban_until is broadcast.
func TestBan_UpsertsHealthAndBroadcasts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	banUntil := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mock.ExpectExec(`INSERT INTO account_health \(vendor, account_id, state, last_check, latency_ms, error_msg, ban_until\) VALUES \(\$1, \$2, 'banned', now\(\), 0, \$3, \$4\) ON CONFLICT \(vendor, account_id\) DO UPDATE SET state = 'banned', last_check = now\(\), error_msg = \$3, ban_until = \$4`).
		WithArgs("baidu", "acct_01", "http 403", banUntil).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.Ban(ctx, types.VendorBaidu, "acct_01", "http 403", banUntil); err != nil {
		t.Fatalf("Ban: %v", err)
	}
	if len(b.events) != 1 {
		t.Fatalf("expected 1 BAN broadcast, got %d", len(b.events))
	}
	if b.events[0].eventType != types.EventBan {
		t.Errorf("event type = %q, want %q", b.events[0].eventType, types.EventBan)
	}
	payload, ok := b.events[0].payload.(types.BanPayload)
	if !ok {
		t.Fatalf("payload type: expected types.BanPayload, got %T", b.events[0].payload)
	}
	if payload.Vendor != types.VendorBaidu || payload.AccountID != "acct_01" || payload.Reason != "http 403" || !payload.BanUntil.Equal(banUntil) {
		t.Errorf("payload = %+v, want baidu/acct_01/http 403/%v", payload, banUntil)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// QA failure: Ban on an account whose health upsert succeeds but the broadcast
// fails → nil error + Warn (eventual consistency, no transaction coupling).
func TestBan_BroadcastFailure_ReturnsNilAndWarns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{err: errors.New("stream reset")}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	var logBuf bytes.Buffer
	prevWriter := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prevWriter)

	mock.ExpectExec(`INSERT INTO account_health`).
		WithArgs("baidu", "ghost", "manual", nil).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.Ban(ctx, types.VendorBaidu, "ghost", "manual", time.Time{}); err != nil {
		t.Fatalf("Ban with failing broadcast must return nil, got %v", err)
	}
	if !strings.Contains(logBuf.String(), "broadcast ban baidu/ghost") {
		t.Errorf("expected Warn log for failed broadcast, got: %s", logBuf.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestUnban_UpsertsHealthyAndBroadcasts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)

	mock.ExpectExec(`INSERT INTO account_health \(vendor, account_id, state, last_check, latency_ms, error_msg, ban_until\) VALUES \(\$1, \$2, 'healthy', now\(\), 0, '', NULL\) ON CONFLICT \(vendor, account_id\) DO UPDATE SET state = 'healthy', last_check = now\(\), error_msg = '', ban_until = NULL`).
		WithArgs("baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.Unban(context.Background(), types.VendorBaidu, "acct_01"); err != nil {
		t.Fatalf("Unban: %v", err)
	}
	if len(b.events) != 1 {
		t.Fatalf("expected 1 UNBAN broadcast, got %d", len(b.events))
	}
	if b.events[0].eventType != types.EventUnban {
		t.Errorf("event type = %q, want %q", b.events[0].eventType, types.EventUnban)
	}
	payload := b.events[0].payload.(types.BanPayload)
	if payload.Vendor != types.VendorBaidu || payload.AccountID != "acct_01" {
		t.Errorf("payload = %+v, want baidu/acct_01", payload)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// ─── SetRateLimit / SetVendorProfile ───

func TestSetRateLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})

	cfg := types.RateLimitConfig{QPS: 4, Burst: 8, ConcurrentLimit: 12}
	rlJSON, _ := json.Marshal(cfg)
	mock.ExpectExec(`UPDATE cloud_account SET rate_limit_config = \$1, updated_at = now\(\) WHERE vendor = \$2 AND account_id = \$3`).
		WithArgs(rlJSON, "baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.SetRateLimit(context.Background(), types.VendorBaidu, "acct_01", cfg); err != nil {
		t.Fatalf("SetRateLimit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSetRateLimit_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})

	mock.ExpectExec(`UPDATE cloud_account SET rate_limit_config = \$1`).
		WithArgs([]byte(`{"qps":1,"burst":2,"concurrent_limit":3}`), "baidu", "ghost").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ar.SetRateLimit(context.Background(), types.VendorBaidu, "ghost", types.RateLimitConfig{QPS: 1, Burst: 2, ConcurrentLimit: 3})
	if err == nil {
		t.Fatal("expected ErrAccountNotFound, got nil")
	}
}

func TestSetVendorProfile(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	ar := NewAccountRegistry(db, &mockBroadcaster{})

	vp := types.VendorProfile{Vendor: types.VendorBaidu, Weight: 4.5, BaseLatencyMs: 200, BandwidthMbps: 100}
	vpJSON, _ := json.Marshal(vp)
	mock.ExpectExec(`UPDATE cloud_account SET vendor_profile = \$1, updated_at = now\(\) WHERE vendor = \$2 AND account_id = \$3`).
		WithArgs(vpJSON, "baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.SetVendorProfile(context.Background(), types.VendorBaidu, "acct_01", vp); err != nil {
		t.Fatalf("SetVendorProfile: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// Given no blob_location references, when DeleteAccount runs without force,
// then health + account rows are deleted in one committed transaction.
func TestDeleteAccount_Happy(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	ar := NewAccountRegistry(db, &mockBroadcaster{})

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT count\(\*\) FROM blob_location WHERE backend_id = \$1`).
		WithArgs("baidu:acct_01").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`DELETE FROM account_health WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM cloud_account WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	n, err := ar.DeleteAccount(context.Background(), types.VendorBaidu, "acct_01", false)
	if err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
	if n != 0 {
		t.Errorf("blobCount = %d, want 0", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// Given blob_location references, when DeleteAccount runs without force, then
// it refuses with ErrAccountHasBlobs plus the count and rolls back.
func TestDeleteAccount_RefusedWithBlobRefs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	ar := NewAccountRegistry(db, &mockBroadcaster{})

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT count\(\*\) FROM blob_location WHERE backend_id = \$1`).
		WithArgs("baidu:acct_01").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	mock.ExpectRollback()

	n, err := ar.DeleteAccount(context.Background(), types.VendorBaidu, "acct_01", false)
	if !errors.Is(err, ErrAccountHasBlobs) {
		t.Fatalf("err = %v, want ErrAccountHasBlobs", err)
	}
	if n != 3 {
		t.Errorf("blobCount = %d, want 3", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// Given blob_location references, when DeleteAccount runs with force, then the
// references cascade before the account row is deleted, all committed.
func TestDeleteAccount_ForceCascade(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	ar := NewAccountRegistry(db, &mockBroadcaster{})

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT count\(\*\) FROM blob_location WHERE backend_id = \$1`).
		WithArgs("baidu:acct_01").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectExec(`DELETE FROM blob_location WHERE backend_id = \$1`).
		WithArgs("baidu:acct_01").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`DELETE FROM account_health WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM cloud_account WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("baidu", "acct_01").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	n, err := ar.DeleteAccount(context.Background(), types.VendorBaidu, "acct_01", true)
	if err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
	if n != 2 {
		t.Errorf("blobCount = %d, want 2", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// Given a missing account, when DeleteAccount runs, then it returns
// ErrAccountNotFound and rolls back.
func TestDeleteAccount_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	ar := NewAccountRegistry(db, &mockBroadcaster{})

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT count\(\*\) FROM blob_location WHERE backend_id = \$1`).
		WithArgs("baidu:ghost").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`DELETE FROM account_health WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("baidu", "ghost").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM cloud_account WHERE vendor = \$1 AND account_id = \$2`).
		WithArgs("baidu", "ghost").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	if _, err := ar.DeleteAccount(context.Background(), types.VendorBaidu, "ghost", false); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("err = %v, want ErrAccountNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// Given account_health marks the account banned, when ListByVendor runs,
// then AccountInfo.Banned is true so the snapshot carries the taint.
func TestListByVendor_BannedFlag(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()
	ar := NewAccountRegistry(db, &mockBroadcaster{})

	credJSON, _ := json.Marshal(types.Credential{RefreshToken: "rt"})
	rlJSON, _ := json.Marshal(types.RateLimitConfig{QPS: 1, Burst: 2, ConcurrentLimit: 5})
	vpJSON, _ := json.Marshal(types.VendorProfile{Vendor: types.VendorBaidu, Weight: 2.0})

	rows := sqlmock.NewRows([]string{"account_id", "credential", "client_config", "rate_limit_config", "vendor_profile", "enabled", "banned"}).
		AddRow("acct_ok", credJSON, nil, rlJSON, vpJSON, true, false).
		AddRow("acct_banned", credJSON, nil, rlJSON, vpJSON, true, true)
	mock.ExpectQuery("FROM cloud_account").
		WithArgs(string(types.VendorBaidu)).
		WillReturnRows(rows)

	accounts, err := ar.ListByVendor(context.Background(), types.VendorBaidu)
	if err != nil {
		t.Fatalf("ListByVendor: %v", err)
	}
	if len(accounts) != 2 {
		t.Fatalf("accounts = %d, want 2", len(accounts))
	}
	if accounts[0].Banned {
		t.Errorf("accounts[0].Banned = true, want false")
	}
	if !accounts[1].Banned {
		t.Errorf("accounts[1].Banned = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
