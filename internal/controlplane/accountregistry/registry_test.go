package accountregistry

import (
	"context"
	"encoding/json"
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
}

type broadcastCall struct {
	eventType string
	payload   any
}

func (m *mockBroadcaster) Broadcast(eventType string, payload any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func (m *mockBroadcaster) reset() {
	m.events = nil
}

func TestCreateAccountAndListByVendor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

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
		RateLimitCfg: types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5},
		VendorProfile: types.VendorProfile{
			Vendor: types.Vendor115, Weight: 3.0, BaseLatencyMs: 100, BandwidthMbps: 50,
		},
		Enabled: true,
	}

	credJSON, _ := json.Marshal(info.Credential)
	rlJSON, _ := json.Marshal(info.RateLimitCfg)
	vpJSON, _ := json.Marshal(info.VendorProfile)

	mock.ExpectExec(`INSERT INTO cloud_account`).
		WithArgs(string(info.Vendor), info.AccountID, credJSON, rlJSON, vpJSON, info.Enabled).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := ar.CreateAccount(ctx, info); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("CreateAccount expectations: %v", err)
	}

	// Now ListByVendor and verify the fields round-trip.
	rows := sqlmock.NewRows([]string{"account_id", "credential", "rate_limit_config", "vendor_profile", "enabled"}).
		AddRow("acct_01", credJSON, rlJSON, vpJSON, true)

	mock.ExpectQuery(`SELECT account_id, credential, rate_limit_config, vendor_profile, enabled FROM cloud_account WHERE vendor = \$1 AND enabled = true ORDER BY account_id`).
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

func TestUpdateCredential_TriggersBroadcast(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

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

	// Verify Broadcast was called.
	if len(b.events) != 1 {
		t.Fatalf("expected 1 broadcast event, got %d", len(b.events))
	}
	if b.events[0].eventType != types.EventCredentialUpdate {
		t.Errorf("event type: expected %s, got %s", types.EventCredentialUpdate, b.events[0].eventType)
	}
	payload, ok := b.events[0].payload.(CredentialChangePayload)
	if !ok {
		t.Fatalf("payload type: expected CredentialChangePayload, got %T", b.events[0].payload)
	}
	if payload.Vendor != types.Vendor115 {
		t.Errorf("payload.Vendor: expected 115, got %s", payload.Vendor)
	}
	if payload.AccountID != "acct_01" {
		t.Errorf("payload.AccountID: expected acct_01, got %s", payload.AccountID)
	}
}

func TestUpdateCredential_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

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
	defer db.Close()

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

func TestOnCredentialChange_Broadcasts(t *testing.T) {
	b := &mockBroadcaster{}
	ar := NewAccountRegistry(nil, b) // db not used by OnCredentialChange
	ctx := context.Background()

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
}

func TestStartSync_EmitsSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

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
			rows := sqlmock.NewRows([]string{"account_id", "credential", "rate_limit_config", "vendor_profile", "enabled"}).
				AddRow("acct_01", credJSON, rlJSON, vpJSON, true)
			mock.ExpectQuery(`SELECT account_id, credential, rate_limit_config, vendor_profile, enabled FROM cloud_account WHERE vendor = \$1 AND enabled = true ORDER BY account_id`).
				WithArgs(string(vendor)).
				WillReturnRows(rows)
		} else {
			rows := sqlmock.NewRows([]string{"account_id", "credential", "rate_limit_config", "vendor_profile", "enabled"})
			mock.ExpectQuery(`SELECT account_id, credential, rate_limit_config, vendor_profile, enabled FROM cloud_account WHERE vendor = \$1 AND enabled = true ORDER BY account_id`).
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
	defer db.Close()

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
		rows := sqlmock.NewRows([]string{"account_id", "credential", "rate_limit_config", "vendor_profile", "enabled"})
		mock.ExpectQuery(`SELECT account_id, credential, rate_limit_config, vendor_profile, enabled FROM cloud_account WHERE vendor = \$1 AND enabled = true ORDER BY account_id`).
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
	defer db.Close()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	rows := sqlmock.NewRows([]string{"account_id", "credential", "rate_limit_config", "vendor_profile", "enabled"})
	mock.ExpectQuery(`SELECT account_id, credential, rate_limit_config, vendor_profile, enabled FROM cloud_account WHERE vendor = \$1 AND enabled = true ORDER BY account_id`).
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
	defer db.Close()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	if ar == nil {
		t.Fatal("NewAccountRegistry returned nil")
	}
}
