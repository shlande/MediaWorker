package events

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	"github.com/shlande/mediaworker/internal/storage/driver/mock"
	"github.com/shlande/mediaworker/internal/types"
)

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func newTestPool(t *testing.T, vendor types.Vendor, accountID string) (*accountpool.AccountPool, *accountpool.Account) {
	t.Helper()
	pool := accountpool.NewAccountPool(nil)
	key := string(vendor) + ":" + accountID
	acct := &accountpool.Account{
		Vendor:       vendor,
		AccountID:    accountID,
		Driver:       mock.NewMockDriver(vendor, mock.MockDriverConfig{}),
		Limiter:      rate.NewLimiter(10, 20),
		CB:           circuitbreaker.New(key, 5, 100*time.Millisecond),
		VendorWeight: 2.0,
	}
	acct.Health.Store(types.HealthState{State: "healthy"})
	pool.AddAccount(acct)
	return pool, acct
}

func mustEvent(t *testing.T, eventType string, payload any) types.Event {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return types.Event{Type: eventType, Payload: data}
}

// Given a pool with a healthy baidu account, When a BAN event arrives, Then
// the account is marked banned and its circuit breaker forced open.
func TestHandleEvent_BAN_marksBannedAndOpensCircuit(t *testing.T) {
	pool, acct := newTestPool(t, types.VendorBaidu, "a1")
	d := NewDispatcher(pool, nil, nil)

	d.HandleEvent(mustEvent(t, types.EventBan, types.BanPayload{
		Vendor: types.VendorBaidu, AccountID: "a1", Reason: "http 403",
	}))

	if h := acct.Health.Load().(types.HealthState); h.State != "banned" {
		t.Errorf("health = %q, want banned", h.State)
	}
	if acct.CB.State() != accountpool.StateOpen {
		t.Errorf("CB state = %d, want %d (open)", acct.CB.State(), accountpool.StateOpen)
	}
}

// Given a banned account, When an UNBAN event arrives, Then health returns to
// healthy and the circuit breaker is force-closed.
func TestHandleEvent_UNBAN_restoresHealthyAndClosesCircuit(t *testing.T) {
	pool, acct := newTestPool(t, types.VendorBaidu, "a1")
	pool.MarkBanned("baidu:a1")
	d := NewDispatcher(pool, nil, nil)

	d.HandleEvent(mustEvent(t, types.EventUnban, types.BanPayload{
		Vendor: types.VendorBaidu, AccountID: "a1",
	}))

	if h := acct.Health.Load().(types.HealthState); h.State != "healthy" {
		t.Errorf("health = %q, want healthy", h.State)
	}
	if acct.CB.State() != accountpool.StateClosed {
		t.Errorf("CB state = %d, want %d (closed)", acct.CB.State(), accountpool.StateClosed)
	}
}

// Given a healthy account, When CIRCUIT_FORCE_OPEN then CIRCUIT_FORCE_CLOSE
// arrive, Then the breaker follows each command.
func TestHandleEvent_CircuitForceOpenClose(t *testing.T) {
	pool, acct := newTestPool(t, types.VendorOneDrive, "od1")
	d := NewDispatcher(pool, nil, nil)

	d.HandleEvent(mustEvent(t, types.EventCircuitForceOpen, types.CircuitPayload{
		Vendor: types.VendorOneDrive, AccountID: "od1",
	}))
	if acct.CB.State() != accountpool.StateOpen {
		t.Errorf("after FORCE_OPEN: CB state = %d, want %d (open)", acct.CB.State(), accountpool.StateOpen)
	}

	d.HandleEvent(mustEvent(t, types.EventCircuitForceClose, types.CircuitPayload{
		Vendor: types.VendorOneDrive, AccountID: "od1",
	}))
	if acct.CB.State() != accountpool.StateClosed {
		t.Errorf("after FORCE_CLOSE: CB state = %d, want %d (closed)", acct.CB.State(), accountpool.StateClosed)
	}
}

// Given a CREDENTIAL_UPDATE carrying a credential body, When handled, Then the
// pool credential is updated immediately.
func TestHandleEvent_CredentialUpdate_withCredential_appliesImmediately(t *testing.T) {
	pool, acct := newTestPool(t, types.VendorBaidu, "a1")
	d := NewDispatcher(pool, nil, nil)

	d.HandleEvent(mustEvent(t, types.EventCredentialUpdate, types.CredentialChangePayload{
		Vendor:    types.VendorBaidu,
		AccountID: "a1",
		Credential: types.Credential{
			RefreshToken: "rt-new",
		},
	}))

	if acct.Credential.RefreshToken != "rt-new" {
		t.Errorf("credential RefreshToken = %q, want rt-new (immediate update)", acct.Credential.RefreshToken)
	}
}

// Given a CREDENTIAL_UPDATE without a credential body (old CP), When handled,
// Then only a Warn is logged and the stored credential is untouched.
func TestHandleEvent_CredentialUpdate_withoutCredential_warnsOnly(t *testing.T) {
	logs := captureLogs(t)
	pool, acct := newTestPool(t, types.VendorBaidu, "a1")
	acct.Credential = types.Credential{RefreshToken: "rt-old"}
	d := NewDispatcher(pool, nil, nil)

	d.HandleEvent(mustEvent(t, types.EventCredentialUpdate, map[string]string{
		"vendor": "baidu", "account_id": "a1",
	}))

	if acct.Credential.RefreshToken != "rt-old" {
		t.Errorf("credential changed to %q, want untouched rt-old", acct.Credential.RefreshToken)
	}
	if !strings.Contains(logs.String(), "awaiting next ACCOUNT_SNAPSHOT") {
		t.Errorf("expected convergence Warn, logs: %s", logs.String())
	}
}

// Given an ACCOUNT_SNAPSHOT, When handled, Then the pool is rebuilt from the
// snapshot (old accounts replaced, driver-less vendors skipped).
func TestHandleEvent_AccountSnapshot_rebuildsPool(t *testing.T) {
	pool, _ := newTestPool(t, types.VendorBaidu, "old_acct")
	d := NewDispatcher(pool, nil, nil)

	entries := []types.AccountSnapshotEntry{
		{
			Vendor:       types.VendorBaidu,
			AccountID:    "bd_new",
			Credential:   types.Credential{RefreshToken: "rt"},
			ClientConfig: types.ClientConfig{ClientID: "cid", ClientSecret: "cs"},
			Enabled:      true,
		},
		{
			Vendor:     types.Vendor115,
			AccountID:  "ck_new",
			Credential: types.Credential{Cookies: map[string]string{"UID": "1"}},
			Enabled:    true,
		},
	}
	d.HandleEvent(mustEvent(t, types.EventAccountSnapshot, entries))

	snap := pool.SnapshotAccounts()
	if len(snap) != 1 {
		t.Fatalf("pool size after snapshot = %d, want 1 (baidu only, 115 skipped)", len(snap))
	}
	if snap[0].AccountID != "bd_new" {
		t.Errorf("pool account = %q, want bd_new (old_acct replaced)", snap[0].AccountID)
	}
	if h := snap[0].Health.Load().(types.HealthState); h.State != "healthy" {
		t.Errorf("rebuilt health = %q, want healthy", h.State)
	}
}

// Given malformed payloads, When handled, Then each logs a Warn and nothing panics.
func TestHandleEvent_badPayload_warnsNoPanic(t *testing.T) {
	logs := captureLogs(t)
	pool, acct := newTestPool(t, types.VendorBaidu, "a1")
	d := NewDispatcher(pool, nil, nil)

	for _, eventType := range []string{
		types.EventAccountSnapshot, types.EventCredentialUpdate,
		types.EventBan, types.EventUnban,
		types.EventCircuitForceOpen, types.EventCircuitForceClose,
	} {
		d.HandleEvent(types.Event{Type: eventType, Payload: []byte("{not json")})
	}

	if h := acct.Health.Load().(types.HealthState); h.State != "healthy" {
		t.Errorf("health = %q after bad payloads, want healthy", h.State)
	}
	if acct.CB.State() != accountpool.StateClosed {
		t.Errorf("CB state = %d after bad payloads, want closed", acct.CB.State())
	}
	if got := strings.Count(logs.String(), "level=WARN"); got != 6 {
		t.Errorf("Warn count = %d, want 6 (one per bad payload)", got)
	}
}

// Given an unknown event type, When handled, Then it is ignored (Debug) without side effects.
func TestHandleEvent_unknownType_ignored(t *testing.T) {
	logs := captureLogs(t)
	pool, acct := newTestPool(t, types.VendorBaidu, "a1")
	d := NewDispatcher(pool, nil, nil)

	d.HandleEvent(mustEvent(t, "SOME_FUTURE_EVENT", map[string]string{"foo": "bar"}))
	d.HandleEvent(mustEvent(t, types.EventQuotaUpdate, map[string]string{"foo": "bar"}))

	if h := acct.Health.Load().(types.HealthState); h.State != "healthy" {
		t.Errorf("health = %q, want healthy (no side effects)", h.State)
	}
	if strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("unknown events must not Warn, logs: %s", logs.String())
	}
}

// Given a nil pool (non-L4 node), When any event arrives, Then nothing panics
// and no state changes; malformed payloads still Warn.
func TestHandleEvent_nilPool_noopNoPanic(t *testing.T) {
	logs := captureLogs(t)
	d := NewDispatcher(nil, nil, nil)

	d.HandleEvent(mustEvent(t, types.EventBan, types.BanPayload{Vendor: types.VendorBaidu, AccountID: "a1"}))
	d.HandleEvent(mustEvent(t, types.EventUnban, types.BanPayload{Vendor: types.VendorBaidu, AccountID: "a1"}))
	d.HandleEvent(mustEvent(t, types.EventCircuitForceOpen, types.CircuitPayload{Vendor: types.VendorBaidu, AccountID: "a1"}))
	d.HandleEvent(mustEvent(t, types.EventCredentialUpdate, types.CredentialChangePayload{
		Vendor: types.VendorBaidu, AccountID: "a1", Credential: types.Credential{RefreshToken: "rt"},
	}))
	d.HandleEvent(mustEvent(t, types.EventAccountSnapshot, []types.AccountSnapshotEntry{
		{Vendor: types.VendorBaidu, AccountID: "a1", Enabled: true},
	}))
	if strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("valid events with nil pool must not Warn, logs: %s", logs.String())
	}

	d.HandleEvent(types.Event{Type: types.EventBan, Payload: []byte("{bad")})
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("bad payload with nil pool must still Warn, logs: %s", logs.String())
	}
}
