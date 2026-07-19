// W0 acceptance gate: control-plane account events propagate through the
// node-side event chain — ACCOUNT_SNAPSHOT rebuilds the real AccountPool,
// BAN marks the target banned + opens its circuit breaker (SelectForRead
// stops selecting it), CIRCUIT_FORCE_CLOSE closes the breaker, UNBAN
// restores healthy + closed. Events are constructed directly as types.Event
// and fed to Dispatcher.HandleEvent: no libp2p, no CP JWT server, no PG.
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/shlande/mediaworker/internal/node/events"
	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/types"
)

// mustAccountEvent serializes payload into a types.Event, mirroring what the
// SyncBroadcaster client would deliver to HandleEvent on the wire.
func mustAccountEvent(t *testing.T, eventType string, payload any) types.Event {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", eventType, err)
	}
	return types.Event{Type: eventType, Payload: data}
}

// captureEventLogs swaps slog.Default to write into the returned buffer.
// HandleEvent is synchronous, so a plain bytes.Buffer is race-free here.
func captureEventLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// accountByID indexes pool.SnapshotAccounts() by account ID for assertions.
func accountByID(pool *accountpool.AccountPool) map[string]*accountpool.Account {
	out := make(map[string]*accountpool.Account)
	for _, a := range pool.SnapshotAccounts() {
		out[a.AccountID] = a
	}
	return out
}

// snapshotEntries builds a two-account baidu snapshot. Rate limits are set
// high so Limiter.Allow() never gates SelectForRead during assertions.
func snapshotEntries() []types.AccountSnapshotEntry {
	rateCfg := types.RateLimitConfig{QPS: 1000, Burst: 1000, ConcurrentLimit: 16}
	return []types.AccountSnapshotEntry{
		{
			Vendor:       types.VendorBaidu,
			AccountID:    "chain-acct-1",
			Credential:   types.Credential{RefreshToken: "rt-1"},
			ClientConfig: types.ClientConfig{ClientID: "cid-1", ClientSecret: "cs-1"},
			RateLimitCfg: rateCfg,
			Enabled:      true,
		},
		{
			Vendor:       types.VendorBaidu,
			AccountID:    "chain-acct-2",
			Credential:   types.Credential{RefreshToken: "rt-2"},
			ClientConfig: types.ClientConfig{ClientID: "cid-2", ClientSecret: "cs-2"},
			RateLimitCfg: rateCfg,
			Enabled:      true,
		},
	}
}

// Given a dispatcher wired to a REAL AccountPool (mock meta for blob
// locations), When the full event chain is fed in order, Then the pool state
// tracks each event exactly.
func TestAccountEvents_SnapshotBanCircuitUnbanChain(t *testing.T) {
	ctx := context.Background()
	const blobHash = "chain-blob"

	// (a) Assemble: mock meta (same pattern as storage_distribution_test.go)
	// + real AccountPool + dispatcher. No libp2p, no CP JWT server.
	meta := &mockMetaClient{
		locations: map[string][]types.BlobLocation{
			blobHash: {
				{BackendID: "baidu:chain-acct-1", FileID: "fid-1", BlobHash: blobHash},
				{BackendID: "baidu:chain-acct-2", FileID: "fid-2", BlobHash: blobHash},
			},
		},
	}
	pool := accountpool.NewAccountPool(meta)
	d := events.NewDispatcher(pool, nil, nil)

	// (b) ACCOUNT_SNAPSHOT with 2 accounts → pool rebuilt with both.
	d.HandleEvent(mustAccountEvent(t, types.EventAccountSnapshot, snapshotEntries()))

	if got := len(pool.SnapshotAccounts()); got != 2 {
		t.Fatalf("after snapshot: pool accounts = %d, want 2", got)
	}
	accounts := accountByID(pool)
	acct1, ok := accounts["chain-acct-1"]
	if !ok {
		t.Fatal("after snapshot: chain-acct-1 missing from pool")
	}
	acct2, ok := accounts["chain-acct-2"]
	if !ok {
		t.Fatal("after snapshot: chain-acct-2 missing from pool")
	}
	for _, a := range []*accountpool.Account{acct1, acct2} {
		if h := a.Health.Load().(types.HealthState); h.State != "healthy" {
			t.Errorf("after snapshot: %s health = %q, want healthy", a.AccountID, h.State)
		}
		if a.CB.State() != accountpool.StateClosed {
			t.Errorf("after snapshot: %s CB = %d, want %d (closed)", a.AccountID, a.CB.State(), accountpool.StateClosed)
		}
	}
	// Baseline: both accounts eligible for reads.
	selected, err := pool.SelectForRead(ctx, blobHash)
	if err != nil {
		t.Fatalf("after snapshot: SelectForRead: %v", err)
	}
	if selected.AccountID != "chain-acct-1" && selected.AccountID != "chain-acct-2" {
		t.Errorf("after snapshot: SelectForRead = %q, want one of the snapshot accounts", selected.AccountID)
	}

	// (c) BAN chain-acct-1 → health banned + CB open + SelectForRead excludes it.
	d.HandleEvent(mustAccountEvent(t, types.EventBan, types.BanPayload{
		Vendor: types.VendorBaidu, AccountID: "chain-acct-1", Reason: "http 403",
	}))

	if h := acct1.Health.Load().(types.HealthState); h.State != "banned" {
		t.Errorf("after BAN: acct-1 health = %q, want banned", h.State)
	}
	if acct1.CB.State() != accountpool.StateOpen {
		t.Errorf("after BAN: acct-1 CB = %d, want %d (open)", acct1.CB.State(), accountpool.StateOpen)
	}
	// acct-2 untouched.
	if h := acct2.Health.Load().(types.HealthState); h.State != "healthy" {
		t.Errorf("after BAN: acct-2 health = %q, want healthy", h.State)
	}
	// SelectForRead must never return the banned account (deterministic:
	// acct-2 is the only eligible candidate).
	selected, err = pool.SelectForRead(ctx, blobHash)
	if err != nil {
		t.Fatalf("after BAN: SelectForRead: %v", err)
	}
	if selected.AccountID != "chain-acct-2" {
		t.Errorf("after BAN: SelectForRead = %q, want chain-acct-2 (banned account excluded)", selected.AccountID)
	}

	// (d) CIRCUIT_FORCE_CLOSE on the banned account → CB closed; health
	// stays banned (the circuit command does not clear health).
	d.HandleEvent(mustAccountEvent(t, types.EventCircuitForceClose, types.CircuitPayload{
		Vendor: types.VendorBaidu, AccountID: "chain-acct-1",
	}))

	if acct1.CB.State() != accountpool.StateClosed {
		t.Errorf("after FORCE_CLOSE: acct-1 CB = %d, want %d (closed)", acct1.CB.State(), accountpool.StateClosed)
	}
	if h := acct1.Health.Load().(types.HealthState); h.State != "banned" {
		t.Errorf("after FORCE_CLOSE: acct-1 health = %q, want still banned", h.State)
	}

	// (e) UNBAN chain-acct-1 → healthy + CB closed; both accounts eligible.
	d.HandleEvent(mustAccountEvent(t, types.EventUnban, types.BanPayload{
		Vendor: types.VendorBaidu, AccountID: "chain-acct-1",
	}))

	if h := acct1.Health.Load().(types.HealthState); h.State != "healthy" {
		t.Errorf("after UNBAN: acct-1 health = %q, want healthy", h.State)
	}
	if acct1.CB.State() != accountpool.StateClosed {
		t.Errorf("after UNBAN: acct-1 CB = %d, want %d (closed)", acct1.CB.State(), accountpool.StateClosed)
	}
	selected, err = pool.SelectForRead(ctx, blobHash)
	if err != nil {
		t.Fatalf("after UNBAN: SelectForRead: %v", err)
	}
	if selected.AccountID != "chain-acct-1" && selected.AccountID != "chain-acct-2" {
		t.Errorf("after UNBAN: SelectForRead = %q, want one of the snapshot accounts", selected.AccountID)
	}
}

// QA failure scenario: a BAN arriving immediately after a snapshot for an
// account the snapshot did NOT contain → Warn logged, no panic, pool
// untouched.
func TestAccountEvents_BanUnknownAccount_WarnsNoPanic(t *testing.T) {
	logs := captureEventLogs(t)

	pool := accountpool.NewAccountPool(&mockMetaClient{})
	d := events.NewDispatcher(pool, nil, nil)

	d.HandleEvent(mustAccountEvent(t, types.EventAccountSnapshot, snapshotEntries()))
	if got := len(pool.SnapshotAccounts()); got != 2 {
		t.Fatalf("after snapshot: pool accounts = %d, want 2", got)
	}

	// BAN an account the snapshot never carried.
	d.HandleEvent(mustAccountEvent(t, types.EventBan, types.BanPayload{
		Vendor: types.VendorBaidu, AccountID: "ghost-acct", Reason: "stale event",
	}))

	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("BAN for unknown account must Warn, logs: %s", logs.String())
	}
	// Pool unchanged: same 2 accounts, all healthy.
	accounts := accountByID(pool)
	if got := len(accounts); got != 2 {
		t.Fatalf("after unknown BAN: pool accounts = %d, want 2 (no phantom account)", got)
	}
	if _, ok := accounts["ghost-acct"]; ok {
		t.Error("after unknown BAN: ghost-acct must not enter the pool")
	}
	for id, a := range accounts {
		if h := a.Health.Load().(types.HealthState); h.State != "healthy" {
			t.Errorf("after unknown BAN: %s health = %q, want healthy", id, h.State)
		}
	}
}
