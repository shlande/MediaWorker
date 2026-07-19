package accountregistry

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/node/events"
	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	"github.com/shlande/mediaworker/internal/storage/driver/mock"
	"github.com/shlande/mediaworker/internal/types"
)

// W0 wire-contract interop: Given registry.Ban emits a BAN broadcast, When
// the payload crosses the wire boundary (JSON serialize → types.Event →
// node-side Dispatcher.HandleEvent), Then the same types.BanPayload shape
// decodes on both sides and the node pool applies the ban. No libp2p, no PG
// beyond sqlmock — the event is constructed in-process exactly as the
// SyncBroadcaster client would deliver it.
func TestBanPayloadInterop(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := &mockBroadcaster{}
	ar := NewAccountRegistry(db, b)
	ctx := context.Background()

	banUntil := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	mockDB.ExpectExec(`INSERT INTO account_health`).
		WithArgs("baidu", "acct-interop", "http 403", banUntil).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ar.Ban(ctx, types.VendorBaidu, "acct-interop", "http 403", banUntil); err != nil {
		t.Fatalf("Ban: %v", err)
	}

	// CP side: exactly one EventBan broadcast carrying a types.BanPayload.
	if len(b.events) != 1 {
		t.Fatalf("expected 1 BAN broadcast, got %d", len(b.events))
	}
	if b.events[0].eventType != types.EventBan {
		t.Fatalf("event type = %q, want %q", b.events[0].eventType, types.EventBan)
	}
	payload, ok := b.events[0].payload.(types.BanPayload)
	if !ok {
		t.Fatalf("payload type: expected types.BanPayload, got %T", b.events[0].payload)
	}

	// Serialize roundtrip: the wire shape decodes back to the identical value.
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal BanPayload: %v", err)
	}
	var back types.BanPayload
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal BanPayload: %v", err)
	}
	if back.Vendor != payload.Vendor || back.AccountID != payload.AccountID ||
		back.Reason != payload.Reason || !back.BanUntil.Equal(payload.BanUntil) {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", back, payload)
	}

	// Node side: a real AccountPool + dispatcher consume the wire event.
	pool := accountpool.NewAccountPool(nil)
	acct := &accountpool.Account{
		Vendor:       types.VendorBaidu,
		AccountID:    "acct-interop",
		Driver:       mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}),
		Limiter:      rate.NewLimiter(10, 20),
		CB:           circuitbreaker.New("baidu:acct-interop", 5, 100*time.Millisecond),
		VendorWeight: 2.0,
	}
	acct.Health.Store(types.HealthState{State: "healthy"})
	pool.AddAccount(acct)

	d := events.NewDispatcher(pool, nil, nil)
	d.HandleEvent(types.Event{Type: b.events[0].eventType, Payload: data})

	if h := acct.Health.Load().(types.HealthState); h.State != "banned" {
		t.Errorf("node pool health = %q, want banned (CP→node interop)", h.State)
	}
	if acct.CB.State() != accountpool.StateOpen {
		t.Errorf("node pool CB = %d, want %d (open)", acct.CB.State(), accountpool.StateOpen)
	}

	if err := mockDB.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
