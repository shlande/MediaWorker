// Package events wires control-plane broadcast events (ACCOUNT_SNAPSHOT,
// CREDENTIAL_UPDATE, BAN, UNBAN, CIRCUIT_FORCE_OPEN/CLOSE) into the node's
// local account pool. The Dispatcher is a pure event consumer: it never makes
// HTTP calls and never changes the SyncBroadcaster wire protocol.
package events

import (
	"encoding/json"
	"log/slog"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/types"
)

// Dispatcher applies control-plane events to the local account pool. A nil
// pool is tolerated: non-L4 nodes have no data plane, so every event is a
// no-op (malformed payloads still Warn-log before the pool is touched).
type Dispatcher struct {
	pool     *accountpool.AccountPool
	tokenMgr *auth.TokenManager
	logger   *slog.Logger
}

// NewDispatcher creates a Dispatcher. A nil logger falls back to slog.Default.
// tokenMgr is held for the L4 wiring seam (todo 17); credential-driven token
// re-registration lands with todo 6's contract.
func NewDispatcher(pool *accountpool.AccountPool, tokenMgr *auth.TokenManager, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{pool: pool, tokenMgr: tokenMgr, logger: logger}
}

// HandleEvent dispatches one broadcast event by type. Unknown types are
// ignored (Debug log) so new CP events never break old nodes.
func (d *Dispatcher) HandleEvent(evt types.Event) {
	switch evt.Type {
	case types.EventAccountSnapshot:
		d.handleAccountSnapshot(evt.Payload)
	case types.EventCredentialUpdate:
		d.handleCredentialUpdate(evt.Payload)
	case types.EventBan:
		d.handleBan(evt.Payload)
	case types.EventUnban:
		d.handleUnban(evt.Payload)
	case types.EventCircuitForceOpen:
		d.handleCircuit(evt.Payload, true)
	case types.EventCircuitForceClose:
		d.handleCircuit(evt.Payload, false)
	default:
		d.logger.Debug("events: unknown event type, ignoring", "type", evt.Type)
	}
}

func (d *Dispatcher) handleAccountSnapshot(payload []byte) {
	var entries []types.AccountSnapshotEntry
	if err := json.Unmarshal(payload, &entries); err != nil {
		d.logger.Warn("events: decode ACCOUNT_SNAPSHOT payload", "err", err)
		return
	}
	if d.pool == nil {
		d.logger.Debug("events: no account pool (non-L4), skipping ACCOUNT_SNAPSHOT", "accounts", len(entries))
		return
	}
	fresh := accountpool.BuildFromSnapshot(entries, nil)
	d.pool.ReplaceAll(accountsFromPool(fresh))
	d.logger.Info("events: account pool rebuilt from snapshot", "accounts", len(fresh.SnapshotAccounts()))
}

func (d *Dispatcher) handleCredentialUpdate(payload []byte) {
	var p types.CredentialChangePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		d.logger.Warn("events: decode CREDENTIAL_UPDATE payload", "err", err)
		return
	}
	if d.pool == nil {
		d.logger.Debug("events: no account pool (non-L4), skipping CREDENTIAL_UPDATE", "vendor", p.Vendor, "account_id", p.AccountID)
		return
	}
	if credentialEmpty(p.Credential) {
		d.logger.Warn("events: CREDENTIAL_UPDATE carries no credential body, awaiting next ACCOUNT_SNAPSHOT (<=60s) to converge",
			"vendor", p.Vendor, "account_id", p.AccountID)
		return
	}
	d.pool.UpdateCredential(string(p.Vendor)+":"+p.AccountID, p.Credential)
	d.logger.Info("events: credential updated", "vendor", p.Vendor, "account_id", p.AccountID)
}

func (d *Dispatcher) handleBan(payload []byte) {
	var p types.BanPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		d.logger.Warn("events: decode BAN payload", "err", err)
		return
	}
	if d.pool == nil {
		d.logger.Debug("events: no account pool (non-L4), skipping BAN", "vendor", p.Vendor, "account_id", p.AccountID)
		return
	}
	d.pool.MarkBanned(string(p.Vendor) + ":" + p.AccountID)
	d.logger.Info("events: account banned", "vendor", p.Vendor, "account_id", p.AccountID, "reason", p.Reason)
}

func (d *Dispatcher) handleUnban(payload []byte) {
	var p types.BanPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		d.logger.Warn("events: decode UNBAN payload", "err", err)
		return
	}
	if d.pool == nil {
		d.logger.Debug("events: no account pool (non-L4), skipping UNBAN", "vendor", p.Vendor, "account_id", p.AccountID)
		return
	}
	key := string(p.Vendor) + ":" + p.AccountID
	d.pool.UpdateHealth(key, types.HealthState{State: "healthy"})
	d.pool.ForceCloseCircuit(string(p.Vendor), p.AccountID)
	d.logger.Info("events: account unbanned", "vendor", p.Vendor, "account_id", p.AccountID)
}

func (d *Dispatcher) handleCircuit(payload []byte, open bool) {
	var p types.CircuitPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		d.logger.Warn("events: decode circuit payload", "err", err, "open", open)
		return
	}
	if d.pool == nil {
		d.logger.Debug("events: no account pool (non-L4), skipping circuit event", "vendor", p.Vendor, "account_id", p.AccountID, "open", open)
		return
	}
	d.pool.ForceCircuit(string(p.Vendor), p.AccountID, open)
	d.logger.Info("events: circuit forced", "vendor", p.Vendor, "account_id", p.AccountID, "open", open)
}

// credentialEmpty reports whether a credential body carries no secret material
// (i.e. the field was absent from the event payload).
func credentialEmpty(c types.Credential) bool {
	return c.Cookies == nil && c.AccessToken == "" && c.RefreshToken == "" && c.TokenExpire.IsZero()
}

// accountsFromPool flattens a freshly built pool into value Accounts for
// ReplaceAll. Field-wise assignment avoids copying the atomic fields of live
// Accounts (vet copylocks); Health and Concurrent are re-stored as values.
func accountsFromPool(p *accountpool.AccountPool) []accountpool.Account {
	snap := p.SnapshotAccounts()
	out := make([]accountpool.Account, len(snap))
	for i, a := range snap {
		out[i].Vendor = a.Vendor
		out[i].AccountID = a.AccountID
		out[i].Credential = a.Credential
		out[i].Driver = a.Driver
		out[i].Limiter = a.Limiter
		out[i].CB = a.CB
		out[i].VendorWeight = a.VendorWeight
		out[i].Concurrent.Store(a.Concurrent.Load())
		if h, ok := a.Health.Load().(types.HealthState); ok {
			out[i].Health.Store(h)
		}
	}
	return out
}
