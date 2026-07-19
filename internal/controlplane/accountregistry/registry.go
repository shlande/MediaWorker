// Package accountregistry implements the control-plane account master database
// (cloud_account PG table) and credential distribution via SyncBroadcaster.
//
// AccountRegistry is the single source of truth for cloud drive accounts. It
// owns CRUD (Create/Revoke/UpdateCredential), provides ListByVendor for
// the snapshot generator, and pushes credential-update events through the
// Broadcaster interface so that every connected node receives the change
// within <1s (incremental broadcast) and the periodic full-snapshot (60s).
package accountregistry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// Broadcaster is the narrow interface AccountRegistry depends on for
// propagating credential and snapshot events to connected nodes.
// SyncBroadcaster satisfies this interface via its Broadcast method.
type Broadcaster interface {
	Broadcast(eventType string, payload any) error
}

// AccountInfo is the full representation of a cloud-drive account as stored
// in the cloud_account table and carried in snapshot/incremental events.
type AccountInfo struct {
	Vendor        types.Vendor          `json:"vendor"`
	AccountID     string                `json:"account_id"`
	Credential    types.Credential      `json:"credential"`
	ClientConfig  types.ClientConfig    `json:"client_config"`
	RateLimitCfg  types.RateLimitConfig `json:"rate_limit_config"`
	VendorProfile types.VendorProfile   `json:"vendor_profile"`
	Enabled       bool                  `json:"enabled"`
}

// AccountRegistry is the PG-backed account master database. It exposes CRUD
// operations for cloud accounts and pushes credential-update events via the
// injected Broadcaster.
type AccountRegistry struct {
	db          *sql.DB
	broadcaster Broadcaster

	// emitSnapshotFn is the function called by StartSync on each tick.
	// Exposed as a field so tests can hook into it for deterministic
	// synchronization. Defaults to (*AccountRegistry).emitSnapshot.
	emitSnapshotFn func(context.Context)
}

// NewAccountRegistry creates an AccountRegistry backed by the given PostgreSQL
// *sql.DB and using b for broadcasting change events.
func NewAccountRegistry(db *sql.DB, b Broadcaster) *AccountRegistry {
	ar := &AccountRegistry{
		db:          db,
		broadcaster: b,
	}
	ar.emitSnapshotFn = ar.emitSnapshot
	return ar
}

// CreateAccount inserts a new cloud_account row. All JSONB fields are
// marshalled with encoding/json.
func (ar *AccountRegistry) CreateAccount(ctx context.Context, info AccountInfo) error {
	credJSON, err := json.Marshal(info.Credential)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal credential: %w", err)
	}
	ccJSON, err := json.Marshal(info.ClientConfig)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal client_config: %w", err)
	}
	rlJSON, err := json.Marshal(info.RateLimitCfg)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal rate_limit_config: %w", err)
	}
	vpJSON, err := json.Marshal(info.VendorProfile)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal vendor_profile: %w", err)
	}

	const query = `
		INSERT INTO cloud_account (vendor, account_id, credential, client_config, rate_limit_config, vendor_profile, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err = ar.db.ExecContext(ctx, query,
		string(info.Vendor), info.AccountID, credJSON, ccJSON, rlJSON, vpJSON, info.Enabled)
	if err != nil {
		return fmt.Errorf("accountregistry: insert account %s/%s: %w", info.Vendor, info.AccountID, err)
	}
	return nil
}

// UpdateCredential replaces the credential for a given account. If the account
// does not exist it returns ErrAccountNotFound. It does NOT broadcast: the
// caller fires OnCredentialChange once after all auth-material writes (B5:
// credential + client_config changes merge into a single CREDENTIAL_UPDATE).
func (ar *AccountRegistry) UpdateCredential(ctx context.Context, vendor types.Vendor, accountID string, cred types.Credential) error {
	credJSON, err := json.Marshal(cred)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal credential: %w", err)
	}

	const query = `UPDATE cloud_account SET credential = $1, updated_at = now() WHERE vendor = $2 AND account_id = $3`
	res, err := ar.db.ExecContext(ctx, query, credJSON, string(vendor), accountID)
	if err != nil {
		return fmt.Errorf("accountregistry: update credential %s/%s: %w", vendor, accountID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: %s/%s", ErrAccountNotFound, vendor, accountID)
	}
	return nil
}

// UpdateClientConfig replaces the static OAuth2 client material for a given
// account. If the account does not exist it returns ErrAccountNotFound. Like
// UpdateCredential it does NOT broadcast — the caller fires OnCredentialChange
// once at the end of a combined auth update.
func (ar *AccountRegistry) UpdateClientConfig(ctx context.Context, vendor types.Vendor, accountID string, cc types.ClientConfig) error {
	ccJSON, err := json.Marshal(cc)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal client_config: %w", err)
	}

	const query = `UPDATE cloud_account SET client_config = $1, updated_at = now() WHERE vendor = $2 AND account_id = $3`
	res, err := ar.db.ExecContext(ctx, query, ccJSON, string(vendor), accountID)
	if err != nil {
		return fmt.Errorf("accountregistry: update client_config %s/%s: %w", vendor, accountID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: %s/%s", ErrAccountNotFound, vendor, accountID)
	}
	return nil
}

// GetAccountSecret returns the stored credential and client config for an
// account (enabled or not). INTERNAL ONLY: consumed by the account connection
// tester (stored mode) and by OnCredentialChange; it must never be referenced
// by any HTTP handler that returns data to clients.
func (ar *AccountRegistry) GetAccountSecret(ctx context.Context, vendor types.Vendor, accountID string) (types.Credential, types.ClientConfig, error) {
	row := ar.db.QueryRowContext(ctx,
		`SELECT credential, client_config FROM cloud_account WHERE vendor = $1 AND account_id = $2`,
		string(vendor), accountID,
	)
	var credJSON, ccJSON []byte
	if err := row.Scan(&credJSON, &ccJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return types.Credential{}, types.ClientConfig{}, fmt.Errorf("%w: %s/%s", ErrAccountNotFound, vendor, accountID)
		}
		return types.Credential{}, types.ClientConfig{}, fmt.Errorf("accountregistry: get secret %s/%s: %w", vendor, accountID, err)
	}
	var cred types.Credential
	if len(credJSON) > 0 {
		if err := json.Unmarshal(credJSON, &cred); err != nil {
			return types.Credential{}, types.ClientConfig{}, fmt.Errorf("accountregistry: unmarshal credential: %w", err)
		}
	}
	var cc types.ClientConfig
	if len(ccJSON) > 0 {
		if err := json.Unmarshal(ccJSON, &cc); err != nil {
			return types.Credential{}, types.ClientConfig{}, fmt.Errorf("accountregistry: unmarshal client_config: %w", err)
		}
	}
	return cred, cc, nil
}

// SetEnabled flips the enabled flag for an account. enabled=true makes the
// account re-enter snapshots via ListByVendor's enabled filter; enabled=false
// removes it at the next ACCOUNT_SNAPSHOT cycle (<=60s). Returns
// ErrAccountNotFound when the account does not exist.
func (ar *AccountRegistry) SetEnabled(ctx context.Context, vendor types.Vendor, accountID string, enabled bool) error {
	const query = `UPDATE cloud_account SET enabled = $1, updated_at = now() WHERE vendor = $2 AND account_id = $3`
	res, err := ar.db.ExecContext(ctx, query, enabled, string(vendor), accountID)
	if err != nil {
		return fmt.Errorf("accountregistry: set enabled %s/%s: %w", vendor, accountID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: %s/%s", ErrAccountNotFound, vendor, accountID)
	}
	return nil
}

// SetRateLimit replaces the per-account rate limit config. Returns
// ErrAccountNotFound when the account does not exist.
func (ar *AccountRegistry) SetRateLimit(ctx context.Context, vendor types.Vendor, accountID string, cfg types.RateLimitConfig) error {
	rlJSON, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal rate_limit_config: %w", err)
	}
	const query = `UPDATE cloud_account SET rate_limit_config = $1, updated_at = now() WHERE vendor = $2 AND account_id = $3`
	res, err := ar.db.ExecContext(ctx, query, rlJSON, string(vendor), accountID)
	if err != nil {
		return fmt.Errorf("accountregistry: set rate limit %s/%s: %w", vendor, accountID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: %s/%s", ErrAccountNotFound, vendor, accountID)
	}
	return nil
}

// SetVendorProfile replaces the per-account vendor profile. Returns
// ErrAccountNotFound when the account does not exist.
func (ar *AccountRegistry) SetVendorProfile(ctx context.Context, vendor types.Vendor, accountID string, vp types.VendorProfile) error {
	vpJSON, err := json.Marshal(vp)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal vendor_profile: %w", err)
	}
	const query = `UPDATE cloud_account SET vendor_profile = $1, updated_at = now() WHERE vendor = $2 AND account_id = $3`
	res, err := ar.db.ExecContext(ctx, query, vpJSON, string(vendor), accountID)
	if err != nil {
		return fmt.Errorf("accountregistry: set vendor profile %s/%s: %w", vendor, accountID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: %s/%s", ErrAccountNotFound, vendor, accountID)
	}
	return nil
}

// Ban marks an account banned in account_health (upsert) and broadcasts a BAN
// event. A zero banUntil stores NULL (no auto-unban deadline). The write is
// NOT transactional with the broadcast: if broadcasting fails the error is
// logged and nil is returned — nodes converge via the next health sync
// (eventual consistency).
func (ar *AccountRegistry) Ban(ctx context.Context, vendor types.Vendor, accountID, reason string, banUntil time.Time) error {
	var banUntilArg any
	if !banUntil.IsZero() {
		banUntilArg = banUntil
	}
	_, err := ar.db.ExecContext(ctx,
		`INSERT INTO account_health (vendor, account_id, state, last_check, latency_ms, error_msg, ban_until)
		 VALUES ($1, $2, 'banned', now(), 0, $3, $4)
		 ON CONFLICT (vendor, account_id) DO UPDATE SET
		   state = 'banned', last_check = now(), error_msg = $3, ban_until = $4`,
		string(vendor), accountID, reason, banUntilArg,
	)
	if err != nil {
		return fmt.Errorf("accountregistry: ban %s/%s: %w", vendor, accountID, err)
	}
	if err := ar.broadcaster.Broadcast(types.EventBan, types.BanPayload{
		Vendor: vendor, AccountID: accountID, Reason: reason, BanUntil: banUntil,
	}); err != nil {
		log.Printf("accountregistry: broadcast ban %s/%s: %v", vendor, accountID, err)
	}
	return nil
}

// Unban marks an account healthy in account_health (upsert, ban_until=NULL)
// and broadcasts an UNBAN event. Same eventual-consistency semantics as Ban.
func (ar *AccountRegistry) Unban(ctx context.Context, vendor types.Vendor, accountID string) error {
	_, err := ar.db.ExecContext(ctx,
		`INSERT INTO account_health (vendor, account_id, state, last_check, latency_ms, error_msg, ban_until)
		 VALUES ($1, $2, 'healthy', now(), 0, '', NULL)
		 ON CONFLICT (vendor, account_id) DO UPDATE SET
		   state = 'healthy', last_check = now(), error_msg = '', ban_until = NULL`,
		string(vendor), accountID,
	)
	if err != nil {
		return fmt.Errorf("accountregistry: unban %s/%s: %w", vendor, accountID, err)
	}
	if err := ar.broadcaster.Broadcast(types.EventUnban, types.BanPayload{
		Vendor: vendor, AccountID: accountID,
	}); err != nil {
		log.Printf("accountregistry: broadcast unban %s/%s: %v", vendor, accountID, err)
	}
	return nil
}

// Revoke disables a cloud account by setting enabled = false. It does not
// delete the row so the account identity (vendor+account_id) is preserved
// for auditing and history.
func (ar *AccountRegistry) Revoke(ctx context.Context, vendor types.Vendor, accountID string) error {
	const query = `UPDATE cloud_account SET enabled = false, updated_at = now() WHERE vendor = $1 AND account_id = $2`
	_, err := ar.db.ExecContext(ctx, query, string(vendor), accountID)
	if err != nil {
		return fmt.Errorf("accountregistry: revoke %s/%s: %w", vendor, accountID, err)
	}
	return nil
}

// ListByVendor returns all enabled accounts for a given vendor, ordered
// by account_id. JSONB columns are unmarshalled into the typed structs.
// client_config may be NULL for rows predating migration 020.
func (ar *AccountRegistry) ListByVendor(ctx context.Context, vendor types.Vendor) ([]AccountInfo, error) {
	const query = `
		SELECT account_id, credential, client_config, rate_limit_config, vendor_profile, enabled
		FROM cloud_account
		WHERE vendor = $1 AND enabled = true
		ORDER BY account_id`

	rows, err := ar.db.QueryContext(ctx, query, string(vendor))
	if err != nil {
		return nil, fmt.Errorf("accountregistry: list by vendor %s: %w", vendor, err)
	}
	defer func() { _ = rows.Close() }()

	var accounts []AccountInfo
	for rows.Next() {
		var a AccountInfo
		a.Vendor = vendor

		var credJSON, ccJSON, rlJSON, vpJSON []byte
		if err := rows.Scan(&a.AccountID, &credJSON, &ccJSON, &rlJSON, &vpJSON, &a.Enabled); err != nil {
			return nil, fmt.Errorf("accountregistry: scan row: %w", err)
		}

		if err := json.Unmarshal(credJSON, &a.Credential); err != nil {
			return nil, fmt.Errorf("accountregistry: unmarshal credential: %w", err)
		}
		if len(ccJSON) > 0 {
			if err := json.Unmarshal(ccJSON, &a.ClientConfig); err != nil {
				return nil, fmt.Errorf("accountregistry: unmarshal client_config: %w", err)
			}
		}
		if err := json.Unmarshal(rlJSON, &a.RateLimitCfg); err != nil {
			return nil, fmt.Errorf("accountregistry: unmarshal rate_limit_config: %w", err)
		}
		if err := json.Unmarshal(vpJSON, &a.VendorProfile); err != nil {
			return nil, fmt.Errorf("accountregistry: unmarshal vendor_profile: %w", err)
		}

		accounts = append(accounts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("accountregistry: iterate rows: %w", err)
	}

	return accounts, nil
}

// OnCredentialChange broadcasts the current credential + client_config for an
// account as one CREDENTIAL_UPDATE event. Callers fire it ONCE at the end of
// an auth-material update (UpdateCredential/UpdateClientConfig never broadcast
// themselves). It reads the stored values via GetAccountSecret so externally
// triggered rotations also carry the live secret material; omitempty payload
// fields keep old nodes compatible. Read or broadcast failures are logged,
// never returned — the next ACCOUNT_SNAPSHOT converges the node regardless.
func (ar *AccountRegistry) OnCredentialChange(ctx context.Context, vendor types.Vendor, accountID string) {
	cred, cc, err := ar.GetAccountSecret(ctx, vendor, accountID)
	if err != nil {
		log.Printf("accountregistry: read secrets for credential broadcast %s/%s: %v", vendor, accountID, err)
		return
	}
	payload := CredentialChangePayload{
		Vendor:       vendor,
		AccountID:    accountID,
		Credential:   cred,
		ClientConfig: cc,
	}

	if err := ar.broadcaster.Broadcast(types.EventCredentialUpdate, payload); err != nil {
		log.Printf("accountregistry: broadcast credential update %s/%s: %v", vendor, accountID, err)
	}
	_ = ctx // available for future tracing/logging
}

// CredentialChangePayload is the event payload carried by CREDENTIAL_UPDATE
// broadcasts. Aliased to the types package's wire-contract definition so the
// node side and the control plane share one shape.
type CredentialChangePayload = types.CredentialChangePayload

// StartSync runs a background goroutine that fetches all enabled accounts
// across every known vendor every interval (typically 60s for production)
// and broadcasts the full list as a snapshot via Broadcaster.
//
// The snapshot is a JSON-encoded []AccountInfo. Callers should provide a
// cancellable context to stop the goroutine.
func (ar *AccountRegistry) StartSync(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Emit one snapshot immediately on start.
		ar.emitSnapshotFn(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ar.emitSnapshotFn(ctx)
			}
		}
	}()
}

// emitSnapshot fetches all enabled accounts across every vendor and broadcasts
// the resulting []AccountInfo as a full snapshot.
func (ar *AccountRegistry) emitSnapshot(ctx context.Context) {
	vendors := []types.Vendor{types.Vendor115, types.VendorBaidu, types.VendorQuark, types.VendorOneDrive, types.VendorAliyundrive}
	var all []AccountInfo

	for _, v := range vendors {
		accts, err := ar.ListByVendor(ctx, v)
		if err != nil {
			log.Printf("accountregistry: snapshot fetch %s: %v", v, err)
			continue
		}
		all = append(all, accts...)
	}

	if len(all) == 0 {
		return // nothing to broadcast
	}

	if err := ar.broadcaster.Broadcast("ACCOUNT_SNAPSHOT", all); err != nil {
		log.Printf("accountregistry: broadcast snapshot: %v", err)
	}
}

// ErrAccountNotFound is returned by UpdateCredential when the target account
// does not exist in the cloud_account table.
var ErrAccountNotFound = fmt.Errorf("accountregistry: account not found")
