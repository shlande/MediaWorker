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
	Vendor         types.Vendor         `json:"vendor"`
	AccountID      string               `json:"account_id"`
	Credential     types.Credential     `json:"credential"`
	RateLimitCfg   types.RateLimitConfig `json:"rate_limit_config"`
	VendorProfile  types.VendorProfile  `json:"vendor_profile"`
	Enabled        bool                 `json:"enabled"`
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
	rlJSON, err := json.Marshal(info.RateLimitCfg)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal rate_limit_config: %w", err)
	}
	vpJSON, err := json.Marshal(info.VendorProfile)
	if err != nil {
		return fmt.Errorf("accountregistry: marshal vendor_profile: %w", err)
	}

	const query = `
		INSERT INTO cloud_account (vendor, account_id, credential, rate_limit_config, vendor_profile, enabled)
		VALUES ($1, $2, $3, $4, $5, $6)`

	_, err = ar.db.ExecContext(ctx, query,
		string(info.Vendor), info.AccountID, credJSON, rlJSON, vpJSON, info.Enabled)
	if err != nil {
		return fmt.Errorf("accountregistry: insert account %s/%s: %w", info.Vendor, info.AccountID, err)
	}
	return nil
}

// UpdateCredential replaces the credential for a given account. If the account
// does not exist it returns ErrAccountNotFound. On success it triggers
// OnCredentialChange to broadcast the update.
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

	ar.OnCredentialChange(ctx, vendor, accountID)
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
func (ar *AccountRegistry) ListByVendor(ctx context.Context, vendor types.Vendor) ([]AccountInfo, error) {
	const query = `
		SELECT account_id, credential, rate_limit_config, vendor_profile, enabled
		FROM cloud_account
		WHERE vendor = $1 AND enabled = true
		ORDER BY account_id`

	rows, err := ar.db.QueryContext(ctx, query, string(vendor))
	if err != nil {
		return nil, fmt.Errorf("accountregistry: list by vendor %s: %w", vendor, err)
	}
	defer rows.Close()

	var accounts []AccountInfo
	for rows.Next() {
		var a AccountInfo
		a.Vendor = vendor

		var credJSON, rlJSON, vpJSON []byte
		if err := rows.Scan(&a.AccountID, &credJSON, &rlJSON, &vpJSON, &a.Enabled); err != nil {
			return nil, fmt.Errorf("accountregistry: scan row: %w", err)
		}

		if err := json.Unmarshal(credJSON, &a.Credential); err != nil {
			return nil, fmt.Errorf("accountregistry: unmarshal credential: %w", err)
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

// OnCredentialChange is called whenever an account credential is updated. It
// marshals a credential-change event and broadcasts it via the Broadcaster so
// that every connected node receives the new credential immediately.
func (ar *AccountRegistry) OnCredentialChange(ctx context.Context, vendor types.Vendor, accountID string) {
	payload := CredentialChangePayload{
		Vendor:    vendor,
		AccountID: accountID,
	}

	if err := ar.broadcaster.Broadcast(types.EventCredentialUpdate, payload); err != nil {
		log.Printf("accountregistry: broadcast credential update %s/%s: %v", vendor, accountID, err)
	}
	_ = ctx // available for future tracing/logging
}

// CredentialChangePayload is the event payload carried by CREDENTIAL_UPDATE
// broadcasts.
type CredentialChangePayload struct {
	Vendor    types.Vendor `json:"vendor"`
	AccountID string       `json:"account_id"`
}

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