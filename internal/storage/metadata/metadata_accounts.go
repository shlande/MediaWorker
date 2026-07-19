package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Admin accounts read-model (ui-admin-apis todo 13) ────────────────────────
//
// Read-only query surface for the admin accounts page. credential and
// client_config JSONB are loaded ONLY to compute CredentialMeta server-side;
// the exported view types carry no secret material and the working copies are
// zeroed immediately after the meta is computed (defense in depth).

// HealthView is the admin-facing health snapshot joined from account_health.
type HealthView struct {
	State     string     `json:"state"`
	LatencyMs int        `json:"latency_ms"`
	ErrorMsg  string     `json:"error_msg"`
	BanUntil  *time.Time `json:"ban_until,omitempty"`
	LastCheck time.Time  `json:"last_check"`
}

// CredentialMeta describes the shape of an account's credentials WITHOUT any
// secret values. AuthType is "cookie" when the credential carries a non-empty
// cookie map, otherwise "oauth2".
type CredentialMeta struct {
	AuthType        string   `json:"auth_type"` // "oauth2" | "cookie"
	HasClientSecret bool     `json:"has_client_secret"`
	HasRefreshToken bool     `json:"has_refresh_token"`
	Region          string   `json:"region,omitempty"`
	CookieKeys      []string `json:"cookie_keys,omitempty"` // sorted key names, cookie auth only
}

// AdminAccountView is one row of the admin accounts list.
type AdminAccountView struct {
	Vendor         string                `json:"vendor"`
	AccountID      string                `json:"account_id"`
	Enabled        bool                  `json:"enabled"`
	RateLimitCfg   types.RateLimitConfig `json:"rate_limit_config"`
	VendorProfile  types.VendorProfile   `json:"vendor_profile"`
	Health         *HealthView           `json:"health,omitempty"` // nil when no account_health row (UI empty state)
	CredentialMeta CredentialMeta        `json:"credential_meta"`
}

// VendorProfileRow is the latest vendor_profile per vendor (DISTINCT ON).
type VendorProfileRow struct {
	Vendor        string              `json:"vendor"`
	VendorProfile types.VendorProfile `json:"vendor_profile"`
}

// clientConfigJSON mirrors the wire shape of types.ClientConfig (todo 1 adds
// it to types.go; migration 020 adds the cloud_account.client_config column).
// Declared locally so this file compiles before that type lands; used only for
// server-side credential_meta computation and never exported.
type clientConfigJSON struct {
	ClientID     string `json:"client_id,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
	RedirectURI  string `json:"redirect_uri,omitempty"`
	Region       string `json:"region,omitempty"`
}

// computeCredentialMeta derives the non-secret credential summary. It reads
// secret material but returns only booleans, key names, and region.
func computeCredentialMeta(cred *types.Credential, cc *clientConfigJSON) CredentialMeta {
	meta := CredentialMeta{
		AuthType:        "oauth2",
		HasClientSecret: cc.ClientSecret != "",
		HasRefreshToken: cred.RefreshToken != "",
		Region:          cc.Region,
	}
	if len(cred.Cookies) > 0 {
		meta.AuthType = "cookie"
		keys := make([]string, 0, len(cred.Cookies))
		for k := range cred.Cookies {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		meta.CookieKeys = keys
	}
	return meta
}

// zeroCredential drops all secret material held by a working credential copy.
func zeroCredential(cred *types.Credential) {
	for k := range cred.Cookies {
		delete(cred.Cookies, k)
	}
	cred.Cookies = nil
	cred.AccessToken = ""
	cred.RefreshToken = ""
	cred.TokenExpire = time.Time{}
}

// zeroClientConfig drops all secret material held by a working client_config copy.
func zeroClientConfig(cc *clientConfigJSON) {
	*cc = clientConfigJSON{}
}

// ListAccounts returns the admin accounts list: cloud_account LEFT JOIN
// account_health, optionally filtered by vendor and/or health state (empty
// string = no filter). credential/client_config are consumed only for
// credential_meta computation; secrets never leave this function.
//
// NOTE: a.client_config is provided by migration 020 (todo 6); integration
// against a live database requires that migration. Unit tests define the
// column shape via sqlmock.
func (c *PGMetadataClient) ListAccounts(ctx context.Context, vendorFilter, stateFilter string) ([]AdminAccountView, error) {
	query := `SELECT a.vendor, a.account_id, a.enabled, a.rate_limit_config, a.vendor_profile, a.credential, COALESCE(a.client_config, '{}'::jsonb), h.state, h.latency_ms, h.error_msg, h.ban_until, h.last_check FROM cloud_account a LEFT JOIN account_health h ON h.vendor = a.vendor AND h.account_id = a.account_id`

	var args []any
	var where []string
	if vendorFilter != "" {
		args = append(args, vendorFilter)
		where = append(where, fmt.Sprintf("a.vendor = $%d", len(args)))
	}
	if stateFilter != "" {
		args = append(args, stateFilter)
		where = append(where, fmt.Sprintf("h.state = $%d", len(args)))
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY a.vendor, a.account_id"

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("metadata: list accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []AdminAccountView
	for rows.Next() {
		var v AdminAccountView
		var rlJSON, vpJSON, credJSON, ccJSON []byte
		var state, errMsg sql.NullString
		var latencyMs sql.NullInt64
		var banUntil, lastCheck sql.NullTime

		if err := rows.Scan(&v.Vendor, &v.AccountID, &v.Enabled, &rlJSON, &vpJSON, &credJSON, &ccJSON, &state, &latencyMs, &errMsg, &banUntil, &lastCheck); err != nil {
			return nil, fmt.Errorf("metadata: scan admin account row: %w", err)
		}
		if len(rlJSON) > 0 {
			if err := json.Unmarshal(rlJSON, &v.RateLimitCfg); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal rate_limit_config: %w", err)
			}
		}
		if len(vpJSON) > 0 {
			if err := json.Unmarshal(vpJSON, &v.VendorProfile); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal vendor_profile: %w", err)
			}
		}

		var cred types.Credential
		if len(credJSON) > 0 {
			if err := json.Unmarshal(credJSON, &cred); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal credential: %w", err)
			}
		}
		var cc clientConfigJSON
		if len(ccJSON) > 0 {
			if err := json.Unmarshal(ccJSON, &cc); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal client_config: %w", err)
			}
		}
		v.CredentialMeta = computeCredentialMeta(&cred, &cc)
		// Defense in depth: the view carries no secret fields; drop the
		// working copies so no secret reference survives past this point.
		zeroCredential(&cred)
		zeroClientConfig(&cc)

		if state.Valid {
			v.Health = &HealthView{
				State:     state.String,
				LatencyMs: int(latencyMs.Int64),
				ErrorMsg:  errMsg.String,
				LastCheck: lastCheck.Time,
			}
			if banUntil.Valid {
				t := banUntil.Time
				v.Health.BanUntil = &t
			}
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate admin accounts: %w", err)
	}
	return out, nil
}

// ListVendorProfiles returns the most recently updated vendor_profile per
// vendor across cloud_account.
func (c *PGMetadataClient) ListVendorProfiles(ctx context.Context) ([]VendorProfileRow, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT DISTINCT ON (vendor) vendor, vendor_profile FROM cloud_account ORDER BY vendor, updated_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: list vendor profiles: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []VendorProfileRow
	for rows.Next() {
		var r VendorProfileRow
		var vpJSON []byte
		if err := rows.Scan(&r.Vendor, &vpJSON); err != nil {
			return nil, fmt.Errorf("metadata: scan vendor profile row: %w", err)
		}
		if len(vpJSON) > 0 {
			if err := json.Unmarshal(vpJSON, &r.VendorProfile); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal vendor_profile: %w", err)
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate vendor profiles: %w", err)
	}
	return out, nil
}
