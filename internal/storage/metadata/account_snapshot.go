package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/shlande/mediaworker/internal/types"
)

// LoadAccountSnapshot reads the account snapshot from cloud_account
// (enabled rows only, credential material included — INTERNAL consumers
// only, never an HTTP surface); entry.Banned carries the scheduling taint
// from account_health.
func (c *PGMetadataClient) LoadAccountSnapshot(ctx context.Context) ([]types.AccountSnapshotEntry, error) {
	const query = `
		SELECT a.vendor, a.account_id, a.credential, COALESCE(a.client_config, '{}'::jsonb),
		       a.rate_limit_config, a.vendor_profile, COALESCE(h.state = 'banned', false)
		FROM cloud_account a
		LEFT JOIN account_health h ON h.vendor = a.vendor AND h.account_id = a.account_id
		WHERE a.enabled = true
		ORDER BY a.vendor, a.account_id`

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("metadata: load account snapshot: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []types.AccountSnapshotEntry
	for rows.Next() {
		var e types.AccountSnapshotEntry
		var credJSON, ccJSON, rlJSON, vpJSON []byte
		if err := rows.Scan(&e.Vendor, &e.AccountID, &credJSON, &ccJSON, &rlJSON, &vpJSON, &e.Banned); err != nil {
			return nil, fmt.Errorf("metadata: scan account snapshot row: %w", err)
		}
		e.Enabled = true
		if len(credJSON) > 0 {
			if err := json.Unmarshal(credJSON, &e.Credential); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal credential %s/%s: %w", e.Vendor, e.AccountID, err)
			}
		}
		if len(ccJSON) > 0 {
			if err := json.Unmarshal(ccJSON, &e.ClientConfig); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal client_config %s/%s: %w", e.Vendor, e.AccountID, err)
			}
		}
		if len(rlJSON) > 0 {
			if err := json.Unmarshal(rlJSON, &e.RateLimitCfg); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal rate_limit_config %s/%s: %w", e.Vendor, e.AccountID, err)
			}
		}
		if len(vpJSON) > 0 {
			if err := json.Unmarshal(vpJSON, &e.VendorProfile); err != nil {
				return nil, fmt.Errorf("metadata: unmarshal vendor_profile %s/%s: %w", e.Vendor, e.AccountID, err)
			}
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate account snapshot: %w", err)
	}
	return out, nil
}
