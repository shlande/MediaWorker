package metadata

import (
	"context"
	"database/sql"
	"fmt"
)

// ─── Dashboard overview aggregates (ui-admin-apis todo 52) ──────────────────
//
// Single-row aggregate queries backing GET /v1/admin/overview. The account
// health rate is the PG-sourced fourth SLO card (the other three come from
// Prometheus); an empty account_health table is a legitimate state (no
// account ever probed), so the absence of rows surfaces as ok=false, never
// as a division-by-zero error.

// AccountHealthRate returns the share of accounts whose latest state is
// 'healthy': COUNT(healthy) / COUNT(*) over account_health. ok is false when
// the table has no rows (NULLIF guards the division, yielding SQL NULL, which
// is scanned as invalid). Errors are returned for real query failures.
func (c *PGMetadataClient) AccountHealthRate(ctx context.Context) (rate float64, ok bool, err error) {
	var v sql.NullFloat64
	if err := c.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FILTER (WHERE state='healthy')::float/NULLIF(COUNT(*),0) FROM account_health`,
	).Scan(&v); err != nil {
		return 0, false, fmt.Errorf("metadata: account health rate: %w", err)
	}
	return v.Float64, v.Valid, nil
}
