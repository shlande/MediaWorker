package metadata

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ─── admin_audit accessors (ui-admin-apis todo 33) ──────────────────────────
//
// Admin-plane write-operation audit trail (migration 018). Every B-section
// mutating endpoint (accounts / whitelist / pin / content / auth) records one
// row per attempted operation; the adminapi PGAuditRecorder is the only
// writer, todo 34's GET /v1/admin/audit is the reader. Insert is fire-and-
// forget from the recorder's perspective (failure is Warn-logged there, never
// propagated into business responses).
//
// detail is JSONB and MUST never carry credential material: ban reasons and
// rate_limit values are allowed; credential/client_config secrets are
// contractually excluded by the adminapi instrumentation (test-locked).

// AdminAuditRow is one row of the admin_audit table.
// Pointer fields map to nullable columns: nil means "absent" (SQL NULL).
type AdminAuditRow struct {
	ID     int64     // BIGSERIAL, zero on insert
	TS     time.Time // zero on insert -> DB default now()
	Kind   string    // "account" | "whitelist" | "pin" | "content" | "auth"
	Actor  string    // admin username
	Action string    // "create|update|rotate|ban|unban|circuit|pin|unpin|delete|add|remove|login|logout"
	Target *string   // resource identifier (vendor:account_id / peer_id / content_id / username)
	IP     *string   // request RemoteAddr
	Result string    // "ok" | "fail"
	Detail []byte    // raw JSON -> JSONB; nil/empty -> NULL. NEVER credential material.
}

// AdminAuditQuery parameterizes ListAdminAudit. Page is 1-based.
type AdminAuditQuery struct {
	Kind     string     // exact kind filter; empty = all kinds
	From     *time.Time // ts >= From (inclusive); nil = unbounded
	To       *time.Time // ts <= To (inclusive); nil = unbounded
	Q        string     // substring match on target via ILIKE %q%; empty = no match filter
	Page     int        // 1-based page number; <1 -> 1
	PageSize int        // rows per page; <1 -> defaultAdminAuditPageSize
}

// defaultAdminAuditPageSize is the fallback page size when the caller passes
// PageSize < 1 (mirrors ListContents pagination defaults).
const defaultAdminAuditPageSize = 20

// InsertAdminAudit inserts one audit row. A zero TS lets the DB apply its
// now() default (COALESCE keeps the statement shape fixed for both cases).
// detail is passed as string: lib/pq encodes []byte as bytea, which has no
// assignment cast to jsonb; text -> jsonb does.
func (c *PGMetadataClient) InsertAdminAudit(ctx context.Context, entry AdminAuditRow) error {
	var ts, target, ip, detail any
	if !entry.TS.IsZero() {
		ts = entry.TS
	}
	if entry.Target != nil {
		target = *entry.Target
	}
	if entry.IP != nil {
		ip = *entry.IP
	}
	if len(entry.Detail) > 0 {
		detail = string(entry.Detail)
	}
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO admin_audit (ts, kind, actor, action, target, ip, result, detail)
		 VALUES (COALESCE($1, now()), $2, $3, $4, $5, $6, $7, $8)`,
		ts, entry.Kind, entry.Actor, entry.Action, target, ip, entry.Result, detail,
	)
	if err != nil {
		return fmt.Errorf("metadata: insert admin audit (kind=%q action=%q actor=%q): %w",
			entry.Kind, entry.Action, entry.Actor, err)
	}
	return nil
}

// adminAuditColumns is the column list shared by ListAdminAudit queries.
const adminAuditColumns = `id, ts, kind, actor, action, target, ip, result, detail`

// ListAdminAudit returns one page of audit rows (ts DESC, id DESC for a
// stable order within one timestamp) plus the total number of rows matching
// the filters across all pages. Filter values and pagination numbers travel
// as bind parameters only.
func (c *PGMetadataClient) ListAdminAudit(ctx context.Context, q AdminAuditQuery) ([]AdminAuditRow, int, error) {
	page := q.Page
	if page < 1 {
		page = 1
	}
	pageSize := q.PageSize
	if pageSize < 1 {
		pageSize = defaultAdminAuditPageSize
	}

	var conds []string
	var args []any
	if q.Kind != "" {
		args = append(args, q.Kind)
		conds = append(conds, fmt.Sprintf(`kind = $%d`, len(args)))
	}
	if q.From != nil {
		args = append(args, *q.From)
		conds = append(conds, fmt.Sprintf(`ts >= $%d`, len(args)))
	}
	if q.To != nil {
		args = append(args, *q.To)
		conds = append(conds, fmt.Sprintf(`ts <= $%d`, len(args)))
	}
	if q.Q != "" {
		args = append(args, "%"+q.Q+"%")
		conds = append(conds, fmt.Sprintf(`target ILIKE $%d`, len(args)))
	}

	where := `TRUE`
	if len(conds) > 0 {
		where = strings.Join(conds, ` AND `)
	}

	var total int64
	if err := c.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM admin_audit WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("metadata: count admin audit: %w", err)
	}

	selectArgs := append(append([]any(nil), args...), pageSize, (page-1)*pageSize)
	query := `SELECT ` + adminAuditColumns + ` FROM admin_audit WHERE ` + where +
		` ORDER BY ts DESC, id DESC` +
		fmt.Sprintf(` LIMIT $%d OFFSET $%d`, len(selectArgs)-1, len(selectArgs))

	rows, err := c.db.QueryContext(ctx, query, selectArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("metadata: list admin audit: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []AdminAuditRow
	for rows.Next() {
		var r AdminAuditRow
		if err := rows.Scan(
			&r.ID, &r.TS, &r.Kind, &r.Actor, &r.Action,
			&r.Target, &r.IP, &r.Result, &r.Detail,
		); err != nil {
			return nil, 0, fmt.Errorf("metadata: scan admin audit row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("metadata: iterate admin audit: %w", err)
	}
	return out, int(total), nil
}
