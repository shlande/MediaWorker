package metadata

import (
	"context"
	"fmt"
	"time"
)

// ─── alert_events accessors (ui-admin-apis todo 51) ───────────────────────────
//
// Alertmanager webhook persistence (migration 019). The control plane receives
// Alertmanager v4 webhook payloads and stores one row per alert instance;
// repeat_interval resends of the same logical alert (same fingerprint+since)
// are deduped by the (fingerprint, since) unique index via upsert. The admin
// alerts panel reads the current firing view via ListAlertEvents("firing", 100).
//
// `since` is not a PostgreSQL keyword (absent from Appendix C), so it is used
// unquoted throughout; verified through MigrateAll in migrate tests.

// AlertEventRow is one row of the alert_events table.
// Pointer fields map to nullable columns: nil means "absent".
type AlertEventRow struct {
	ID          int64      // BIGSERIAL, zero on insert
	Fingerprint string     // Alertmanager fingerprint (or alertname+startsAt hash fallback)
	Name        string     // labels.alertname
	Severity    *string    // labels.severity
	Target      *string    // labels.instance preferred, labels.peer_id fallback
	Detail      []byte     // JSONB document (raw JSON); nil/empty -> NULL
	Status      string     // "firing" | "resolved" (Alertmanager status)
	Since       *time.Time // alert startsAt
	ReceivedAt  time.Time  // CP-side insert/refresh timestamp, zero on insert (DB default now())
}

// InsertAlertEvent inserts one alert event, or refreshes status/received_at
// when the same (fingerprint, since) already exists. The upsert is the dedup
// mechanism for Alertmanager repeat_interval resends: re-delivering the same
// logical alert never creates a second row.
func (c *PGMetadataClient) InsertAlertEvent(ctx context.Context, row AlertEventRow) error {
	// detail is passed as string: lib/pq encodes []byte as bytea, which has no
	// assignment cast to jsonb; text -> jsonb does.
	var detail any
	if len(row.Detail) > 0 {
		detail = string(row.Detail)
	}
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO alert_events
		   (fingerprint, name, severity, target, detail, status, since)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (fingerprint, since)
		 DO UPDATE SET status = EXCLUDED.status, received_at = now()`,
		row.Fingerprint, row.Name, row.Severity, row.Target, detail, row.Status, row.Since,
	)
	if err != nil {
		return fmt.Errorf("metadata: insert alert event %q/%q: %w", row.Fingerprint, row.Name, err)
	}
	return nil
}

// alertEventsColumns is the column list shared by both ListAlertEvents forms.
const alertEventsColumns = `id, fingerprint, name, severity, target, detail, status, since, received_at`

// ListAlertEvents returns up to limit alert events, newest first
// (received_at DESC). status="" lists all statuses; otherwise only rows with
// the exact status (e.g. "firing" for the current-alerts view) are returned.
// An empty result is a nil-error empty slice.
func (c *PGMetadataClient) ListAlertEvents(ctx context.Context, status string, limit int) ([]AlertEventRow, error) {
	query := `SELECT ` + alertEventsColumns + `
			 FROM alert_events ORDER BY received_at DESC LIMIT $1`
	args := []any{limit}
	if status != "" {
		query = `SELECT ` + alertEventsColumns + `
			 FROM alert_events WHERE status = $1 ORDER BY received_at DESC LIMIT $2`
		args = []any{status, limit}
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("metadata: list alert events (status=%q): %w", status, err)
	}
	defer func() { _ = rows.Close() }()

	var out []AlertEventRow
	for rows.Next() {
		var r AlertEventRow
		if err := rows.Scan(
			&r.ID, &r.Fingerprint, &r.Name, &r.Severity, &r.Target,
			&r.Detail, &r.Status, &r.Since, &r.ReceivedAt,
		); err != nil {
			return nil, fmt.Errorf("metadata: scan alert event row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate alert events: %w", err)
	}
	return out, nil
}
