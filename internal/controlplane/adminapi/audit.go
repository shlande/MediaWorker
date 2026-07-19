package adminapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/storage/metadata"
)

// ─── PG-backed AuditRecorder (ui-admin-apis todo 33) ────────────────────────
//
// PGAuditRecorder persists every admin write-operation audit entry into the
// admin_audit table (migration 018). Recording is fire-and-forget BY
// CONTRACT: an insert failure is Warn-logged and swallowed — auditing must
// never change a business response, so Record has no error return and never
// panics. Nil safety runs both ways: a nil *PGAuditRecorder and a nil
// inserter are both silent no-ops (todo 54 assembles the wiring; handlers
// must tolerate an unconfigured recorder until then).

// AdminAuditInserter is the persistence surface PGAuditRecorder needs.
// *metadata.PGMetadataClient satisfies it (metadata_admin_audit.go).
type AdminAuditInserter interface {
	InsertAdminAudit(ctx context.Context, entry metadata.AdminAuditRow) error
}

// PGAuditRecorder implements AuditRecorder against admin_audit.
// It is stateless apart from the inserter and safe for concurrent use.
type PGAuditRecorder struct {
	mc AdminAuditInserter
}

// NewPGAuditRecorder builds the recorder; mc may be nil (no-op recorder).
func NewPGAuditRecorder(mc AdminAuditInserter) *PGAuditRecorder {
	return &PGAuditRecorder{mc: mc}
}

// Record maps the entry onto an admin_audit row and inserts it. Any failure
// (detail marshal, DB insert) degrades to a Warn log; the request path is
// never blocked or failed by auditing.
func (p *PGAuditRecorder) Record(ctx context.Context, entry AuditEntry) {
	if p == nil || p.mc == nil {
		return
	}
	row := metadata.AdminAuditRow{
		TS:     entry.TS,
		Kind:   entry.Kind,
		Actor:  entry.Actor,
		Action: entry.Action,
		Result: entry.Result,
	}
	if entry.Target != "" {
		row.Target = &entry.Target
	}
	if entry.IP != "" {
		row.IP = &entry.IP
	}
	if len(entry.Detail) > 0 {
		detail, err := json.Marshal(entry.Detail)
		if err != nil {
			slog.Warn("admin audit: detail marshal failed; recording without detail",
				"kind", entry.Kind, "action", entry.Action, "error", err)
		} else {
			row.Detail = detail
		}
	}
	if err := p.mc.InsertAdminAudit(ctx, row); err != nil {
		slog.Warn("admin audit: insert failed (business response unaffected)",
			"kind", entry.Kind, "action", entry.Action, "actor", entry.Actor, "error", err)
	}
}

// ─── Write-handler instrumentation helper ───────────────────────────────────

// recordWriteAudit emits one audit entry for an admin WRITE operation at a
// terminal decision point (success or failure). The actor is the bearer-token
// username from the middleware context ("" when absent — auth=true routes
// always carry it). A nil recorder is a no-op. Read operations are never
// audited through here, and an unparseable request body is not an auditable
// operation either (nothing was attempted).
func recordWriteAudit(r *http.Request, audit AuditRecorder, kind, action, target, result string, detail map[string]any) {
	if audit == nil {
		return
	}
	_, actor, _, _ := UserFromCtx(r.Context())
	audit.Record(r.Context(), AuditEntry{
		TS:     time.Now(),
		Kind:   kind,
		Actor:  actor,
		Action: action,
		Target: target,
		IP:     r.RemoteAddr,
		Result: result,
		Detail: detail,
	})
}
