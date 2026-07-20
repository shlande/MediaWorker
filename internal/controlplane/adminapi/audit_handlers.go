package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/storage/metadata"
)

// ─── Audit query API (ui-admin-apis todo 34) ────────────────────────────────
//
// GET /v1/admin/audit?kind=&from=&to=&q=&page= serves the unified audit view
// from TWO disjoint sources, selected by the kind parameter:
//
//   - kind=jwt (or empty)        -> AuditLog in-memory ring (JWT issuance)
//   - kind=admin                 -> admin_audit table, all kinds
//   - kind=account|whitelist|pin|content|auth -> admin_audit table, that kind
//
// The sources are NEVER merged, sorted, or paginated across: kind picks exactly
// one source and the response comes from it alone. This is the contractual
// meaning of the kind parameter (plan todo 34) — the UI always queries one
// source at a time. The q parameter is source-relative: on the jwt source it
// matches peer_id, on the admin source it matches target (handled inside
// ListAdminAudit).

// AuditLogQuerier is the JWT audit surface the handler needs.
// *cpjwt.AuditLog satisfies it (auditlog.go todo 34 ring + Query).
type AuditLogQuerier interface {
	Query(filter cpjwt.AuditFilter) []cpjwt.AuditEntry
}

// AdminAuditLister is the admin_audit read surface the handler needs.
// *metadata.PGMetadataClient satisfies it (metadata_admin_audit.go todo 33).
type AdminAuditLister interface {
	ListAdminAudit(ctx context.Context, q metadata.AdminAuditQuery) ([]metadata.AdminAuditRow, int, error)
}

// auditEntryResponse is the wire shape shared by both sources
// ({ts, kind, actor, action, target, ip, result} per docs/ui-api-requirements.md:140).
type auditEntryResponse struct {
	TS     string `json:"ts"`
	Kind   string `json:"kind"`
	Actor  string `json:"actor"`
	Action string `json:"action"`
	Target string `json:"target"`
	IP     string `json:"ip"`
	Result string `json:"result"`
}

type auditQueryResponse struct {
	Entries []auditEntryResponse `json:"entries"`
	Total   int                  `json:"total"`
}

// adminKinds routes kind values to the admin_audit source. "admin" means all
// kinds (empty Kind filter to ListAdminAudit); the rest are exact filters.
// kind=jwt and the empty kind are NOT in this map — they select the jwt ring.
var adminKinds = map[string]bool{
	"admin": true, "account": true, "whitelist": true,
	"pin": true, "content": true, "auth": true,
}

// parseAuditTime parses one optional RFC3339 query parameter. Empty value is
// (nil, nil); a malformed value is (nil, error) -> 400.
func parseAuditTime(v string) (*time.Time, error) {
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil, fmt.Errorf("invalid time %q: must be RFC3339", v)
	}
	return &t, nil
}

func mapJWTEntry(e cpjwt.AuditEntry) auditEntryResponse {
	peer := string(e.PeerID)
	return auditEntryResponse{
		TS:     e.Timestamp.Format(time.RFC3339),
		Kind:   "jwt",
		Actor:  peer,
		Action: "jwt_issue",
		Target: peer,
		IP:     e.RemoteIP,
		Result: e.Result,
	}
}

func mapAdminRow(r metadata.AdminAuditRow) auditEntryResponse {
	out := auditEntryResponse{
		TS:     r.TS.Format(time.RFC3339),
		Kind:   r.Kind,
		Actor:  r.Actor,
		Action: r.Action,
		Result: r.Result,
	}
	if r.Target != nil {
		out.Target = *r.Target
	}
	if r.IP != nil {
		out.IP = *r.IP
	}
	return out
}

// auditQueryHandler returns an http.Handler for GET /v1/admin/audit.
// auditLog may be nil only when no jwt queries are expected; a nil auditLog
// with kind=jwt is a 500 (wiring bug), never a silent empty result.
//
//	@Summary		审计日志查询
//	@Description	从 JWT 环缓冲或 admin_audit 表查询审计记录
//	@Tags			admin-audit
//	@Produce		json
//	@Param			kind		query	string	false	"来源（jwt|admin|account|whitelist|pin|content|auth）"
//	@Param			from		query	string	false	"起始时间（RFC3339）"
//	@Param			to			query	string	false	"结束时间（RFC3339）"
//	@Param			q			query	string	false	"搜索关键词（jwt 源按 peer_id；admin 源按 target）"
//	@Param			page		query	int		false	"页码"
//	@Param			page_size	query	int		false	"每页条数"
//	@Success		200			{object}	auditQueryResponse
//	@Failure		400			{object}	types.ErrorResponse	"无效 kind 或时间格式"
//	@Failure		401			{object}	types.ErrorResponse
//	@Failure		403			{object}	types.ErrorResponse
//	@Failure		500			{object}	types.ErrorResponse
//	@Security		AdminBearer
//	@Router			/v1/admin/audit [get]
func auditQueryHandler(auditLog AuditLogQuerier, mc AdminAuditLister) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		// LOCKED DEFAULT: empty kind selects the jwt source (NOT a merged
		// all-source view). Cross-source merge/sort/pagination is contractually
		// excluded (plan todo 34); the UI passes an explicit kind for the
		// admin_audit table. Tests pin this behavior.
		kind := q.Get("kind")
		if kind == "" {
			kind = "jwt"
		}
		if kind != "jwt" && !adminKinds[kind] {
			WriteError(w, http.StatusBadRequest, fmt.Sprintf("unknown kind %q", kind))
			return
		}

		from, err := parseAuditTime(q.Get("from"))
		if err != nil {
			WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		to, err := parseAuditTime(q.Get("to"))
		if err != nil {
			WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		page, pageSize := ParsePage(r)

		if kind == "jwt" {
			if auditLog == nil {
				WriteError(w, http.StatusInternalServerError, "jwt audit log not wired")
				return
			}
			f := cpjwt.AuditFilter{Q: q.Get("q")}
			if from != nil {
				f.From = *from
			}
			if to != nil {
				f.To = *to
			}
			matched := auditLog.Query(f)
			start := (page - 1) * pageSize
			if start > len(matched) {
				start = len(matched)
			}
			end := start + pageSize
			if end > len(matched) {
				end = len(matched)
			}
			entries := make([]auditEntryResponse, 0, end-start)
			for _, e := range matched[start:end] {
				entries = append(entries, mapJWTEntry(e))
			}
			WriteJSON(w, http.StatusOK, auditQueryResponse{Entries: entries, Total: len(matched)})
			return
		}

		if mc == nil {
			WriteError(w, http.StatusInternalServerError, "admin audit store not wired")
			return
		}
		aq := metadata.AdminAuditQuery{
			Q:        q.Get("q"),
			From:     from,
			To:       to,
			Page:     page,
			PageSize: pageSize,
		}
		if kind != "admin" {
			aq.Kind = kind
		}
		rows, total, err := mc.ListAdminAudit(r.Context(), aq)
		if err != nil {
			WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list admin audit: %v", err))
			return
		}
		entries := make([]auditEntryResponse, 0, len(rows))
		for _, row := range rows {
			entries = append(entries, mapAdminRow(row))
		}
		WriteJSON(w, http.StatusOK, auditQueryResponse{Entries: entries, Total: total})
	})
}

// ─── Audit export API (ui-admin-apis todo 35) ───────────────────────────────
//
// GET /v1/admin/audit/export?同筛选 streams audit entries as JSON-lines
// (application/x-ndjson) with Content-Disposition attachment. Same query
// logic as todo 34 but with page_size cap relaxed to 10000 and forced ts
// ASC order. Lines are encoded and flushed individually — no buffering.
//
// Must NOT: no CSV support; no non-JSON lines in the export body.

const exportMaxPageSize = 10000

// exportFilename returns a download filename with the current UTC timestamp.
func exportFilename() string {
	return "audit-" + time.Now().UTC().Format("20060102T150405") + ".jsonl"
}

// auditExportHandler builds the streaming export handler. Dependencies match
// auditQueryHandler exactly.
//
//	@Summary		审计日志导出
//	@Description	以 NDJSON 格式流式导出审计记录（application/x-ndjson，Content-Disposition attachment）
//	@Tags			admin-audit
//	@Produce		json
//	@Param			kind	query	string	false	"来源（jwt|admin|account|whitelist|pin|content|auth）"
//	@Param			from	query	string	false	"起始时间（RFC3339）"
//	@Param			to		query	string	false	"结束时间（RFC3339）"
//	@Param			q		query	string	false	"搜索关键词"
//	@Success		200		{file}	application/x-ndjson
//	@Failure		400		{object}	types.ErrorResponse	"无效 kind 或时间格式"
//	@Failure		401		{object}	types.ErrorResponse
//	@Failure		403		{object}	types.ErrorResponse
//	@Failure		500		{object}	types.ErrorResponse
//	@Security		AdminBearer
//	@Router			/v1/admin/audit/export [get]
func auditExportHandler(auditLog AuditLogQuerier, mc AdminAuditLister) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		kind := q.Get("kind")
		if kind == "" {
			kind = "jwt"
		}
		if kind != "jwt" && !adminKinds[kind] {
			WriteError(w, http.StatusBadRequest, fmt.Sprintf("unknown kind %q", kind))
			return
		}

		from, err := parseAuditTime(q.Get("from"))
		if err != nil {
			WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		to, err := parseAuditTime(q.Get("to"))
		if err != nil {
			WriteError(w, http.StatusBadRequest, err.Error())
			return
		}

		// ── Headers ──────────────────────────────────────────────────
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Content-Disposition",
			`attachment; filename="`+exportFilename()+`"`)
		flusher, canFlush := w.(http.Flusher)
		if !canFlush {
			WriteError(w, http.StatusInternalServerError, "streaming not supported")
			return
		}

		// ── JWT source ───────────────────────────────────────────────
		if kind == "jwt" {
			if auditLog == nil {
				WriteError(w, http.StatusInternalServerError, "jwt audit log not wired")
				return
			}
			f := cpjwt.AuditFilter{Q: q.Get("q"), Limit: exportMaxPageSize}
			if from != nil {
				f.From = *from
			}
			if to != nil {
				f.To = *to
			}
			matched := auditLog.Query(f)

			// AuditLog.Query returns newest-first (ts DESC). Export
			// contract is ts ASC — iterate backwards.
			for i := len(matched) - 1; i >= 0; i-- {
				line, err := json.Marshal(mapJWTEntry(matched[i]))
				if err != nil {
					// json.Marshal on a struct at this point can
					// only fail on unmarshallable types; fail
					// hard — client already has headers.
					return
				}
				_, _ = w.Write(line)
				_, _ = w.Write([]byte("\n"))
				flusher.Flush()
			}
			return
		}

		// ── admin source ─────────────────────────────────────────────
		if mc == nil {
			WriteError(w, http.StatusInternalServerError, "admin audit store not wired")
			return
		}
		aq := metadata.AdminAuditQuery{
			Q:    q.Get("q"),
			From: from,
			To:   to,
			// Export does not paginate through the admin table with
			// an offset — ListAdminAudit at this page_size covers
			// the full export up to the cap.
			Page:     1,
			PageSize: exportMaxPageSize,
		}
		if kind != "admin" {
			aq.Kind = kind
		}

		// Export contract: ts ASC. ListAdminAudit sorts ts DESC.
		// Fetch the DESC result and reverse in memory — the data
		// is at most exportMaxPageSize rows so the reversal is
		// bounded.
		rows, _, err := mc.ListAdminAudit(r.Context(), aq)
		if err != nil {
			WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list admin audit: %v", err))
			return
		}
		for i := len(rows) - 1; i >= 0; i-- {
			line, err := json.Marshal(mapAdminRow(rows[i]))
			if err != nil {
				return
			}
			_, _ = w.Write(line)
			_, _ = w.Write([]byte("\n"))
			flusher.Flush()
		}
	})
}

// ─── Route registration (for todo 54) ──────────────────────────────────────

// RegisterAuditRoutes mounts the audit query and export handlers on srv.
// D1-compliant: main.go is NOT edited here. Todo 54 must pass the real
// AuditLog instance created in cmd/control-plane/main.go and the PG metadata
// client.
func RegisterAuditRoutes(srv *Server, auditLog AuditLogQuerier, mc AdminAuditLister) {
	srv.Handle("GET /v1/admin/audit", auditQueryHandler(auditLog, mc), true)
	srv.Handle("GET /v1/admin/audit/export", auditExportHandler(auditLog, mc), true)
}
