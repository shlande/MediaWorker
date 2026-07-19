package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/accountregistry"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Narrow metadata dependency (interface → testable) ────────────────────
//
// AdminAccountsReader is the read-model surface the accounts handler needs
// from the metadata layer. The production implementation is
// *metadata.PGMetadataClient (todo 13). Todo 54 wires this interface to the
// Server via RegisterAccountsRoutes.

// AdminAccountsReader defines the admin accounts read surface.
type AdminAccountsReader interface {
	ListAccounts(ctx context.Context, vendorFilter, stateFilter string) ([]metadata.AdminAccountView, error)
}

// ─── Wire response types ──────────────────────────────────────────────────
//
// These types control the exact JSON shape for the admin accounts endpoint.
// field names diverge from AdminAccountView JSON tags where the UI contract
// requires different keys (e.g. rate_limit vs rate_limit_config, concurrent
// vs concurrent_limit).

type accountHealthResponse struct {
	State     string `json:"state"`
	LatencyMs int    `json:"latency_ms"`
	ErrorMsg  string `json:"error_msg,omitempty"`
	BanUntil  string `json:"ban_until,omitempty"` // RFC3339; omitempty handles nil
	LastCheck string `json:"last_check"`
}

type accountRateLimitResponse struct {
	QPS        float64 `json:"qps"`
	Burst      int     `json:"burst"`
	Concurrent int     `json:"concurrent"`
}

type accountRowResponse struct {
	Vendor         string                   `json:"vendor"`
	AccountID      string                   `json:"account_id"`
	Enabled        bool                     `json:"enabled"`
	Health         *accountHealthResponse   `json:"health"` // null when the account has no health row
	RateLimit      accountRateLimitResponse `json:"rate_limit"`
	VendorProfile  types.VendorProfile      `json:"vendor_profile"`
	BaseLatencyMs  int                      `json:"base_latency_ms"`
	CredentialMeta metadata.CredentialMeta  `json:"credential_meta"`
}

type accountsSummary struct {
	Total   int            `json:"total"`
	ByState map[string]int `json:"by_state"`
}

type accountsResponse struct {
	Accounts []accountRowResponse `json:"accounts"`
	Summary  accountsSummary      `json:"summary"`
}

// ─── Mapping helpers ──────────────────────────────────────────────────────

func mapAccountRow(v *metadata.AdminAccountView) accountRowResponse {
	row := accountRowResponse{
		Vendor:    v.Vendor,
		AccountID: v.AccountID,
		Enabled:   v.Enabled,
		RateLimit: accountRateLimitResponse{
			QPS:        v.RateLimitCfg.QPS,
			Burst:      v.RateLimitCfg.Burst,
			Concurrent: v.RateLimitCfg.ConcurrentLimit,
		},
		VendorProfile:  v.VendorProfile,
		BaseLatencyMs:  v.VendorProfile.BaseLatencyMs,
		CredentialMeta: v.CredentialMeta,
	}
	if v.Health != nil {
		h := v.Health
		health := &accountHealthResponse{
			State:     h.State,
			LatencyMs: h.LatencyMs,
			ErrorMsg:  h.ErrorMsg,
			LastCheck: h.LastCheck.Format("2006-01-02T15:04:05Z"),
		}
		if h.BanUntil != nil {
			health.BanUntil = h.BanUntil.Format("2006-01-02T15:04:05Z")
		}
		row.Health = health
	}
	return row
}

func computeSummary(views []metadata.AdminAccountView) accountsSummary {
	summary := accountsSummary{
		Total:   len(views),
		ByState: map[string]int{"healthy": 0, "degraded": 0, "banned": 0},
	}
	for _, v := range views {
		state := "healthy" // no health row = awaiting first probe
		if v.Health != nil {
			state = v.Health.State
		}
		summary.ByState[state]++
	}
	return summary
}

// ─── Handler ───────────────────────────────────────────────────────────────

// listAccountsHandler returns an http.Handler that serves GET /v1/admin/accounts.
func listAccountsHandler(mc AdminAccountsReader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		vendorFilter := query.Get("vendor")
		stateFilter := query.Get("state")

		views, err := mc.ListAccounts(r.Context(), vendorFilter, stateFilter)
		if err != nil {
			WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list accounts: %v", err))
			return
		}

		rows := make([]accountRowResponse, 0, len(views))
		for i := range views {
			rows = append(rows, mapAccountRow(&views[i]))
		}

		WriteJSON(w, http.StatusOK, accountsResponse{
			Accounts: rows,
			Summary:  computeSummary(views),
		})
	})
}

// ─── Route registration (for todo 54) ─────────────────────────────────────

// RegisterAccountsRoutes mounts the accounts read + write + ops handlers on
// srv. It is designed to be a one-line call in todo 54's route consolidation.
// mc serves GET (todo 20); registry serves POST/PUT/rotate/ban/unban (todo 26/
// 27, B2 CRUD); broadcaster serves circuit force open/close (todo 27).
// audit receives one entry per write attempt (todo 33); nil disables it.
func RegisterAccountsRoutes(srv *Server, mc AdminAccountsReader, registry AdminAccountsWriter, broadcaster EventBroadcaster, audit AuditRecorder) {
	srv.Handle("GET /v1/admin/accounts", listAccountsHandler(mc), true)
	srv.Handle("POST /v1/admin/accounts", createAccountHandler(registry, audit), true)
	srv.Handle("PUT /v1/admin/accounts/{vendor}/{id}", updateAccountHandler(registry, audit), true)
	srv.Handle("POST /v1/admin/accounts/{vendor}/{id}/rotate", rotateAccountHandler(registry, audit), true)
	srv.Handle("POST /v1/admin/accounts/{vendor}/{id}/ban", banAccountHandler(registry, audit), true)
	srv.Handle("POST /v1/admin/accounts/{vendor}/{id}/unban", unbanAccountHandler(registry, audit), true)
	srv.Handle("POST /v1/admin/accounts/{vendor}/{id}/circuit", circuitAccountHandler(broadcaster, audit), true)
}

// ─── Write side (todo 26, B2 structured CRUD) ─────────────────────────────
//
// allow: SIZE_OK — the B2 create/update surface is one cohesive HTTP unit;
// the orchestrator constrains todos 26/27 to this file (todo 27 appends
// rotate/ban/circuit here). Todo 33 adds one mechanical Record call per
// terminal branch (no new logic units). Validation/assembly lives in
// vendorrules.go.
//
// Secret zero-leak: no response body below ever carries credential material —
// 201/202 echo only vendor + account_id (+ static warnings/note).

// AdminAccountsWriter is the write-model surface the accounts handlers need.
// *accountregistry.AccountRegistry satisfies it (todo 6).
type AdminAccountsWriter interface {
	CreateAccount(ctx context.Context, info accountregistry.AccountInfo) error
	AccountAuthWriter
	SetEnabled(ctx context.Context, vendor types.Vendor, accountID string, enabled bool) error
	SetRateLimit(ctx context.Context, vendor types.Vendor, accountID string, cfg types.RateLimitConfig) error
	SetVendorProfile(ctx context.Context, vendor types.Vendor, accountID string, vp types.VendorProfile) error
	// Ban/Unban write account_health and broadcast BAN/UNBAN (todo 6).
	Ban(ctx context.Context, vendor types.Vendor, accountID, reason string, banUntil time.Time) error
	Unban(ctx context.Context, vendor types.Vendor, accountID string) error
}

// EventBroadcaster is the event-emit surface the circuit handler needs.
// *syncbroadcaster.SyncBroadcaster satisfies it; nil is tolerated (500).
type EventBroadcaster interface {
	Broadcast(eventType string, payload any) error
}

type createAccountRequest struct {
	Vendor        string                 `json:"vendor"`
	AccountID     string                 `json:"account_id"`
	Enabled       *bool                  `json:"enabled,omitempty"`
	RateLimit     *types.RateLimitConfig `json:"rate_limit,omitempty"`
	VendorProfile *types.VendorProfile   `json:"vendor_profile,omitempty"`
	Auth          map[string]any         `json:"auth,omitempty"`
}

type updateAccountRequest struct {
	Vendor        *string                `json:"vendor,omitempty"` // must match path when present
	AccountID     *string                `json:"account_id,omitempty"`
	Enabled       *bool                  `json:"enabled,omitempty"`
	RateLimit     *types.RateLimitConfig `json:"rate_limit,omitempty"`
	VendorProfile *types.VendorProfile   `json:"vendor_profile,omitempty"`
	Auth          map[string]any         `json:"auth,omitempty"`
}

type createAccountResponse struct {
	Vendor    string   `json:"vendor"`
	AccountID string   `json:"account_id"`
	Warnings  []string `json:"warnings,omitempty"`
}

type updateAccountResponse struct {
	Vendor      string   `json:"vendor"`
	AccountID   string   `json:"account_id"`
	Effective   string   `json:"effective"`
	Convergence string   `json:"convergence"`
	Note        string   `json:"note,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`
}

// writeFieldErrors emits the unified B4 validation error body.
func writeFieldErrors(w http.ResponseWriter, fieldErrors map[string]string) {
	WriteJSON(w, http.StatusBadRequest, map[string]any{
		"error":        "validation failed",
		"field_errors": fieldErrors,
	})
}

// writeRegistryError maps registry errors to status codes (404 for missing
// accounts, 500 otherwise). Error text never carries credential material.
func writeRegistryError(w http.ResponseWriter, verb string, err error) {
	if errors.Is(err, accountregistry.ErrAccountNotFound) {
		WriteError(w, http.StatusNotFound, "account not found")
		return
	}
	WriteError(w, http.StatusInternalServerError, fmt.Sprintf("%s account: %v", verb, err))
}

// isUniqueViolation detects a PG unique-constraint violation (lib/pq 23505)
// without importing the driver: pq.Error exposes SQLState().
func isUniqueViolation(err error) bool {
	var st interface{ SQLState() string }
	return errors.As(err, &st) && st.SQLState() == "23505"
}

// createAccountHandler serves POST /v1/admin/accounts (B2 创建).
func createAccountHandler(registry AdminAccountsWriter, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req createAccountRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		target := req.Vendor + ":" + req.AccountID
		info, fieldErrors, warnings := BuildAccountInfo(req)
		if len(fieldErrors) > 0 {
			recordWriteAudit(r, audit, "account", "create", target, "fail", nil)
			writeFieldErrors(w, fieldErrors)
			return
		}
		if err := registry.CreateAccount(r.Context(), info); err != nil {
			recordWriteAudit(r, audit, "account", "create", target, "fail", nil)
			if isUniqueViolation(err) {
				WriteError(w, http.StatusConflict, "account exists")
				return
			}
			writeRegistryError(w, "create", err)
			return
		}
		var detail map[string]any
		if len(warnings) > 0 {
			detail = map[string]any{"warnings": warnings}
		}
		recordWriteAudit(r, audit, "account", "create", target, "ok", detail)
		WriteJSON(w, http.StatusCreated, createAccountResponse{
			Vendor: req.Vendor, AccountID: req.AccountID, Warnings: warnings,
		})
	})
}

// updateAccountHandler serves PUT /v1/admin/accounts/{vendor}/{id}
// (B2 更新，含凭据轮换). All body fields are optional; absent = unchanged.
// The audit detail carries only non-secret changed fields (enabled /
// rate_limit / vendor_profile / auth_changed flag) — never auth material.
func updateAccountHandler(registry AdminAccountsWriter, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendor := types.Vendor(r.PathValue("vendor"))
		id := r.PathValue("id")
		target := string(vendor) + ":" + id
		if pathErrors := validateAccountPath(vendor, id); len(pathErrors) > 0 {
			recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
			writeFieldErrors(w, pathErrors)
			return
		}

		var req updateAccountRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.Vendor != nil && *req.Vendor != string(vendor) || req.AccountID != nil && *req.AccountID != id {
			recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
			WriteError(w, http.StatusBadRequest, "vendor/account_id in body does not match path")
			return
		}
		if req.Enabled == nil && req.RateLimit == nil && req.VendorProfile == nil && len(req.Auth) == 0 {
			recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
			WriteError(w, http.StatusBadRequest, "no fields to update")
			return
		}
		if req.RateLimit != nil {
			if fe := ValidateRateLimit(*req.RateLimit); len(fe) > 0 {
				recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
				writeFieldErrors(w, fe)
				return
			}
		}

		var warnings []string
		if len(req.Auth) > 0 {
			fe, wns, err := ApplyAuthPatch(r.Context(), registry, vendor, id, req.Auth)
			if err != nil {
				recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
				writeRegistryError(w, "update", err)
				return
			}
			if len(fe) > 0 {
				recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
				writeFieldErrors(w, fe)
				return
			}
			warnings = wns
		}
		if req.Enabled != nil {
			if err := registry.SetEnabled(r.Context(), vendor, id, *req.Enabled); err != nil {
				recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
				writeRegistryError(w, "update", err)
				return
			}
		}
		if req.RateLimit != nil {
			if err := registry.SetRateLimit(r.Context(), vendor, id, *req.RateLimit); err != nil {
				recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
				writeRegistryError(w, "update", err)
				return
			}
		}
		note := ""
		if req.VendorProfile != nil {
			vp := *req.VendorProfile
			vp.Vendor = vendor
			if err := registry.SetVendorProfile(r.Context(), vendor, id, vp); err != nil {
				recordWriteAudit(r, audit, "account", "update", target, "fail", nil)
				writeRegistryError(w, "update", err)
				return
			}
			note = vendorProfileNote
		}

		detail := map[string]any{"auth_changed": len(req.Auth) > 0}
		if req.Enabled != nil {
			detail["enabled"] = *req.Enabled
		}
		if req.RateLimit != nil {
			detail["rate_limit"] = *req.RateLimit
		}
		if req.VendorProfile != nil {
			detail["vendor_profile"] = *req.VendorProfile
		}
		recordWriteAudit(r, audit, "account", "update", target, "ok", detail)
		WriteJSON(w, http.StatusAccepted, updateAccountResponse{
			Vendor:      string(vendor),
			AccountID:   id,
			Effective:   "propagating",
			Convergence: "credential=event_<1s; enabled=next_snapshot_<=60s",
			Note:        note,
			Warnings:    warnings,
		})
	})
}

// validateAccountPath enforces the vendor enum + account_id shape on path
// parameters; a non-empty result is written as a 400 field_errors body.
func validateAccountPath(vendor types.Vendor, id string) map[string]string {
	pathErrors := map[string]string{}
	if _, ok := VendorRules[vendor]; !ok {
		pathErrors["vendor"] = vendorEnumHint
	}
	if err := ValidateAccountID(id); err != nil {
		pathErrors["account_id"] = accountIDHint
	}
	return pathErrors
}

// ─── Operations (todo 27: rotate / ban / unban / circuit) ─────────────────
//
// All four respond 202 {vendor, account_id, effective:"propagating"} — the
// write/event is accepted by the control plane; nodes apply it via the
// broadcast dispatcher (todo 9). Circuit does NOT write account_health: the
// breaker is node-local semantics; the 202 only means the event was emitted.
// Ban needs no second confirmation (the UI owns that interaction).

// accountOpResponse is the shared 202 body for the four operation endpoints.
// Warnings is populated only by rotate (e.g. 115 missing recommended keys).
type accountOpResponse struct {
	Vendor    string   `json:"vendor"`
	AccountID string   `json:"account_id"`
	Effective string   `json:"effective"`
	Warnings  []string `json:"warnings,omitempty"`
}

type banAccountRequest struct {
	Reason   string `json:"reason,omitempty"`
	BanUntil string `json:"ban_until,omitempty"` // RFC3339; default +24h
}

type circuitAccountRequest struct {
	Action string `json:"action"`
}

// defaultBanDuration applies when ban_until is absent from the ban body.
const defaultBanDuration = 24 * time.Hour

// rotateAccountHandler serves POST /v1/admin/accounts/{vendor}/{id}/rotate.
// The request body IS the vendor's auth field set; internally it is exactly
// the PUT-with-only-auth path (todo 26's ApplyAuthPatch) — on any credential/
// client_config change ONE CREDENTIAL_UPDATE broadcast fires with the new
// material, applied immediately by nodes (todo 9 dispatcher).
func rotateAccountHandler(registry AdminAccountsWriter, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendor := types.Vendor(r.PathValue("vendor"))
		id := r.PathValue("id")
		target := string(vendor) + ":" + id
		if pathErrors := validateAccountPath(vendor, id); len(pathErrors) > 0 {
			recordWriteAudit(r, audit, "account", "rotate", target, "fail", nil)
			writeFieldErrors(w, pathErrors)
			return
		}
		var auth map[string]any
		if err := json.NewDecoder(r.Body).Decode(&auth); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if len(auth) == 0 {
			recordWriteAudit(r, audit, "account", "rotate", target, "fail", nil)
			writeFieldErrors(w, map[string]string{"auth": "required"})
			return
		}
		fieldErrors, warnings, err := ApplyAuthPatch(r.Context(), registry, vendor, id, auth)
		if err != nil {
			recordWriteAudit(r, audit, "account", "rotate", target, "fail", nil)
			writeRegistryError(w, "rotate", err)
			return
		}
		if len(fieldErrors) > 0 {
			recordWriteAudit(r, audit, "account", "rotate", target, "fail", nil)
			writeFieldErrors(w, fieldErrors)
			return
		}
		var detail map[string]any
		if len(warnings) > 0 {
			detail = map[string]any{"warnings": warnings}
		}
		recordWriteAudit(r, audit, "account", "rotate", target, "ok", detail)
		WriteJSON(w, http.StatusAccepted, accountOpResponse{
			Vendor: string(vendor), AccountID: id, Effective: "propagating", Warnings: warnings,
		})
	})
}

// banAccountHandler serves POST /v1/admin/accounts/{vendor}/{id}/ban.
// ban_until defaults to +24h; an empty body is accepted (all defaults).
// The audit detail carries the ban reason + expiry (spec-sanctioned fields).
func banAccountHandler(registry AdminAccountsWriter, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendor := types.Vendor(r.PathValue("vendor"))
		id := r.PathValue("id")
		target := string(vendor) + ":" + id
		if pathErrors := validateAccountPath(vendor, id); len(pathErrors) > 0 {
			recordWriteAudit(r, audit, "account", "ban", target, "fail", nil)
			writeFieldErrors(w, pathErrors)
			return
		}
		var req banAccountRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		banUntil := time.Now().Add(defaultBanDuration)
		if req.BanUntil != "" {
			parsed, err := time.Parse(time.RFC3339, req.BanUntil)
			if err != nil {
				recordWriteAudit(r, audit, "account", "ban", target, "fail", nil)
				writeFieldErrors(w, map[string]string{"ban_until": "must be RFC3339"})
				return
			}
			banUntil = parsed
		}
		if err := registry.Ban(r.Context(), vendor, id, req.Reason, banUntil); err != nil {
			recordWriteAudit(r, audit, "account", "ban", target, "fail", nil)
			writeRegistryError(w, "ban", err)
			return
		}
		recordWriteAudit(r, audit, "account", "ban", target, "ok", map[string]any{
			"reason":    req.Reason,
			"ban_until": banUntil.UTC().Format(time.RFC3339),
		})
		WriteJSON(w, http.StatusAccepted, accountOpResponse{
			Vendor: string(vendor), AccountID: id, Effective: "propagating",
		})
	})
}

// unbanAccountHandler serves POST /v1/admin/accounts/{vendor}/{id}/unban.
func unbanAccountHandler(registry AdminAccountsWriter, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendor := types.Vendor(r.PathValue("vendor"))
		id := r.PathValue("id")
		target := string(vendor) + ":" + id
		if pathErrors := validateAccountPath(vendor, id); len(pathErrors) > 0 {
			recordWriteAudit(r, audit, "account", "unban", target, "fail", nil)
			writeFieldErrors(w, pathErrors)
			return
		}
		if err := registry.Unban(r.Context(), vendor, id); err != nil {
			recordWriteAudit(r, audit, "account", "unban", target, "fail", nil)
			writeRegistryError(w, "unban", err)
			return
		}
		recordWriteAudit(r, audit, "account", "unban", target, "ok", nil)
		WriteJSON(w, http.StatusAccepted, accountOpResponse{
			Vendor: string(vendor), AccountID: id, Effective: "propagating",
		})
	})
}

// circuitAccountHandler serves POST /v1/admin/accounts/{vendor}/{id}/circuit.
// It broadcasts CIRCUIT_FORCE_OPEN/CLOSE directly via the injected
// broadcaster; a nil broadcaster (not wired) yields 500, never a panic.
func circuitAccountHandler(broadcaster EventBroadcaster, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendor := types.Vendor(r.PathValue("vendor"))
		id := r.PathValue("id")
		target := string(vendor) + ":" + id
		if pathErrors := validateAccountPath(vendor, id); len(pathErrors) > 0 {
			recordWriteAudit(r, audit, "account", "circuit", target, "fail", nil)
			writeFieldErrors(w, pathErrors)
			return
		}
		var req circuitAccountRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		var eventType string
		switch req.Action {
		case "force_open":
			eventType = types.EventCircuitForceOpen
		case "force_close":
			eventType = types.EventCircuitForceClose
		default:
			recordWriteAudit(r, audit, "account", "circuit", target, "fail", nil)
			writeFieldErrors(w, map[string]string{"action": "must be one of force_open|force_close"})
			return
		}
		if broadcaster == nil {
			recordWriteAudit(r, audit, "account", "circuit", target, "fail", nil)
			WriteError(w, http.StatusInternalServerError, "circuit broadcaster not configured")
			return
		}
		if err := broadcaster.Broadcast(eventType, types.CircuitPayload{Vendor: vendor, AccountID: id}); err != nil {
			recordWriteAudit(r, audit, "account", "circuit", target, "fail", nil)
			WriteError(w, http.StatusInternalServerError, fmt.Sprintf("broadcast circuit event: %v", err))
			return
		}
		recordWriteAudit(r, audit, "account", "circuit", target, "ok", map[string]any{"action": req.Action})
		WriteJSON(w, http.StatusAccepted, accountOpResponse{
			Vendor: string(vendor), AccountID: id, Effective: "propagating",
		})
	})
}
