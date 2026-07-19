package adminapi

import (
	"context"
	"fmt"
	"net/http"

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
	Vendor         string                    `json:"vendor"`
	AccountID      string                    `json:"account_id"`
	Enabled        bool                      `json:"enabled"`
	Health         *accountHealthResponse    `json:"health"` // null when the account has no health row
	RateLimit      accountRateLimitResponse  `json:"rate_limit"`
	VendorProfile  types.VendorProfile       `json:"vendor_profile"`
	BaseLatencyMs  int                       `json:"base_latency_ms"`
	CredentialMeta metadata.CredentialMeta   `json:"credential_meta"`
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
		Vendor:        v.Vendor,
		AccountID:     v.AccountID,
		Enabled:       v.Enabled,
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

// RegisterAccountsRoutes mounts the accounts read handler on srv. It is
// designed to be a one-line call in todo 54's route consolidation.
func RegisterAccountsRoutes(srv *Server, mc AdminAccountsReader) {
	srv.Handle("GET /v1/admin/accounts", listAccountsHandler(mc), true)
}
