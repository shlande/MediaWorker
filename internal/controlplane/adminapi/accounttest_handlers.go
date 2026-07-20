package adminapi

// accounttest_handlers.go implements the B3 connection-test endpoint
// (docs/account-backend-adjustments.md:122-135): POST /v1/admin/accounts/test
// with dual-mode bodies — draft {"vendor","auth":{...}} tests unsaved form
// content, stored {"vendor","account_id"} tests the in-DB credentials. The
// driver's error_msg is returned verbatim in 422 responses (the B3 core
// experience: operators must see "invalid_grant" vs a wrong client_secret).
//
// SECRET HYGIENE: auth material flows only into the tester; it is never
// logged, never echoed in responses, and never written to admin_audit detail
// (audit instrumentation is todo 33's — when it lands it must record
// action="test" + result only).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/accountregistry"
	"github.com/shlande/mediaworker/internal/controlplane/accounttester"
	"github.com/shlande/mediaworker/internal/types"
)

// accountTestTimeout bounds the whole probe (token refresh + vendor API
// call) so a hung vendor endpoint cannot hold the admin connection.
const accountTestTimeout = 10 * time.Second

// accountTestRequest is the dual-mode B3 body. AccountID present selects
// stored mode; otherwise Auth selects draft mode.
type accountTestRequest struct {
	Vendor    string         `json:"vendor"`
	AccountID string         `json:"account_id"`
	Auth      map[string]any `json:"auth"`
}

// accountTestHandler serves POST /v1/admin/accounts/test.
//
//	@Summary		账号连通性测试
//	@Description	连接测试：draft 模式传入 auth 字段测试未保存凭据，stored 模式传入 account_id 测试已存储凭据
//	@Tags			admin-accounts
//	@Accept			json
//	@Produce		json
//	@Param			request	body		accountTestRequest	true	"测试请求"
//	@Success		200		{object}	object				"{state, latency_ms}"
//	@Failure		400		{object}	types.ErrorResponse	"无效请求体或字段验证失败"
//	@Failure		401		{object}	types.ErrorResponse
//	@Failure		403		{object}	types.ErrorResponse
//	@Failure		404		{object}	types.ErrorResponse	"已存储账号不存在"
//	@Failure		422		{object}	object				"{state, error_msg}"
//	@Failure		500		{object}	types.ErrorResponse
//	@Failure		501		{object}	object				"{error, vendor}"
//	@Security		AdminBearer
//	@Router			/v1/admin/accounts/test [post]
func accountTestHandler(tester *accounttester.Tester) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req accountTestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		stored := req.AccountID != ""
		if !stored && len(req.Auth) == 0 {
			WriteError(w, http.StatusBadRequest, "account_id or auth is required")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), accountTestTimeout)
		defer cancel()

		vendor := types.Vendor(req.Vendor)
		var state types.HealthState
		var err error
		if stored {
			state, err = tester.TestStored(ctx, vendor, req.AccountID)
		} else {
			state, err = tester.TestDraft(ctx, vendor, req.Auth)
		}
		if err != nil {
			writeAccountTestError(w, req.Vendor, err)
			return
		}

		if state.State == "healthy" {
			WriteJSON(w, http.StatusOK, map[string]any{
				"state":      state.State,
				"latency_ms": state.Latency.Milliseconds(),
			})
			return
		}
		// 422 with the driver error_msg verbatim (degraded/banned/error).
		WriteJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"state":     state.State,
			"error_msg": state.ErrorMsg,
		})
	})
}

// writeAccountTestError maps tester failures: draft validation → 400 with
// the B4 field_errors body; mock vendors → 501 with the vendor field; unknown
// stored account → 404; anything else → 500 without secret material.
func writeAccountTestError(w http.ResponseWriter, vendor string, err error) {
	var ve *accounttester.ValidationError
	switch {
	case errors.As(err, &ve):
		writeFieldErrors(w, ve.FieldErrors)
	case errors.Is(err, accounttester.ErrDriverNotImplemented):
		WriteJSON(w, http.StatusNotImplemented, map[string]any{
			"error":  "driver not implemented",
			"vendor": vendor,
		})
	case errors.Is(err, accountregistry.ErrAccountNotFound):
		WriteError(w, http.StatusNotFound, "account not found")
	default:
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("test account: %v", err))
	}
}

// RegisterAccountTestRoutes mounts the B3 connection-test endpoint (auth
// required). D1: todo 54 calls this from cmd/control-plane/main.go — this
// file never edits main.go itself.
func RegisterAccountTestRoutes(srv *Server, tester *accounttester.Tester) {
	srv.Handle("POST /v1/admin/accounts/test", accountTestHandler(tester), true)
}
