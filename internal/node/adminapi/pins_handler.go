package adminapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/shlande/mediaworker/internal/node/pinstore"
	"github.com/shlande/mediaworker/internal/node/planlog"
)

// ─── Narrow interfaces ───────────────────────────────────────────────────────

// PinListReader is the narrow subset of *pinstore.PinStore used by
// GET /v1/pins. Production wires the real *PinStore; tests provide a mock.
type PinListReader interface {
	List(filter pinstore.PinFilter) []pinstore.PinEntry
}

// PinRetrier is the narrow subset of *pinstore.PinStore used by
// POST /v1/pins/{hash}/retry. Production wires the real *PinStore.
type PinRetrier interface {
	RetryPin(blobHash string) bool
}

// PinPlanLogReader is the narrow subset of *planlog.Log used by
// GET /v1/pin-plans/recent. Production wires the real *Log.
type PinPlanLogReader interface {
	Recent(limit int) []planlog.Record
}

// ─── Wire types ──────────────────────────────────────────────────────────────

// pinEntry is the per-pin response element for GET /v1/pins.
// Fields match the wire contract (docs/ui-api-requirements.md §4.2).
type pinEntry struct {
	BlobHash  string `json:"blob_hash"`
	ContentID string `json:"content_id"`
	Role      string `json:"role"`
	Size      int64  `json:"size"`
	PinnedAt  string `json:"pinned_at"`
	State     string `json:"state"`
	LastError string `json:"last_error,omitempty"`
}

// pinSummary carries aggregate counts for the /v1/pins response.
type pinSummary struct {
	Total   int `json:"total"`
	Ready   int `json:"ready"`
	Pulling int `json:"pulling"`
	Failed  int `json:"failed"`
}

// pinsResponse is the object-wrapped GET /v1/pins body.
type pinsResponse struct {
	Pins    []pinEntry `json:"pins"`
	Summary pinSummary `json:"summary"`
}

// ─── Route registration ──────────────────────────────────────────────────────

// RegisterPinsRoutes mounts the three pin-related admin endpoints on srv.
// pinStore must satisfy both PinListReader and PinRetrier; planLog must
// satisfy PinPlanLogReader. Per orchestrator decision D1, this function does
// not edit main.go; todo 49 consolidates all node-admin route mounts.
func RegisterPinsRoutes(srv *Server, pinStore any, planLog PinPlanLogReader) {
	listReader, _ := pinStore.(PinListReader)
	retrier, _ := pinStore.(PinRetrier)

	srv.Handle("GET /v1/pins", handlePinsList(listReader))
	srv.Handle("POST /v1/pins/{hash}/retry", handlePinsRetry(retrier))
	srv.Handle("GET /v1/pin-plans/recent", handlePinPlansRecent(planLog))
}

// ─── GET /v1/pins ────────────────────────────────────────────────────────────

// handlePinsList 返回已钉扎的 blob 列表，支持 role/content_id/ready 过滤。
//
//	@Summary		钉扎列表
//	@Description	返回已钉扎的 blob 条目，支持按 role、content_id、ready 过滤。
//	@Tags			node-admin
//	@Produce		json
//	@Param			role		query	string	false	"按角色过滤"
//	@Param			content_id	query	string	false	"按内容 ID 过滤"
//	@Param			ready		query	bool	false	"按就绪状态过滤"
//	@Success		200			{object}	pinsResponse
//	@Failure		400			{object}	types.ErrorResponse	"ready must be true or false"
//	@Security		AdminToken
//	@Router			/v1/pins [get]
func handlePinsList(ps PinListReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ps == nil {
			WriteJSON(w, http.StatusOK, pinsResponse{Pins: []pinEntry{}})
			return
		}

		filter := pinstore.PinFilter{
			Role:      r.URL.Query().Get("role"),
			ContentID: r.URL.Query().Get("content_id"),
		}

		if raw := r.URL.Query().Get("ready"); raw != "" {
			b, err := strconv.ParseBool(raw)
			if err != nil {
				WriteError(w, http.StatusBadRequest, "ready must be true or false")
				return
			}
			filter.Ready = &b
		}

		entries := ps.List(filter)
		pins := make([]pinEntry, 0, len(entries))
		var summary pinSummary

		for i := range entries {
			e := &entries[i]
			pins = append(pins, pinEntry{
				BlobHash:  e.BlobHash,
				ContentID: e.ContentID,
				Role:      e.Role,
				Size:      e.Size,
				PinnedAt:  e.PinnedAt.UTC().Format(time.RFC3339),
				State:     e.State,
				LastError: e.LastError,
			})

			summary.Total++
			switch e.State {
			case pinstore.PinStateReady:
				summary.Ready++
			case pinstore.PinStatePulling:
				summary.Pulling++
			case pinstore.PinStateFailed:
				summary.Failed++
			}
		}

		WriteJSON(w, http.StatusOK, pinsResponse{Pins: pins, Summary: summary})
	}
}

// ─── POST /v1/pins/{hash}/retry ──────────────────────────────────────────────

// handlePinsRetry 重新拉取失败的钉扎项。
//
//	@Summary		重试钉扎
//	@Description	将指定 blob 哈希对应的失败钉扎项重新加入拉取队列。
//	@Tags			node-admin
//	@Produce		json
//	@Param			hash	path	string	true	"blob SHA-256 哈希"
//	@Success		202		{object}	map[string]string
//	@Failure		404		{object}	types.ErrorResponse
//	@Security		AdminToken
//	@Router			/v1/pins/{hash}/retry [post]
func handlePinsRetry(ps PinRetrier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ps == nil {
			WriteError(w, http.StatusNotFound, "pin not found or not in failed state")
			return
		}

		hash := r.PathValue("hash")
		if !ps.RetryPin(hash) {
			WriteError(w, http.StatusNotFound, "pin not found or not in failed state")
			return
		}

		WriteJSON(w, http.StatusAccepted, map[string]string{"status": "retrying"})
	}
}

// ─── GET /v1/pin-plans/recent ────────────────────────────────────────────────

// handlePinPlansRecent 返回最近收到的钉扎计划记录。
//
//	@Summary		最近的钉扎计划
//	@Description	返回 syncbroadcaster 最近收到的 PinPlan 记录，可选 limit 参数限制条数。
//	@Tags			node-admin
//	@Produce		json
//	@Param			limit	query	int	false	"返回条数上限"
//	@Success		200		{array}	planlog.Record
//	@Failure		400		{object}	types.ErrorResponse	"limit must be a positive integer"
//	@Security		AdminToken
//	@Router			/v1/pin-plans/recent [get]
func handlePinPlansRecent(pl PinPlanLogReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if pl == nil {
			WriteJSON(w, http.StatusOK, []planlog.Record{})
			return
		}

		limit := 0
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 {
				WriteError(w, http.StatusBadRequest, "limit must be a positive integer")
				return
			}
			limit = parsed
		}

		records := pl.Recent(limit)
		WriteJSON(w, http.StatusOK, records)
	}
}
