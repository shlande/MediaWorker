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

		for _, e := range entries {
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
