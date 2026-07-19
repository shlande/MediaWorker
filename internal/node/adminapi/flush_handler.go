package adminapi

import (
	"context"
	"encoding/json"
	"net/http"

	"golang.org/x/sync/singleflight"
)

// WarmCacheFlusher is the narrow subset of *cache.WarmCache needed for the
// flush handler. Tests satisfy it with a mock; production wires the real
// WarmCache.
type WarmCacheFlusher interface {
	Flush(ctx context.Context) error
}

type flushRequest struct {
	Partitions []string `json:"partitions"`
}

type flushResponse struct {
	Status     string   `json:"status"`
	Partitions []string `json:"partitions"`
}

// RegisterFlushRoutes mounts POST /v1/admin/flush-cache on srv. warmCache
// may be nil — a nil cache produces 409. Per orchestrator decision D1, this
// function does not edit main.go; todo 49 consolidates all node-admin route
// mounts.
func RegisterFlushRoutes(srv *Server, warmCache WarmCacheFlusher) {
	var sfg singleflight.Group

	srv.Handle("POST /v1/admin/flush-cache", func(w http.ResponseWriter, r *http.Request) {
		var req flushRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if len(req.Partitions) == 0 {
			WriteError(w, http.StatusBadRequest, "partitions is required")
			return
		}

		// Validate partition support.
		for _, p := range req.Partitions {
			switch p {
			case "warm":
			// supported
			case "prefix":
				WriteError(w, http.StatusBadRequest, "prefix partition is pin-managed; use unpin")
				return
			case "cold":
				WriteError(w, http.StatusBadRequest, "cold partition is not wired")
				return
			default:
				WriteError(w, http.StatusBadRequest, "unsupported partition: "+p)
				return
			}
		}

		if warmCache == nil {
			WriteError(w, http.StatusConflict, "warm cache is not configured")
			return
		}

		WriteJSON(w, http.StatusAccepted, flushResponse{
			Status:     "flushing",
			Partitions: req.Partitions,
		})

		// Flush asynchronously via goroutine so the handler returns 202
		// immediately. Singleflight deduplicates concurrent flushes:
		// a second POST while the first is still running joins the same
		// call and does NOT start a second Flush.
		go func() {
			_, _, _ = sfg.Do("warm", func() (any, error) {
				return nil, warmCache.Flush(context.Background())
			})
		}()
	})
}
