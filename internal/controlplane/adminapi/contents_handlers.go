package adminapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/shlande/mediaworker/internal/storage/metadata"
)

// ─── Admin contents read-model (ui-admin-apis todo 28) ────────────────────
//
// Two endpoints:
//
//	(a) GET /v1/admin/contents?sort=&type=&replicas=&page=
//	    → mc.ListContents (todo 14) → merge dl.CountByContent() (tod 16)
//	    → replicas filter: replicas=degraded produces have < want
//	(b) GET /v1/admin/contents/{id}
//	    → mc.GetContentDetail (todo 21) → ErrContentNotFound → 404
//
// Soft-deleted contents are excluded from the list by the SQL layer
// (metadata_contents.go WHERE deleted_at IS NULL). The detail endpoint
// returns soft-deleted rows and marks them pending_delete.
//
// K (=ingest redundancy, replicas.want) is a hardcoded constant (2) merged
// at this layer per the contract: the SQL query only exposes replicas_have.
// pin_node_count is in-memory only, sourced from DispatchLog (todo 16).
//
// TODO 30 (DELETE /v1/admin/contents/{id}) appends its handler to this file.
// Write-side route registration is deliberately kept below the read-only block.

// ─── Narrow dependency interfaces (testable, shared across handlers) ──────

// ContentsListReader is the read-model surface the contents list handler
// needs from the metadata layer. Production implementation:
// *metadata.PGMetadataClient (todo 14).
type ContentsListReader interface {
	ListContents(ctx context.Context, q metadata.ListContentsQuery) ([]metadata.AdminContentRow, int, error)
}

// ContentsDetailReader is the metadata surface for the contents/{id}
// endpoint. Production implementation: *metadata.PGMetadataClient (todo 21).
type ContentsDetailReader interface {
	GetContentDetail(ctx context.Context, contentID string) (*metadata.AdminContentDetail, error)
}

// PinCountReader is the dispatch-log bookkeeping seam needed to merge
// pin_node_count into the contents list response. Production: *DispatchLog.
type PinCountReader interface {
	CountByContent() map[string]int
}

// ─── K constant ───────────────────────────────────────────────────────────

// ReplicasWant is the ingest redundancy target (=K). It is hardcoded here
// because the SQL query (todo 14) only computes replicas_have; the API layer
// merges want into the response as replicas:{have,want}. If K is ever made
// configurable, replace this constant with the runtime value.
const ReplicasWant = 2

// ─── Route registration (D1-compliant: no main.go edit) ───────────────────

// RegisterContentsRoutes mounts the contents read endpoints. mc is the
// metadata client (list + detail); dlog supplies the in-memory pin_node_count
// map for the list endpoint. TODO 30 extends this function to also register
// the DELETE handler.
func RegisterContentsRoutes(srv *Server, mc struct {
	ContentsListReader
	ContentsDetailReader
}, dlog PinCountReader) {
	srv.Handle("GET /v1/admin/contents", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		listContents(w, r, mc, dlog)
	}), true)

	srv.Handle("GET /v1/admin/contents/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		getContentDetail(w, r, mc)
	}), true)

	// TODO 30: srv.Handle("DELETE /v1/admin/contents/{id}", ..., true)
}

// ─── List handler: GET /v1/admin/contents ─────────────────────────────────

// contentRowResponse is the per-row JSON shape for the contents list.
// pin_node_count is merged here (in-memory only, never from PG).
type contentRowResponse struct {
	ContentID     string             `json:"content_id"`
	Title         string             `json:"title"`
	ContentType   string             `json:"content_type"`
	TotalBytes    int64              `json:"total_bytes"`
	BlobCount     int                `json:"blob_count"`
	Replicas      replicasResponse   `json:"replicas"`
	Window24h     int64              `json:"window_24h"`
	PinNodeCount  int                `json:"pin_node_count"`
	PendingDelete bool               `json:"pending_delete"`
}

type replicasResponse struct {
	Have int `json:"have"`
	Want int `json:"want"`
}

func listContents(w http.ResponseWriter, r *http.Request, mc ContentsListReader, dlog PinCountReader) {
	q := r.URL.Query()

	page, pageSize := ParsePage(r)

	query := metadata.ListContentsQuery{
		Sort:     q.Get("sort"),
		Type:     q.Get("type"),
		Page:     page,
		PageSize: pageSize,
	}

	rows, total, err := mc.ListContents(r.Context(), query)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "internal error")
		return
	}

	pinCounts := dlog.CountByContent()

	var replicasFilter string
	if q.Has("replicas") {
		replicasFilter = q.Get("replicas")
	}

	out := make([]contentRowResponse, 0, len(rows))
	for _, row := range rows {
		// replicas filter: "degraded" → have < want
		if replicasFilter == "degraded" && row.ReplicasHave >= ReplicasWant {
			continue
		}

		title := row.Title
		if title == "" {
			if len(row.ContentID) >= 8 {
				title = row.ContentID[:8]
			} else {
				title = row.ContentID
			}
		}

		pinNodeCount := pinCounts[row.ContentID] // map default 0 for missing keys

		out = append(out, contentRowResponse{
			ContentID:   row.ContentID,
			Title:       title,
			ContentType: row.ContentType,
			TotalBytes:  row.TotalBytes,
			BlobCount:   row.BlobCount,
			Replicas: replicasResponse{
				Have: row.ReplicasHave,
				Want: ReplicasWant,
			},
			Window24h:     row.Window24h,
			PinNodeCount:  pinNodeCount,
			PendingDelete: false, // SQL layer filters deleted_at; list never shows deleted
		})
	}

	if out == nil {
		out = []contentRowResponse{}
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"contents":   out,
		"total":      total,
		"page":       page,
		"page_size":  pageSize,
	})
}

// ─── Detail handler: GET /v1/admin/contents/{id} ──────────────────────────

type contentDetailMetaResponse struct {
	ContentID    string `json:"content_id"`
	Title        string `json:"title"`
	ContentType  string `json:"content_type"`
	TypeMetadata []byte `json:"type_metadata"`          // raw passthrough, never parsed
	CreatedAt    string `json:"created_at"`
	PendingDelete bool  `json:"pending_delete"`
}

type contentDetailBlobResponse struct {
	Hash         string         `json:"hash"`
	Role         string         `json:"role"`
	SortOrder    int            `json:"sort_order"`
	BusinessMeta map[string]any `json:"business_meta,omitempty"`
	Size         int64          `json:"size"`
	BlobType     string         `json:"blob_type"`
}

type contentDetailLocationResponse struct {
	BlobHash      string  `json:"blob_hash"`
	BackendID     string  `json:"backend_id"`
	FileID        string  `json:"file_id"`
	AccountHealth *string `json:"account_health"`
}

type contentDetailResponse struct {
	Meta      contentDetailMetaResponse       `json:"meta"`
	Blobs     []contentDetailBlobResponse     `json:"blobs"`
	Locations []contentDetailLocationResponse `json:"locations"`
}

// contentIDMinLen is the minimum length for a well-formed content ID. Shorter
// IDs are rejected with 404 (not 500) to match acceptance criteria.
const contentIDMinLen = 4

func getContentDetail(w http.ResponseWriter, r *http.Request, mc ContentsDetailReader) {
	id := r.PathValue("id")
	if len(id) < contentIDMinLen {
		WriteError(w, http.StatusNotFound, "content not found")
		return
	}

	detail, err := mc.GetContentDetail(r.Context(), id)
	if err != nil {
		if errors.Is(err, metadata.ErrContentNotFound) {
			WriteError(w, http.StatusNotFound, "content not found")
			return
		}
		WriteError(w, http.StatusInternalServerError, "internal error")
		return
	}

	meta := detail.Meta
	title := meta.Title
	if title == "" {
		if len(meta.ContentID) >= 8 {
			title = meta.ContentID[:8]
		} else {
			title = meta.ContentID
		}
	}

	resp := contentDetailResponse{
		Meta: contentDetailMetaResponse{
			ContentID:     meta.ContentID,
			Title:         title,
			ContentType:   meta.ContentType,
			TypeMetadata:  meta.TypeMetadata,
			CreatedAt:     "", // placeholder until ContentMeta carries CreatedAt
			PendingDelete: meta.DeletedAt != nil,
		},
	}

	for _, b := range detail.Blobs {
		resp.Blobs = append(resp.Blobs, contentDetailBlobResponse{
			Hash:         b.Hash,
			Role:         b.Role,
			SortOrder:    b.SortOrder,
			BusinessMeta: b.BusinessMeta,
			Size:         b.Size,
			BlobType:     b.BlobType,
		})
	}
	if resp.Blobs == nil {
		resp.Blobs = []contentDetailBlobResponse{}
	}

	for _, loc := range detail.Locations {
		resp.Locations = append(resp.Locations, contentDetailLocationResponse{
			BlobHash:      loc.BlobHash,
			BackendID:     loc.BackendID,
			FileID:        loc.FileID,
			AccountHealth: loc.AccountHealth,
		})
	}
	if resp.Locations == nil {
		resp.Locations = []contentDetailLocationResponse{}
	}

	WriteJSON(w, http.StatusOK, resp)
}


