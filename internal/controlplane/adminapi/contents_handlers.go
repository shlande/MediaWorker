package adminapi

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"regexp"

	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// uuidRegex matches the standard 8-4-4-4-12 hex UUID format (case-insensitive).
// content_id is UUID PRIMARY KEY in the content table; PostgreSQL rejects
// non-UUID strings with SQLSTATE 22P02 (invalid_input_syntax), which is
// neither ErrContentNotFound nor sql.ErrNoRows. The handler validates the
// format before calling the storage layer so malformed IDs get 404, not 500.
var uuidRegex = regexp.MustCompile(`^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`)

// ─── Admin contents read-model (ui-admin-apis todo 28) ────────────────────
//
// Three endpoints:
//
//	(a) GET /v1/admin/contents?sort=&type=&replicas=&page=
//	    → mc.ListContents (todo 14) → merge dl.CountByContent() (tod 16)
//	    → replicas filter: replicas=degraded produces have < want
//	(b) GET /v1/admin/contents/{id}
//	    → mc.GetContentDetail (todo 21) → ErrContentNotFound → 404
//	(c) DELETE /v1/admin/contents/{id} (todo 30)
//	    → mc.GetContentMeta → 404/200-idempotent → mc.SoftDeleteContent → 200
//
// Soft-deleted contents are excluded from the list by the SQL layer
// (metadata_contents.go WHERE deleted_at IS NULL). The detail endpoint
// returns soft-deleted rows and marks them pending_delete.
//
// K (=ingest redundancy, replicas.want) is a hardcoded constant (2) merged
// at this layer per the contract: the SQL query only exposes replicas_have.
// pin_node_count is in-memory only, sourced from DispatchLog (todo 16).

// ─── Narrow dependency interfaces (testable, shared across handlers) ──────

type ContentsListReader interface {
	ListContents(ctx context.Context, q metadata.ListContentsQuery) ([]metadata.AdminContentRow, int, error)
}

type ContentsDetailReader interface {
	GetContentDetail(ctx context.Context, contentID string) (*metadata.AdminContentDetail, error)
}

type ContentMetaReader interface {
	GetContentMeta(ctx context.Context, contentID string) (*types.ContentMeta, error)
}

type ContentDeleter interface {
	SoftDeleteContent(ctx context.Context, contentID string) error
}

type PinCountReader interface {
	CountByContent() map[string]int
}

// ─── K constant ───────────────────────────────────────────────────────────

const ReplicasWant = 2

// ─── Route registration (D1-compliant: no main.go edit) ───────────────────

func RegisterContentsRoutes(srv *Server, mc struct {
	ContentsListReader
	ContentsDetailReader
	ContentMetaReader
}, dlog PinCountReader, deleter ContentDeleter, audit AuditRecorder) {
	srv.Handle("GET /v1/admin/contents", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		listContents(w, r, mc, dlog)
	}), true)

	srv.Handle("GET /v1/admin/contents/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		getContentDetail(w, r, mc)
	}), true)

	srv.Handle("DELETE /v1/admin/contents/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deleteContent(w, r, mc.ContentMetaReader, deleter, audit)
	}), true)
}

// ─── List handler: GET /v1/admin/contents ─────────────────────────────────

type contentRowResponse struct {
	ContentID     string           `json:"content_id"`
	Title         string           `json:"title"`
	ContentType   string           `json:"content_type"`
	TotalBytes    int64            `json:"total_bytes"`
	BlobCount     int              `json:"blob_count"`
	Replicas      replicasResponse `json:"replicas"`
	Window24h     int64            `json:"window_24h"`
	PinNodeCount  int              `json:"pin_node_count"`
	PendingDelete bool             `json:"pending_delete"`
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

		pinNodeCount := pinCounts[row.ContentID]

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
			PendingDelete: false,
		})
	}

	if out == nil {
		out = []contentRowResponse{}
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"contents":  out,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// ─── Detail handler: GET /v1/admin/contents/{id} ──────────────────────────

type contentDetailMetaResponse struct {
	ContentID     string `json:"content_id"`
	Title         string `json:"title"`
	ContentType   string `json:"content_type"`
	TypeMetadata  []byte `json:"type_metadata"`
	CreatedAt     string `json:"created_at"`
	PendingDelete bool   `json:"pending_delete"`
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

func getContentDetail(w http.ResponseWriter, r *http.Request, mc ContentsDetailReader) {
	id := r.PathValue("id")
	if !uuidRegex.MatchString(id) {
		WriteError(w, http.StatusNotFound, "content not found")
		return
	}

	detail, err := mc.GetContentDetail(r.Context(), id)
	if err != nil {
		if errors.Is(err, metadata.ErrContentNotFound) || errors.Is(err, sql.ErrNoRows) {
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
			CreatedAt:     "",
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

// ─── Delete handler: DELETE /v1/admin/contents/{id} ─────────────────────────

type contentDeleteResponse struct {
	ContentID     string `json:"content_id"`
	PendingDelete bool   `json:"pending_delete"`
	Note          string `json:"note"`
}

const deleteNoteFirst = "blobs become orphans; janitor sweeps after min_age"
const deleteNoteAlreadyDeleted = "already_deleted"

func deleteContent(w http.ResponseWriter, r *http.Request, metaReader ContentMetaReader, deleter ContentDeleter, audit AuditRecorder) {
	id := r.PathValue("id")
	if !uuidRegex.MatchString(id) {
		recordWriteAudit(r, audit, "content", "delete", id, "fail", nil)
		WriteError(w, http.StatusNotFound, "content not found")
		return
	}

	cm, err := metaReader.GetContentMeta(r.Context(), id)
	if err != nil {
		recordWriteAudit(r, audit, "content", "delete", id, "fail", nil)
		if errors.Is(err, sql.ErrNoRows) {
			WriteError(w, http.StatusNotFound, "content not found")
			return
		}
		WriteError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if cm.DeletedAt != nil {
		recordWriteAudit(r, audit, "content", "delete", id, "ok", map[string]any{"note": deleteNoteAlreadyDeleted})
		WriteJSON(w, http.StatusOK, contentDeleteResponse{
			ContentID:     id,
			PendingDelete: true,
			Note:          deleteNoteAlreadyDeleted,
		})
		return
	}

	if err := deleter.SoftDeleteContent(r.Context(), id); err != nil {
		recordWriteAudit(r, audit, "content", "delete", id, "fail", nil)
		WriteError(w, http.StatusInternalServerError, "internal error")
		return
	}

	recordWriteAudit(r, audit, "content", "delete", id, "ok", nil)
	WriteJSON(w, http.StatusOK, contentDeleteResponse{
		ContentID:     id,
		PendingDelete: true,
		Note:          deleteNoteFirst,
	})
}
