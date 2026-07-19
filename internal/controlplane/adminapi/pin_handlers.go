package adminapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Narrow dependency interfaces ────────────────────────────────────────

// PinContentMetaReader is the read-model surface the pin handlers need from
// the metadata layer: fetch content metadata and blobs+roles for validation
// and space computation. The production implementation is
// *metadata.PGMetadataClient (satisfies ContentMetaClient).
type PinContentMetaReader interface {
	GetContentMeta(ctx context.Context, contentID string) (*types.ContentMeta, error)
	GetContentBlobs(ctx context.Context, contentID string) ([]types.BlobDescriptor, []types.BlobRole, error)
}

// PinOrchestrator is the minimal orchestrator seam for manual pin dispatch.
// Its only method is SendManualPlan (todo 16). The production implementation
// is *pinstrategy.PinOrchestrator.
type PinOrchestrator interface {
	SendManualPlan(contentID string, targets []string, pinBlobs, unpinBlobs []string) ([]uint64, error)
}

// ─── Request / response types ────────────────────────────────────────────

type manualPinRequest struct {
	ContentID   string   `json:"content_id"`
	TargetNodes []string `json:"target_nodes"`
	Blobs       []string `json:"blobs,omitempty"` // blob_hash filter; empty = all
}

type skipEntry struct {
	PeerID string `json:"peer_id"`
	Reason string `json:"reason"`
}

type manualPinResponse struct {
	Seq     []uint64    `json:"seq"`
	Skipped []skipEntry `json:"skipped"`
}

// ─── Route registration ──────────────────────────────────────────────────

// RegisterPinRoutes mounts the manual pin/unpin handlers on srv. Called once
// by the route-consolidation task (todo 54). Per D1, does NOT edit
// cmd/control-plane/main.go. audit receives one entry per pin/unpin attempt
// (todo 33); nil disables it.
func RegisterPinRoutes(srv *Server, mc PinContentMetaReader, reg *noderegistry.Registry, po PinOrchestrator, audit AuditRecorder) {
	srv.Handle("POST /v1/admin/pin", manualPinHandler(mc, reg, po, audit), true)
	srv.Handle("POST /v1/admin/unpin", manualUnpinHandler(mc, reg, po, audit), true)
}

// ─── POST /v1/admin/pin ─────────────────────────────────────────────────

func manualPinHandler(mc PinContentMetaReader, reg *noderegistry.Registry, po PinOrchestrator, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req manualPinRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		if req.ContentID == "" {
			recordWriteAudit(r, audit, "pin", "pin", "", "fail", nil)
			WriteError(w, http.StatusBadRequest, "content_id is required")
			return
		}
		if len(req.TargetNodes) == 0 {
			recordWriteAudit(r, audit, "pin", "pin", req.ContentID, "fail", nil)
			WriteError(w, http.StatusBadRequest, "target_nodes is required")
			return
		}

		// 1. Validate content exists.
		_, err := mc.GetContentMeta(r.Context(), req.ContentID)
		if err != nil {
			recordWriteAudit(r, audit, "pin", "pin", req.ContentID, "fail", nil)
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, http.StatusNotFound, "content not found")
				return
			}
			WriteError(w, http.StatusInternalServerError, "metadata query failed")
			return
		}

		// 2. Resolve blobs: requested filter or all content blobs.
		allBlobs, _, err := mc.GetContentBlobs(r.Context(), req.ContentID)
		if err != nil {
			recordWriteAudit(r, audit, "pin", "pin", req.ContentID, "fail", nil)
			WriteError(w, http.StatusInternalServerError, "blob query failed")
			return
		}

		var resolvedBlobs []types.BlobDescriptor
		if len(req.Blobs) == 0 {
			resolvedBlobs = allBlobs
		} else {
			blobMap := make(map[string]types.BlobDescriptor, len(allBlobs))
			for _, b := range allBlobs {
				blobMap[b.BlobHash] = b
			}
			for _, hash := range req.Blobs {
				if b, ok := blobMap[hash]; ok {
					resolvedBlobs = append(resolvedBlobs, b)
				}
			}
			if len(resolvedBlobs) == 0 {
				recordWriteAudit(r, audit, "pin", "pin", req.ContentID, "fail", nil)
				WriteError(w, http.StatusUnprocessableEntity, "none of the requested blobs belong to this content")
				return
			}
		}

		// 3. Compute total pin bytes (unknown sizes → 0).
		totalBytes := int64(0)
		for _, b := range resolvedBlobs {
			if b.Size > 0 {
				totalBytes += b.Size
			}
		}

		// 4. Build node ID → view map for space check + existence validation.
		nodeByID := buildNodeIDMap(reg)

		var (
			dispatchTargets []string
			skipped         []skipEntry
		)
		for _, nodeID := range req.TargetNodes {
			view, ok := nodeByID[nodeID]
			if !ok {
				skipped = append(skipped, skipEntry{PeerID: nodeID, Reason: "node_not_found"})
				continue
			}
			remaining := view.PrefixSpace.TotalBytes - view.PrefixSpace.UsedBytes
			if remaining < totalBytes {
				skipped = append(skipped, skipEntry{PeerID: nodeID, Reason: "insufficient_space"})
				continue
			}
			dispatchTargets = append(dispatchTargets, nodeID)
		}

		// 5. All targets filtered → 422.
		if len(dispatchTargets) == 0 {
			recordWriteAudit(r, audit, "pin", "pin", req.ContentID, "fail", nil)
			resp := manualPinResponse{Skipped: skipped}
			WriteJSON(w, http.StatusUnprocessableEntity, resp)
			return
		}

		// 6. Build pin blob hash list.
		pinBlobs := make([]string, len(resolvedBlobs))
		for i, b := range resolvedBlobs {
			pinBlobs[i] = b.BlobHash
		}

		// 7. Dispatch.
		seqs, firstErr := po.SendManualPlan(req.ContentID, dispatchTargets, pinBlobs, nil)
		resp := manualPinResponse{Seq: seqs, Skipped: skipped}
		if firstErr != nil && len(seqs) == 0 {
			recordWriteAudit(r, audit, "pin", "pin", req.ContentID, "fail", nil)
			WriteError(w, http.StatusInternalServerError, "all target dispatches failed")
			return
		}
		detail := map[string]any{"target_nodes": dispatchTargets}
		if len(skipped) > 0 {
			detail["skipped"] = len(skipped)
		}
		recordWriteAudit(r, audit, "pin", "pin", req.ContentID, "ok", detail)
		WriteJSON(w, http.StatusAccepted, resp)
	})
}

// ─── POST /v1/admin/unpin ───────────────────────────────────────────────

func manualUnpinHandler(mc PinContentMetaReader, reg *noderegistry.Registry, po PinOrchestrator, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req manualPinRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		if req.ContentID == "" {
			recordWriteAudit(r, audit, "pin", "unpin", "", "fail", nil)
			WriteError(w, http.StatusBadRequest, "content_id is required")
			return
		}
		if len(req.TargetNodes) == 0 {
			recordWriteAudit(r, audit, "pin", "unpin", req.ContentID, "fail", nil)
			WriteError(w, http.StatusBadRequest, "target_nodes is required")
			return
		}

		// 1. Validate content exists.
		_, err := mc.GetContentMeta(r.Context(), req.ContentID)
		if err != nil {
			recordWriteAudit(r, audit, "pin", "unpin", req.ContentID, "fail", nil)
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, http.StatusNotFound, "content not found")
				return
			}
			WriteError(w, http.StatusInternalServerError, "metadata query failed")
			return
		}

		// 2. Determine which blobs to unpin.
		var unpinBlobs []string
		if len(req.Blobs) == 0 {
			allBlobs, _, err := mc.GetContentBlobs(r.Context(), req.ContentID)
			if err != nil {
				recordWriteAudit(r, audit, "pin", "unpin", req.ContentID, "fail", nil)
				WriteError(w, http.StatusInternalServerError, "blob query failed")
				return
			}
			unpinBlobs = make([]string, len(allBlobs))
			for i, b := range allBlobs {
				unpinBlobs[i] = b.BlobHash
			}
		} else {
			unpinBlobs = req.Blobs
		}

		if len(unpinBlobs) == 0 {
			recordWriteAudit(r, audit, "pin", "unpin", req.ContentID, "ok", nil)
			WriteJSON(w, http.StatusAccepted, manualPinResponse{})
			return
		}

		// 3. Validate all target nodes exist (no space check for unpin).
		nodeByID := buildNodeIDMap(reg)
		var skipped []skipEntry
		for _, nodeID := range req.TargetNodes {
			if _, ok := nodeByID[nodeID]; !ok {
				skipped = append(skipped, skipEntry{PeerID: nodeID, Reason: "node_not_found"})
			}
		}
		if len(skipped) > 0 {
			recordWriteAudit(r, audit, "pin", "unpin", req.ContentID, "fail", nil)
			resp := manualPinResponse{Skipped: skipped}
			WriteJSON(w, http.StatusUnprocessableEntity, resp)
			return
		}

		// 4. Dispatch unpin.
		seqs, firstErr := po.SendManualPlan(req.ContentID, req.TargetNodes, nil, unpinBlobs)
		if firstErr != nil && len(seqs) == 0 {
			recordWriteAudit(r, audit, "pin", "unpin", req.ContentID, "fail", nil)
			WriteError(w, http.StatusInternalServerError, "all target dispatches failed")
			return
		}
		recordWriteAudit(r, audit, "pin", "unpin", req.ContentID, "ok", map[string]any{"target_nodes": req.TargetNodes})
		WriteJSON(w, http.StatusAccepted, manualPinResponse{Seq: seqs})
	})
}
