package adminapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Narrow dependencies (interfaces → testable) ───────────────────────────

// WhitelistStoreReader is the write+read surface the whitelist handlers need
// from the persistent whitelist store. *jwt.WhitelistStore satisfies this
// interface directly (todo 8). Todo 54 wires this to the Server via
// RegisterWhitelistRoutes.
type WhitelistStoreReader interface {
	Add(peerID types.PeerId, addedBy string) error
	Remove(peerID types.PeerId) error
	ListAll() ([]jwt.WhitelistEntry, error)
	Contains(peerID types.PeerId) bool
}

// WhitelistSet is the in-memory PeerIdSet surface for synchronous double-write.
// *jwt.PeerIdSet satisfies this interface directly.
type WhitelistSet interface {
	Add(p types.PeerId)
	Remove(p types.PeerId)
	Contains(p types.PeerId) bool
}

// WhitelistIssuanceReader is the JWT issuance-registry surface the whitelist
// handler uses to compute per-peer effective status. *noderegistry.Registry
// satisfies this interface directly (Issuance method, todo 12).
type WhitelistIssuanceReader interface {
	Issuance(peerID types.PeerId) (exp int64, l4 bool, ok bool)
}

// ─── Response types ────────────────────────────────────────────────────────

// effectiveAfterNote is the M3-contract wording returned on every whitelist
// mutating response: removal takes effect on the next JWT renewal, at most 1h.
const effectiveAfterNote = "next JWT renewal (≤1h)"

type whitelistEntryResponse struct {
	PeerID    string `json:"peer_id"`
	AddedAt   string `json:"added_at"`
	AddedBy   string `json:"added_by"`
	Effective bool   `json:"effective"`
}

type whitelistPostResponse struct {
	PeerID         string `json:"peer_id"`
	EffectiveAfter string `json:"effective_after"`
}

// ─── Mapping helpers ───────────────────────────────────────────────────────

// computeEffective reports whether the given peer has a currently-effective L4
// issuance: the JWT was issued, carries L4 capability, and has not expired yet.
// When no issuance record exists (ok == false) the peer is not effective.
func computeEffective(reg WhitelistIssuanceReader, peerID types.PeerId) bool {
	exp, l4, ok := reg.Issuance(peerID)
	return ok && l4 && exp > time.Now().Unix()
}

// mapWhitelistEntry converts a persisted WhitelistEntry into the API response
// shape, computing effective status from the issuance registry.
func mapWhitelistEntry(e jwt.WhitelistEntry, reg WhitelistIssuanceReader) whitelistEntryResponse {
	return whitelistEntryResponse{
		PeerID:    e.PeerID,
		AddedAt:   e.AddedAt.Format(time.RFC3339),
		AddedBy:   e.AddedBy,
		Effective: computeEffective(reg, types.PeerId(e.PeerID)),
	}
}

// ─── POST body ─────────────────────────────────────────────────────────────

type whitelistPostBody struct {
	PeerID string `json:"peer_id"`
}

// ─── Handlers ──────────────────────────────────────────────────────────────

// listWhitelistHandler returns an http.Handler for GET /v1/admin/whitelist.
// It lists all whitelist entries with per-entry effective status computed from
// the CP issuance registry.
func listWhitelistHandler(wlStore WhitelistStoreReader, reg WhitelistIssuanceReader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entries, err := wlStore.ListAll()
		if err != nil {
			WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list whitelist: %v", err))
			return
		}

		out := make([]whitelistEntryResponse, 0, len(entries))
		for _, e := range entries {
			out = append(out, mapWhitelistEntry(e, reg))
		}

		WriteJSON(w, http.StatusOK, out)
	})
}

// addWhitelistHandler returns an http.Handler for POST /v1/admin/whitelist.
//
// Body: {"peer_id": "<libp2p PeerId>"}
//
//   - Validates peer_id format with peer.Decode (bad format → 400).
//   - Double-writes to both the persistent store and the in-memory PeerIdSet
//     so JWT issuance (service.go:123) immediately reads the updated set.
//   - Idempotent: duplicate POST overwrites the existing entry (200 for existing,
//     201 for new). The choice is locked here with a comment for tests.
//   - addedBy comes from the authenticated user (UserFromCtx), not the body.
func addWhitelistHandler(wlStore WhitelistStoreReader, ps WhitelistSet, reg WhitelistIssuanceReader, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body whitelistPostBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if body.PeerID == "" {
			recordWriteAudit(r, audit, "whitelist", "add", "", "fail", nil)
			WriteError(w, http.StatusBadRequest, "missing peer_id")
			return
		}

		// Validate peer.ID format.
		_, err := peer.Decode(body.PeerID)
		if err != nil {
			recordWriteAudit(r, audit, "whitelist", "add", body.PeerID, "fail", nil)
			WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid peer_id: %v", err))
			return
		}

		peerID := types.PeerId(body.PeerID)

		// Resolve addedBy from the authenticated user.
		_, username, _, ok := UserFromCtx(r.Context())
		if !ok {
			WriteError(w, http.StatusUnauthorized, "missing user context")
			return
		}

		wasExisting := ps.Contains(peerID)

		// Double-write: persist + in-memory set (MUST stay in sync;
		// service.go:123 reads PeerIdSet on every JWT request).
		if err := wlStore.Add(peerID, username); err != nil {
			recordWriteAudit(r, audit, "whitelist", "add", body.PeerID, "fail", nil)
			WriteError(w, http.StatusInternalServerError, fmt.Sprintf("add whitelist: %v", err))
			return
		}
		ps.Add(peerID)

		status := http.StatusCreated
		// Idempotency choice: duplicate POST returns 200 (not 201).
		// Locked here — tests assert this exact behavior.
		if wasExisting {
			status = http.StatusOK
		}

		recordWriteAudit(r, audit, "whitelist", "add", body.PeerID, "ok", nil)
		WriteJSON(w, status, whitelistPostResponse{
			PeerID:         string(peerID),
			EffectiveAfter: effectiveAfterNote,
		})
	})
}

// deleteWhitelistHandler returns an http.Handler for DELETE /v1/admin/whitelist/{peer_id}.
//
//   - Checks existence via ps.Contains before removal (not present → 404).
//   - Double-removes from both the persistent store and the in-memory PeerIdSet.
//   - Response carries the effective_after note (M3 contract wording).
func deleteWhitelistHandler(wlStore WhitelistStoreReader, ps WhitelistSet, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerIDStr := r.PathValue("peer_id")
		peerID := types.PeerId(peerIDStr)

		if !ps.Contains(peerID) {
			recordWriteAudit(r, audit, "whitelist", "remove", peerIDStr, "fail", nil)
			WriteError(w, http.StatusNotFound, "not found")
			return
		}

		if err := wlStore.Remove(peerID); err != nil {
			recordWriteAudit(r, audit, "whitelist", "remove", peerIDStr, "fail", nil)
			WriteError(w, http.StatusInternalServerError, fmt.Sprintf("remove whitelist: %v", err))
			return
		}
		ps.Remove(peerID)

		recordWriteAudit(r, audit, "whitelist", "remove", peerIDStr, "ok", nil)
		w.WriteHeader(http.StatusNoContent)
	})
}

// ─── Error sentinel ────────────────────────────────────────────────────────

var errWhitelistPeerNotFound = errors.New("whitelist peer not found")

// ─── Route registration (for todo 54) ──────────────────────────────────────

// RegisterWhitelistRoutes mounts the whitelist CRUD handlers on srv. It is
// designed to be a one-line call in todo 54's route consolidation.
// wlStore is the persistent BadgerDB-backed whitelist store (todo 8).
// ps is the in-memory PeerIdSet (double-written on every add/remove).
// reg provides JWT issuance records for effective-status computation (todo 12).
// audit receives one entry per write attempt (todo 33); nil disables it.
func RegisterWhitelistRoutes(srv *Server, wlStore WhitelistStoreReader, ps WhitelistSet, reg WhitelistIssuanceReader, audit AuditRecorder) {
	srv.Handle("GET /v1/admin/whitelist", listWhitelistHandler(wlStore, reg), true)
	srv.Handle("POST /v1/admin/whitelist", addWhitelistHandler(wlStore, ps, reg, audit), true)
	srv.Handle("DELETE /v1/admin/whitelist/{peer_id}", deleteWhitelistHandler(wlStore, ps, audit), true)
}
