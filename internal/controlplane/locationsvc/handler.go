// Package locationsvc implements the control-plane blob-location query API
// (GET /v1/blob-locations/{hash}). It is mounted onto the JWT HTTP server's
// mux via JWTHTTPServer.RegisterLocationHandler so no new listening port is
// introduced (plan line 176).
//
// The handler authenticates callers via a control-plane-signed capability JWT
// (Authorization: Bearer <jwt>) and authorizes only tokens carrying the Edge
// capability. It then queries the metadata BlobStoreClient for the blob's
// K-redundant storage locations and returns them as JSON.
package locationsvc

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// locationsResponse is the JSON body returned on success.
type locationsResponse struct {
	Locations []types.BlobLocation `json:"locations"`
}

// Handler implements GET /v1/blob-locations/{hash}. A nil mc (constructed via
// NewHandlerWithNilClient) is allowed and yields HTTP 503 on every request —
// this lets the control plane keep the route mounted even when PG is
// unavailable, so edges see a deterministic contract instead of a 404 route.
type Handler struct {
	pubKey ed25519.PublicKey
	mc     metadata.BlobStoreClient
}

// NewHandler builds a Handler that verifies JWTs against controlPlanePubKey
// and queries mc for blob locations. mc may be nil; in that case the handler
// is still safe to mount but every authenticated request returns 503.
func NewHandler(controlPlanePubKey ed25519.PublicKey, mc metadata.BlobStoreClient) *Handler {
	return &Handler{pubKey: controlPlanePubKey, mc: mc}
}

// ServeHTTP implements http.Handler.
//
// Status codes:
//   - 200: locations found (JSON {"locations":[...]})
//   - 401: missing, malformed, expired, or bad-signature JWT
//   - 403: JWT valid but lacks the Edge capability
//   - 404: hash valid but no locations stored
//   - 500: metadata query returned an unexpected error
//   - 503: BlobStoreClient not configured (mc == nil)
func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	hash := req.PathValue("hash")
	if hash == "" {
		writeJSONError(w, http.StatusBadRequest, "missing hash path value")
		return
	}

	payload, err := h.authenticate(req)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if !payload.Capabilities.Edge {
		writeJSONError(w, http.StatusForbidden, "edge capability required")
		return
	}

	if h.mc == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "metadata client unavailable")
		return
	}

	locs, err := h.mc.GetBlobLocations(req.Context(), hash)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "metadata query failed")
		return
	}

	if len(locs) == 0 {
		writeJSONError(w, http.StatusNotFound, "no locations for hash")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(locationsResponse{Locations: locs})
}

// authenticate extracts and verifies the Bearer JWT. Returns the decoded
// payload on success. Any failure (missing header, wrong scheme, malformed
// JWT, bad signature, expiry) yields a non-nil error → caller emits 401.
func (h *Handler) authenticate(req *http.Request) (*types.NodeJWTPayload, error) {
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return nil, errors.New("locationsvc: missing Authorization header")
	}

	const scheme = "Bearer "
	if !strings.HasPrefix(authHeader, scheme) {
		return nil, errors.New("locationsvc: authorization scheme must be Bearer")
	}
	token := strings.TrimPrefix(authHeader, scheme)
	if token == "" {
		return nil, errors.New("locationsvc: empty bearer token")
	}

	payload, err := sjwt.VerifyJWTAnyPeerID(types.CapabilityJWT(token), h.pubKey)
	if err != nil {
		return nil, fmt.Errorf("locationsvc: verify jwt: %w", err)
	}
	return payload, nil
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
