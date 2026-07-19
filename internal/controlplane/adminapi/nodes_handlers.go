package adminapi

import (
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Narrow registry dependency (interface → testable) ────────────────────

// NodesReader is the read-model surface the nodes handler needs from the
// node registry. The production implementation is *noderegistry.Registry
// (satisfies this interface directly — todo 12). Todo 54 wires this
// interface to the Server via RegisterNodesRoutes.
type NodesReader interface {
	Snapshot() []noderegistry.NodeView
	Issuance(peerID types.PeerId) (exp int64, l4 bool, ok bool)
	ShouldHaveRenewed(peerID types.PeerId, now time.Time) bool
}

// ─── Response types ───────────────────────────────────────────────────────

type nodeSpaceResponse struct {
	Used  int64 `json:"used"`
	Total int64 `json:"total"`
}

type nodeJWTResponse struct {
	Exp               string `json:"exp"`
	ShouldHaveRenewed bool   `json:"should_have_renewed"`
}

// nodeListItemResponse is the per-node shape in the GET /v1/admin/nodes
// list response. JWT is nil when the node has no issuance record.
type nodeListItemResponse struct {
	PeerID       string             `json:"peer_id"`
	Capabilities []string           `json:"capabilities"`
	PrefixSpace  nodeSpaceResponse  `json:"prefix_space"`
	WarmSpace    nodeSpaceResponse  `json:"warm_space"`
	Healthy      bool               `json:"healthy"`
	LastSeen     string             `json:"last_seen"`
	Region       string             `json:"region"`
	Version      string             `json:"version"`
	UptimeSec    int64              `json:"uptime_sec,omitempty"`
	ConnCount    int                `json:"conn_count"`
	JWT          *nodeJWTResponse   `json:"jwt"`
}

// ─── Mapping helpers (shared — todo 31 reuses mapNodeToListItem) ──────────

// capabilityStrings converts a NodeCapabilities bitmask into a sorted string
// slice suitable for JSON serialization. Order is the canonical struct-field
// order (edge, l4_backhaul, relay_provider, peer_icp).
func capabilityStrings(caps types.NodeCapabilities) []string {
	out := make([]string, 0, 4)
	if caps.Edge {
		out = append(out, "edge")
	}
	if caps.L4Backhaul {
		out = append(out, "l4_backhaul")
	}
	if caps.RelayProvider {
		out = append(out, "relay_provider")
	}
	if caps.PeerICP {
		out = append(out, "peer_icp")
	}
	return out
}

// mapNodeToListItem converts a NodeView into the admin API response shape.
// reg is the same NodesReader that powers the handler — it provides per-node
// JWT issuance state. now is the handler's current clock (injected for
// deterministic tests). Todo 31 reuses this function for the node-detail
// endpoint.
func mapNodeToListItem(v noderegistry.NodeView, reg NodesReader, now time.Time) nodeListItemResponse {
	r := nodeListItemResponse{
		PeerID:       string(v.PeerID),
		Capabilities: capabilityStrings(v.Capabilities),
		PrefixSpace: nodeSpaceResponse{
			Used:  v.PrefixSpace.UsedBytes,
			Total: v.PrefixSpace.TotalBytes,
		},
		WarmSpace: nodeSpaceResponse{
			Used:  v.WarmSpace.UsedBytes,
			Total: v.WarmSpace.TotalBytes,
		},
		Healthy:   v.Healthy,
		LastSeen:  v.ReceivedAt.Format(time.RFC3339),
		Region:    v.Region,
		Version:   v.Version,
		ConnCount: v.ConnCount,
	}
	if v.StartedAt > 0 {
		r.UptimeSec = now.Unix() - v.StartedAt
	}
	// JWT status: null when no issuance record exists.
	if exp, _, ok := reg.Issuance(v.PeerID); ok {
		r.JWT = &nodeJWTResponse{
			Exp:               time.Unix(exp, 0).Format(time.RFC3339),
			ShouldHaveRenewed: reg.ShouldHaveRenewed(v.PeerID, now),
		}
	}
	return r
}

// capabilityFilter returns true when the query filter is empty (passthrough)
// or when the node's capabilities include the named capability.
func capabilityFilter(caps types.NodeCapabilities, filter string) bool {
	if filter == "" {
		return true
	}
	switch filter {
	case "edge":
		return caps.Edge
	case "l4_backhaul":
		return caps.L4Backhaul
	case "relay_provider":
		return caps.RelayProvider
	case "peer_icp":
		return caps.PeerICP
	}
	return false
}

// ─── Handler ───────────────────────────────────────────────────────────────

// listNodesHandler returns an http.Handler that serves GET /v1/admin/nodes.
// now returns the current time and is injected so tests can control the clock.
func listNodesHandler(reg NodesReader, now func() time.Time) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		healthyFilter := query.Get("healthy")
		capFilter := query.Get("capability")

		views := reg.Snapshot()
		out := make([]nodeListItemResponse, 0, len(views))
		for _, v := range views {
			// Healthy filter
			switch healthyFilter {
			case "true":
				if !v.Healthy {
					continue
				}
			case "false":
				if v.Healthy {
					continue
				}
			}
			// Capability filter
			if !capabilityFilter(v.Capabilities, capFilter) {
				continue
			}
		out = append(out, mapNodeToListItem(v, reg, now()))
	}

	WriteJSON(w, http.StatusOK, out)
	})
}

// ─── Route registration (for todo 54) ─────────────────────────────────────

// RegisterNodesRoutes mounts the nodes list handler on srv. It is designed
// to be a one-line call in todo 54's route consolidation. Todo 31 appends
// the node-detail handler via the same function.
func RegisterNodesRoutes(srv *Server, reg NodesReader) {
	srv.Handle("GET /v1/admin/nodes", listNodesHandler(reg, time.Now), true)
}
