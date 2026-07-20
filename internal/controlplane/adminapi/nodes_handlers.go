package adminapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

type NodesReader interface {
	Snapshot() []noderegistry.NodeView
	Get(peerID types.PeerId) (noderegistry.NodeView, bool)
	Issuance(peerID types.PeerId) (exp int64, l4 bool, ok bool)
	ShouldHaveRenewed(peerID types.PeerId, now time.Time) bool
}

type NodeHistoryReader interface {
	GetNodeStatusHistory(ctx context.Context, peerID string, limit int) ([]metadata.NodeStatusHistoryRow, error)
}

type PinPlanLogReader interface {
	RecentByNode(nodeID string, limit int) []pinstrategy.DispatchRecord
}

type nodeSpaceResponse struct {
	Used  int64 `json:"used"`
	Total int64 `json:"total"`
}

type nodeJWTResponse struct {
	Exp               string `json:"exp"`
	ShouldHaveRenewed bool   `json:"should_have_renewed"`
}

type nodeListItemResponse struct {
	PeerID       string            `json:"peer_id"`
	Capabilities []string          `json:"capabilities"`
	PrefixSpace  nodeSpaceResponse `json:"prefix_space"`
	WarmSpace    nodeSpaceResponse `json:"warm_space"`
	Healthy      bool              `json:"healthy"`
	LastSeen     string            `json:"last_seen"`
	Region       string            `json:"region"`
	Version      string            `json:"version"`
	UptimeSec    int64             `json:"uptime_sec,omitempty"`
	ConnCount    int               `json:"conn_count"`
	JWT          *nodeJWTResponse  `json:"jwt"`
}

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

func mapNodeToListItem(v noderegistry.NodeView, reg NodesReader, now time.Time) nodeListItemResponse {
	r := nodeListItemResponse{
		PeerID:       string(v.PeerID),
		Capabilities: capabilityStrings(v.Capabilities),
		PrefixSpace:  nodeSpaceResponse{Used: v.PrefixSpace.UsedBytes, Total: v.PrefixSpace.TotalBytes},
		WarmSpace:    nodeSpaceResponse{Used: v.WarmSpace.UsedBytes, Total: v.WarmSpace.TotalBytes},
		Healthy:      v.Healthy,
		LastSeen:     v.ReceivedAt.Format(time.RFC3339),
		Region:       v.Region,
		Version:      v.Version,
		ConnCount:    v.ConnCount,
	}
	if v.StartedAt > 0 {
		r.UptimeSec = now.Unix() - v.StartedAt
	}
	if exp, _, ok := reg.Issuance(v.PeerID); ok {
		r.JWT = &nodeJWTResponse{
			Exp:               time.Unix(exp, 0).Format(time.RFC3339),
			ShouldHaveRenewed: reg.ShouldHaveRenewed(v.PeerID, now),
		}
	}
	return r
}

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

// listNodesHandler serves GET /v1/admin/nodes.
//
//	@Summary		节点列表
//	@Description	返回所有边缘节点及其容量、健康状态、最近派单计划
//	@Tags			admin-nodes
//	@Produce		json
//	@Param			healthy		query		string	false	"健康筛选（true|false）"
//	@Param			capability	query		string	false	"能力筛选（edge|l4_backhaul|relay_provider|peer_icp）"
//	@Success		200			{array}		nodeListItemResponse
//	@Failure		401			{object}	types.ErrorResponse
//	@Failure		403			{object}	types.ErrorResponse
//	@Security		AdminBearer
//	@Router			/v1/admin/nodes [get]
func listNodesHandler(reg NodesReader, now func() time.Time) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		healthyFilter := query.Get("healthy")
		capFilter := query.Get("capability")

		views := reg.Snapshot()
		out := make([]nodeListItemResponse, 0, len(views))
		for _, v := range views {
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
			if !capabilityFilter(v.Capabilities, capFilter) {
				continue
			}
			out = append(out, mapNodeToListItem(v, reg, now()))
		}
		WriteJSON(w, http.StatusOK, out)
	})
}

func RegisterNodesRoutes(srv *Server, reg NodesReader, historyReader NodeHistoryReader, pinPlanLog PinPlanLogReader, logger *slog.Logger, now func() time.Time) {
	srv.Handle("GET /v1/admin/nodes", listNodesHandler(reg, now), true)
	srv.Handle("GET /v1/admin/nodes/{peer_id}", nodeDetailHandler(reg, historyReader, pinPlanLog, now, logger), true)
}
