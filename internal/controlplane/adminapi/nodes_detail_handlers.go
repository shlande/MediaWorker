package adminapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

type nodeDetailResponse struct {
	PeerID         string                       `json:"peer_id"`
	Capabilities   []string                     `json:"capabilities"`
	PrefixSpace    nodeSpaceResponse            `json:"prefix_space"`
	WarmSpace      nodeSpaceResponse            `json:"warm_space"`
	ColdSpace      *nodeSpaceResponse           `json:"cold_space"`
	Healthy        bool                         `json:"healthy"`
	LastSeen       string                       `json:"last_seen"`
	Region         string                       `json:"region"`
	Version        string                       `json:"version"`
	UptimeSec      int64                        `json:"uptime_sec,omitempty"`
	ConnCount      int                          `json:"conn_count"`
	JWT            *nodeJWTResponse             `json:"jwt"`
	RecentReports  []nodeHistoryReportResponse  `json:"recent_reports"`
	RecentPinPlans []pinPlanRecordResponse      `json:"recent_pin_plans"`
}

type nodeHistoryReportResponse struct {
	ID          int64   `json:"id"`
	PeerID      string  `json:"peer_id"`
	NodeID      *string `json:"node_id"`
	Healthy     bool    `json:"healthy"`
	PrefixUsed  *int64  `json:"prefix_used"`
	PrefixTotal *int64  `json:"prefix_total"`
	WarmUsed    *int64  `json:"warm_used"`
	WarmTotal   *int64  `json:"warm_total"`
	ConnCount   *int32  `json:"conn_count"`
	Region      *string `json:"region"`
	Version     *string `json:"version"`
	ReportedAt  string  `json:"reported_at"`
	ReceivedAt  string  `json:"received_at"`
}

type pinPlanRecordResponse struct {
	Seq        uint64 `json:"seq"`
	TargetNode string `json:"target_node"`
	ContentID  string `json:"content_id"`
	Pins       int    `json:"pins"`
	Unpins     int    `json:"unpins"`
	Trigger    string `json:"trigger"`
	SentAt     string `json:"sent_at"`
}

// nodeDetailHandler serves GET /v1/admin/nodes/{peer_id}.
//
//	@Summary		节点详情
//	@Description	返回指定节点的完整信息，含最近状态历史与最近派单计划
//	@Tags			admin-nodes
//	@Produce		json
//	@Param			peer_id	path		string	true	"libp2p PeerID"
//	@Success		200		{object}	nodeDetailResponse
//	@Failure		401		{object}	types.ErrorResponse
//	@Failure		403		{object}	types.ErrorResponse
//	@Failure		404		{object}	types.ErrorResponse	"节点不存在"
//	@Security		AdminBearer
//	@Router			/v1/admin/nodes/{peer_id} [get]
func nodeDetailHandler(reg NodesReader, historyReader NodeHistoryReader, pinPlanLog PinPlanLogReader, now func() time.Time, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerIDStr := r.PathValue("peer_id")
		peerID := types.PeerId(peerIDStr)

		view, ok := reg.Get(peerID)
		if !ok {
			WriteError(w, http.StatusNotFound, "node not found")
			return
		}

		base := mapNodeToListItem(view, reg, now())
		resp := nodeDetailResponse{
			PeerID:       base.PeerID,
			Capabilities: base.Capabilities,
			PrefixSpace:  base.PrefixSpace,
			WarmSpace:    base.WarmSpace,
			Healthy:      base.Healthy,
			LastSeen:     base.LastSeen,
			Region:       base.Region,
			Version:      base.Version,
			UptimeSec:    base.UptimeSec,
			ConnCount:    base.ConnCount,
			JWT:          base.JWT,
		}

		if view.ColdSpace != nil {
			resp.ColdSpace = &nodeSpaceResponse{
				Used:  view.ColdSpace.UsedBytes,
				Total: view.ColdSpace.TotalBytes,
			}
		}

		resp.RecentReports = fetchRecentReports(r.Context(), historyReader, peerIDStr, logger)
		resp.RecentPinPlans = fetchRecentPinPlans(pinPlanLog, view.NodeID)

		WriteJSON(w, http.StatusOK, resp)
	})
}

func fetchRecentReports(ctx context.Context, hr NodeHistoryReader, peerID string, logger *slog.Logger) []nodeHistoryReportResponse {
	rows, err := hr.GetNodeStatusHistory(ctx, peerID, 10)
	if err != nil {
		logger.WarnContext(ctx, "adminapi: node detail: history query failed, degrading to empty recent_reports", "peer_id", peerID, "err", err)
		return []nodeHistoryReportResponse{}
	}
	out := make([]nodeHistoryReportResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, nodeHistoryReportResponse{
			ID:          r.ID,
			PeerID:      r.PeerID,
			NodeID:      r.NodeID,
			Healthy:     r.Healthy,
			PrefixUsed:  r.PrefixUsed,
			PrefixTotal: r.PrefixTotal,
			WarmUsed:    r.WarmUsed,
			WarmTotal:   r.WarmTotal,
			ConnCount:   r.ConnCount,
			Region:      r.Region,
			Version:     r.Version,
			ReportedAt:  r.ReportedAt.Format(time.RFC3339),
			ReceivedAt:  r.ReceivedAt.Format(time.RFC3339),
		})
	}
	return out
}

func fetchRecentPinPlans(pl PinPlanLogReader, nodeID string) []pinPlanRecordResponse {
	records := pl.RecentByNode(nodeID, 10)
	out := make([]pinPlanRecordResponse, 0, len(records))
	for _, r := range records {
		out = append(out, pinPlanRecordResponse{
			Seq:        r.Seq,
			TargetNode: r.TargetNode,
			ContentID:  r.ContentID,
			Pins:       r.Pins,
			Unpins:     r.Unpins,
			Trigger:    r.Trigger,
			SentAt:     r.SentAt.Format(time.RFC3339),
		})
	}
	return out
}
