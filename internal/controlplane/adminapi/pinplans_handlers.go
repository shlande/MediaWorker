package adminapi

import (
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
)

// PinPlanItem is one row in GET /v1/admin/pin-plans response.
// ack_state uses English enums acked/pending; the two-state contract
// maps to the Chinese wording "已下发"(acked) / "待节点上报"(pending)
// per docs/ui-adjustments.md:69.
type PinPlanItem struct {
	Seq        uint64    `json:"seq"`
	TargetNode string    `json:"target_node"`
	ContentID  string    `json:"content_id"`
	Pins       int       `json:"pins"`
	Unpins     int       `json:"unpins"`
	Trigger    string    `json:"trigger"`
	SentAt     time.Time `json:"sent_at"`
	AckState   string    `json:"ack_state"`
}

const (
	ackStateAcked   = "acked"
	ackStatePending = "pending"
)

// RegisterPinPlansRoutes mounts the GET /v1/admin/pin-plans handler on srv.
// Called once by the route-consolidation task (todo 54).
func RegisterPinPlansRoutes(srv *Server, dl *pinstrategy.DispatchLog, reg *noderegistry.Registry) {
	srv.Handle("GET /v1/admin/pin-plans", pinPlansHandler(dl, reg), true)
}

// pinPlansHandler serves GET /v1/admin/pin-plans.
//
//	@Summary		固定计划列表
//	@Description	返回分页的 pin/unpin 派单记录，含节点 Ack 状态
//	@Tags			admin-pin
//	@Produce		json
//	@Param			page		query	int	false	"页码"	default(1)
//	@Param			page_size	query	int	false	"每页条数"	default(20)
//	@Success		200			{array}	PinPlanItem
//	@Failure		401			{object}	types.ErrorResponse
//	@Failure		403			{object}	types.ErrorResponse
//	@Security		AdminBearer
//	@Router			/v1/admin/pin-plans [get]
func pinPlansHandler(dl *pinstrategy.DispatchLog, reg *noderegistry.Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, pageSize := ParsePage(r)

		records := dl.Snapshot()

		nodeByID := buildNodeIDMap(reg)

		start := (page - 1) * pageSize
		if start >= len(records) {
			WriteJSON(w, http.StatusOK, []PinPlanItem{})
			return
		}

		end := start + pageSize
		if end > len(records) {
			end = len(records)
		}
		pageRecords := records[start:end]

		items := make([]PinPlanItem, 0, len(pageRecords))
		for _, rec := range pageRecords {
			ackState := ackStatePending
			if view, ok := nodeByID[rec.TargetNode]; ok {
				// SentAt == ReceivedAt boundary counts as acked.
				if !rec.SentAt.After(view.ReceivedAt) {
					ackState = ackStateAcked
				}
			}
			items = append(items, PinPlanItem{
				Seq:        rec.Seq,
				TargetNode: rec.TargetNode,
				ContentID:  rec.ContentID,
				Pins:       rec.Pins,
				Unpins:     rec.Unpins,
				Trigger:    rec.Trigger,
				SentAt:     rec.SentAt,
				AckState:   ackState,
			})
		}
		WriteJSON(w, http.StatusOK, items)
	})
}

// buildNodeIDMap returns a map from NodeID to NodeView by indexing the
// registry snapshot. DispatchRecord.TargetNode is the NodeID string
// (libp2p host ID), while the Registry is keyed by PeerId — so we
// iterate Snapshot() and match on the NodeID field.
func buildNodeIDMap(reg *noderegistry.Registry) map[string]noderegistry.NodeView {
	snapshot := reg.Snapshot()
	out := make(map[string]noderegistry.NodeView, len(snapshot))
	for _, v := range snapshot {
		if v.NodeID != "" {
			out[v.NodeID] = v
		}
	}
	return out
}
