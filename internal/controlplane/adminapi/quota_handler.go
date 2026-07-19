package adminapi

import (
	"net/http"
	"sort"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/storage/quota"
)

// ─── Narrow node dependency (interface → testable) ────────────────────────

// nodeSnapshotter is the read surface the quota handler needs from the node
// registry. *noderegistry.Registry satisfies it.
type nodeSnapshotter interface {
	Snapshot() []noderegistry.NodeView
}

// nodeOnlineMaxAge is the freshness window for "online": a node is online
// when its latest report was received within 2× the 30s report interval
// (todo 11 reporter cadence).
const nodeOnlineMaxAge = 60 * time.Second

// quotaSafetyMargin mirrors the allocator's 20% distribution margin:
// global_qps × 0.8 is what gets divided across nodes.
const quotaSafetyMargin = 0.8

// ─── Wire types ────────────────────────────────────────────────────────────

type quotaAllocationRow struct {
	PeerID    string  `json:"peer_id"`
	BaseShare float64 `json:"base_share"`
}

type quotaResponse struct {
	GlobalQPS   float64              `json:"global_qps"`
	NodeCount   int                  `json:"node_count"`
	BaseShare   float64              `json:"base_share"`
	Allocations []quotaAllocationRow `json:"allocations"`
}

// ─── Handler ───────────────────────────────────────────────────────────────

// quotaHandler serves GET /v1/admin/quota: the simplified v1 quota view
// (docs/ui-api-requirements.md §3.5 — usage/utilization/borrow-ledger fields
// deliberately absent). The allocator keys allocations by the node's libp2p
// host ID, which the UI contract exposes as peer_id.
func quotaHandler(qa *quota.QuotaAllocator, nodes nodeSnapshotter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		globalQPS := qa.GlobalQPS()

		online := 0
		if nodes != nil {
			cutoff := time.Now().Add(-nodeOnlineMaxAge)
			for _, v := range nodes.Snapshot() {
				if v.ReceivedAt.After(cutoff) {
					online++
				}
			}
		}

		// max(online, 1): a zero-node fleet still yields a defined base_share
		// instead of dividing by zero.
		baseShare := globalQPS * quotaSafetyMargin / float64(max(online, 1))

		perNode := map[string]float64{}
		for _, nodeShares := range qa.Allocations() {
			for nodeID, share := range nodeShares {
				perNode[nodeID] += share
			}
		}
		allocations := make([]quotaAllocationRow, 0, len(perNode))
		for nodeID, share := range perNode {
			allocations = append(allocations, quotaAllocationRow{PeerID: nodeID, BaseShare: share})
		}
		sort.Slice(allocations, func(i, j int) bool { return allocations[i].PeerID < allocations[j].PeerID })

		WriteJSON(w, http.StatusOK, quotaResponse{
			GlobalQPS:   globalQPS,
			NodeCount:   online,
			BaseShare:   baseShare,
			Allocations: allocations,
		})
	})
}

// ─── Route registration (for todo 54) ─────────────────────────────────────

// RegisterQuotaRoutes mounts the quota view handler on srv. One-line call in
// todo 54's route consolidation; reg may be nil (node_count then reads 0).
func RegisterQuotaRoutes(srv *Server, qa *quota.QuotaAllocator, reg nodeSnapshotter) {
	srv.Handle("GET /v1/admin/quota", quotaHandler(qa, reg), true)
}
