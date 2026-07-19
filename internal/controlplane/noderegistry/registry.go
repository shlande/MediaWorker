// Package noderegistry implements the control plane's in-memory view of
// edge-node status. It is the runtime authoritative source for two things:
//
//  1. The latest NodeStatusReport per peer (upserted by the SyncBroadcaster
//     subscribe loop in cmd/control-plane).
//  2. JWT issuance records (recorded by JWTService on every successful
//     issuance), which power the "should have renewed but didn't"
//     (应续未续) inference for the admin nodes API.
//
// Both are memory-only: state is lost on restart (accepted for v1). The
// persisted audit trail lives elsewhere (auditlog = audit stream,
// node_status_history = admin history); this registry is deliberately the
// only place issuance state is tracked for status decisions.
package noderegistry

import (
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// RenewWindowSeconds is how far before JWT expiry a node is expected to
// renew. It mirrors jwt policy refresh_before_seconds (300s default).
const RenewWindowSeconds = 300

// NodeView is the control plane's current view of one edge node: the node's
// latest status report plus the CP-side receipt timestamp.
//
// PeerID is the primary key (types.PeerId). NodeID is the libp2p host ID
// string carried by the report and stored here as a plain field.
type NodeView struct {
	PeerID            types.PeerId
	NodeID            string
	Capabilities      types.NodeCapabilities
	PrefixSpace       types.PartitionStatus
	WarmSpace         types.PartitionStatus
	ColdSpace         *types.PartitionStatus // nil until cold cache is wired
	Healthy           bool
	LastUpdate        int64     // node-side report timestamp (unix seconds)
	ReceivedAt        time.Time // CP-side receipt time of the latest report
	Region            string
	Version           string
	StartedAt         int64
	ConnCount         int
	JWTRefreshFail24h int
}

// issuanceRecord is one peer's latest JWT issuance fact.
type issuanceRecord struct {
	exp int64 // JWT expiry (unix seconds)
	l4  bool  // whether the issued JWT carried the L4Backhaul capability
}

// Registry is a thread-safe in-memory store of node views and JWT issuance
// records. The zero value is not usable; construct with NewRegistry.
type Registry struct {
	mu        sync.RWMutex
	nodes     map[types.PeerId]NodeView
	issuances map[types.PeerId]issuanceRecord
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		nodes:     make(map[types.PeerId]NodeView),
		issuances: make(map[types.PeerId]issuanceRecord),
	}
}

// UpsertReport stores report as the peer's current view, stamped with the
// CP-side receipt time. A newer report fully replaces the previous view.
func (r *Registry) UpsertReport(report types.NodeStatusReport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[report.PeerID] = NodeView{
		PeerID:            report.PeerID,
		NodeID:            report.NodeID,
		Capabilities:      report.Capabilities,
		PrefixSpace:       report.PrefixSpace,
		WarmSpace:         report.WarmSpace,
		ColdSpace:         report.ColdSpace,
		Healthy:           report.Healthy,
		LastUpdate:        report.LastUpdate,
		ReceivedAt:        time.Now(),
		Region:            report.Region,
		Version:           report.Version,
		StartedAt:         report.StartedAt,
		ConnCount:         report.ConnCount,
		JWTRefreshFail24h: report.JWTRefreshFail24h,
	}
}

// Snapshot returns a copy of all current node views (unordered).
func (r *Registry) Snapshot() []NodeView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NodeView, 0, len(r.nodes))
	for _, v := range r.nodes {
		out = append(out, v)
	}
	return out
}

// Get returns the current view for peerID, or ok=false if the peer has
// never reported.
func (r *Registry) Get(peerID types.PeerId) (NodeView, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.nodes[peerID]
	return v, ok
}

// RecordIssuance records that a JWT with expiry exp (unix seconds) and
// L4Backhaul capability l4 was issued to peerID. Called by JWTService on
// every successful issuance; memory-only by design (v1).
func (r *Registry) RecordIssuance(peerID types.PeerId, exp int64, l4 bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.issuances[peerID] = issuanceRecord{exp: exp, l4: l4}
}

// Issuance returns the latest issuance record for peerID, or ok=false if no
// JWT has been issued to that peer since this process started.
func (r *Registry) Issuance(peerID types.PeerId) (exp int64, l4 bool, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.issuances[peerID]
	return rec.exp, rec.l4, ok
}

// ShouldHaveRenewed reports whether peerID "should have renewed but didn't":
// a JWT was issued to the peer, now is past the renewal window boundary
// (exp - RenewWindowSeconds), and the peer's latest status report was
// received before that boundary (or never). A peer that has reported since
// the boundary is still checking in and is not flagged.
func (r *Registry) ShouldHaveRenewed(peerID types.PeerId, now time.Time) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.issuances[peerID]
	if !ok {
		return false
	}
	renewBy := time.Unix(rec.exp-RenewWindowSeconds, 0)
	if !now.After(renewBy) {
		return false
	}
	view, reported := r.nodes[peerID]
	if reported && !view.ReceivedAt.Before(renewBy) {
		return false
	}
	return true
}
