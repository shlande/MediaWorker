package quota

import (
	"context"
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// Broadcaster abstracts the control-plane broadcast primitive.
// Satisfied by syncbroadcaster.SyncBroadcaster.
type Broadcaster interface {
	Broadcast(eventType string, payload any) error
}

// BorrowRequest is a node's request to borrow extra quota for an account.
type BorrowRequest struct {
	AccountKey string  `json:"account_key"`
	Requested  float64 `json:"requested"`
}

// BorrowGrant is the control plane's response granting borrowed quota to a node.
type BorrowGrant struct {
	NodeID   string    `json:"node_id"`
	ExtraQPS float64   `json:"extra_qps"`
	Until    time.Time `json:"until"`
}

// QuotaAllocator manages per-account, per-node quota distribution on the
// control plane. It periodically rebalances allocations based on global
// limit configurations and node-reported load, and handles borrow requests
// from resource-starved nodes.
//
// The allocator accepts a 20% safety margin (§8.3): global QPS × 0.8 is
// distributed across nodes. The remaining 20% serves as a buffer for
// reporting latency and borrow grants.
type QuotaAllocator struct {
	mu          sync.RWMutex
	globalLimit map[string]types.RateLimitConfig // key = "vendor:account_id"
	allocation  map[string]map[string]float64    // [account][nodeID] = qps_share
	broadcaster Broadcaster
	actualUsage map[string]float64 // [account] = aggregated QPS from node reports

	// nodeLoads tracks the most recently reported load for each node per account.
	// load is in range [0.0, 1.0].
	nodeLoads map[string]map[string]float64 // [account][nodeID] = load
}

// NewQuotaAllocator creates a QuotaAllocator with the given broadcaster.
func NewQuotaAllocator(b Broadcaster) *QuotaAllocator {
	return &QuotaAllocator{
		globalLimit: make(map[string]types.RateLimitConfig),
		allocation:  make(map[string]map[string]float64),
		broadcaster: b,
		actualUsage: make(map[string]float64),
		nodeLoads:   make(map[string]map[string]float64),
	}
}

// SetGlobalLimit sets or updates the global rate limit configuration for an account.
func (qa *QuotaAllocator) SetGlobalLimit(accountKey string, cfg types.RateLimitConfig) {
	qa.mu.Lock()
	defer qa.mu.Unlock()
	qa.globalLimit[accountKey] = cfg
}

// getActiveNodes returns the list of active node IDs for the given account.
// It extracts them from the allocation map. Returns nil if no nodes are registered.
func (qa *QuotaAllocator) getActiveNodes(accountKey string) []string {
	nodes := qa.allocation[accountKey]
	if nodes == nil {
		return nil
	}
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	return ids
}

// getNodeLoad returns the latest load value for a node on a given account.
// Defaults to 0.0 if no report has been received.
func (qa *QuotaAllocator) getNodeLoad(accountKey, nodeID string) float64 {
	loads, ok := qa.nodeLoads[accountKey]
	if !ok {
		return 0.0
	}
	return loads[nodeID]
}

// RegisterNode adds a node to the allocation for an account with an initial
// equal-share allocation. If the node is already registered it is skipped.
func (qa *QuotaAllocator) RegisterNode(accountKey, nodeID string) {
	qa.mu.Lock()
	defer qa.mu.Unlock()

	if qa.allocation[accountKey] == nil {
		qa.allocation[accountKey] = make(map[string]float64)
		qa.nodeLoads[accountKey] = make(map[string]float64)
	}
	if _, exists := qa.allocation[accountKey][nodeID]; exists {
		return // already registered
	}
	// Give the new node an initial share so RegisterNode + Rebalance works
	// without a gap.
	qa.allocation[accountKey][nodeID] = 0.0
	qa.nodeLoads[accountKey][nodeID] = 0.0
}

// ReportNodeLoad updates the load for a node on a given account and returns the
// current allocation for that node (may be 0.0 if not yet rebalanced).
func (qa *QuotaAllocator) ReportNodeLoad(accountKey, nodeID string, load float64) float64 {
	qa.mu.Lock()
	defer qa.mu.Unlock()

	if qa.nodeLoads[accountKey] == nil {
		return 0.0
	}
	if load < 0.0 {
		load = 0.0
	}
	if load > 1.0 {
		load = 1.0
	}
	qa.nodeLoads[accountKey][nodeID] = load
	return qa.allocation[accountKey][nodeID]
}

// Rebalance recalculates quota shares for every account based on global limits
// and per-node load. It broadcasts a QUOTA_UPDATE event for each account whose
// allocation changed.
//
// Formula: baseShare = globalQPS * 0.8 / numNodes
//
//	adjustedShare = baseShare * (1.0 - load * 0.5)
//
// A node at 100% load gets half the base share; a node at 0% gets the full base share.
func (qa *QuotaAllocator) Rebalance(ctx context.Context) {
	qa.mu.Lock()
	defer qa.mu.Unlock()

	for accountKey, cfg := range qa.globalLimit {
		nodes := qa.getActiveNodes(accountKey)
		if len(nodes) == 0 {
			continue
		}

		// Base share: 80% of global QPS divided equally among nodes.
		baseShare := cfg.QPS * 0.8 / float64(len(nodes))

		// Per-node adjustment.
		for _, nodeID := range nodes {
			load := qa.getNodeLoad(accountKey, nodeID)
			share := baseShare * (1.0 - load*0.5)
			qa.allocation[accountKey][nodeID] = share
		}

		// Broadcast the updated allocation for this account.
		qa.broadcastQuotaUpdate(ctx, accountKey, qa.allocation[accountKey])
	}
}

// HandleBorrowRequests processes a set of borrow requests from a node.
// It approves a request if the aggregated actual usage across all nodes
// is below 80% of the global limit. Approved grants have a 30-second validity.
func (qa *QuotaAllocator) HandleBorrowRequests(nodeID string, requests []BorrowRequest) {
	qa.mu.Lock()
	defer qa.mu.Unlock()

	for _, req := range requests {
		cfg, hasLimit := qa.globalLimit[req.AccountKey]
		if !hasLimit {
			continue
		}

		globalUsage := qa.actualUsage[req.AccountKey]
		if globalUsage < cfg.QPS*0.8 {
			// Approve: give the node a temporary 30% boost on its current allocation.
			currentShare := qa.allocation[req.AccountKey][nodeID]
			borrowedShare := currentShare * 1.3
			extraQPS := borrowedShare - currentShare

			grant := BorrowGrant{
				NodeID:   nodeID,
				ExtraQPS: extraQPS,
				Until:    time.Now().Add(30 * time.Second),
			}
			_ = qa.broadcaster.Broadcast(types.EventQuotaBorrow, grant)
		}
		// Usage >= 80%: deny silently — the node retries next report cycle.
	}
}

// ReportActualUsage updates the aggregated actual QPS usage for an account.
func (qa *QuotaAllocator) ReportActualUsage(accountKey string, usage float64) {
	qa.mu.Lock()
	defer qa.mu.Unlock()
	qa.actualUsage[accountKey] = usage
}

// ─── Read-only accessors (admin quota view, ui-admin-apis todo 53) ─────────

// GlobalQPS returns the sum of configured QPS limits across all accounts.
func (qa *QuotaAllocator) GlobalQPS() float64 {
	qa.mu.RLock()
	defer qa.mu.RUnlock()
	var total float64
	for _, cfg := range qa.globalLimit {
		total += cfg.QPS
	}
	return total
}

// Allocations returns a deep copy of the current [account][nodeID] = qps_share
// map. Mutating the result never leaks into the allocator.
func (qa *QuotaAllocator) Allocations() map[string]map[string]float64 {
	qa.mu.RLock()
	defer qa.mu.RUnlock()
	out := make(map[string]map[string]float64, len(qa.allocation))
	for account, nodes := range qa.allocation {
		shares := make(map[string]float64, len(nodes))
		for nodeID, share := range nodes {
			shares[nodeID] = share
		}
		out[account] = shares
	}
	return out
}

// AccountKeys returns a copy of the configured global-limit account keys
// ("vendor:account_id"). Callers registering nodes per account (the CP
// subscribe loop) iterate this instead of tracking keys separately, so
// runtime SetGlobalLimit additions are picked up automatically.
func (qa *QuotaAllocator) AccountKeys() []string {
	qa.mu.RLock()
	defer qa.mu.RUnlock()
	keys := make([]string, 0, len(qa.globalLimit))
	for k := range qa.globalLimit {
		keys = append(keys, k)
	}
	return keys
}

// Run starts a background ticker that calls Rebalance at the given interval.
// It blocks until ctx is cancelled.
func (qa *QuotaAllocator) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run one immediate rebalance on start.
	qa.Rebalance(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			qa.Rebalance(ctx)
		}
	}
}

// broadcastQuotaUpdate sends a QUOTA_UPDATE event with the per-node allocation.
// The context is used for cancellation; the broadcast itself is best-effort.
func (qa *QuotaAllocator) broadcastQuotaUpdate(_ context.Context, accountKey string, nodeAlloc map[string]float64) {
	_ = qa.broadcaster.Broadcast(types.EventQuotaUpdate, map[string]any{
		"account_key": accountKey,
		"allocation":  nodeAlloc,
	})
}
