package pinstrategy

import (
	"sort"
	"sync"
	"time"
)

// Trigger values for DispatchRecord.Trigger.
const (
	// TriggerAuto marks plans dispatched by the orchestrator's automatic
	// paths (ingest initial pin and periodic rebalance).
	TriggerAuto = "auto"
	// TriggerManual marks plans dispatched via SendManualPlan (admin UI).
	TriggerManual = "manual"
)

// dispatchLogPerNodeCap is the number of newest records retained per node.
// NOTE: this ring bound also caps Stats1h accuracy — a node receiving more
// than 50 plans within one hour has its oldest in-window records evicted,
// so Stats1h may undercount for such hot nodes. Accepted for v1 (in-memory
// bookkeeping only; restart resets all state).
const dispatchLogPerNodeCap = 50

// statsWindow is the sliding window used by Stats1h.
const statsWindow = time.Hour

// DispatchRecord is one dispatched per-node PinPlan, recorded after a
// successful send. Failed sends are NOT recorded (locked decision, see
// sendNodePinPlan) — the log reflects what actually left the CP.
type DispatchRecord struct {
	Seq        uint64
	TargetNode string
	ContentID  string
	Pins       int
	Unpins     int
	Trigger    string // TriggerAuto | TriggerManual
	SentAt     time.Time
}

// DispatchLog is the orchestrator's in-memory bookkeeping of dispatched
// PinPlans. It backs three admin-UI contracts (docs/ui-api-requirements.md):
//   - per-node recent plan log (node detail page),
//   - pin_node_count aggregation (dashboard hot contents / content list),
//   - pin_stats_1h counters (dashboard).
//
// In-memory only: restart resets all state (v1 accepted; UI shows
// "process-local statistics"). All methods are safe for concurrent use.
type DispatchLog struct {
	mu sync.Mutex
	// perNode holds each node's newest records, oldest-first, capped at
	// dispatchLogPerNodeCap (ring: oldest evicted on overflow).
	perNode map[string][]DispatchRecord
	// pinState tracks the "currently should pin" flag per (content, node):
	// a record with Pins>0 sets true, a record with Unpins>0 sets false.
	// When a record carries both (replacing blobs of the same content),
	// Pins wins — the node still pins the content afterwards.
	pinState map[string]map[string]bool // contentID -> nodeID -> shouldPin
}

// NewDispatchLog creates an empty DispatchLog.
func NewDispatchLog() *DispatchLog {
	return &DispatchLog{
		perNode:  make(map[string][]DispatchRecord),
		pinState: make(map[string]map[string]bool),
	}
}

// Add appends a record to the target node's ring and updates the
// (content, node) pin state.
func (l *DispatchLog) Add(r DispatchRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ring := append(l.perNode[r.TargetNode], r)
	if len(ring) > dispatchLogPerNodeCap {
		ring = ring[len(ring)-dispatchLogPerNodeCap:]
	}
	l.perNode[r.TargetNode] = ring

	nodes, ok := l.pinState[r.ContentID]
	if !ok {
		nodes = make(map[string]bool)
		l.pinState[r.ContentID] = nodes
	}
	if r.Unpins > 0 {
		nodes[r.TargetNode] = false
	}
	if r.Pins > 0 {
		nodes[r.TargetNode] = true
	}
}

// RecentByNode returns up to limit newest records for nodeID,
// newest-first. A limit <= 0 or larger than the retained count returns all
// retained records (at most dispatchLogPerNodeCap).
func (l *DispatchLog) RecentByNode(nodeID string, limit int) []DispatchRecord {
	l.mu.Lock()
	defer l.mu.Unlock()

	ring := l.perNode[nodeID]
	if limit <= 0 || limit > len(ring) {
		limit = len(ring)
	}
	out := make([]DispatchRecord, 0, limit)
	for i := len(ring) - 1; i >= len(ring)-limit; i-- {
		out = append(out, ring[i])
	}
	return out
}

// CountByContent returns, per content, the number of DISTINCT nodes that
// currently should pin it (pinState flag true). Contents with zero pinned
// nodes are omitted from the map.
func (l *DispatchLog) CountByContent() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make(map[string]int, len(l.pinState))
	for contentID, nodes := range l.pinState {
		n := 0
		for _, shouldPin := range nodes {
			if shouldPin {
				n++
			}
		}
		if n > 0 {
			out[contentID] = n
		}
	}
	return out
}

// Stats1h aggregates retained records whose SentAt falls inside the 1h
// sliding window ending at now (a record exactly 1h old is included).
//
// Accuracy bound: records live in per-node rings of dispatchLogPerNodeCap,
// so a record YOUNGER than 1h may already have been evicted for hot nodes;
// Stats1h then undercounts. This bound is inherent to the in-memory v1
// design and is documented in the task evidence.
//
// Returns (batches, pins, unpins, manual): number of in-window records,
// summed Pins, summed Unpins, and the count of records with
// Trigger == TriggerManual.
func (l *DispatchLog) Stats1h(now time.Time) (batches, pins, unpins, manual int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := now.Add(-statsWindow)
	for _, ring := range l.perNode {
		for _, r := range ring {
			if r.SentAt.Before(cutoff) {
				continue
			}
			batches++
			pins += r.Pins
			unpins += r.Unpins
			if r.Trigger == TriggerManual {
				manual++
			}
		}
	}
	return batches, pins, unpins, manual
}

// Snapshot returns all retained DispatchRecords across all nodes, sorted by
// SentAt descending (newest first). Used by the admin pin-plans query API.
func (l *DispatchLog) Snapshot() []DispatchRecord {
	l.mu.Lock()
	defer l.mu.Unlock()

	var all []DispatchRecord
	for _, ring := range l.perNode {
		all = append(all, ring...)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].SentAt.After(all[j].SentAt)
	})
	return all
}
