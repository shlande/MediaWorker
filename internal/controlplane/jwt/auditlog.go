package jwt

import (
	"encoding/json"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// auditRingCapacity bounds the in-memory query ring (ui-admin-apis todo 34).
// The ring backs GET /v1/admin/audit?kind=jwt; 10000 entries is enough for the
// UI's recent-history view at the expected issuance rate, and keeps the
// Log-path allocation-free in steady state.
const auditRingCapacity = 10000

// AuditLog is a structured logger for JWT signing audit records. Beyond the
// JSON-lines sink it keeps every entry in a fixed-size in-memory ring so the
// admin API can query recent issuance history without a database. The ring is
// written synchronously on Log (same mutex as the line writer), so it is safe
// for concurrent use from the JWT request path.
type AuditLog struct {
	mu      sync.Mutex
	out     *log.Logger
	entries []AuditEntry // ring storage, len == auditRingCapacity once warm
	head    int          // next write slot in entries
	count   int          // entries stored, saturates at cap
}

// AuditFilter parameterizes Query. From/To are inclusive bounds on the entry
// timestamp (zero = unbounded); Q is a case-sensitive substring match on
// PeerID (empty = no filter); Limit caps the result count (<=0 = all matches).
type AuditFilter struct {
	From  time.Time
	To    time.Time
	Q     string
	Limit int
}

// AuditEntry is a single audit record for JWT issuance.
type AuditEntry struct {
	Event          string       `json:"event"`
	PeerID         types.PeerId `json:"peer_id"`
	RemoteIP       string       `json:"remote_ip"`
	L4Whitelisted  bool         `json:"l4_whitelisted"`
	BandwidthQuota int64        `json:"bandwidth_quota"`
	Exp            int64        `json:"exp"`
	Result         string       `json:"result"`
	Reason         string       `json:"reason,omitempty"`
	Timestamp      time.Time    `json:"timestamp"`
}

// NewAuditLog creates an AuditLog that writes JSON lines to w. If w is nil,
// output is discarded.
func NewAuditLog(w io.Writer) *AuditLog {
	out := log.New(io.Discard, "", 0)
	if w != nil {
		out = log.New(w, "", 0)
	}
	return &AuditLog{out: out}
}

// Log records a JWT issuance event as a JSON line.
func (a *AuditLog) Log(peerID types.PeerId, remoteIP string, l4Whitelisted bool, bandwidthQuota int64, exp int64, result string, reason string) {
	entry := AuditEntry{
		Event:          "jwt_issue",
		PeerID:         peerID,
		RemoteIP:       remoteIP,
		L4Whitelisted:  l4Whitelisted,
		BandwidthQuota: bandwidthQuota,
		Exp:            exp,
		Result:         result,
		Reason:         reason,
		Timestamp:      time.Now(),
	}
	b, _ := json.Marshal(entry)
	a.mu.Lock()
	a.out.Println(string(b))
	a.appendRingLocked(entry)
	a.mu.Unlock()
}

// appendRingLocked stores entry in the ring, overwriting the oldest entry
// once the capacity is reached. Caller must hold a.mu.
func (a *AuditLog) appendRingLocked(entry AuditEntry) {
	if a.entries == nil {
		a.entries = make([]AuditEntry, auditRingCapacity)
	}
	a.entries[a.head] = entry
	a.head = (a.head + 1) % auditRingCapacity
	if a.count < auditRingCapacity {
		a.count++
	}
}

// Query returns ring entries matching f, newest first. Iterating the ring
// backwards yields a stable ts-DESC order for entries written in time order;
// out-of-order clocks (tests) are returned in write order, not resorted.
func (a *AuditLog) Query(f AuditFilter) []AuditEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []AuditEntry
	for i := 0; i < a.count; i++ {
		e := a.entries[(a.head-1-i+auditRingCapacity)%auditRingCapacity]
		if !f.From.IsZero() && e.Timestamp.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && e.Timestamp.After(f.To) {
			continue
		}
		if f.Q != "" && !strings.Contains(string(e.PeerID), f.Q) {
			continue
		}
		out = append(out, e)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out
}
