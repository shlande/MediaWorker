package jwt

import (
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// AuditLog is a simple structured logger for JWT signing audit records.
type AuditLog struct {
	mu  sync.Mutex
	out *log.Logger
}

// AuditEntry is a single audit record for JWT issuance.
type AuditEntry struct {
	Event          string       `json:"event"`
	PeerID         types.PeerId `json:"peer_id"`
	RemoteIP       string       `json:"remote_ip"`
	L4Whitelisted  bool         `json:"l4_whitelisted"`
	BandwidthQuota int64        `json:"bandwidth_quota"`
	Exp            int64        `json:"exp"`
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
func (a *AuditLog) Log(peerID types.PeerId, remoteIP string, l4Whitelisted bool, bandwidthQuota int64, exp int64) {
	entry := AuditEntry{
		Event:          "jwt_issue",
		PeerID:         peerID,
		RemoteIP:       remoteIP,
		L4Whitelisted:  l4Whitelisted,
		BandwidthQuota: bandwidthQuota,
		Exp:            exp,
		Timestamp:      time.Now(),
	}
	b, _ := json.Marshal(entry)
	a.mu.Lock()
	a.out.Println(string(b))
	a.mu.Unlock()
}
