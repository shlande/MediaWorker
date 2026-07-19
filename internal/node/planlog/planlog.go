// Package planlog is the node's local receive log for control-plane PinPlans:
// a mutex-guarded ring buffer (capacity 50) of per-plan counts. v1 keeps it
// in memory only — a restart resets the log — and records counts, never full
// plan payloads. todo 44/49 expose it via GET /v1/pin-plans/recent.
package planlog

import (
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// capacity bounds the ring; the oldest record is evicted on overflow.
const capacity = 50

// Record is one received PinPlan, summarized to counts. Field JSON tags match
// the GET /v1/pin-plans/recent wire contract (docs/ui-api-requirements.md §4.3).
type Record struct {
	Seq        uint64    `json:"seq"`
	ReceivedAt time.Time `json:"received_at"`
	Pins       int       `json:"pins"`
	Unpins     int       `json:"unpins"`
	Applied    bool      `json:"applied"` // false when the pin store is disabled (prefix cache off)
}

// Log is a fixed-capacity ring of Records, safe for concurrent Add/Recent.
type Log struct {
	mu   sync.Mutex
	ring [capacity]Record
	next int // write position of the NEXT record
	size int // valid entries, capped at capacity
}

// New returns an empty Log.
func New() *Log { return &Log{} }

// Add appends r, evicting the oldest record when full.
func (l *Log) Add(r Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ring[l.next] = r
	l.next = (l.next + 1) % capacity
	if l.size < capacity {
		l.size++
	}
}

// Recent returns the newest-first records, at most limit entries; limit <= 0
// or greater than the stored count returns everything stored (max 50).
func (l *Log) Recent(limit int) []Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.size
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]Record, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, l.ring[(l.next-1-i+capacity)%capacity])
	}
	return out
}

// Counts totals the pin/unpin instructions in a plan, mirroring
// pinstrategy.HandlePinPlan's authoritative shape: PinBlobMetas when present
// (new CP), legacy PinBlobs otherwise.
func Counts(plan types.PinPlan) (pins, unpins int) {
	for _, u := range plan.Updates {
		if len(u.PinBlobMetas) > 0 {
			pins += len(u.PinBlobMetas)
		} else {
			pins += len(u.PinBlobs)
		}
		unpins += len(u.UnpinBlobs)
	}
	return pins, unpins
}
