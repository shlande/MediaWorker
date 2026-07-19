package jwt_test

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// logEntry records one audit event through the public Log API. Log stamps
// time.Now() internally, so From/To tests bound windows against real
// call-time clocks rather than injected timestamps.
func logEntry(l *jwt.AuditLog, peerID string) {
	l.Log(types.PeerId(peerID), "10.0.0.1", true, 1024, 999, "ok", "")
}

// Given a fresh AuditLog, when entries are logged, then Query returns them
// newest-first with full field fidelity.
func TestAuditLogQuery_ReturnsNewestFirst(t *testing.T) {
	l := jwt.NewAuditLog(io.Discard)
	logEntry(l, "peer-a")
	logEntry(l, "peer-b")
	logEntry(l, "peer-c")

	got := l.Query(jwt.AuditFilter{})
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3", len(got))
	}
	want := []string{"peer-c", "peer-b", "peer-a"}
	for i, w := range want {
		if string(got[i].PeerID) != w {
			t.Errorf("entry[%d].PeerID = %q, want %q", i, got[i].PeerID, w)
		}
		if got[i].Result != "ok" || got[i].RemoteIP != "10.0.0.1" || got[i].Event != "jwt_issue" {
			t.Errorf("entry[%d] lost fields: %+v", i, got[i])
		}
		if got[i].Timestamp.IsZero() {
			t.Errorf("entry[%d] has zero timestamp", i)
		}
	}
}

// Given logged entries, when Q is set, then only peer_ids containing the
// substring match.
func TestAuditLogQuery_QFiltersPeerIDSubstring(t *testing.T) {
	l := jwt.NewAuditLog(io.Discard)
	logEntry(l, "12D3KooWabc")
	logEntry(l, "12D3KooWdef")
	logEntry(l, "QmXyzabc")

	got := l.Query(jwt.AuditFilter{Q: "abc"})
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	for _, e := range got {
		if string(e.PeerID) != "QmXyzabc" && string(e.PeerID) != "12D3KooWabc" {
			t.Errorf("unexpected peer %q in q-filtered result", e.PeerID)
		}
	}
}

// Given logged entries, when From/To bound the window, then only entries
// inside the inclusive window match.
func TestAuditLogQuery_FromToWindow(t *testing.T) {
	l := jwt.NewAuditLog(io.Discard)
	logEntry(l, "peer-a")
	mid := time.Now()
	logEntry(l, "peer-b")

	// From after peer-a's write, To in the future: only peer-b.
	got := l.Query(jwt.AuditFilter{From: mid, To: time.Now().Add(time.Second)})
	if len(got) != 1 || string(got[0].PeerID) != "peer-b" {
		t.Fatalf("got %+v, want only peer-b", got)
	}

	// To before any write: nothing.
	got = l.Query(jwt.AuditFilter{To: time.Now().Add(-time.Hour)})
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}

	// Unbounded: everything.
	got = l.Query(jwt.AuditFilter{From: time.Now().Add(-time.Hour)})
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
}

// Given more logged entries than the limit, when Limit is set, then the
// newest Limit entries come back.
func TestAuditLogQuery_LimitCapsNewest(t *testing.T) {
	l := jwt.NewAuditLog(io.Discard)
	for _, p := range []string{"p1", "p2", "p3", "p4", "p5"} {
		logEntry(l, p)
	}
	got := l.Query(jwt.AuditFilter{Limit: 2})
	if len(got) != 2 || string(got[0].PeerID) != "p5" || string(got[1].PeerID) != "p4" {
		t.Fatalf("got %+v, want [p5 p4]", got)
	}
}

// Given a ring filled past capacity, when querying, then the oldest entries
// have been overwritten and exactly capacity entries remain.
func TestAuditLogQuery_RingWrapsAtCapacity(t *testing.T) {
	l := jwt.NewAuditLog(io.Discard)
	const total = 10000 + 500
	for i := 0; i < total; i++ {
		l.Log(types.PeerId("peer"), "1.1.1.1", false, 0, int64(i), "ok", "")
	}
	got := l.Query(jwt.AuditFilter{})
	if len(got) != 10000 {
		t.Fatalf("got %d entries, want ring capacity 10000", len(got))
	}
	// Newest entry is the last write (exp = total-1); oldest retained is
	// exp = total-10000 (the first 500 were overwritten).
	if got[0].Exp != int64(total-1) {
		t.Errorf("newest Exp = %d, want %d", got[0].Exp, total-1)
	}
	if got[len(got)-1].Exp != int64(total-10000) {
		t.Errorf("oldest Exp = %d, want %d", got[len(got)-1].Exp, total-10000)
	}
}

// Given concurrent writers and readers, when run under -race, then no data
// race occurs and the ring stays consistent.
func TestAuditLogQuery_ConcurrentLogAndQuery(t *testing.T) {
	l := jwt.NewAuditLog(io.Discard)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				l.Log(types.PeerId("peer"), "1.1.1.1", false, 0, 1, "ok", "")
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if got := l.Query(jwt.AuditFilter{Limit: 10}); len(got) > 10 {
					t.Errorf("Query returned %d > limit 10", len(got))
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := l.Query(jwt.AuditFilter{}); len(got) != 800 {
		t.Fatalf("got %d entries, want 800", len(got))
	}
}
