package pinstrategy

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// TestDispatchLog_RecentByNode_ThreePlans: 3 plans sent to one node →
// RecentByNode returns all 3, newest-first, with the recorded fields intact.
func TestDispatchLog_RecentByNode_ThreePlans(t *testing.T) {
	// Given: an orchestrator wired to a mock broadcaster.
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(&mockContentMetaClient{}, &mockPopularityClient{}, bcast)

	// When: 3 plans are dispatched to node-A (auto trigger).
	for i, contentID := range []string{"c1", "c2", "c3"} {
		_, err := po.sendNodePinPlan(types.NodePinPlan{
			NodeID:    "node-A",
			ContentID: contentID,
			PinBlobs:  []string{contentID},
		}, TriggerAuto)
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	// Then: RecentByNode returns 3 records, newest first.
	recs := po.DispatchLog().RecentByNode("node-A", 10)
	if len(recs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(recs))
	}
	if recs[0].ContentID != "c3" || recs[2].ContentID != "c1" {
		t.Fatalf("expected newest-first order c3..c1, got %s..%s", recs[0].ContentID, recs[2].ContentID)
	}
	if recs[0].Trigger != TriggerAuto || recs[0].TargetNode != "node-A" || recs[0].Pins != 1 {
		t.Fatalf("record fields wrong: %+v", recs[0])
	}
	if recs[0].SentAt.IsZero() {
		t.Fatal("expected SentAt to be set")
	}

	// And: limit truncates to the newest N.
	recs = po.DispatchLog().RecentByNode("node-A", 2)
	if len(recs) != 2 || recs[0].ContentID != "c3" || recs[1].ContentID != "c2" {
		t.Fatalf("limit=2 should return c3,c2; got %+v", recs)
	}

	// And: an unknown node yields an empty slice.
	if got := po.DispatchLog().RecentByNode("node-unknown", 10); len(got) != 0 {
		t.Fatalf("expected empty for unknown node, got %d", len(got))
	}
}

// TestDispatchLog_RingKeepsNewest50: per-node ring caps at 50, evicting oldest.
func TestDispatchLog_RingKeepsNewest50(t *testing.T) {
	l := NewDispatchLog()
	for i := 0; i < 55; i++ {
		l.Add(DispatchRecord{Seq: uint64(i + 1), TargetNode: "node-A", ContentID: "c", Pins: 1, SentAt: time.Now()})
	}
	recs := l.RecentByNode("node-A", 0) // 0 = all retained
	if len(recs) != 50 {
		t.Fatalf("expected ring to keep 50, got %d", len(recs))
	}
	if recs[0].Seq != 55 || recs[49].Seq != 6 {
		t.Fatalf("expected seqs 55..6 retained, got %d..%d", recs[0].Seq, recs[49].Seq)
	}
}

// TestDispatchLog_CountByContent_DedupedNodes: same content pinned on 2 nodes
// counts 2; after one node receives an unpin record it counts 1.
func TestDispatchLog_CountByContent_DedupedNodes(t *testing.T) {
	l := NewDispatchLog()
	now := time.Now()

	// Given: content c1 pinned on node-A and node-B (twice on node-B — dedup).
	l.Add(DispatchRecord{Seq: 1, TargetNode: "node-A", ContentID: "c1", Pins: 3, Trigger: TriggerAuto, SentAt: now})
	l.Add(DispatchRecord{Seq: 2, TargetNode: "node-B", ContentID: "c1", Pins: 3, Trigger: TriggerAuto, SentAt: now})
	l.Add(DispatchRecord{Seq: 3, TargetNode: "node-B", ContentID: "c1", Pins: 1, Trigger: TriggerAuto, SentAt: now})

	// Then: distinct node count is 2, not 3 records.
	if got := l.CountByContent()["c1"]; got != 2 {
		t.Fatalf("expected CountByContent=2, got %d", got)
	}

	// When: node-A receives an unpin record for c1.
	l.Add(DispatchRecord{Seq: 4, TargetNode: "node-A", ContentID: "c1", Unpins: 3, Trigger: TriggerAuto, SentAt: now})

	// Then: only node-B remains.
	if got := l.CountByContent()["c1"]; got != 1 {
		t.Fatalf("expected CountByContent=1 after unpin, got %d", got)
	}

	// When: node-B is also unpinned.
	l.Add(DispatchRecord{Seq: 5, TargetNode: "node-B", ContentID: "c1", Unpins: 1, Trigger: TriggerAuto, SentAt: now})

	// Then: c1 disappears from the map (zero-count contents are omitted).
	if _, ok := l.CountByContent()["c1"]; ok {
		t.Fatal("expected c1 to be omitted once no node pins it")
	}
}

// TestDispatchLog_Stats1h_WindowBoundary: a record 61 minutes old is excluded;
// records inside the window aggregate batches/pins/unpins/manual.
func TestDispatchLog_Stats1h_WindowBoundary(t *testing.T) {
	l := NewDispatchLog()
	now := time.Now()

	// Given: one record 61min old, one exactly 1h old (boundary, included),
	// one recent auto, one recent manual.
	l.Add(DispatchRecord{Seq: 1, TargetNode: "node-A", ContentID: "old", Pins: 5, Trigger: TriggerAuto, SentAt: now.Add(-61 * time.Minute)})
	l.Add(DispatchRecord{Seq: 2, TargetNode: "node-A", ContentID: "edge", Pins: 1, Trigger: TriggerAuto, SentAt: now.Add(-time.Hour)})
	l.Add(DispatchRecord{Seq: 3, TargetNode: "node-A", ContentID: "c1", Pins: 2, Unpins: 1, Trigger: TriggerAuto, SentAt: now.Add(-time.Minute)})
	l.Add(DispatchRecord{Seq: 4, TargetNode: "node-B", ContentID: "c2", Pins: 4, Trigger: TriggerManual, SentAt: now.Add(-time.Minute)})

	// When:
	batches, pins, unpins, manual := l.Stats1h(now)

	// Then: the 61min-old record is not counted.
	if batches != 3 {
		t.Fatalf("expected 3 in-window batches, got %d", batches)
	}
	if pins != 7 { // 1 + 2 + 4, excluding the 61min-old 5
		t.Fatalf("expected pins=7, got %d", pins)
	}
	if unpins != 1 {
		t.Fatalf("expected unpins=1, got %d", unpins)
	}
	if manual != 1 {
		t.Fatalf("expected manual=1, got %d", manual)
	}
}

// TestSendManualPlan_MonotonicSeqsManualTrigger: SendManualPlan dispatches to
// all targets, returns monotonically increasing seqs, and records trigger=manual.
func TestSendManualPlan_MonotonicSeqsManualTrigger(t *testing.T) {
	// Given: an orchestrator that has already dispatched one auto plan (seq>0).
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(&mockContentMetaClient{}, &mockPopularityClient{}, bcast)
	if _, err := po.sendNodePinPlan(types.NodePinPlan{NodeID: "node-A", ContentID: "c0", PinBlobs: []string{"x"}}, TriggerAuto); err != nil {
		t.Fatalf("warmup send: %v", err)
	}

	// When: a manual plan goes to 3 targets.
	seqs, err := po.SendManualPlan("vid-1", []string{"node-A", "node-B", "node-C"}, []string{"b1", "b2"}, nil)

	// Then: no error, 3 strictly increasing seqs.
	if err != nil {
		t.Fatalf("SendManualPlan: %v", err)
	}
	if len(seqs) != 3 {
		t.Fatalf("expected 3 seqs, got %d", len(seqs))
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("expected monotonically increasing seqs, got %v", seqs)
		}
	}

	// And: each target's latest record carries trigger=manual and the blob counts.
	for i, target := range []string{"node-A", "node-B", "node-C"} {
		recs := po.DispatchLog().RecentByNode(target, 1)
		if len(recs) != 1 {
			t.Fatalf("expected 1 record for %s, got %d", target, len(recs))
		}
		r := recs[0]
		if r.Trigger != TriggerManual {
			t.Fatalf("expected trigger=manual for %s, got %q", target, r.Trigger)
		}
		if r.Seq != seqs[i] || r.ContentID != "vid-1" || r.Pins != 2 || r.Unpins != 0 {
			t.Fatalf("record mismatch for %s: %+v vs seq %d", target, r, seqs[i])
		}
	}

	// And: pin_node_count for vid-1 is 3 distinct nodes.
	if got := po.DispatchLog().CountByContent()["vid-1"]; got != 3 {
		t.Fatalf("expected CountByContent=3, got %d", got)
	}
}

// TestSendNodePinPlan_SendError_NotRecorded locks the failed-send choice:
// a failed send is Warn-logged and NOT recorded in the dispatch log.
func TestSendNodePinPlan_SendError_NotRecorded(t *testing.T) {
	// Given: a broadcaster that always fails, and log output captured.
	bcast := &mockBroadcaster{sendErr: errors.New("node unreachable")}
	po := NewPinOrchestrator(&mockContentMetaClient{}, &mockPopularityClient{}, bcast)
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	// When: a plan dispatch fails.
	_, err := po.sendNodePinPlan(types.NodePinPlan{NodeID: "node-A", ContentID: "vid-1", PinBlobs: []string{"b1"}}, TriggerAuto)

	// Then: the error is returned (auto callers ignore it — fire-and-forget)...
	if err == nil {
		t.Fatal("expected send error to be returned")
	}
	// ...a WARN line is logged...
	if !strings.Contains(buf.String(), "WARN") || !strings.Contains(buf.String(), "node unreachable") {
		t.Fatalf("expected WARN log with send error, got: %q", buf.String())
	}
	// ...and the failed send is NOT recorded (locked decision).
	if got := po.DispatchLog().RecentByNode("node-A", 10); len(got) != 0 {
		t.Fatalf("failed send must not be recorded, got %+v", got)
	}
	if got := po.DispatchLog().CountByContent()["vid-1"]; got != 0 {
		t.Fatalf("failed send must not affect pin_node_count, got %d", got)
	}
	if batches, _, _, _ := po.DispatchLog().Stats1h(time.Now()); batches != 0 {
		t.Fatalf("failed send must not affect Stats1h, got %d batches", batches)
	}
}

// TestSendManualPlan_PartialFailure: one failing target is skipped — its seq
// is omitted, the first error is returned, and successful targets are recorded.
func TestSendManualPlan_PartialFailure(t *testing.T) {
	bcast := &mockBroadcaster{failOn: map[string]bool{"node-B": true}}
	po := NewPinOrchestrator(&mockContentMetaClient{}, &mockPopularityClient{}, bcast)

	seqs, err := po.SendManualPlan("vid-1", []string{"node-A", "node-B", "node-C"}, []string{"b1"}, nil)
	if err == nil {
		t.Fatal("expected first send error to be returned")
	}
	if len(seqs) != 2 {
		t.Fatalf("expected 2 successful seqs, got %d (%v)", len(seqs), seqs)
	}
	if got := po.DispatchLog().RecentByNode("node-B", 1); len(got) != 0 {
		t.Fatalf("failed target must not be recorded, got %+v", got)
	}
	if got := po.DispatchLog().CountByContent()["vid-1"]; got != 2 {
		t.Fatalf("expected CountByContent=2, got %d", got)
	}
}
