package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

type fakeHistoryWriter struct {
	insertErr error
	pruneErr  error
	inserts   []metadata.NodeStatusHistoryRow
	prunes    []string
}

func (f *fakeHistoryWriter) InsertNodeStatusHistory(_ context.Context, row metadata.NodeStatusHistoryRow) error {
	f.inserts = append(f.inserts, row)
	return f.insertErr
}

func (f *fakeHistoryWriter) PruneNodeStatusHistory(_ context.Context, peerID string, _ int) error {
	f.prunes = append(f.prunes, peerID)
	return f.pruneErr
}

func testReport(peer types.PeerId) types.NodeStatusReport {
	return types.NodeStatusReport{
		NodeID:      "12D3KooWNode",
		PeerID:      peer,
		PrefixSpace: types.PartitionStatus{TotalBytes: 1000, UsedBytes: 400, BlobCount: 7},
		WarmSpace:   types.PartitionStatus{TotalBytes: 5000, UsedBytes: 1200, BlobCount: 30},
		Healthy:     true,
		LastUpdate:  1_750_000_000,
		Region:      "cn-east",
		Version:     "v0.9.1",
		ConnCount:   12,
	}
}

// Given a history writer whose inserts fail, when a report is handled, then
// the registry is still updated (Warn-only semantics).
func TestHandleNodeStatusReport_HistoryFailureKeepsRegistryUpdate(t *testing.T) {
	reg := noderegistry.NewRegistry()
	fw := &fakeHistoryWriter{insertErr: errors.New("pg down")}
	counts := map[types.PeerId]int{}
	report := testReport("peer-a")

	handleNodeStatusReport(context.Background(), report, reg, fw, counts)

	view, ok := reg.Get("peer-a")
	if !ok {
		t.Fatal("registry missing peer after history insert failure")
	}
	if !view.Healthy || view.ConnCount != 12 || view.Region != "cn-east" {
		t.Errorf("registry view = %+v, want fields from report", view)
	}
	if got := len(fw.inserts); got != 1 {
		t.Errorf("insert attempts = %d, want 1", got)
	}
}

// Given 10 reports from the same peer, when handled, then history is
// inserted 10 times and pruned exactly once (every-10th cadence).
func TestHandleNodeStatusReport_PruneEveryTenthPerPeer(t *testing.T) {
	reg := noderegistry.NewRegistry()
	fw := &fakeHistoryWriter{}
	counts := map[types.PeerId]int{}
	ctx := context.Background()

	for range nodeStatusPruneEvery - 1 {
		handleNodeStatusReport(ctx, testReport("peer-a"), reg, fw, counts)
	}
	if got := len(fw.prunes); got != 0 {
		t.Fatalf("prunes after 9 reports = %d, want 0", got)
	}

	handleNodeStatusReport(ctx, testReport("peer-a"), reg, fw, counts)
	if got := len(fw.prunes); got != 1 {
		t.Fatalf("prunes after 10 reports = %d, want 1", got)
	}
	if fw.prunes[0] != "peer-a" {
		t.Errorf("pruned peer = %q, want %q", fw.prunes[0], "peer-a")
	}
	if got := len(fw.inserts); got != 10 {
		t.Errorf("inserts = %d, want 10", got)
	}

	// A different peer has its own counter.
	handleNodeStatusReport(ctx, testReport("peer-b"), reg, fw, counts)
	if got := len(fw.prunes); got != 1 {
		t.Errorf("prunes after peer-b first report = %d, want still 1", got)
	}
}

// Given a failing prune, when the 10th report arrives, then the error is
// Warn-only: inserts keep flowing and the registry stays correct.
func TestHandleNodeStatusReport_PruneFailureIsWarnOnly(t *testing.T) {
	reg := noderegistry.NewRegistry()
	fw := &fakeHistoryWriter{pruneErr: errors.New("pg down")}
	counts := map[types.PeerId]int{}
	ctx := context.Background()

	for range nodeStatusPruneEvery {
		handleNodeStatusReport(ctx, testReport("peer-a"), reg, fw, counts)
	}

	if got := len(fw.inserts); got != 10 {
		t.Errorf("inserts = %d, want 10 (prune failure must not stop inserts)", got)
	}
	if _, ok := reg.Get("peer-a"); !ok {
		t.Fatal("registry missing peer after prune failure")
	}
}

// Given a nil history writer (PG-unavailable startup), when a report is
// handled, then only the registry is updated — no panic, no prune.
func TestHandleNodeStatusReport_NilWriterSkipsHistory(t *testing.T) {
	reg := noderegistry.NewRegistry()
	counts := map[types.PeerId]int{}

	for range nodeStatusPruneEvery {
		handleNodeStatusReport(context.Background(), testReport("peer-a"), reg, nil, counts)
	}

	if _, ok := reg.Get("peer-a"); !ok {
		t.Fatal("registry missing peer with nil writer")
	}
	if got := counts["peer-a"]; got != 0 {
		t.Errorf("counts[peer-a] = %d, want 0 (counter untouched when writer nil)", got)
	}
}

// Given a full report, when converted to a history row, then all mapped
// columns carry the report values.
func TestNodeStatusHistoryRowFromReport_Mapping(t *testing.T) {
	row := nodeStatusHistoryRowFromReport(testReport("peer-a"))

	if row.PeerID != "peer-a" {
		t.Errorf("PeerID = %q, want %q", row.PeerID, "peer-a")
	}
	if row.NodeID == nil || *row.NodeID != "12D3KooWNode" {
		t.Errorf("NodeID = %v, want 12D3KooWNode", row.NodeID)
	}
	if !row.Healthy {
		t.Error("Healthy = false, want true")
	}
	if row.PrefixUsed == nil || *row.PrefixUsed != 400 {
		t.Errorf("PrefixUsed = %v, want 400", row.PrefixUsed)
	}
	if row.PrefixTotal == nil || *row.PrefixTotal != 1000 {
		t.Errorf("PrefixTotal = %v, want 1000", row.PrefixTotal)
	}
	if row.WarmUsed == nil || *row.WarmUsed != 1200 {
		t.Errorf("WarmUsed = %v, want 1200", row.WarmUsed)
	}
	if row.WarmTotal == nil || *row.WarmTotal != 5000 {
		t.Errorf("WarmTotal = %v, want 5000", row.WarmTotal)
	}
	if row.ConnCount == nil || *row.ConnCount != 12 {
		t.Errorf("ConnCount = %v, want 12", row.ConnCount)
	}
	if row.Region == nil || *row.Region != "cn-east" {
		t.Errorf("Region = %v, want cn-east", row.Region)
	}
	if row.Version == nil || *row.Version != "v0.9.1" {
		t.Errorf("Version = %v, want v0.9.1", row.Version)
	}
	if want := time.Unix(1_750_000_000, 0); !row.ReportedAt.Equal(want) {
		t.Errorf("ReportedAt = %v, want %v", row.ReportedAt, want)
	}

	// Old-build report: empty NodeID/Region/Version map to NULL columns.
	bare := types.NodeStatusReport{PeerID: "peer-old", LastUpdate: 1}
	bareRow := nodeStatusHistoryRowFromReport(bare)
	if bareRow.NodeID != nil || bareRow.Region != nil || bareRow.Version != nil {
		t.Errorf("empty fields must map to nil, got NodeID=%v Region=%v Version=%v",
			bareRow.NodeID, bareRow.Region, bareRow.Version)
	}
}
