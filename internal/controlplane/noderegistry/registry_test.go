package noderegistry_test

import (
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/types"
)

func fullReport(peerID types.PeerId) types.NodeStatusReport {
	return types.NodeStatusReport{
		NodeID: "12D3KooWNode",
		PeerID: peerID,
		Capabilities: types.NodeCapabilities{
			Edge:       true,
			PeerICP:    true,
			L4Backhaul: true,
		},
		PrefixSpace:       types.PartitionStatus{TotalBytes: 1000, UsedBytes: 400, BlobCount: 7},
		WarmSpace:         types.PartitionStatus{TotalBytes: 5000, UsedBytes: 1200, BlobCount: 30},
		ColdSpace:         &types.PartitionStatus{TotalBytes: 9000, UsedBytes: 100, BlobCount: 3},
		Healthy:           true,
		LastUpdate:        1_750_000_000,
		Region:            "cn-east",
		Version:           "v0.9.1",
		StartedAt:         1_749_000_000,
		ConnCount:         12,
		JWTRefreshFail24h: 2,
	}
}

// TestUpsertReport_SnapshotCarriesAllFields asserts a report round-trips
// into the snapshot with every field preserved and ReceivedAt stamped.
func TestUpsertReport_SnapshotCarriesAllFields(t *testing.T) {
	reg := noderegistry.NewRegistry()
	report := fullReport("peer-a")

	before := time.Now()
	reg.UpsertReport(report)
	after := time.Now()

	snap := reg.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("Snapshot len = %d, want 1", len(snap))
	}
	v := snap[0]

	if v.PeerID != report.PeerID {
		t.Errorf("PeerID = %q, want %q", v.PeerID, report.PeerID)
	}
	if v.NodeID != report.NodeID {
		t.Errorf("NodeID = %q, want %q", v.NodeID, report.NodeID)
	}
	if v.Capabilities != report.Capabilities {
		t.Errorf("Capabilities = %+v, want %+v", v.Capabilities, report.Capabilities)
	}
	if v.PrefixSpace != report.PrefixSpace {
		t.Errorf("PrefixSpace = %+v, want %+v", v.PrefixSpace, report.PrefixSpace)
	}
	if v.WarmSpace != report.WarmSpace {
		t.Errorf("WarmSpace = %+v, want %+v", v.WarmSpace, report.WarmSpace)
	}
	if v.ColdSpace == nil || *v.ColdSpace != *report.ColdSpace {
		t.Errorf("ColdSpace = %+v, want %+v", v.ColdSpace, report.ColdSpace)
	}
	if v.Healthy != report.Healthy {
		t.Errorf("Healthy = %v, want %v", v.Healthy, report.Healthy)
	}
	if v.LastUpdate != report.LastUpdate {
		t.Errorf("LastUpdate = %d, want %d", v.LastUpdate, report.LastUpdate)
	}
	if v.Region != report.Region {
		t.Errorf("Region = %q, want %q", v.Region, report.Region)
	}
	if v.Version != report.Version {
		t.Errorf("Version = %q, want %q", v.Version, report.Version)
	}
	if v.StartedAt != report.StartedAt {
		t.Errorf("StartedAt = %d, want %d", v.StartedAt, report.StartedAt)
	}
	if v.ConnCount != report.ConnCount {
		t.Errorf("ConnCount = %d, want %d", v.ConnCount, report.ConnCount)
	}
	if v.JWTRefreshFail24h != report.JWTRefreshFail24h {
		t.Errorf("JWTRefreshFail24h = %d, want %d", v.JWTRefreshFail24h, report.JWTRefreshFail24h)
	}
	if v.ReceivedAt.Before(before) || v.ReceivedAt.After(after) {
		t.Errorf("ReceivedAt = %v, want within [%v, %v]", v.ReceivedAt, before, after)
	}
}

// TestUpsertReport_LatestWins asserts a second report replaces the first.
func TestUpsertReport_LatestWins(t *testing.T) {
	reg := noderegistry.NewRegistry()
	peer := types.PeerId("peer-a")

	r1 := fullReport(peer)
	r1.Healthy = true
	reg.UpsertReport(r1)

	r2 := fullReport(peer)
	r2.Healthy = false
	r2.ConnCount = 99
	reg.UpsertReport(r2)

	v, ok := reg.Get(peer)
	if !ok {
		t.Fatal("Get returned ok=false after upsert")
	}
	if v.Healthy {
		t.Error("Healthy = true, want false (latest report wins)")
	}
	if v.ConnCount != 99 {
		t.Errorf("ConnCount = %d, want 99", v.ConnCount)
	}
	if got := len(reg.Snapshot()); got != 1 {
		t.Errorf("Snapshot len = %d, want 1 (same peer replaced, not appended)", got)
	}
}

// TestGet_UnknownPeer asserts Get reports ok=false for unseen peers.
func TestGet_UnknownPeer(t *testing.T) {
	reg := noderegistry.NewRegistry()
	if _, ok := reg.Get("ghost"); ok {
		t.Error("Get(ghost) ok = true, want false")
	}
}

// TestRecordIssuance_Roundtrip asserts issuance records are stored and
// retrievable with exp and l4 intact.
func TestRecordIssuance_Roundtrip(t *testing.T) {
	reg := noderegistry.NewRegistry()
	reg.RecordIssuance("peer-a", 1_750_003_600, true)

	exp, l4, ok := reg.Issuance("peer-a")
	if !ok {
		t.Fatal("Issuance ok = false after RecordIssuance")
	}
	if exp != 1_750_003_600 {
		t.Errorf("exp = %d, want 1750003600", exp)
	}
	if !l4 {
		t.Error("l4 = false, want true")
	}

	if _, _, ok := reg.Issuance("peer-b"); ok {
		t.Error("Issuance(peer-b) ok = true, want false (never issued)")
	}
}

// TestShouldHaveRenewed covers the boundary matrix:
//   - no issuance record            → false
//   - now before exp-300s           → false
//   - past boundary, no new report  → true
//   - past boundary, fresh report   → false
//   - report newer than boundary    → false even well past expiry
func TestShouldHaveRenewed(t *testing.T) {
	const peer = types.PeerId("peer-a")
	exp := time.Now().Unix() + 600 // expires in 10min; renew-by = exp-300 = now+300
	renewBy := time.Unix(exp-noderegistry.RenewWindowSeconds, 0)

	t.Run("no issuance record -> false", func(t *testing.T) {
		reg := noderegistry.NewRegistry()
		if reg.ShouldHaveRenewed(peer, renewBy.Add(time.Hour)) {
			t.Error("ShouldHaveRenewed = true without issuance record, want false")
		}
	})

	t.Run("before renew window -> false", func(t *testing.T) {
		reg := noderegistry.NewRegistry()
		reg.RecordIssuance(peer, exp, false)
		if reg.ShouldHaveRenewed(peer, renewBy.Add(-time.Second)) {
			t.Error("ShouldHaveRenewed = true before exp-300s, want false")
		}
	})

	t.Run("at boundary exactly -> false", func(t *testing.T) {
		reg := noderegistry.NewRegistry()
		reg.RecordIssuance(peer, exp, false)
		if reg.ShouldHaveRenewed(peer, renewBy) {
			t.Error("ShouldHaveRenewed = true at exactly exp-300s, want false (strict >)")
		}
	})

	t.Run("past boundary with no report -> true", func(t *testing.T) {
		reg := noderegistry.NewRegistry()
		reg.RecordIssuance(peer, exp, false)
		if !reg.ShouldHaveRenewed(peer, renewBy.Add(time.Second)) {
			t.Error("ShouldHaveRenewed = false past boundary with no report, want true")
		}
	})

	t.Run("past boundary with stale report -> true", func(t *testing.T) {
		reg := noderegistry.NewRegistry()
		reg.RecordIssuance(peer, exp, false)
		reg.UpsertReport(fullReport(peer)) // ReceivedAt = now, which is BEFORE renewBy (now+300)
		if !reg.ShouldHaveRenewed(peer, renewBy.Add(time.Second)) {
			t.Error("ShouldHaveRenewed = false with stale report, want true")
		}
	})

	t.Run("past boundary with fresh report -> false", func(t *testing.T) {
		reg := noderegistry.NewRegistry()
		alreadyDueExp := time.Now().Unix() - 60
		reg.RecordIssuance(peer, alreadyDueExp, false)
		reg.UpsertReport(fullReport(peer))
		if reg.ShouldHaveRenewed(peer, time.Now()) {
			t.Error("ShouldHaveRenewed = true with fresh report, want false")
		}
	})
}

// TestSnapshot_IsolatesInternalState asserts mutating the returned slice
// does not affect the registry.
func TestSnapshot_IsolatesInternalState(t *testing.T) {
	reg := noderegistry.NewRegistry()
	reg.UpsertReport(fullReport("peer-a"))

	snap := reg.Snapshot()
	snap[0].Healthy = false
	snap[0].ConnCount = -1

	v, _ := reg.Get("peer-a")
	if !v.Healthy || v.ConnCount != 12 {
		t.Errorf("registry state mutated via snapshot: %+v", v)
	}
}
