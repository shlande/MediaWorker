package pinstrategy

import (
	"testing"

	"github.com/shlande/mediaworker/internal/node/pinstore"
	"github.com/shlande/mediaworker/internal/types"
)

func newTestStore(t *testing.T) *pinstore.PinStore {
	t.Helper()
	ps, err := pinstore.NewPinStore(t.TempDir(), t.TempDir(), 1<<30, func(string) ([]byte, error) {
		return []byte("blob-bytes"), nil
	})
	if err != nil {
		t.Fatalf("NewPinStore: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close() })
	return ps
}

// Given a plan whose update carries PinBlobMetas (new CP), When handled, Then
// the metas drive ApplyPin — including their sizes — and the plain PinBlobs
// list is not consulted.
func TestHandlePinPlan_UsesMetasWhenPresent(t *testing.T) {
	ps := newTestStore(t)

	plan := types.PinPlan{
		Seq: 1, TargetNode: "node_a",
		Updates: []types.PinUpdate{{
			ContentID: "cont_1",
			PinBlobs:  []string{"h_stale_ignored"},
			PinBlobMetas: []types.PinBlobMeta{
				{BlobHash: "h_init", BlobType: "mp4_init_segment", Role: "init", Size: 100},
				{BlobHash: "h_seg1", BlobType: "m4s_media_segment", Role: "media", Size: 200},
			},
		}},
	}
	HandlePinPlan(plan, ps, nil, nil)

	for _, hash := range []string{"h_init", "h_seg1"} {
		if !ps.IsPinned(hash) {
			t.Errorf("blob %s not pinned via metas path", hash)
		}
	}
	if ps.IsPinned("h_stale_ignored") {
		t.Error("plain PinBlobs entry must not be applied when metas are present")
	}
	if got := ps.QuerySpace().TotalPinnedSize; got != 300 {
		t.Errorf("TotalPinnedSize = %d, want 300 (meta sizes 100+200)", got)
	}
}

// Given an old-payload plan (no content_id, no metas), When handled, Then the
// legacy findBlob* lookup drives ApplyPin exactly as before.
func TestHandlePinPlan_LegacyPayloadFallsBack(t *testing.T) {
	ps := newTestStore(t)

	plan := types.PinPlan{
		Seq: 2, TargetNode: "node_a",
		Updates: []types.PinUpdate{{
			PinBlobs:   []string{"h_init"},
			UnpinBlobs: []string{"h_seg1"},
		}},
	}
	blobs := []types.BlobDescriptor{
		{BlobHash: "h_init", BlobType: "mp4_init_segment", Size: 100},
		{BlobHash: "h_seg1", BlobType: "m4s_media_segment", Size: 200},
	}
	roles := []types.BlobRole{{BlobHash: "h_init", Role: "init"}}

	// Pre-pin h_seg1 so the unpin has an effect to observe.
	ps.ApplyPin("h_seg1", "m4s_media_segment", "media", 200)
	if !ps.IsPinned("h_seg1") {
		t.Fatal("setup: h_seg1 should be pinned")
	}

	HandlePinPlan(plan, ps, blobs, roles)

	if !ps.IsPinned("h_init") {
		t.Error("h_init not pinned via legacy path")
	}
	if ps.IsPinned("h_seg1") {
		t.Error("h_seg1 should have been unpinned")
	}
	if got := ps.QuerySpace().TotalPinnedSize; got != 100 {
		t.Errorf("TotalPinnedSize = %d, want 100 (legacy size lookup)", got)
	}
}

// Given a legacy payload referencing a blob absent from the local metadata,
// When handled, Then conservative zero defaults are used (no panic) and the
// pin is still recorded.
func TestHandlePinPlan_LegacyPayloadUnknownBlob_Defaults(t *testing.T) {
	ps := newTestStore(t)

	plan := types.PinPlan{
		Seq: 3, TargetNode: "node_a",
		Updates: []types.PinUpdate{{PinBlobs: []string{"h_unknown"}}},
	}
	HandlePinPlan(plan, ps, nil, nil)

	if !ps.IsPinned("h_unknown") {
		t.Error("unknown blob should still be pinned with zero defaults")
	}
	if got := ps.QuerySpace().TotalPinnedSize; got != 0 {
		t.Errorf("TotalPinnedSize = %d, want 0 (default size for unknown blob)", got)
	}
}
