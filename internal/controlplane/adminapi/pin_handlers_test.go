package adminapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mock PinContentMetaReader ────────────────────────────────────────────

type mockPinContentMeta struct {
	meta     *types.ContentMeta
	metaErr  error
	blobs    []types.BlobDescriptor
	roles    []types.BlobRole
	blobsErr error
}

func (m *mockPinContentMeta) GetContentMeta(_ context.Context, contentID string) (*types.ContentMeta, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return m.meta, nil
}

func (m *mockPinContentMeta) GetContentBlobs(_ context.Context, contentID string) ([]types.BlobDescriptor, []types.BlobRole, error) {
	if m.blobsErr != nil {
		return nil, nil, m.blobsErr
	}
	return m.blobs, m.roles, nil
}

// ─── Mock PinOrchestrator ────────────────────────────────────────────────

type mockPinOrchestrator struct {
	seqs         []uint64
	err          error
	lastContent  string
	lastTargets  []string
	lastPins     []string
	lastUnpins   []string
	callCount    int
}

func (m *mockPinOrchestrator) SendManualPlan(contentID string, targets []string, pinBlobs, unpinBlobs []string) ([]uint64, error) {
	m.callCount++
	m.lastContent = contentID
	m.lastTargets = append([]string(nil), targets...)
	m.lastPins = append([]string(nil), pinBlobs...)
	m.lastUnpins = append([]string(nil), unpinBlobs...)
	return m.seqs, m.err
}

// ─── Helpers ─────────────────────────────────────────────────────────────

func makePinServer(mc PinContentMetaReader, reg *noderegistry.Registry, po PinOrchestrator) *Server {
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterPinRoutes(srv, mc, reg, po, nil)
	return srv
}

func pinJSONBody(req manualPinRequest) *bytes.Buffer {
	b, _ := json.Marshal(req)
	return bytes.NewBuffer(b)
}

func postWithToken(t *testing.T, srv *Server, path string, body *bytes.Buffer) *http.Response {
	t.Helper()
	token := signAdminToken(t, []byte("test-secret-key-for-admin-tokens"))
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodPost, ts.URL+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func decodeManualPinResponse(t *testing.T, resp *http.Response) manualPinResponse {
	t.Helper()
	defer resp.Body.Close()
	var r manualPinResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return r
}

// ─── Tests: POST /v1/admin/pin ──────────────────────────────────────────

func TestManualPin_HappyPath_ReturnsSeqs(t *testing.T) {
	// Given: content exists with 2 blobs (sizes 500+300=800), two target nodes
	// with plenty of space.
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "content-1", ContentType: "dash_video"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "abc-def-1", BlobType: "mp4_init_segment", Size: 500},
			{BlobHash: "abc-def-2", BlobType: "m4s_media_segment", Size: 300},
		},
	}

	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 10000, UsedBytes: 100},
	})
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-B",
		NodeID:      "node-B",
		PrefixSpace: types.PartitionStatus{TotalBytes: 10000, UsedBytes: 100},
	})

	po := &mockPinOrchestrator{seqs: []uint64{1, 2}}

	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "content-1",
		TargetNodes: []string{"node-A", "node-B"},
	}))

	// When/Then: 202 with seqs for both nodes.
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	r := decodeManualPinResponse(t, resp)
	if len(r.Seq) != 2 {
		t.Errorf("len(seq) = %d, want 2", len(r.Seq))
	}
	if r.Seq[0] != 1 || r.Seq[1] != 2 {
		t.Errorf("seq = %v, want [1, 2]", r.Seq)
	}
	if len(r.Skipped) != 0 {
		t.Errorf("len(skipped) = %d, want 0", len(r.Skipped))
	}

	// Then: orchestrator received correct call with both blob hashes.
	if po.lastContent != "content-1" {
		t.Errorf("orchestrator content = %q, want content-1", po.lastContent)
	}
	if len(po.lastTargets) != 2 {
		t.Errorf("orchestrator targets = %v, want [node-A, node-B]", po.lastTargets)
	}
	if !blobHashesEqual(po.lastPins, []string{"abc-def-1", "abc-def-2"}) {
		t.Errorf("orchestrator pins = %v, want [abc-def-1, abc-def-2]", po.lastPins)
	}
	if len(po.lastUnpins) != 0 {
		t.Errorf("orchestrator unpins = %v, want []", po.lastUnpins)
	}
}

func TestManualPin_SpaceInsufficientNodeMovedToSkipped(t *testing.T) {
	// Given: content with 5000 bytes, node-A has only 100 remaining, node-B has plenty.
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "big-hash", Size: 5000},
		},
	}

	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 500, UsedBytes: 400}, // remaining: 100
	})
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-B",
		NodeID:      "node-B",
		PrefixSpace: types.PartitionStatus{TotalBytes: 20000, UsedBytes: 100}, // remaining: 19900
	})

	po := &mockPinOrchestrator{seqs: []uint64{3}}
	srv := makePinServer(mc, reg, po)

	// When: pin request targets both nodes.
	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A", "node-B"},
	}))

	// Then: 202, node-A in skipped, node-B dispatched.
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	r := decodeManualPinResponse(t, resp)
	if len(r.Seq) != 1 || r.Seq[0] != 3 {
		t.Errorf("seq = %v, want [3]", r.Seq)
	}
	if len(r.Skipped) != 1 {
		t.Fatalf("len(skipped) = %d, want 1", len(r.Skipped))
	}
	if r.Skipped[0].PeerID != "node-A" || r.Skipped[0].Reason != "insufficient_space" {
		t.Errorf("skipped[0] = {%q, %q}, want {node-A, insufficient_space}", r.Skipped[0].PeerID, r.Skipped[0].Reason)
	}
	// Then: orchestrator only dispatched node-B.
	if len(po.lastTargets) != 1 || po.lastTargets[0] != "node-B" {
		t.Errorf("orchestrator targets = %v, want [node-B]", po.lastTargets)
	}
}

func TestManualPin_AllTargetsInsufficient_422(t *testing.T) {
	// Given: content with 5000 bytes, both nodes have insufficient space.
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "big-hash", Size: 5000},
		},
	}

	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 1000, UsedBytes: 1000},
	})
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-B",
		NodeID:      "node-B",
		PrefixSpace: types.PartitionStatus{TotalBytes: 3000, UsedBytes: 3000},
	})

	po := &mockPinOrchestrator{}
	srv := makePinServer(mc, reg, po)

	// When: pin request with both nodes.
	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A", "node-B"},
	}))

	// Then: 422, both nodes skipped, orchestrator never called.
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	r := decodeManualPinResponse(t, resp)
	if len(r.Seq) != 0 {
		t.Errorf("len(seq) = %d, want 0", len(r.Seq))
	}
	if len(r.Skipped) != 2 {
		t.Errorf("len(skipped) = %d, want 2", len(r.Skipped))
	}
	if po.callCount != 0 {
		t.Errorf("orchestrator called %d times, want 0", po.callCount)
	}
}

func TestManualPin_ContentMissing_404(t *testing.T) {
	// Given: metadata returns sql.ErrNoRows.
	mc := &mockPinContentMeta{metaErr: sql.ErrNoRows}
	reg := noderegistry.NewRegistry()
	po := &mockPinOrchestrator{}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "missing-content",
		TargetNodes: []string{"node-A"},
	}))

	// Then: 404.
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestManualPin_EmptyTargets_400(t *testing.T) {
	mc := &mockPinContentMeta{meta: &types.ContentMeta{ContentID: "c1"}}
	reg := noderegistry.NewRegistry()
	po := &mockPinOrchestrator{}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{},
	}))

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestManualPin_EmptyContentID_400(t *testing.T) {
	mc := &mockPinContentMeta{}
	reg := noderegistry.NewRegistry()
	po := &mockPinOrchestrator{}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "",
		TargetNodes: []string{"node-A"},
	}))

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestManualPin_NoToken_401(t *testing.T) {
	mc := &mockPinContentMeta{meta: &types.ContentMeta{ContentID: "c1"}}
	reg := noderegistry.NewRegistry()
	po := &mockPinOrchestrator{}
	srv := makePinServer(mc, reg, po)

	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A"},
	}))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestManualPin_BlobFilterRespectsRequest(t *testing.T) {
	// Given: content has 3 blobs, request specifies only 2 of them.
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "hash-a", Size: 100},
			{BlobHash: "hash-b", Size: 200},
			{BlobHash: "hash-c", Size: 300},
		},
	}
	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 10000, UsedBytes: 0},
	})
	po := &mockPinOrchestrator{seqs: []uint64{7}}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A"},
		Blobs:       []string{"hash-a", "hash-c"},
	}))

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	// Then: only those two blobs were dispatched.
	if !blobHashesEqual(po.lastPins, []string{"hash-a", "hash-c"}) {
		t.Errorf("orchestrator pins = %v, want [hash-a, hash-c]", po.lastPins)
	}
}

func TestManualPin_UnknownSizeTreatedAsZero(t *testing.T) {
	// Given: blob with Size=0 (unknown) — space check should not fail.
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "tiny", Size: 0},
		},
	}
	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 0, UsedBytes: 0}, // remaining: 0
	})
	po := &mockPinOrchestrator{seqs: []uint64{8}}
	srv := makePinServer(mc, reg, po)

	// When: total bytes = 0, remaining = 0 → 0 >= 0 → passes space check.
	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A"},
	}))

	// Then: node dispatched (0 bytes doesn't trigger insufficient).
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	r := decodeManualPinResponse(t, resp)
	if len(r.Seq) != 1 || len(r.Skipped) != 0 {
		t.Errorf("seq=%v skipped=%v, want seqs present with no skipped", r.Seq, r.Skipped)
	}
}

// ─── Tests: POST /v1/admin/unpin ────────────────────────────────────────

func TestManualUnpin_HappyPath_NoSpaceCheck(t *testing.T) {
	// Given: node-A has zero remaining space but unpin should succeed anyway.
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "hash-x", Size: 5000},
		},
	}
	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100, UsedBytes: 100}, // remaining: 0
	})
	po := &mockPinOrchestrator{seqs: []uint64{10}}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/unpin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A"},
	}))

	// Then: 202, no space check applied.
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	r := decodeManualPinResponse(t, resp)
	if len(r.Seq) != 1 || r.Seq[0] != 10 {
		t.Errorf("seq = %v, want [10]", r.Seq)
	}
	if len(po.lastUnpins) != 1 || po.lastUnpins[0] != "hash-x" {
		t.Errorf("unpins = %v, want [hash-x]", po.lastUnpins)
	}
	if len(po.lastPins) != 0 {
		t.Errorf("pins = %v, want []", po.lastPins)
	}
}

func TestManualUnpin_SkipsSpaceFilter(t *testing.T) {
	// Given: same setup as TestManualPin_AllTargetsInsufficient_422 but using unpin.
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "big-hash", Size: 5000},
		},
	}
	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 1000, UsedBytes: 1000},
	})
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-B",
		NodeID:      "node-B",
		PrefixSpace: types.PartitionStatus{TotalBytes: 3000, UsedBytes: 3000},
	})
	po := &mockPinOrchestrator{seqs: []uint64{11, 12}}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/unpin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A", "node-B"},
	}))

	// Then: 202, both nodes dispatched (pin would have failed with 422).
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	r := decodeManualPinResponse(t, resp)
	if len(r.Seq) != 2 {
		t.Errorf("len(seq) = %d, want 2", len(r.Seq))
	}
	if len(po.lastTargets) != 2 {
		t.Errorf("targets = %v, want [node-A, node-B]", po.lastTargets)
	}
}

func TestManualUnpin_NodeNotFound_422(t *testing.T) {
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{{BlobHash: "h1", Size: 100}},
	}
	reg := noderegistry.NewRegistry()
	// Don't register any nodes.
	po := &mockPinOrchestrator{}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/unpin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"ghost-node"},
	}))

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	r := decodeManualPinResponse(t, resp)
	if len(r.Skipped) == 0 || r.Skipped[0].PeerID != "ghost-node" {
		t.Errorf("skipped = %v, want [{ghost-node, node_not_found}]", r.Skipped)
	}
}

func TestManualUnpin_ExplicitBlobs(t *testing.T) {
	// Given: explicit blob list on unpin ignores GetContentBlobs.
	mc := &mockPinContentMeta{
		meta:  &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{}, // won't be called
	}
	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 10000, UsedBytes: 0},
	})
	po := &mockPinOrchestrator{seqs: []uint64{20}}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/unpin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A"},
		Blobs:       []string{"hash-explicit-1", "hash-explicit-2"},
	}))

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if !blobHashesEqual(po.lastUnpins, []string{"hash-explicit-1", "hash-explicit-2"}) {
		t.Errorf("unpins = %v, want [hash-explicit-1, hash-explicit-2]", po.lastUnpins)
	}
}

func TestManualUnpin_NoToken_401(t *testing.T) {
	mc := &mockPinContentMeta{meta: &types.ContentMeta{ContentID: "c1"}}
	reg := noderegistry.NewRegistry()
	po := &mockPinOrchestrator{}
	srv := makePinServer(mc, reg, po)

	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/admin/unpin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A"},
	}))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────

func blobHashesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	// Order-insensitive comparison since blob order may not be deterministic.
	seen := make(map[string]int, len(a))
	for _, h := range a {
		seen[h]++
	}
	for _, h := range b {
		seen[h]--
		if seen[h] < 0 {
			return false
		}
	}
	return true
}

// ─── Compile-time interface satisfaction checks ──────────────────────────

// Verify that *noderegistry.Registry satisfies the buildNodeIDMap input.
var _ interface{ Snapshot() []noderegistry.NodeView } = (*noderegistry.Registry)(nil)

// ─── Response shape validation ───────────────────────────────────────────

func TestManualPin_ResponseShape(t *testing.T) {
	mc := &mockPinContentMeta{
		meta: &types.ContentMeta{ContentID: "c1"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "hash-1", Size: 100},
		},
	}
	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID: "peer-A", NodeID: "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 1000, UsedBytes: 0},
	})
	po := &mockPinOrchestrator{seqs: []uint64{42}}
	srv := makePinServer(mc, reg, po)

	resp := postWithToken(t, srv, "/v1/admin/pin", pinJSONBody(manualPinRequest{
		ContentID:   "c1",
		TargetNodes: []string{"node-A"},
	}))

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	r := decodeManualPinResponse(t, resp)
	if len(r.Seq) != 1 || r.Seq[0] != 42 {
		t.Errorf("seq = %v, want [42]", r.Seq)
	}
	if len(r.Skipped) != 0 {
		t.Errorf("len(skipped) = %d, want 0", len(r.Skipped))
	}
}
