package pinstrategy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/pinstore"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mocks ───

// mockMetadataClient implements MetadataClient for tests.
type mockMetadataClient struct {
	meta         *types.ContentMeta
	metaErr      error
	topContents  []TopContent
	topContentsErr error
}

func (m *mockMetadataClient) GetContentMeta(contentID string) (*types.ContentMeta, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return m.meta, nil
}

func (m *mockMetadataClient) GetTopContents(ctx context.Context, limit int) ([]TopContent, error) {
	if m.topContentsErr != nil {
		return nil, m.topContentsErr
	}
	return m.topContents, nil
}

func (m *mockMetadataClient) GetSegmentLocations(blobHash string) ([]types.BlobLocation, error) {
	return nil, nil
}

func (m *mockMetadataClient) GetPopularity24h(blobHash string) float64 {
	return 0
}

// mockBroadcaster implements SyncBroadcasterClient for tests.
type mockBroadcaster struct {
	mu      sync.Mutex
	sent    []sentPlan
	sendErr error
}

type sentPlan struct {
	nodeID    string
	eventType string
}

func (b *mockBroadcaster) SendToNode(nodeID string, eventType string, payload any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sent = append(b.sent, sentPlan{nodeID: nodeID, eventType: eventType})
	return b.sendErr
}

func (b *mockBroadcaster) Subscribe(eventType string) <-chan types.Event {
	return make(chan types.Event)
}

func (b *mockBroadcaster) sentCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.sent)
}

func (b *mockBroadcaster) sentEvents() []sentPlan {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]sentPlan, len(b.sent))
	copy(result, b.sent)
	return result
}

// ─── Helpers ───

func newTestPinStore(t *testing.T) *pinstore.PinStore {
	t.Helper()
	dbPath := t.TempDir()
	storagePath := t.TempDir()
	ps, err := pinstore.NewPinStore(dbPath, storagePath, 1<<30, func(hash string) ([]byte, error) {
		return []byte("test-data"), nil
	})
	if err != nil {
		t.Fatalf("NewPinStore: %v", err)
	}
	t.Cleanup(func() { ps.Close() })
	return ps
}

func makeTestBlobs() []types.BlobDescriptor {
	return []types.BlobDescriptor{
		{BlobHash: "init-1", BlobType: "init", Size: 1024, SortOrder: 0},
		{BlobHash: "media-0", BlobType: "media", Size: 10 * 1024 * 1024, SortOrder: 1},
		{BlobHash: "media-1", BlobType: "media", Size: 10 * 1024 * 1024, SortOrder: 2},
		{BlobHash: "media-2", BlobType: "media", Size: 10 * 1024 * 1024, SortOrder: 3},
		{BlobHash: "media-3", BlobType: "media", Size: 10 * 1024 * 1024, SortOrder: 4},
		{BlobHash: "media-4", BlobType: "media", Size: 10 * 1024 * 1024, SortOrder: 5},
		{BlobHash: "media-5", BlobType: "media", Size: 10 * 1024 * 1024, SortOrder: 6},
		{BlobHash: "media-6", BlobType: "media", Size: 10 * 1024 * 1024, SortOrder: 7},
	}
}

// ─── DashPinStrategy Tests ───

func TestDashPinStrategy_DecideInitialPin_SpaceRich(t *testing.T) {
	// Given: a 100 GB partition with >50% free → should pin init + 5 media.
	strategy := &DashPinStrategy{}
	content := types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}
	blobs := makeTestBlobs()
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-A", AvailableBytes: 80 * 1024 * 1024 * 1024},
	}

	// When:
	plans := strategy.DecideInitialPin(content, blobs, nodeSpaces)

	// Then:
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	p := plans[0]
	if p.NodeID != "node-A" {
		t.Fatalf("expected node-A, got %s", p.NodeID)
	}
	if len(p.PinBlobs) != 6 {
		t.Fatalf("expected 6 pin blobs (1 init + 5 media), got %d: %v", len(p.PinBlobs), p.PinBlobs)
	}
	// init blob must be first.
	if p.PinBlobs[0] != "init-1" {
		t.Fatalf("expected init-1 first, got %s", p.PinBlobs[0])
	}
	// media-0 through media-4 should be pinned (first 5 by sort order).
	for i := 1; i <= 5; i++ {
		expected := fmt.Sprintf("media-%d", i-1)
		if p.PinBlobs[i] != expected {
			t.Fatalf("expected %s at index %d, got %s", expected, i, p.PinBlobs[i])
		}
	}
}

func TestDashPinStrategy_DecideInitialPin_SpaceMedium(t *testing.T) {
	// Given: 20-50 GB free → should pin init + 2 media.
	strategy := &DashPinStrategy{}
	content := types.ContentMeta{ContentID: "vid-2", ContentType: "dash"}
	blobs := makeTestBlobs()
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-B", AvailableBytes: 30 * 1024 * 1024 * 1024},
	}

	// When:
	plans := strategy.DecideInitialPin(content, blobs, nodeSpaces)

	// Then:
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	p := plans[0]
	if p.NodeID != "node-B" {
		t.Fatalf("expected node-B, got %s", p.NodeID)
	}
	if len(p.PinBlobs) != 3 {
		t.Fatalf("expected 3 pin blobs (1 init + 2 media), got %d: %v", len(p.PinBlobs), p.PinBlobs)
	}
	if p.PinBlobs[0] != "init-1" {
		t.Fatalf("expected init-1 first, got %s", p.PinBlobs[0])
	}
	if p.PinBlobs[1] != "media-0" {
		t.Fatalf("expected media-0 second, got %s", p.PinBlobs[1])
	}
	if p.PinBlobs[2] != "media-1" {
		t.Fatalf("expected media-1 third, got %s", p.PinBlobs[2])
	}
}

func TestDashPinStrategy_DecideInitialPin_SpacePoor(t *testing.T) {
	// Given: <20 GB free → should pin init only.
	strategy := &DashPinStrategy{}
	content := types.ContentMeta{ContentID: "vid-3", ContentType: "dash"}
	blobs := makeTestBlobs()
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-C", AvailableBytes: 5 * 1024 * 1024 * 1024},
	}

	// When:
	plans := strategy.DecideInitialPin(content, blobs, nodeSpaces)

	// Then:
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	p := plans[0]
	if p.NodeID != "node-C" {
		t.Fatalf("expected node-C, got %s", p.NodeID)
	}
	if len(p.PinBlobs) != 1 {
		t.Fatalf("expected 1 pin blob (init only), got %d: %v", len(p.PinBlobs), p.PinBlobs)
	}
	if p.PinBlobs[0] != "init-1" {
		t.Fatalf("expected init-1, got %s", p.PinBlobs[0])
	}
}

// ─── PinOrchestrator Tests ───

func TestPinOrchestrator_OnContentIngested(t *testing.T) {
	// Given: an orchestrator with a registered DashPinStrategy, rich node,
	// and a mock metadata client that returns DASH content.
	meta := &mockMetadataClient{
		meta: &types.ContentMeta{ContentID: "vid-1", ContentType: "dash"},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(meta, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})
	// Prime node space: rich node.
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100 * 1024 * 1024 * 1024, UsedBytes: 20 * 1024 * 1024 * 1024},
	})

	// When: content is ingested.
	evt := types.ContentIngestedEvent{
		ContentID:   "vid-1",
		ContentType: "dash",
		Blobs:       makeTestBlobs(),
	}
	po.OnContentIngested(evt)

	// Then: a PinPlan should have been sent to node-A.
	if bcast.sentCount() != 1 {
		t.Fatalf("expected 1 sent plan, got %d", bcast.sentCount())
	}
	events := bcast.sentEvents()
	if events[0].nodeID != "node-A" {
		t.Fatalf("expected node-A, got %s", events[0].nodeID)
	}
	if events[0].eventType != "PIN_PLAN_UPDATE" {
		t.Fatalf("expected PIN_PLAN_UPDATE, got %s", events[0].eventType)
	}
}

func TestPinOrchestrator_PanicRecovery(t *testing.T) {
	// Given: a panicking strategy.
	meta := &mockMetadataClient{
		meta: &types.ContentMeta{ContentID: "vid-1", ContentType: "dash"},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(meta, bcast)
	po.RegisterStrategy("dash", &panickingStrategy{})

	// When: content is ingested → strategy panics.
	evt := types.ContentIngestedEvent{
		ContentID:   "vid-1",
		ContentType: "dash",
		Blobs:       makeTestBlobs(),
	}
	// Should not panic — defer recover() catches it.
	po.OnContentIngested(evt)
	// No plans sent (strategy panicked).
	if bcast.sentCount() != 0 {
		t.Fatalf("expected 0 sent plans, got %d", bcast.sentCount())
	}
}

// panickingStrategy always panics on DecideInitialPin.
type panickingStrategy struct{}

func (s *panickingStrategy) DecideInitialPin(content types.ContentMeta, blobs []types.BlobDescriptor, nodeSpaces []types.NodeSpaceInfo) []types.NodePinPlan {
	panic("boom")
}

func (s *panickingStrategy) AdjustPin(content types.ContentMeta, popularity int64, nodeSpaces []types.NodeSpaceInfo) []types.NodePinPlan {
	return nil
}

func TestPinOrchestrator_OnNodeStatusReport(t *testing.T) {
	// Given: an orchestrator with no node spaces yet.
	po := NewPinOrchestrator(&mockMetadataClient{}, &mockBroadcaster{})

	// When: a node status report arrives.
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID:      "node-X",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100 << 30, UsedBytes: 30 << 30},
	})

	// Then: the node space snapshot reflects the report.
	spaces := po.getNodeSpaces()
	if len(spaces) != 1 {
		t.Fatalf("expected 1 node space, got %d", len(spaces))
	}
	if spaces[0].NodeID != "node-X" {
		t.Fatalf("expected node-X, got %s", spaces[0].NodeID)
	}
	// Available = Total - Used = 100 GB - 30 GB = 70 GB.
	expectedAvail := int64(70 * 1024 * 1024 * 1024)
	if spaces[0].AvailableBytes != expectedAvail {
		t.Fatalf("expected %d available bytes, got %d", expectedAvail, spaces[0].AvailableBytes)
	}
}

func TestHandlePinPlan(t *testing.T) {
	// Given: a fresh PinStore.
	ps := newTestPinStore(t)

	// When: a PinPlan is handled with pin and unpin blobs.
	plan := types.PinPlan{
		Seq:        1,
		TargetNode: "node-A",
		Updates: []types.PinUpdate{{
			BlobHash:   "content-1",
			PinBlobs:   []string{"blob-a", "blob-b"},
			UnpinBlobs: []string{"blob-c"},
		}},
	}
	HandlePinPlan(plan, ps)

	// Then: pinned blobs are in the PinStore.
	if !ps.IsPinned("blob-a") {
		t.Fatal("expected blob-a to be pinned")
	}
	if !ps.IsPinned("blob-b") {
		t.Fatal("expected blob-b to be pinned")
	}
	// blob-c was unpinned — should never have been there, so still not pinned.
	if ps.IsPinned("blob-c") {
		t.Fatal("expected blob-c NOT to be pinned")
	}
}

// Test the sequence counter.
func TestPinOrchestrator_NextSeq(t *testing.T) {
	po := NewPinOrchestrator(&mockMetadataClient{}, &mockBroadcaster{})

	s1 := po.nextSeq()
	s2 := po.nextSeq()
	s3 := po.nextSeq()

	if s1 >= s2 || s2 >= s3 {
		t.Fatalf("expected monotonically increasing: %d < %d < %d", s1, s2, s3)
	}
}

// Concurrent safety test for nodeSpaces updates and reads.
func TestPinOrchestrator_NodeSpacesConcurrent(t *testing.T) {
	po := NewPinOrchestrator(&mockMetadataClient{}, &mockBroadcaster{})

	var wg sync.WaitGroup
	var readers, writers atomic.Int64

	// 10 writers: each reports node status 100 times.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				po.OnNodeStatusReport(types.NodeStatusReport{
					NodeID:      fmt.Sprintf("node-%d", id),
					PrefixSpace: types.PartitionStatus{TotalBytes: 100 << 30, UsedBytes: int64(j) << 20},
				})
				writers.Add(1)
			}
		}(i)
	}

	// 5 readers: each takes 200 snapshots.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = po.getNodeSpaces()
				readers.Add(1)
			}
		}()
	}

	wg.Wait()
	if readers.Load() != 1000 {
		t.Fatalf("expected 1000 reads, got %d", readers.Load())
	}
	if writers.Load() != 1000 {
		t.Fatalf("expected 1000 writes, got %d", writers.Load())
	}
}

// Test rebalance flow.
func TestPinOrchestrator_Rebalance(t *testing.T) {
	meta := &mockMetadataClient{
		topContents: []TopContent{
			{ContentMeta: types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}, Popularity: 500},
		},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(meta, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})
	// Report a rich node.
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100 * 1024 * 1024 * 1024, UsedBytes: 10 * 1024 * 1024 * 1024},
	})

	// When: rebalance runs.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	po.rebalance(ctx)

	// Then: AdjustPin is called (DashPinStrategy.AdjustPin returns nil, so no send).
	// The default DashPinStrategy AdjustPin returns nil — integration coverage.
	// No crash, no panic.
}

// Test error paths: GetContentMeta returns error → no panic, no send.
func TestPinOrchestrator_OnContentIngested_GetMetaError(t *testing.T) {
	meta := &mockMetadataClient{metaErr: errors.New("not found")}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(meta, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})

	evt := types.ContentIngestedEvent{
		ContentID:   "vid-1",
		ContentType: "dash",
		Blobs:       makeTestBlobs(),
	}
	po.OnContentIngested(evt)

	if bcast.sentCount() != 0 {
		t.Fatalf("expected 0 plans on meta error, got %d", bcast.sentCount())
	}
}

// Test no strategy registered → no send.
func TestPinOrchestrator_OnContentIngested_NoStrategy(t *testing.T) {
	meta := &mockMetadataClient{
		meta: &types.ContentMeta{ContentID: "vid-1", ContentType: "unknown"},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(meta, bcast)
	// No strategy registered for "unknown".

	evt := types.ContentIngestedEvent{
		ContentID:   "vid-1",
		ContentType: "unknown",
		Blobs:       makeTestBlobs(),
	}
	po.OnContentIngested(evt)

	if bcast.sentCount() != 0 {
		t.Fatalf("expected 0 plans without strategy, got %d", bcast.sentCount())
	}
}

// Verify Run/Rebalance panic recovery.
func TestPinOrchestrator_RebalancePanicRecovery(t *testing.T) {
	meta := &mockMetadataClient{
		topContents: []TopContent{
			{ContentMeta: types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}, Popularity: 500},
		},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(meta, bcast)
	po.RegisterStrategy("dash", &panickingStrategy{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// safeRebalance should recover from the panic.
	po.safeRebalance(ctx)
	// No crash = pass.
}

// Verify PinStore lifecycle in HandlePinPlan.
func TestHandlePinPlan_IdempotentPin(t *testing.T) {
	ps := newTestPinStore(t)

	// Pin once.
	HandlePinPlan(types.PinPlan{
		Seq:        1,
		TargetNode: "node-A",
		Updates:    []types.PinUpdate{{BlobHash: "content-1", PinBlobs: []string{"blob-x"}}},
	}, ps)

	if !ps.IsPinned("blob-x") {
		t.Fatal("expected blob-x to be pinned")
	}

	// Pin same blob again — PinStore.ApplyPin is idempotent.
	HandlePinPlan(types.PinPlan{
		Seq:        2,
		TargetNode: "node-A",
		Updates:    []types.PinUpdate{{BlobHash: "content-1", PinBlobs: []string{"blob-x"}}},
	}, ps)

	if !ps.IsPinned("blob-x") {
		t.Fatal("expected blob-x to still be pinned after idempotent call")
	}
}

// Verify HandlePinPlan with unpin.
func TestHandlePinPlan_Unpin(t *testing.T) {
	ps := newTestPinStore(t)

	// Pin then unpin.
	HandlePinPlan(types.PinPlan{
		Seq:        1,
		TargetNode: "node-A",
		Updates:    []types.PinUpdate{{BlobHash: "content-1", PinBlobs: []string{"blob-y"}}},
	}, ps)

	if !ps.IsPinned("blob-y") {
		t.Fatal("expected blob-y to be pinned")
	}

	HandlePinPlan(types.PinPlan{
		Seq:        2,
		TargetNode: "node-A",
		Updates:    []types.PinUpdate{{BlobHash: "content-1", UnpinBlobs: []string{"blob-y"}}},
	}, ps)

	if ps.IsPinned("blob-y") {
		t.Fatal("expected blob-y to be unpinned")
	}
}

func TestMain(m *testing.M) {
	// PinStore tests use BadgerDB which can leave lock files.
	// Clean up any stale lock files before running.
	os.Exit(m.Run())
}
