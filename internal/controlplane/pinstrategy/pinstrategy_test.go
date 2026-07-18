package pinstrategy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/node/pinstore"
	nodepin "github.com/shlande/mediaworker/internal/node/pinstrategy"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mocks ───

// mockContentMetaClient implements metadata.ContentMetaClient for tests.
type mockContentMetaClient struct {
	meta            *types.ContentMeta
	metaErr         error
	contentBlobs    []types.BlobDescriptor
	contentRoles    []types.BlobRole
	contentBlobsErr error
}

func (m *mockContentMetaClient) GetContentMeta(ctx context.Context, contentID string) (*types.ContentMeta, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return m.meta, nil
}

func (m *mockContentMetaClient) GetContentBlobs(ctx context.Context, contentID string) ([]types.BlobDescriptor, []types.BlobRole, error) {
	if m.contentBlobsErr != nil {
		return nil, nil, m.contentBlobsErr
	}
	return m.contentBlobs, m.contentRoles, nil
}

func (m *mockContentMetaClient) GetTopContents(ctx context.Context, limit int) ([]metadata.TopContent, error) {
	return nil, nil
}

func (m *mockContentMetaClient) GetPopularity24h(ctx context.Context, contentID string) float64 {
	return 0
}

func (m *mockContentMetaClient) WriteContentMeta(ctx context.Context, tx *sql.Tx, content types.ContentMeta, blobs []types.BlobDescriptor, roles []types.BlobRole) error {
	return nil
}

// mockPopularityClient implements metadata.PopularityClient for tests.
type mockPopularityClient struct {
	topContents    []metadata.TopContent
	topContentsErr error
	popularity     map[string]float64

	// lastTopContentsLimit captures the limit arg passed to the most recent
	// GetTopContents call. Useful for asserting that Run/rebalance parameterize
	// the popularity query with the configured TopContentsLimit.
	lastTopContentsLimit int
	topContentsCalls     atomic.Int64
}

func (m *mockPopularityClient) GetTopContents(ctx context.Context, limit int) ([]metadata.TopContent, error) {
	m.lastTopContentsLimit = limit
	m.topContentsCalls.Add(1)
	if m.topContentsErr != nil {
		return nil, m.topContentsErr
	}
	return m.topContents, nil
}

func (m *mockPopularityClient) GetPopularity24h(ctx context.Context, contentID string) float64 {
	if m.popularity != nil {
		return m.popularity[contentID]
	}
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

// makeTestBlobs returns test blobs WITHOUT SortOrder (BlobDescriptor has no SortOrder field).
func makeTestBlobs() []types.BlobDescriptor {
	return []types.BlobDescriptor{
		{BlobHash: "init-1", BlobType: "mp4_init_segment", Size: 1024},
		{BlobHash: "media-0", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
		{BlobHash: "media-1", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
		{BlobHash: "media-2", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
		{BlobHash: "media-3", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
		{BlobHash: "media-4", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
		{BlobHash: "media-5", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
		{BlobHash: "media-6", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
	}
}

// makeTestRoles returns the BlobRole array for dash content, with SortOrder in roles.
func makeTestRoles() []types.BlobRole {
	return []types.BlobRole{
		{BlobHash: "init-1", Role: "init", SortOrder: 0},
		{BlobHash: "media-0", Role: "media", SortOrder: 1},
		{BlobHash: "media-1", Role: "media", SortOrder: 2},
		{BlobHash: "media-2", Role: "media", SortOrder: 3},
		{BlobHash: "media-3", Role: "media", SortOrder: 4},
		{BlobHash: "media-4", Role: "media", SortOrder: 5},
		{BlobHash: "media-5", Role: "media", SortOrder: 6},
		{BlobHash: "media-6", Role: "media", SortOrder: 7},
	}
}

// makeTestContentIngestedEvent creates a ContentIngestedEvent with test blobs and roles.
func makeTestContentIngestedEvent(contentID, contentType string) types.ContentIngestedEvent {
	return types.ContentIngestedEvent{
		ContentID:   contentID,
		ContentType: contentType,
		Blobs:       makeTestBlobs(),
		Roles:       makeTestRoles(),
		Timestamp:   time.Now().Unix(),
	}
}

// ─── DashPinStrategy Tests ───

func TestDashPinStrategy_DecideInitialPin_SpaceRich(t *testing.T) {
	// Given: a 100 GB partition with >50 GB free → should pin init + 5 media.
	strategy := &DashPinStrategy{}
	content := types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}
	blobs := makeTestBlobs()
	roles := makeTestRoles()
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-A", AvailableBytes: 80 * 1024 * 1024 * 1024},
	}

	// When:
	plans := strategy.DecideInitialPin(content, blobs, roles, nodeSpaces)

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
	roles := makeTestRoles()
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-B", AvailableBytes: 30 * 1024 * 1024 * 1024},
	}

	// When:
	plans := strategy.DecideInitialPin(content, blobs, roles, nodeSpaces)

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
	roles := makeTestRoles()
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-C", AvailableBytes: 5 * 1024 * 1024 * 1024},
	}

	// When:
	plans := strategy.DecideInitialPin(content, blobs, roles, nodeSpaces)

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

func TestDashPinStrategy_DecideInitialPin_FiltersByRole(t *testing.T) {
	// Given: blobs whose roles specify which are init and which are media.
	strategy := &DashPinStrategy{}
	content := types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}
	blobs := makeTestBlobs()
	roles := makeTestRoles()
	// Swap the roles of init-1 and media-0 to verify role-based filtering.
	roles[0].Role = "media" // init-1 is now media
	roles[1].Role = "init"  // media-0 is now init
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-A", AvailableBytes: 100 * 1024 * 1024 * 1024},
	}

	// When:
	plans := strategy.DecideInitialPin(content, blobs, roles, nodeSpaces)

	// Then: "media-0" should now be the init (first), "init-1" should be filtered as media.
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	p := plans[0]
	if p.PinBlobs[0] != "media-0" {
		t.Fatalf("expected media-0 as init (role-swapped), got %s", p.PinBlobs[0])
	}
	// "init-1" should appear among media blobs.
	foundInit1 := false
	for _, hash := range p.PinBlobs[1:] {
		if hash == "init-1" {
			foundInit1 = true
			break
		}
	}
	if !foundInit1 {
		t.Fatalf("expected init-1 to appear in media blobs (role-swapped), got pin list: %v", p.PinBlobs)
	}
}

func TestDashPinStrategy_DecideInitialPin_SortsByRoleSortOrder(t *testing.T) {
	// Given: media blobs in the BlobDescriptor list are not in SortOrder order.
	// The roles array carries the correct SortOrder. The strategy must sort by
	// roleOf().SortOrder, not the slice index.
	strategy := &DashPinStrategy{}
	content := types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}
	// Blobs in arbitrary order.
	blobs := []types.BlobDescriptor{
		{BlobHash: "init-1", BlobType: "mp4_init_segment", Size: 1024},
		{BlobHash: "media-5", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
		{BlobHash: "media-0", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
		{BlobHash: "media-2", BlobType: "m4s_media_segment", Size: 10 * 1024 * 1024},
	}
	roles := []types.BlobRole{
		{BlobHash: "init-1", Role: "init", SortOrder: 0},
		{BlobHash: "media-0", Role: "media", SortOrder: 1},
		{BlobHash: "media-2", Role: "media", SortOrder: 3},
		{BlobHash: "media-5", Role: "media", SortOrder: 6},
	}
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-A", AvailableBytes: 100 * 1024 * 1024 * 1024},
	}

	// When:
	plans := strategy.DecideInitialPin(content, blobs, roles, nodeSpaces)

	// Then: media blobs should be sorted by SortOrder → media-0, media-2, media-5.
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	p := plans[0]
	if p.PinBlobs[0] != "init-1" {
		t.Fatalf("expected init-1 first, got %s", p.PinBlobs[0])
	}
	if p.PinBlobs[1] != "media-0" {
		t.Fatalf("expected media-0 second, got %s", p.PinBlobs[1])
	}
	if p.PinBlobs[2] != "media-2" {
		t.Fatalf("expected media-2 third, got %s", p.PinBlobs[2])
	}
	if p.PinBlobs[3] != "media-5" {
		t.Fatalf("expected media-5 fourth, got %s", p.PinBlobs[3])
	}
}

func TestDashPinStrategy_AdjustPin_NewSignature(t *testing.T) {
	// Given: DashPinStrategy.AdjustPin should accept blobs and roles and produce real plans.
	strategy := &DashPinStrategy{}
	content := types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}
	blobs := makeTestBlobs()
	roles := makeTestRoles()
	nodeSpaces := []types.NodeSpaceInfo{
		{NodeID: "node-A", AvailableBytes: 80 * 1024 * 1024 * 1024},
	}

	// When: AdjustPin is called with blobs + roles (new signature).
	plans := strategy.AdjustPin(content, blobs, roles, 500, nodeSpaces)

	// Then: Should produce real plans (not nil), same logic as DecideInitialPin.
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan from AdjustPin, got %d", len(plans))
	}
	p := plans[0]
	if p.NodeID != "node-A" {
		t.Fatalf("expected node-A, got %s", p.NodeID)
	}
	if len(p.PinBlobs) != 6 {
		t.Fatalf("expected 6 pin blobs (1 init + 5 media), got %d: %v", len(p.PinBlobs), p.PinBlobs)
	}
}

// ─── PinOrchestrator Tests ───

func TestPinOrchestrator_OnContentIngested(t *testing.T) {
	// Given: an orchestrator with a registered DashPinStrategy, rich node,
	// and an event that carries both blobs and roles.
	cm := &mockContentMetaClient{}
	pop := &mockPopularityClient{}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})
	// Prime node space: rich node.
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100 * 1024 * 1024 * 1024, UsedBytes: 20 * 1024 * 1024 * 1024},
	})

	// When: content is ingested with full blobs + roles in the event.
	evt := makeTestContentIngestedEvent("vid-1", "dash")
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

func TestPinOrchestrator_OnContentIngested_PreheatsCache(t *testing.T) {
	// Given: an orchestrator with empty blob cache.
	cm := &mockContentMetaClient{
		contentBlobs:    makeTestBlobs(),
		contentRoles:    makeTestRoles(),
	}
	pop := &mockPopularityClient{}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100 * 1024 * 1024 * 1024, UsedBytes: 20 * 1024 * 1024 * 1024},
	})

	// When: content is ingested.
	evt := makeTestContentIngestedEvent("vid-1", "dash")
	po.OnContentIngested(evt)

	// Then: blobs + roles should be cached (no need to query metadata).
	po.bcMu.RLock()
	entry, ok := po.blobCache.Get("vid-1")
	po.bcMu.RUnlock()
	if !ok {
		t.Fatal("expected blob cache to contain vid-1 after OnContentIngested")
	}
	if len(entry.Blobs) != len(makeTestBlobs()) {
		t.Fatalf("expected %d cached blobs, got %d", len(makeTestBlobs()), len(entry.Blobs))
	}
	if len(entry.Roles) != len(makeTestRoles()) {
		t.Fatalf("expected %d cached roles, got %d", len(makeTestRoles()), len(entry.Roles))
	}
}

func TestPinOrchestrator_OnContentIngested_DoesNotCallGetContentMeta(t *testing.T) {
	// Given: an orchestrator with a contentMetaClient that returns an error for GetContentMeta.
	// The new code should NOT call GetContentMeta at all — it uses the event data directly.
	cm := &mockContentMetaClient{
		metaErr: errors.New("should not be called"),
	}
	pop := &mockPopularityClient{}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100 * 1024 * 1024 * 1024, UsedBytes: 20 * 1024 * 1024 * 1024},
	})

	// When: content is ingested.
	evt := makeTestContentIngestedEvent("vid-1", "dash")
	// Should NOT panic or return early — GetContentMeta is never called.
	po.OnContentIngested(evt)

	// Then: a plan was still sent because event data is used directly.
	if bcast.sentCount() != 1 {
		t.Fatalf("expected 1 sent plan (event data used directly, not GetContentMeta), got %d", bcast.sentCount())
	}
}

func TestPinOrchestrator_PanicRecovery(t *testing.T) {
	// Given: a panicking strategy.
	cm := &mockContentMetaClient{}
	pop := &mockPopularityClient{}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	po.RegisterStrategy("dash", &panickingStrategy{})

	// When: content is ingested → strategy panics.
	evt := makeTestContentIngestedEvent("vid-1", "dash")
	// Should not panic — defer recover() catches it.
	po.OnContentIngested(evt)
	// No plans sent (strategy panicked).
	if bcast.sentCount() != 0 {
		t.Fatalf("expected 0 sent plans, got %d", bcast.sentCount())
	}
}

// panickingStrategy always panics on DecideInitialPin.
type panickingStrategy struct{}

func (s *panickingStrategy) DecideInitialPin(content types.ContentMeta, blobs []types.BlobDescriptor, roles []types.BlobRole, nodeSpaces []types.NodeSpaceInfo) []types.NodePinPlan {
	panic("boom")
}

func (s *panickingStrategy) AdjustPin(content types.ContentMeta, blobs []types.BlobDescriptor, roles []types.BlobRole, popularity int64, nodeSpaces []types.NodeSpaceInfo) []types.NodePinPlan {
	return nil
}

func TestPinOrchestrator_OnNodeStatusReport(t *testing.T) {
	// Given: an orchestrator with no node spaces yet.
	po := NewPinOrchestrator(&mockContentMetaClient{}, &mockPopularityClient{}, &mockBroadcaster{})

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

func TestPinOrchestrator_Rebalance_FetchesContentBlobs(t *testing.T) {
	// Given: a popularity client with top content, and a contentMetaClient that
	// provides blobs+roles. The orchestrator should call GetContentBlobs during rebalance.
	cm := &mockContentMetaClient{
		contentBlobs: makeTestBlobs(),
		contentRoles: makeTestRoles(),
	}
	pop := &mockPopularityClient{
		topContents: []metadata.TopContent{
			{ContentMeta: types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}, Popularity: 500},
		},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100 * 1024 * 1024 * 1024, UsedBytes: 10 * 1024 * 1024 * 1024},
	})

	// When: rebalance runs.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	po.rebalance(ctx)

	// Then: AdjustPin was called with resolved blobs+roles → plan sent.
	if bcast.sentCount() != 1 {
		t.Fatalf("expected 1 sent plan from rebalance, got %d", bcast.sentCount())
	}
	events := bcast.sentEvents()
	if events[0].nodeID != "node-A" {
		t.Fatalf("expected node-A, got %s", events[0].nodeID)
	}
}

func TestPinOrchestrator_Rebalance_UsesCachedBlobs(t *testing.T) {
	// Given: an orchestrator with pre-warmed blob cache for vid-1.
	// The metadata client's GetContentBlobs is set to return an error, so any
	// call to it would fail. The cache should serve the data instead.
	cm := &mockContentMetaClient{
		contentBlobsErr: errors.New("should not be called — cache hit expected"),
	}
	pop := &mockPopularityClient{
		topContents: []metadata.TopContent{
			{ContentMeta: types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}, Popularity: 500},
		},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})
	po.OnNodeStatusReport(types.NodeStatusReport{
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 100 * 1024 * 1024 * 1024, UsedBytes: 10 * 1024 * 1024 * 1024},
	})

	// Pre-warm the cache.
	po.cacheContentBlobs("vid-1", makeTestBlobs(), makeTestRoles())

	// When: rebalance runs.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	po.rebalance(ctx)

	// Then: cached data was used → plan sent without calling GetContentBlobs.
	if bcast.sentCount() != 1 {
		t.Fatalf("expected 1 sent plan from rebalance (cache hit), got %d", bcast.sentCount())
	}
}

func TestPinOrchestrator_SendNodePinPlan_NoBlobHashInPinUpdate(t *testing.T) {
	// Given: a PinOrchestrator.
	cm := &mockContentMetaClient{}
	pop := &mockPopularityClient{}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)

	// When: sendNodePinPlan is called with a NodePinPlan.
	np := types.NodePinPlan{
		NodeID:     "node-A",
		ContentID:  "vid-1",
		PinBlobs:   []string{"blob-a", "blob-b"},
		UnpinBlobs: []string{"blob-c"},
	}
	po.sendNodePinPlan(np)

	// Then: the PinUpdate in the PinPlan should NOT have a BlobHash field.
	if bcast.sentCount() != 1 {
		t.Fatalf("expected 1 sent plan, got %d", bcast.sentCount())
	}
	events := bcast.sentEvents()
	if events[0].eventType != "PIN_PLAN_UPDATE" {
		t.Fatalf("expected PIN_PLAN_UPDATE, got %s", events[0].eventType)
	}
}

func TestPinOrchestrator_RebalancePanicRecovery(t *testing.T) {
	cm := &mockContentMetaClient{}
	pop := &mockPopularityClient{
		topContents: []metadata.TopContent{
			{ContentMeta: types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}, Popularity: 500},
		},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	po.RegisterStrategy("dash", &panickingStrategy{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// safeRebalance should recover from the panic.
	po.safeRebalance(ctx)
	// No crash = pass.
}

// Test the sequence counter.
func TestPinOrchestrator_NextSeq(t *testing.T) {
	po := NewPinOrchestrator(&mockContentMetaClient{}, &mockPopularityClient{}, &mockBroadcaster{})

	s1 := po.nextSeq()
	s2 := po.nextSeq()
	s3 := po.nextSeq()

	if s1 >= s2 || s2 >= s3 {
		t.Fatalf("expected monotonically increasing: %d < %d < %d", s1, s2, s3)
	}
}

// Concurrent safety test for nodeSpaces updates and reads.
func TestPinOrchestrator_NodeSpacesConcurrent(t *testing.T) {
	po := NewPinOrchestrator(&mockContentMetaClient{}, &mockPopularityClient{}, &mockBroadcaster{})

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

// ─── HandlePinPlan Integration Tests ───

// TestHandlePinPlan tests the node-side pin handler with the new PinUpdate
// (no BlobHash field). Only PinBlobs and UnpinBlobs are used.
func TestHandlePinPlan(t *testing.T) {
	// Given: a fresh PinStore.
	ps := newTestPinStore(t)

	// When: a PinPlan is handled with pin and unpin blobs.
	plan := types.PinPlan{
		Seq:        1,
		TargetNode: "node-A",
		Updates: []types.PinUpdate{{
			PinBlobs:   []string{"blob-a", "blob-b"},
			UnpinBlobs: []string{"blob-c"},
		}},
	}
	nodepin.HandlePinPlan(plan, ps, nil, nil)

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

// Verify PinStore lifecycle in HandlePinPlan.
func TestHandlePinPlan_IdempotentPin(t *testing.T) {
	ps := newTestPinStore(t)

	// Pin once.
	nodepin.HandlePinPlan(types.PinPlan{
		Seq:        1,
		TargetNode: "node-A",
		Updates:    []types.PinUpdate{{PinBlobs: []string{"blob-x"}}},
	}, ps, nil, nil)

	if !ps.IsPinned("blob-x") {
		t.Fatal("expected blob-x to be pinned")
	}

	// Pin same blob again — PinStore.ApplyPin is idempotent.
	nodepin.HandlePinPlan(types.PinPlan{
		Seq:        2,
		TargetNode: "node-A",
		Updates:    []types.PinUpdate{{PinBlobs: []string{"blob-x"}}},
	}, ps, nil, nil)

	if !ps.IsPinned("blob-x") {
		t.Fatal("expected blob-x to still be pinned after idempotent call")
	}
}

// Verify HandlePinPlan with unpin.
func TestHandlePinPlan_Unpin(t *testing.T) {
	ps := newTestPinStore(t)

	// Pin then unpin.
	nodepin.HandlePinPlan(types.PinPlan{
		Seq:        1,
		TargetNode: "node-A",
		Updates:    []types.PinUpdate{{PinBlobs: []string{"blob-y"}}},
	}, ps, nil, nil)

	if !ps.IsPinned("blob-y") {
		t.Fatal("expected blob-y to be pinned")
	}

	nodepin.HandlePinPlan(types.PinPlan{
		Seq:        2,
		TargetNode: "node-A",
		Updates:    []types.PinUpdate{{UnpinBlobs: []string{"blob-y"}}},
	}, ps, nil, nil)

	if ps.IsPinned("blob-y") {
		t.Fatal("expected blob-y to be unpinned")
	}
}

// Verify error paths: no strategy registered → no send.
func TestPinOrchestrator_OnContentIngested_NoStrategy(t *testing.T) {
	cm := &mockContentMetaClient{}
	pop := &mockPopularityClient{}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	// No strategy registered for any content type.

	evt := makeTestContentIngestedEvent("vid-1", "unknown")
	po.OnContentIngested(evt)

	if bcast.sentCount() != 0 {
		t.Fatalf("expected 0 plans without strategy, got %d", bcast.sentCount())
	}
}

// ---------------------------------------------------------------------------
// T16: top_contents_limit parameterization
// ---------------------------------------------------------------------------

// TestRebalance_TopContentsLimit_PropagatesToPopularityQuery asserts that when
// Run is called with topContentsLimit=7, the rebalance loop's popularity
// query receives limit=7 (not the hardcoded legacy 5000).
func TestRebalance_TopContentsLimit_PropagatesToPopularityQuery(t *testing.T) {
	cm := &mockContentMetaClient{
		contentBlobs: makeTestBlobs(),
		contentRoles: makeTestRoles(),
	}
	pop := &mockPopularityClient{
		topContents: []metadata.TopContent{
			{ContentMeta: types.ContentMeta{ContentID: "vid-1", ContentType: "dash"}, Popularity: 500},
		},
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)
	po.RegisterStrategy("dash", &DashPinStrategy{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run with topContentsLimit=7 and a short interval so one rebalance fires.
	go po.Run(ctx, 50*time.Millisecond, 7)

	// Wait for at least one GetTopContents call.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && pop.topContentsCalls.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	if pop.topContentsCalls.Load() == 0 {
		t.Fatal("expected at least one GetTopContents call within 2s")
	}

	if pop.lastTopContentsLimit != 7 {
		t.Fatalf("expected popularity query limit=7, got %d", pop.lastTopContentsLimit)
	}
}

// TestRebalance_TopContentsLimit_ZeroFallsBackToDefault asserts that a zero
// topContentsLimit (config field omitted) falls back to DefaultTopContentsLimit
// (5000) — preserves pre-T16 behaviour bit-for-bit.
func TestRebalance_TopContentsLimit_ZeroFallsBackToDefault(t *testing.T) {
	cm := &mockContentMetaClient{}
	pop := &mockPopularityClient{
		topContentsErr: errors.New("intentional error to short-circuit after query"),
	}
	bcast := &mockBroadcaster{}
	po := NewPinOrchestrator(cm, pop, bcast)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go po.Run(ctx, 50*time.Millisecond, 0)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && pop.topContentsCalls.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	if pop.topContentsCalls.Load() == 0 {
		t.Fatal("expected at least one GetTopContents call within 2s")
	}

	if pop.lastTopContentsLimit != DefaultTopContentsLimit {
		t.Fatalf("expected default limit=%d, got %d", DefaultTopContentsLimit, pop.lastTopContentsLimit)
	}
}
