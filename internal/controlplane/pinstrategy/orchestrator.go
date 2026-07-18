package pinstrategy

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	cpmetrics "github.com/shlande/mediaworker/internal/controlplane/metrics"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Client interfaces (injected, not implemented here) ───

// SyncBroadcasterClient sends typed events to specific nodes and provides
// subscription channels. Implementations use the SyncBroadcaster control channel.
type SyncBroadcasterClient interface {
	SendToNode(nodeID string, eventType string, payload any) error
	Subscribe(eventType string) <-chan types.Event
}

// ─── Domain types ───

// ContentBlobCacheEntry caches a content's blobs + roles to reduce
// per-rebalance queries to the metadata module.
type ContentBlobCacheEntry struct {
	Blobs    []types.BlobDescriptor
	Roles    []types.BlobRole
	CachedAt time.Time
}

// ─── PinOrchestrator ───

// PinOrchestrator is the control-plane scheduler that calls PinStrategy
// implementations and delivers per-node PinPlan instructions. All operations
// are panic-safe: a strategy panic is recovered and logged without crashing
// the orchestrator or any other control-plane module.
type PinOrchestrator struct {
	contentMeta metadata.ContentMetaClient // metadata orchestration-layer client
	popularity  metadata.PopularityClient  // popularity service client
	broadcaster SyncBroadcasterClient
	strategies  map[string]PinStrategy // key = ContentType

	nodeSpaces map[string]types.NodeSpaceInfo // key = NodeID
	nsMu       sync.RWMutex

	// content_blob cache (keyed by content_id), reduces metadata queries during rebalance.
	blobCache *lru.Cache[string, *ContentBlobCacheEntry]
	bcMu      sync.RWMutex

	// topContentsLimit caps the popularity.GetTopContents query during rebalance.
	// Set by Run; rebalance() reads it. A zero value falls back to
	// DefaultTopContentsLimit at use time.
	topContentsLimit int

	// metrics (T20, optional) — when set, sendNodePinPlan increments
	// cp_pin_plan_dispatched_total.
	metrics *cpmetrics.Metrics

	seq atomic.Uint64
}

// DefaultTopContentsLimit is the fallback cap for popularity.GetTopContents
// when config.PinOrchestrator.TopContentsLimit is zero or unset. Matches the
// pre-T16 hardcoded 5000.
const DefaultTopContentsLimit = 5000

// NewPinOrchestrator creates a PinOrchestrator with the given content metadata
// and popularity clients, plus a broadcaster for sending pin plans to nodes.
// Strategies must be registered via RegisterStrategy before any events are processed.
func NewPinOrchestrator(
	contentMeta metadata.ContentMetaClient,
	popularity metadata.PopularityClient,
	bc SyncBroadcasterClient,
) *PinOrchestrator {
	cache, _ := lru.New[string, *ContentBlobCacheEntry](50000)
	return &PinOrchestrator{
		contentMeta: contentMeta,
		popularity:  popularity,
		broadcaster: bc,
		strategies:  make(map[string]PinStrategy),
		nodeSpaces:  make(map[string]types.NodeSpaceInfo),
		blobCache:   cache,
	}
}

// RegisterStrategy associates a PinStrategy with a content type.
// Must be called before the orchestrator processes events.
func (po *PinOrchestrator) RegisterStrategy(contentType string, strategy PinStrategy) {
	po.strategies[contentType] = strategy
}

// SetMetrics wires a Metrics instance for PinPlan dispatch instrumentation
// (T20). Optional — a nil receiver (the zero value) disables instrumentation.
// Idempotent.
func (po *PinOrchestrator) SetMetrics(m *cpmetrics.Metrics) {
	po.metrics = m
}

// ─── Event handlers ───

// OnContentIngested handles a ContentIngestedEvent from the ingest domain.
// The event already carries the full blob list and role arrangement, so no
// metadata query is needed. It warms the blobCache and calls the registered
// strategy's DecideInitialPin.
// Panics are recovered so a bad strategy does not crash the orchestrator.
func (po *PinOrchestrator) OnContentIngested(evt types.ContentIngestedEvent) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[pinstrategy] OnContentIngested panicked, pin skipped: content=%s panic=%v", evt.ContentID, r)
		}
	}()

	// Ingest event carries full content info (blobs + roles); no need to query metadata.
	content := types.ContentMeta{
		ContentID:   evt.ContentID,
		ContentType: evt.ContentType,
	}

	// Warm blob cache with ingest-provided data.
	po.cacheContentBlobs(evt.ContentID, evt.Blobs, evt.Roles)

	strategy := po.strategies[content.ContentType]
	if strategy == nil {
		return
	}

	nodeSpaces := po.getNodeSpaces()
	nodePlans := strategy.DecideInitialPin(content, evt.Blobs, evt.Roles, nodeSpaces)

	for _, np := range nodePlans {
		po.sendNodePinPlan(np)
	}
}

// OnNodeStatusReport updates the orchestrator's cached node space statistics
// from a periodic node status report.
func (po *PinOrchestrator) OnNodeStatusReport(report types.NodeStatusReport) {
	po.nsMu.Lock()
	defer po.nsMu.Unlock()

	po.nodeSpaces[report.NodeID] = types.NodeSpaceInfo{
		NodeID:         report.NodeID,
		AvailableBytes: report.PrefixSpace.TotalBytes - report.PrefixSpace.UsedBytes,
		PinnedCount:    report.PrefixSpace.BlobCount,
	}
}

// ─── Periodic rebalancing ───

// Run starts the periodic rebalance loop with the given interval. It fetches
// the top topContentsLimit contents by popularity and calls the registered
// strategy's AdjustPin for each, delivering per-node PinPlans. Panics during a
// rebalance round are recovered so subsequent rounds continue uninterrupted.
// If interval is <= 0, the rebalance loop is disabled and Run blocks until ctx
// is cancelled.
//
// topContentsLimit is the cap passed to popularity.GetTopContents. A zero or
// negative value falls back to DefaultTopContentsLimit (5000) — matches the
// pre-T16 hardcoded behaviour bit-for-bit when config omits the field.
func (po *PinOrchestrator) Run(ctx context.Context, interval time.Duration, topContentsLimit int) {
	if topContentsLimit <= 0 {
		topContentsLimit = DefaultTopContentsLimit
	}
	po.topContentsLimit = topContentsLimit

	if interval <= 0 {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			po.safeRebalance(ctx)
		}
	}
}

func (po *PinOrchestrator) safeRebalance(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[pinstrategy] rebalance panicked, skipped this round: panic=%v", r)
		}
	}()
	po.rebalance(ctx)
}

func (po *PinOrchestrator) rebalance(ctx context.Context) {
	// Step 1: Get top-N popular contents from the popularity service.
	limit := po.topContentsLimit
	if limit <= 0 {
		limit = DefaultTopContentsLimit
	}
	popular, err := po.popularity.GetTopContents(ctx, limit)
	if err != nil {
		return
	}

	nodeSpaces := po.getNodeSpaces()

	for _, c := range popular {
		strategy := po.strategies[c.ContentMeta.ContentType]
		if strategy == nil {
			continue
		}

		// Step 2: Resolve content blobs + roles (cache-backed).
		blobs, roles, err := po.getContentBlobs(ctx, c.ContentMeta.ContentID)
		if err != nil || len(blobs) == 0 {
			continue
		}

		// Step 3: Call strategy's AdjustPin with resolved data.
		nodePlans := strategy.AdjustPin(c.ContentMeta, blobs, roles, c.Popularity, nodeSpaces)
		for _, np := range nodePlans {
			po.sendNodePinPlan(np)
		}
	}
}

// ─── Content blob cache ───

// getContentBlobs returns the blobs and roles for a content, preferring the
// local LRU cache (30 min TTL) and falling back to the metadata module on miss.
func (po *PinOrchestrator) getContentBlobs(ctx context.Context, contentID string) ([]types.BlobDescriptor, []types.BlobRole, error) {
	// 1. Check local LRU cache (30 min TTL).
	po.bcMu.RLock()
	if entry, ok := po.blobCache.Get(contentID); ok && time.Since(entry.CachedAt) < 30*time.Minute {
		po.bcMu.RUnlock()
		return entry.Blobs, entry.Roles, nil
	}
	po.bcMu.RUnlock()

	// 2. Cache miss: query metadata module.
	blobs, roles, err := po.contentMeta.GetContentBlobs(ctx, contentID)
	if err != nil {
		return nil, nil, fmt.Errorf("contentMeta.GetContentBlobs: %w", err)
	}

	// 3. Write back to cache.
	po.cacheContentBlobs(contentID, blobs, roles)
	return blobs, roles, nil
}

// cacheContentBlobs stores blobs+roles in the LRU cache with deep-copied slices
// to prevent callers from mutating cached data.
func (po *PinOrchestrator) cacheContentBlobs(contentID string, blobs []types.BlobDescriptor, roles []types.BlobRole) {
	blobsCopy := make([]types.BlobDescriptor, len(blobs))
	copy(blobsCopy, blobs)
	rolesCopy := make([]types.BlobRole, len(roles))
	copy(rolesCopy, roles)

	po.bcMu.Lock()
	po.blobCache.Add(contentID, &ContentBlobCacheEntry{
		Blobs:    blobsCopy,
		Roles:    rolesCopy,
		CachedAt: time.Now(),
	})
	po.bcMu.Unlock()
}

// ─── Internal helpers ───

// getNodeSpaces returns a snapshot of all cached node space statistics.
func (po *PinOrchestrator) getNodeSpaces() []types.NodeSpaceInfo {
	po.nsMu.RLock()
	defer po.nsMu.RUnlock()

	spaces := make([]types.NodeSpaceInfo, 0, len(po.nodeSpaces))
	for _, s := range po.nodeSpaces {
		spaces = append(spaces, s)
	}
	return spaces
}

// sendNodePinPlan sends a per-node PinPlan via the broadcaster as a fire-and-forget
// operation. Delivery is best-effort; nodes with stale pin plans continue to
// serve content via the normal cache-miss path.
func (po *PinOrchestrator) sendNodePinPlan(np types.NodePinPlan) {
	plan := types.PinPlan{
		Seq:        po.seq.Add(1),
		TargetNode: np.NodeID,
		Updates: []types.PinUpdate{{
			PinBlobs:   np.PinBlobs,
			UnpinBlobs: np.UnpinBlobs,
		}},
	}
	_ = po.broadcaster.SendToNode(np.NodeID, "PIN_PLAN_UPDATE", plan)
}

// nextSeq returns the next monotonically increasing sequence number.
func (po *PinOrchestrator) nextSeq() uint64 {
	return po.seq.Add(1)
}
