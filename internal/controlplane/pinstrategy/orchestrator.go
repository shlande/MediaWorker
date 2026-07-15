package pinstrategy

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Client interfaces (injected, not implemented here) ───

// MetadataClient queries the control-plane metadata service for content
// information, top-content lists, and blob location data. Implementations
// talk to the storage gRPC service.
type MetadataClient interface {
	GetContentMeta(contentID string) (*types.ContentMeta, error)
	GetTopContents(ctx context.Context, limit int) ([]TopContent, error)
	GetSegmentLocations(blobHash string) ([]types.BlobLocation, error)
	GetPopularity24h(blobHash string) float64
}

// TopContent pairs content metadata with its 24-hour popularity for rebalancing.
type TopContent struct {
	ContentMeta types.ContentMeta
	Popularity  int64
}

// SyncBroadcasterClient sends typed events to specific nodes and provides
// subscription channels. Implementations use the SyncBroadcaster control channel.
type SyncBroadcasterClient interface {
	SendToNode(nodeID string, eventType string, payload any) error
	Subscribe(eventType string) <-chan types.Event
}

// ─── PinOrchestrator ───

// PinOrchestrator is the control-plane scheduler that calls PinStrategy
// implementations and delivers per-node PinPlan instructions. All operations
// are panic-safe: a strategy panic is recovered and logged without crashing
// the orchestrator or any other control-plane module.
type PinOrchestrator struct {
	metadata    MetadataClient
	broadcaster SyncBroadcasterClient
	strategies  map[string]PinStrategy // key = ContentType

	nodeSpaces map[string]types.NodeSpaceInfo // key = NodeID
	nsMu       sync.RWMutex

	seq atomic.Uint64
}

// NewPinOrchestrator creates a PinOrchestrator with the given metadata and
// broadcaster clients. Strategies must be registered via RegisterStrategy
// before any events are processed.
func NewPinOrchestrator(mc MetadataClient, bc SyncBroadcasterClient) *PinOrchestrator {
	return &PinOrchestrator{
		metadata:    mc,
		broadcaster: bc,
		strategies:  make(map[string]PinStrategy),
		nodeSpaces:  make(map[string]types.NodeSpaceInfo),
	}
}

// RegisterStrategy associates a PinStrategy with a content type.
// Must be called before the orchestrator processes events.
func (po *PinOrchestrator) RegisterStrategy(contentType string, strategy PinStrategy) {
	po.strategies[contentType] = strategy
}

// ─── Event handlers ───

// OnContentIngested handles a ContentIngestedEvent from the ingest domain.
// It calls the registered strategy's DecideInitialPin and sends the resulting
// NodePinPlans to each target node. Panics are recovered so a bad strategy
// does not crash the orchestrator.
func (po *PinOrchestrator) OnContentIngested(evt types.ContentIngestedEvent) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[pinstrategy] OnContentIngested panicked, pin skipped: content=%s panic=%v", evt.ContentID, r)
		}
	}()

	content, err := po.metadata.GetContentMeta(evt.ContentID)
	if err != nil {
		return
	}

	strategy := po.strategies[content.ContentType]
	if strategy == nil {
		return
	}

	nodeSpaces := po.getNodeSpaces()
	nodePlans := strategy.DecideInitialPin(*content, evt.Blobs, nodeSpaces)

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
// the top 5000 contents by popularity and calls the registered strategy's
// AdjustPin for each, delivering per-node PinPlans. Panics during a rebalance
// round are recovered so subsequent rounds continue uninterrupted. If interval
// is <= 0, the rebalance loop is disabled and Run blocks until ctx is cancelled.
func (po *PinOrchestrator) Run(ctx context.Context, interval time.Duration) {
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
	popular, err := po.metadata.GetTopContents(ctx, 5000)
	if err != nil {
		return
	}

	nodeSpaces := po.getNodeSpaces()

	for _, tc := range popular {
		strategy := po.strategies[tc.ContentMeta.ContentType]
		if strategy == nil {
			continue
		}

		nodePlans := strategy.AdjustPin(tc.ContentMeta, tc.Popularity, nodeSpaces)
		for _, np := range nodePlans {
			po.sendNodePinPlan(np)
		}
	}
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
			BlobHash:   np.ContentID,
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


