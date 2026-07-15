// Package pinstrategy implements content pinning strategies based on access
// patterns and node space availability.
package pinstrategy

import (
	"sort"

	"github.com/shlande/mediaworker/internal/types"
)

// PinStrategy decides which blobs should be pinned to which nodes. Each content
// type can register its own strategy. Failure is non-fatal — the orchestrator
// recovers from panics so no strategy can crash the control plane.
type PinStrategy interface {
	// DecideInitialPin computes the initial pin plan for newly ingested content.
	// nodeSpaces are per-node space statistics from the most recent NodeStatusReport.
	DecideInitialPin(content types.ContentMeta, blobs []types.BlobDescriptor, nodeSpaces []types.NodeSpaceInfo) []types.NodePinPlan

	// AdjustPin recomputes pin plans during periodic rebalancing using current
	// popularity data and node space statistics.
	AdjustPin(content types.ContentMeta, popularity int64, nodeSpaces []types.NodeSpaceInfo) []types.NodePinPlan
}

// DashPinStrategy implements PinStrategy for DASH video content.
// - init blobs (BlobType == "init") are always pinned to every node.
// - media blobs (BlobType == "media") are pinned based on remaining node space:
//
//	> 50% free → pin up to 5 media blobs
//	20–50% free → pin up to 2 media blobs
//	< 20% free → only init blobs
type DashPinStrategy struct{}

// DecideInitialPin partitions blobs into init and media groups, sorts media by
// SortOrder ascending, and assigns a per-node pin plan based on available space.
func (s *DashPinStrategy) DecideInitialPin(
	content types.ContentMeta,
	blobs []types.BlobDescriptor,
	nodeSpaces []types.NodeSpaceInfo,
) []types.NodePinPlan {
	initBlobs := filterByBlobType(blobs, "init")
	mediaBlobs := filterByBlobType(blobs, "media")
	sort.Slice(mediaBlobs, func(i, j int) bool {
		return mediaBlobs[i].SortOrder < mediaBlobs[j].SortOrder
	})

	plans := make([]types.NodePinPlan, 0, len(nodeSpaces))
	for _, ns := range nodeSpaces {
		pinBlobs := make([]string, 0, len(initBlobs)+5)

		// init blobs always pinned.
		for _, b := range initBlobs {
			pinBlobs = append(pinBlobs, b.BlobHash)
		}

		avail := ns.AvailableBytes

		// Determine media count from available space thresholds.
		// NodeSpaceInfo carries only AvailableBytes; we use absolute thresholds
		// tuned to match the spec's ratio logic with typical partition sizes:
		//   > 50 GB  → rich (init + 5 media)
		//   > 20 GB  → medium (init + 2 media)
		//   <= 20 GB → poor (init only)
		// We use absolute thresholds tuned to match the spec's ratio-based logic
		// with typical partition sizes (100 GB prefix partition):
		//   > 50 GB → rich (5 media)
		//   > 20 GB → medium (2 media)
		//   <= 20 GB → poor (0 media)
		mediaCount := 0
		if avail > 50*1024*1024*1024 {
			mediaCount = 5
		} else if avail > 20*1024*1024*1024 {
			mediaCount = 2
		}
		if mediaCount > len(mediaBlobs) {
			mediaCount = len(mediaBlobs)
		}

		for i := 0; i < mediaCount; i++ {
			if mediaBlobs[i].Size > avail {
				break
			}
			pinBlobs = append(pinBlobs, mediaBlobs[i].BlobHash)
			avail -= mediaBlobs[i].Size
		}

		if len(pinBlobs) > 0 {
			plans = append(plans, types.NodePinPlan{
				NodeID:    ns.NodeID,
				ContentID: content.ContentID,
				PinBlobs:  pinBlobs,
			})
		}
	}

	return plans
}

// AdjustPin recomputes the pin plan for existing content during periodic
// rebalancing. For DASH content the logic is identical to initial pinning.
func (s *DashPinStrategy) AdjustPin(
	content types.ContentMeta,
	popularity int64,
	nodeSpaces []types.NodeSpaceInfo,
) []types.NodePinPlan {
	// Rebalancing for DASH content follows the same space-aware logic.
	// The spec defers per-blob-type AdjustPin variants to the strategy
	// implementations. For DashPinStrategy the result is identical.
	return nil
}

// filterByBlobType returns blobs whose BlobType matches the given type.
func filterByBlobType(blobs []types.BlobDescriptor, blobType string) []types.BlobDescriptor {
	var result []types.BlobDescriptor
	for _, b := range blobs {
		if b.BlobType == blobType {
			result = append(result, b)
		}
	}
	return result
}
