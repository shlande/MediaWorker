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
	// blobs: content-addressed blob list (BlobHash + BlobType + Size).
	// roles: per-content arrangement info (role + sort_order + business_meta).
	// nodeSpaces: per-node space statistics from the most recent NodeStatusReport.
	DecideInitialPin(content types.ContentMeta, blobs []types.BlobDescriptor, roles []types.BlobRole, nodeSpaces []types.NodeSpaceInfo) []types.NodePinPlan

	// AdjustPin recomputes pin plans during periodic rebalancing using current
	// popularity data, blobs+roles (resolved by the orchestrator via content_blob
	// lookup), and node space statistics.
	AdjustPin(content types.ContentMeta, blobs []types.BlobDescriptor, roles []types.BlobRole, popularity int64, nodeSpaces []types.NodeSpaceInfo) []types.NodePinPlan
}

// DashPinStrategy implements PinStrategy for DASH video content.
// It uses BlobRole.Role (not BlobType) to partition blobs:
//   - init blobs (role="init") are always pinned to every node.
//   - media blobs (role="media") are pinned based on remaining node space:
//     > 50 GB free → pin up to 5 media blobs
//     > 20 GB free → pin up to 2 media blobs
//     ≤ 20 GB free → only init blobs
// Sorting uses BlobRole.SortOrder (not BlobDescriptor.SortOrder).
type DashPinStrategy struct{}

// DecideInitialPin partitions blobs into init and media groups by role, sorts
// media by sort_order ascending (from roles), and assigns a per-node pin plan
// based on available space.
func (s *DashPinStrategy) DecideInitialPin(
	content types.ContentMeta,
	blobs []types.BlobDescriptor,
	roles []types.BlobRole,
	nodeSpaces []types.NodeSpaceInfo,
) []types.NodePinPlan {
	initBlobs := filterBlobsByRole(blobs, roles, "init")
	mediaBlobs := filterBlobsByRole(blobs, roles, "media")
	sort.Slice(mediaBlobs, func(i, j int) bool {
		return roleOf(mediaBlobs[i].BlobHash, roles).SortOrder < roleOf(mediaBlobs[j].BlobHash, roles).SortOrder
	})

	return s.buildNodePlans(content, initBlobs, mediaBlobs, nodeSpaces)
}

// AdjustPin recomputes the pin plan for existing content during periodic
// rebalancing. For DASH content the logic is identical to initial pinning.
func (s *DashPinStrategy) AdjustPin(
	content types.ContentMeta,
	blobs []types.BlobDescriptor,
	roles []types.BlobRole,
	popularity int64,
	nodeSpaces []types.NodeSpaceInfo,
) []types.NodePinPlan {
	initBlobs := filterBlobsByRole(blobs, roles, "init")
	mediaBlobs := filterBlobsByRole(blobs, roles, "media")
	sort.Slice(mediaBlobs, func(i, j int) bool {
		return roleOf(mediaBlobs[i].BlobHash, roles).SortOrder < roleOf(mediaBlobs[j].BlobHash, roles).SortOrder
	})

	return s.buildNodePlans(content, initBlobs, mediaBlobs, nodeSpaces)
}

// buildNodePlans computes per-node pin plans given pre-separated init and media
// blobs and node space snapshots.
func (s *DashPinStrategy) buildNodePlans(
	content types.ContentMeta,
	initBlobs, mediaBlobs []types.BlobDescriptor,
	nodeSpaces []types.NodeSpaceInfo,
) []types.NodePinPlan {
	plans := make([]types.NodePinPlan, 0, len(nodeSpaces))
	for _, ns := range nodeSpaces {
		pinBlobs := make([]string, 0, len(initBlobs)+5)

		// init blobs always pinned.
		for _, b := range initBlobs {
			pinBlobs = append(pinBlobs, b.BlobHash)
		}

		avail := ns.AvailableBytes

		// Determine media count from available space thresholds.
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

// filterBlobsByRole returns blobs from `blobs` whose BlobHash appears in
// `roles` with the given role string.
func filterBlobsByRole(blobs []types.BlobDescriptor, roles []types.BlobRole, role string) []types.BlobDescriptor {
	roleSet := make(map[string]bool, len(roles))
	for _, r := range roles {
		if r.Role == role {
			roleSet[r.BlobHash] = true
		}
	}
	result := make([]types.BlobDescriptor, 0, len(roleSet))
	for _, b := range blobs {
		if roleSet[b.BlobHash] {
			result = append(result, b)
		}
	}
	return result
}

// roleOf returns the BlobRole for the given blob hash, or the zero value if
// not found.
func roleOf(blobHash string, roles []types.BlobRole) types.BlobRole {
	for _, r := range roles {
		if r.BlobHash == blobHash {
			return r
		}
	}
	return types.BlobRole{}
}
