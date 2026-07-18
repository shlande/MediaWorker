// Package pinstrategy provides the node-side handler for pin plans.
package pinstrategy

import (
	"github.com/shlande/mediaworker/internal/node/pinstore"
	"github.com/shlande/mediaworker/internal/types"
)

// HandlePinPlan processes a PinPlan received by a node. For each PinUpdate it
// calls ApplyPin for every PinBlob (looking up blobType/role/size from the
// provided blobs+roles metadata) and ApplyUnpin for every UnpinBlob.
//
// blobs + roles: from ContentIngestedEvent, arriving with the PinPlan or from
// local cache. If a blob is not found in blobs/roles, conservative defaults
// (empty string/0) are used.
func HandlePinPlan(plan types.PinPlan, ps *pinstore.PinStore, blobs []types.BlobDescriptor, roles []types.BlobRole) {
	for _, update := range plan.Updates {
		for _, pinHash := range update.PinBlobs {
			ps.ApplyPin(pinHash, findBlobType(pinHash, blobs), findRole(pinHash, roles), findBlobSize(pinHash, blobs))
		}
		for _, unpinHash := range update.UnpinBlobs {
			ps.ApplyUnpin(unpinHash)
		}
	}
}

func findBlobType(blobHash string, blobs []types.BlobDescriptor) string {
	for _, b := range blobs {
		if b.BlobHash == blobHash {
			return b.BlobType
		}
	}
	return ""
}

func findRole(blobHash string, roles []types.BlobRole) string {
	for _, r := range roles {
		if r.BlobHash == blobHash {
			return r.Role
		}
	}
	return ""
}

func findBlobSize(blobHash string, blobs []types.BlobDescriptor) int64 {
	for _, b := range blobs {
		if b.BlobHash == blobHash {
			return b.Size
		}
	}
	return 0
}
