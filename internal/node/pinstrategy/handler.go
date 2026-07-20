// Package pinstrategy provides the node-side handler for pin plans.
package pinstrategy

import (
	"github.com/shlande/mediaworker/internal/node/pinstore"
	"github.com/shlande/mediaworker/internal/types"
)

// HandlePinPlan processes a PinPlan received by a node. For each PinUpdate it
// applies pins and unpins:
//
//   - When update.PinBlobMetas is non-empty (new CP), the metas are
//     authoritative: every pin uses the meta's blobType/role/size and the
//     update's content_id is threaded into the store.
//   - Otherwise (old CP payload), the legacy path looks up blobType/role/size
//     from the provided blobs+roles metadata; a blob not found there gets
//     conservative defaults (empty string/0) and content_id stays empty.
//
// blobs + roles: from ContentIngestedEvent, arriving with the PinPlan or from
// local cache.
func HandlePinPlan(plan types.PinPlan, ps *pinstore.PinStore, blobs []types.BlobDescriptor, roles []types.BlobRole) {
	for _, update := range plan.Updates {
		if len(update.PinBlobMetas) > 0 {
			for _, meta := range update.PinBlobMetas {
				ps.ApplyPin(meta.BlobHash, meta.BlobType, meta.Role, meta.Size, update.ContentID)
			}
		} else {
			for _, pinHash := range update.PinBlobs {
				ps.ApplyPin(pinHash, findBlobType(pinHash, blobs), findRole(pinHash, roles), findBlobSize(pinHash, blobs), update.ContentID)
			}
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
