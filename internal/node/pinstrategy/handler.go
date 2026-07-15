// Package pinstrategy provides the node-side handler for pin plans.
package pinstrategy

import (
	"github.com/shlande/mediaworker/internal/node/pinstore"
	"github.com/shlande/mediaworker/internal/types"
)

// HandlePinPlan processes a PinPlan received by a node. For each PinUpdate it
// calls ApplyPin for every PinBlob and ApplyUnpin for every UnpinBlob on the
// provided PinStore. This function is intended to be called on edge nodes when
// they receive a PinPlan from the control plane.
func HandlePinPlan(plan types.PinPlan, ps *pinstore.PinStore) {
	for _, update := range plan.Updates {
		for _, pinHash := range update.PinBlobs {
			// ApplyPin requires blobType and size. We pass conservative defaults
			// since the node-side handler doesn't have full blob metadata.
			ps.ApplyPin(pinHash, "", 0)
		}
		for _, unpinHash := range update.UnpinBlobs {
			ps.ApplyUnpin(unpinHash)
		}
	}
}
