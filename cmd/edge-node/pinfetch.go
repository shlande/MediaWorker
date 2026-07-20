package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"
)

// pinFetchTimeout bounds a single pin fetch through the network backhaul
// path. Warm-cache hits return instantly; ICP/L4 fetches are streams and
// need an upper bound so a stuck sibling does not leak the fetch goroutine.
const pinFetchTimeout = 2 * time.Minute

// makePinFetchFunc assembles the fetchFunc injected into pinstore.NewPinStore.
//
// Source order (docs/distribution/README.md §9.1 — fetchPinnedBlob "走正常回源
// 路径", i.e. the same path a client request would take):
//  1. warm cache (local, instant)
//  2. the node's normal backhaul path — non-L4: ICP sibling → L4 stream;
//     L4: ICP sibling → local data plane — via the BackhaulManager method the
//     caller selects for this node's capability.
//
// Campaign F5: the previous closure consulted only the warm cache, so every
// pin on a cold node failed with "not in warm cache" and sat in State=failed
// forever. warmGet is nil when the warm cache is disabled; backhaulFetch is
// never nil in production wiring (the BackhaulManager is always constructed)
// but the nil branch is kept explicit for tests and partial wiring.
func makePinFetchFunc(
	warmGet func(blobHash string) ([]byte, bool),
	backhaulFetch func(ctx context.Context, w io.Writer, blobHash string) error,
) func(blobHash string) ([]byte, error) {
	return func(blobHash string) ([]byte, error) {
		if warmGet != nil {
			if data, ok := warmGet(blobHash); ok {
				return data, nil
			}
		}
		if backhaulFetch == nil {
			return nil, fmt.Errorf("blob %s not in warm cache and no backhaul source configured", blobHash)
		}
		ctx, cancel := context.WithTimeout(context.Background(), pinFetchTimeout)
		defer cancel()
		var buf bytes.Buffer
		if err := backhaulFetch(ctx, &buf, blobHash); err != nil {
			return nil, fmt.Errorf("pin fetch %s via backhaul: %w", blobHash, err)
		}
		return buf.Bytes(), nil
	}
}
