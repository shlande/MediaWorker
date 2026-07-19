package backhaul

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/shlande/mediaworker/internal/node/cache"
)

func (bm *BackhaulManager) HandleBlobL4(ctx context.Context, w io.Writer, blobHash string) error {
	// Tracking scope (todo 25): only the local data-plane backhaul below is
	// instrumented. Cache hits and sibling-ICP fetches are NOT backhaul and
	// are deliberately excluded from observations/bytes.
	if data, ok := bm.cache.Get(blobHash); ok {
		bm.recordCacheRequest(true)
		_, err := w.Write(data)
		return err
	}
	bm.recordCacheRequest(false)

	if bm.icpFetcher != nil {
		reader, ok, err := bm.icpFetcher.FetchFromPeer(ctx, blobHash)
		if err == nil && ok {
			bm.recordICPRequest(true)
			rc := reader.(io.ReadCloser)
			defer func() { _ = rc.Close() }()
			if wc, isCacheWriter := bm.cache.(CacheWriter); isCacheWriter {
				return streamThrough(w, rc, wc, blobHash)
			}
			_, copyErr := io.Copy(w, rc)
			return copyErr
		}
		bm.recordICPRequest(false)
	}

	// Observations are recorded INSIDE the singleflight closure so concurrent
	// waiters on the same blob count as exactly one backhaul attempt.
	result, err, _ := bm.sfGroup.Do(blobHash, func() (any, error) {
		start := time.Now()
		stream, fetchErr := bm.dataPlane.FetchBlobLocal(ctx, blobHash)
		if fetchErr != nil {
			bm.recordObservation(observation{ts: time.Now(), latencyMs: time.Since(start).Milliseconds()})
			return nil, fetchErr
		}
		if wc, isCacheWriter := bm.cache.(CacheWriter); isCacheWriter {
			data, drainErr := drainAndCache(stream, stream, wc, blobHash)
			if drainErr != nil {
				bm.recordObservation(observation{ts: time.Now(), latencyMs: time.Since(start).Milliseconds()})
				return nil, drainErr
			}
			bm.recordObservation(observation{ts: time.Now(), success: true, latencyMs: time.Since(start).Milliseconds(), bytes: int64(len(data))})
			return data, nil
		}
		defer func() { _ = stream.Close() }()
		var buf bytes.Buffer
		n, copyErr := io.Copy(&buf, stream)
		bm.recordObservation(observation{ts: time.Now(), success: copyErr == nil, latencyMs: time.Since(start).Milliseconds(), bytes: n})
		if copyErr != nil {
			return nil, copyErr
		}
		return buf.Bytes(), nil
	})
	if err != nil {
		return fmt.Errorf("blob not found: %w", err)
	}

	_, writeErr := w.Write(result.([]byte))
	return writeErr
}

var _ CacheWriter = (*cache.WarmCache)(nil)
