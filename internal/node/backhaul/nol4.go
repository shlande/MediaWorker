package backhaul

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
)

func (bm *BackhaulManager) HandleBlobNoL4(ctx context.Context, w io.Writer, blobHash string) error {
	if data, ok := bm.cache.Get(blobHash); ok {
		_, err := w.Write(data)
		return err
	}

	if bm.icpFetcher != nil {
		reader, ok, err := bm.icpFetcher.FetchFromPeer(ctx, blobHash)
		if err == nil && ok {
			rc := reader.(io.ReadCloser)
			defer rc.Close()
			if wc, isCacheWriter := bm.cache.(CacheWriter); isCacheWriter {
				return streamThrough(w, rc, wc, blobHash)
			}
			_, copyErr := io.Copy(w, rc)
			return copyErr
		}
	}

	if bm.l4Fetcher == nil {
		return errors.New("blob not found (L4 unavailable)")
	}

	result, err, _ := bm.sfGroup.Do(blobHash, func() (any, error) {
		stream, fetchErr := bm.l4Fetcher.FetchFromL4Node(ctx, blobHash)
		if fetchErr != nil {
			return nil, fetchErr
		}
		if wc, isCacheWriter := bm.cache.(CacheWriter); isCacheWriter {
			return drainAndCache(stream.(io.Reader), stream.(io.Closer), wc, blobHash)
		}
		rc := stream.(io.ReadCloser)
		defer rc.Close()
		var buf bytes.Buffer
		if _, copyErr := io.Copy(&buf, rc); copyErr != nil {
			return nil, copyErr
		}
		return buf.Bytes(), nil
	})
	if err != nil {
		return fmt.Errorf("blob not found (L4 unavailable): %w", err)
	}

	_, writeErr := w.Write(result.([]byte))
	return writeErr
}
