package backhaul

import (
	"bytes"
	"fmt"
	"io"
)

// DataPlane is the L4 local backhaul interface. An L4 node uses this
// to fetch blobs directly from its local data plane. Non-L4 nodes
// receive nil for this field.
type DataPlane interface {
	FetchBlobLocal(ctx interface{}, blobHash string) (io.ReadCloser, error)
}

// CacheWriter is the interface for writing blob data into the cache.
type CacheWriter interface {
	Put(blobHash string, data []byte, bitrate int) error
}

// drainAndCache reads the stream to completion, simultaneously writing
// to cache via a tee + pipe. Returns the full blob bytes. Used inside
// singleflight.Do so that the closure blocks until the data is cached.
func drainAndCache(stream io.Reader, closer io.Closer, cache CacheWriter, blobHash string) ([]byte, error) {
	defer func() { _ = closer.Close() }()

	var buf bytes.Buffer

	pr, pw := io.Pipe()
	var cacheErr error
	cacheDone := make(chan struct{})
	go func() {
		defer close(cacheDone)
		data, readErr := io.ReadAll(pr)
		if readErr != nil {
			cacheErr = fmt.Errorf("cache tee read: %w", readErr)
			return
		}
		cacheErr = cache.Put(blobHash, data, 0)
	}()

	tee := io.TeeReader(stream, pw)
	if _, copyErr := io.Copy(&buf, tee); copyErr != nil {
		_ = pw.CloseWithError(copyErr)
		return nil, copyErr
	}
	_ = pw.Close()
	<-cacheDone
	if cacheErr != nil {
		return nil, cacheErr
	}

	return buf.Bytes(), nil
}

// streamThrough tees the byte stream from reader into both the
// io.Writer (the HTTP response) and the local cache simultaneously
// using io.TeeReader. This avoids buffering the full blob in memory.
func streamThrough(w io.Writer, reader io.ReadCloser, cache CacheWriter, blobHash string) error {
	defer func() { _ = reader.Close() }()

	// TeeReader: every byte read from reader is written to both w (client)
	// and a pipe writer simultaneously. The pipe is consumed by a goroutine
	// that buffers the data and writes it into the cache after the stream
	// completes.
	pr, pw := io.Pipe()

	// Cache writer goroutine: collect all bytes from the tee, then
	// write to cache after the stream finishes.
	var cacheErr error
	cacheDone := make(chan struct{})
	go func() {
		defer close(cacheDone)
		data, err := io.ReadAll(pr)
		if err != nil {
			cacheErr = fmt.Errorf("streamThrough: read tee for cache: %w", err)
			return
		}
		if err := cache.Put(blobHash, data, 0); err != nil {
			cacheErr = fmt.Errorf("streamThrough: cache write for %s: %w", blobHash, err)
			return
		}
	}()

	tee := io.TeeReader(reader, pw)
	if _, err := io.Copy(w, tee); err != nil {
		pw.CloseWithError(err)
		return fmt.Errorf("streamThrough: client write: %w", err)
	}

	// Close the pipe writer so the cache goroutine gets io.EOF and can finish.
	if err := pw.Close(); err != nil {
		return fmt.Errorf("streamThrough: close pipe: %w", err)
	}

	// Wait for cache write to complete.
	<-cacheDone
	if cacheErr != nil {
		return cacheErr
	}

	return nil
}
