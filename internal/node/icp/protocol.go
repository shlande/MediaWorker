// Package icp implements inter-cache peer (ICP) communication for sibling node
// cache cooperation. It provides libp2p stream protocols /edge/blob/head/1.0.0
// (HEAD probe) and /edge/blob/get/1.0.0 (GET fetch with streaming).
package icp

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ─── Protocol IDs ──────────────────────────────────────────────────────────

const (
	// BlobHeadProtocol is the stream protocol for HEAD probes. The client
	// sends a varint-prefixed blob hash and reads a 1-byte response:
	// 0x01 = HIT, 0x00 = MISS.
	BlobHeadProtocol = protocol.ID("/edge/blob/head/1.0.0")

	// BlobGetProtocol is the stream protocol for GET fetches. The client
	// sends a varint-prefixed blob hash and reads the blob data as a stream.
	BlobGetProtocol = protocol.ID("/edge/blob/get/1.0.0")
)

// ─── BlobStore interface ───────────────────────────────────────────────────

// A BlobStore provides synchronous operations for checking and retrieving
// blobs. It is the data store abstraction behind the ICP protocol handlers.
type BlobStore interface {
	// Has returns true if the blob identified by blobHash exists in the store.
	Has(blobHash string) bool

	// Get returns a reader for the blob data. The caller must close the
	// returned reader after consuming it. Returns an error if the blob
	// does not exist.
	Get(blobHash string) (io.ReadCloser, error)
}

// ─── Client: HEAD probe ────────────────────────────────────────────────────

// FetchFromPeerHead probes a target peer for blob existence via the
// BlobHeadProtocol stream. It opens a stream with a 10ms timeout, writes the
// varint-prefixed blob hash, and reads a 1-byte response.
//
// Returns true if the peer reports a HIT (0x01), false for MISS (0x00).
// On timeout or stream error, returns false and the error.
func FetchFromPeerHead(ctx context.Context, h host.Host, targetPeer peer.ID, blobHash string) (bool, error) {
	headCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	stream, err := h.NewStream(headCtx, targetPeer, BlobHeadProtocol)
	if err != nil {
		return false, fmt.Errorf("open head stream to %s: %w", targetPeer, err)
	}
	defer func() { _ = stream.Close() }()

	if err := writeBlobHash(stream, blobHash); err != nil {
		return false, fmt.Errorf("write blob hash to %s: %w", targetPeer, err)
	}

	// Signal that the client has finished writing.
	if c, ok := stream.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
	}

	resp := make([]byte, 1)
	if _, err := io.ReadFull(stream, resp); err != nil {
		return false, fmt.Errorf("read head response from %s: %w", targetPeer, err)
	}

	return resp[0] == 0x01, nil
}

// ─── Client: GET fetch (streaming) ────────────────────────────────────────

// FetchFromPeerGet opens a stream to the target peer using the
// BlobGetProtocol, writes the varint-prefixed blob hash, and returns the
// raw stream as an io.ReadCloser.
//
// The caller is responsible for closing the returned stream after
// consuming all data. The data is NOT buffered — the caller should pipe it
// directly to its destination (e.g., an HTTP ResponseWriter).
func FetchFromPeerGet(ctx context.Context, h host.Host, targetPeer peer.ID, blobHash string) (io.ReadCloser, error) {
	stream, err := h.NewStream(ctx, targetPeer, BlobGetProtocol)
	if err != nil {
		return nil, fmt.Errorf("open get stream to %s: %w", targetPeer, err)
	}

	if err := writeBlobHash(stream, blobHash); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("write blob hash to %s: %w", targetPeer, err)
	}

	// Signal that the client has finished writing.
	if c, ok := stream.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
	}

	return stream, nil
}

// ─── Client: combined HEAD + GET ───────────────────────────────────────────

// FetchFromPeer first probes the target peer with a 10ms HEAD, then — if the
// peer reports a HIT — opens a GET stream and returns it as a streaming
// io.ReadCloser.
//
// Return values:
//   - io.ReadCloser: the streaming GET response (nil on MISS or error)
//   - bool: true if the peer reported a HIT and the GET stream was opened
//   - error: non-nil only on HEAD timeout or stream error; nil on MISS
func FetchFromPeer(ctx context.Context, h host.Host, targetPeer peer.ID, blobHash string) (io.ReadCloser, bool, error) {
	has, err := FetchFromPeerHead(ctx, h, targetPeer, blobHash)
	if err != nil {
		return nil, false, fmt.Errorf("head probe to %s: %w", targetPeer, err)
	}
	if !has {
		return nil, false, nil
	}

	stream, err := FetchFromPeerGet(ctx, h, targetPeer, blobHash)
	if err != nil {
		return nil, false, fmt.Errorf("get fetch from %s: %w", targetPeer, err)
	}

	return stream, true, nil
}

// ─── Server: HEAD handler ─────────────────────────────────────────────────

// HandleBlobHead reads a varint-prefixed blob hash from the stream, checks
// the BlobStore, and writes a 1-byte response: 0x01 for HIT, 0x00 for MISS.
// The stream is always closed before returning.
func HandleBlobHead(stream network.Stream, blobStore BlobStore) error {
	defer func() { _ = stream.Close() }()

	blobHash, err := readBlobHash(stream)
	if err != nil {
		return fmt.Errorf("handle head: %w", err)
	}

	resp := byte(0x00)
	if blobStore.Has(blobHash) {
		resp = 0x01
	}

	if _, err := stream.Write([]byte{resp}); err != nil {
		return fmt.Errorf("write head response: %w", err)
	}

	return nil
}

// ─── Server: GET handler ──────────────────────────────────────────────────

// HandleBlobGet reads a varint-prefixed blob hash from the stream, retrieves
// the blob from the BlobStore, and streams the blob data to the peer via
// io.Copy. No data is buffered in memory — the blob is copied directly from
// the store reader to the stream.
// The stream is always closed before returning.
func HandleBlobGet(stream network.Stream, blobStore BlobStore) error {
	defer func() { _ = stream.Close() }()

	blobHash, err := readBlobHash(stream)
	if err != nil {
		return fmt.Errorf("handle get: %w", err)
	}

	reader, err := blobStore.Get(blobHash)
	if err != nil {
		// Reset before the deferred Close so the client sees a stream error,
		// not a clean EOF.
		_ = stream.Reset()
		return fmt.Errorf("get blob %s: %w", blobHash, err)
	}
	defer func() { _ = reader.Close() }()

	if _, err := io.Copy(stream, reader); err != nil {
		return fmt.Errorf("stream blob %s: %w", blobHash, err)
	}

	return nil
}

// ─── Registration ─────────────────────────────────────────────────────────

// RegisterHandlers registers the BlobHeadProtocol and BlobGetProtocol stream
// handlers on the given libp2p host. Incoming streams are dispatched to
// HandleBlobHead and HandleBlobGet respectively. On handler error, the
// stream is reset.
func RegisterHandlers(h host.Host, store BlobStore) {
	h.SetStreamHandler(BlobHeadProtocol, func(s network.Stream) {
		if err := HandleBlobHead(s, store); err != nil {
			_ = s.Reset()
		}
	})
	h.SetStreamHandler(BlobGetProtocol, func(s network.Stream) {
		if err := HandleBlobGet(s, store); err != nil {
			_ = s.Reset()
		}
	})
}

// ─── Wire helpers ──────────────────────────────────────────────────────────

// writeBlobHash writes the blob hash to the stream as a varint-prefixed
// string: first the length as a varint-encoded uint64, then the raw bytes.
func writeBlobHash(w io.Writer, blobHash string) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(blobHash)))
	if _, err := w.Write(buf[:n]); err != nil {
		return fmt.Errorf("write blob hash length: %w", err)
	}
	if _, err := io.WriteString(w, blobHash); err != nil {
		return fmt.Errorf("write blob hash: %w", err)
	}
	return nil
}

// readBlobHash reads a varint-prefixed blob hash from the stream. It uses a
// bufio.Reader to handle byte-at-a-time varint decoding, then reads the hash
// bytes from the same buffered reader to avoid data loss.
func readBlobHash(r io.Reader) (string, error) {
	br := bufio.NewReader(r)
	length, err := binary.ReadUvarint(br)
	if err != nil {
		return "", fmt.Errorf("read blob hash length: %w", err)
	}
	hash := make([]byte, length)
	if _, err := io.ReadFull(br, hash); err != nil {
		return "", fmt.Errorf("read blob hash: %w", err)
	}
	return string(hash), nil
}
