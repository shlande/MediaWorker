package routing

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/shlande/mediaworker/internal/types"
)

// blobGetProtoID is the libp2p protocol ID for proxy blob fetches.
const blobGetProtoID = protocol.ID("/edge/blob/get/1.0.0")

// ─── Interfaces (injected; concrete impls live in other packages) ───────

// BlobRouterBackhaul is the subset of backhaul.BackhaulManager that
// EdgeRouter needs. It decouples routing from the backhaul implementation.
type BlobRouterBackhaul interface {
	HandleBlobL4(ctx context.Context, w io.Writer, blobHash string) error
	HandleBlobNoL4(ctx context.Context, w io.Writer, blobHash string) error
}

// PrimaryChecker abstracts hash-ring primary-node queries.
type PrimaryChecker interface {
	Get(blobHash string) types.PeerId
	IsPrimary(blobHash string) bool
}

// PrefixCache is the interface for prefix-blob storage.
type PrefixCache interface {
	Get(blobHash string) ([]byte, bool)
}

// ─── EdgeRouter ────────────────────────────────────────────────────────

// EdgeRouter routes incoming blob requests on an edge node. It checks
// whether this node is the primary for the blob (via the hash ring). If
// not, it proxies to the primary via a libp2p stream. If it is the
// primary, it delegates to the BlobRouterBackhaul, choosing the L4 or
// non-L4 code path based on this node's capabilities.
type EdgeRouter struct {
	hashRing    PrimaryChecker
	backhaul    BlobRouterBackhaul
	selfPeer    types.PeerId
	isL4        bool
	host        host.Host
	prefixCache PrefixCache
}

// NewEdgeRouter creates an EdgeRouter wired to a hash ring, backhaul
// manager, self identity, and a libp2p host for proxying.
func NewEdgeRouter(
	ring PrimaryChecker,
	bm BlobRouterBackhaul,
	self types.PeerId,
	isL4 bool,
	h host.Host,
) *EdgeRouter {
	return &EdgeRouter{
		hashRing: ring,
		backhaul: bm,
		selfPeer: self,
		isL4:     isL4,
		host:     h,
	}
}

// HandleBlobRequest routes a blob request. If this node is not primary
// for the blob hash, it proxies to the primary via libp2p stream. If it
// is primary, it serves using the backhaul pipeline.
//
// Availability-first: if proxyToPeer fails (peer unreachable, stream
// error, etc.), the request falls back to serveAsPrimary so the caller
// still gets bytes if this node happens to have them (warm cache, ICP
// fetch from siblings, etc.). The failure is logged at Warn level.
func (er *EdgeRouter) HandleBlobRequest(ctx context.Context, w io.Writer, blobHash string) error {
	if !er.isPrimaryNode(blobHash) {
		targetID := er.hashRing.Get(blobHash)
		if err := er.proxyToPeer(ctx, w, peer.ID(targetID), blobHash); err != nil {
			slog.Warn("proxy to peer failed, falling back to local",
				"err", err,
				"blobHash", blobHash,
				"target", targetID,
			)
			return er.serveAsPrimary(ctx, w, blobHash)
		}
		return nil
	}
	return er.serveAsPrimary(ctx, w, blobHash)
}

// HandleBlobHTTP is the HTTP handler wrapper for HandleBlobRequest.
func (er *EdgeRouter) HandleBlobHTTP(w http.ResponseWriter, r *http.Request, blobHash string) {
	if err := er.HandleBlobRequest(r.Context(), w, blobHash); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// isPrimaryNode returns whether self is the hash ring primary for blobHash.
func (er *EdgeRouter) isPrimaryNode(blobHash string) bool {
	return er.hashRing.IsPrimary(blobHash)
}

// serveAsPrimary delegates to the backhaul manager, choosing the L4 or
// non-L4 path based on this node's L4Backhaul capability.
func (er *EdgeRouter) serveAsPrimary(ctx context.Context, w io.Writer, blobHash string) error {
	if er.isL4 {
		return er.backhaul.HandleBlobL4(ctx, w, blobHash)
	}
	return er.backhaul.HandleBlobNoL4(ctx, w, blobHash)
}

// proxyToPeer opens a libp2p stream to the target peer, sends the blob
// hash, and pipes the response back to the writer. This is used when the
// current node is not primary for the requested blob.
func (er *EdgeRouter) proxyToPeer(ctx context.Context, w io.Writer, targetPeer peer.ID, blobHash string) error {
	if er.host == nil {
		return fmt.Errorf("no libp2p host for proxy to %s", targetPeer.ShortString())
	}

	s, err := er.host.NewStream(ctx, targetPeer, blobGetProtoID)
	if err != nil {
		return fmt.Errorf("open stream to %s: %w", targetPeer.ShortString(), err)
	}
	defer s.Close()

	// Send blob hash: varint-prefixed, matching ICP wire format.
	hashBytes := []byte(blobHash)
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(len(hashBytes)))
	if _, err := s.Write(lenBuf[:n]); err != nil {
		s.Reset()
		return fmt.Errorf("write hash length to %s: %w", targetPeer.ShortString(), err)
	}
	if _, err := s.Write(hashBytes); err != nil {
		s.Reset()
		return fmt.Errorf("write hash to %s: %w", targetPeer.ShortString(), err)
	}

	// Close write side so peer sees EOF after hash.
	if err := s.CloseWrite(); err != nil {
		s.Reset()
		return fmt.Errorf("close write side: %w", err)
	}

	// Pipe the response stream to the writer.
	if _, err := io.Copy(w, s); err != nil {
		return fmt.Errorf("read response from %s: %w", targetPeer.ShortString(), err)
	}

	return nil
}

// HandlePrefixPull serves a prefix blob directly from the prefix cache.
func (er *EdgeRouter) HandlePrefixPull(w io.Writer, blobHash string) error {
	if er.prefixCache == nil {
		return fmt.Errorf("prefix cache not configured")
	}
	if data, ok := er.prefixCache.Get(blobHash); ok {
		_, err := w.Write(data)
		return err
	}
	return fmt.Errorf("prefix blob %s not found in cache", blobHash)
}

// SetPrefixCache wires an optional prefix cache for HandlePrefixPull.
func (er *EdgeRouter) SetPrefixCache(cache PrefixCache) {
	er.prefixCache = cache
}
