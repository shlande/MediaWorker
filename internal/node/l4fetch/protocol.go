// Package l4fetch implements the L4 stream backhaul protocol /edge/l4/get/1.0.0.
// L4 nodes register a server handler that streams blob data to non-L4 peers;
// non-L4 nodes use the Fetcher client to pull blobs from L4-capable peers via
// round-robin selection.
//
// Wire format (identical to ICP): varint length prefix + blob hash bytes.
// See internal/node/icp/protocol.go for the original wire helpers; this package
// contains its own private copies to avoid coupling.
package l4fetch

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Protocol ID ────────────────────────────────────────────────────────────

// L4GetProtocol is the stream protocol for fetching blobs from L4-capable
// edge nodes via streaming backhaul.
const L4GetProtocol = protocol.ID("/edge/l4/get/1.0.0")

// ─── Sentinel errors ────────────────────────────────────────────────────────

// ErrNoL4NodeAvailable is returned by Fetcher.FetchFromL4Node when no
// ActivePeers advertise L4Backhaul capability.
var ErrNoL4NodeAvailable = errors.New("no L4 node available")

// ─── PeerLister interface ───────────────────────────────────────────────────

// PeerLister provides the list of currently active peers. peerstore.PeerEntryStore
// already satisfies this interface via its ActivePeers() method.
type PeerLister interface {
	ActivePeers() []types.PeerStoreEntry
}

// ─── Server: FetchFunc + RegisterHandler ────────────────────────────────────

// FetchFunc is the server-side callback that writes blob data for blobHash to w.
// The caller (l4fetch handler) provides the stream as w. On error, the handler
// calls stream.Reset() so the client observes a stream error instead of a clean
// EOF — matching icp.HandleBlobGet semantics.
type FetchFunc func(ctx context.Context, w io.Writer, blobHash string) error

// RegisterHandler sets a stream handler on h for L4GetProtocol. Incoming
// streams are dispatched to handleL4Get, which reads the varint-prefixed blob
// hash and calls fetch.
func RegisterHandler(h host.Host, fetch FetchFunc) {
	h.SetStreamHandler(L4GetProtocol, func(s network.Stream) {
		if err := handleL4Get(s, fetch); err != nil {
			_ = s.Reset()
		}
	})
}

func handleL4Get(stream network.Stream, fetch FetchFunc) error {
	defer func() { _ = stream.Close() }()

	blobHash, err := readBlobHash(stream)
	if err != nil {
		return fmt.Errorf("handle l4 get: %w", err)
	}

	if err := fetch(context.Background(), stream, blobHash); err != nil {
		_ = stream.Reset()
		return fmt.Errorf("fetch blob %s: %w", blobHash, err)
	}

	return nil
}

// ─── Client: Fetcher ────────────────────────────────────────────────────────

// Fetcher discovers L4-capable peers via PeerLister and fetches blob data
// through /edge/l4/get/1.0.0 streams with round-robin peer selection.
type Fetcher struct {
	h     host.Host
	peers PeerLister
	rr    atomic.Uint64
	log   *slog.Logger
}

// NewFetcher creates a Fetcher that uses h to open streams and peers to
// discover L4-capable nodes.
func NewFetcher(h host.Host, peers PeerLister) *Fetcher {
	return &Fetcher{
		h:     h,
		peers: peers,
		log:   slog.Default().With("component", "l4fetch"),
	}
}

// FetchFromL4Node attempts to fetch blobHash from a peer that advertises
// L4Backhaul capability. It selects peers via round-robin, trying each
// candidate at most once. The returned value is an io.ReadCloser backed by
// a libp2p stream — callers must close it after consuming the data.
//
// Signature is dictated by the backhaul.L4Fetcher interface:
//
//	FetchFromL4Node(ctx context.Context, blobHash string) (interface{}, error)
func (f *Fetcher) FetchFromL4Node(ctx context.Context, blobHash string) (interface{}, error) {
	candidates := f.l4Candidates()
	if len(candidates) == 0 {
		return nil, ErrNoL4NodeAvailable
	}

	start := int(f.rr.Add(1)) % len(candidates)
	errs := make([]error, 0, len(candidates))

	for i := range len(candidates) {
		idx := (start + i) % len(candidates)
		entry := candidates[idx]
		pid, err := peer.Decode(string(entry.PeerID))
		if err != nil {
			errs = append(errs, fmt.Errorf("decode peer id %s: %w", entry.PeerID, err))
			continue
		}

		stream, fetchErr := f.openStream(ctx, pid)
		if fetchErr != nil {
			// Reseed libp2p peerstore from our BadgerDB entry: after ~90s idle,
			// the internal peerstore expires addresses while our store still has
			// fresh addrs (refreshed ~30s by DHT discovery). One retry.
			if len(entry.Addrs) > 0 {
				if mas := parseEntryAddrs(entry.Addrs); len(mas) > 0 {
					f.h.Peerstore().AddAddrs(pid, mas, 10*time.Minute)
					if stream2, err2 := f.openStream(ctx, pid); err2 == nil {
						stream = stream2
						fetchErr = nil
					}
				}
			}
			if fetchErr != nil {
				errs = append(errs, fmt.Errorf("peer %s: %w", pid, fetchErr))
				continue
			}
		}

		if err := writeBlobHash(stream, blobHash); err != nil {
			_ = stream.Close()
			errs = append(errs, fmt.Errorf("peer %s write: %w", pid, err))
			continue
		}

		// Signal end of write side; server can now respond.
		if c, ok := stream.(interface{ CloseWrite() error }); ok {
			_ = c.CloseWrite()
		}

		return stream, nil
	}

	if len(errs) == 0 {
		return nil, ErrNoL4NodeAvailable
	}
	return nil, fmt.Errorf("l4 fetch all %d candidate(s) failed: %w", len(errs), errors.Join(errs...))
}

// openStream opens a libp2p stream to pid with a 10s dial timeout. After the
// stream is established, the returned stream is detached from the timeout so
// data transfer is governed by the caller's original ctx.
func (f *Fetcher) openStream(ctx context.Context, pid peer.ID) (network.Stream, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return f.h.NewStream(dialCtx, pid, L4GetProtocol)
}

// parseEntryAddrs parses a []string of multiaddr representations into
// []multiaddr.Multiaddr. Invalid addrs are skipped with a logged warning.
func parseEntryAddrs(addrs []string) []multiaddr.Multiaddr {
	mas := make([]multiaddr.Multiaddr, 0, len(addrs))
	for _, a := range addrs {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			slog.Default().Warn("l4fetch: skipping unparseable multiaddr in peer entry",
				"addr", a, "error", err,
			)
			continue
		}
		mas = append(mas, ma)
	}
	return mas
}

// l4Candidates returns ActivePeers filtered to those with L4Backhaul capability.
// ActivePeers() already excludes Stale peers and peers with Score < -20.
func (f *Fetcher) l4Candidates() []types.PeerStoreEntry {
	all := f.peers.ActivePeers()
	candidates := make([]types.PeerStoreEntry, 0, len(all))
	for _, e := range all {
		if e.Capabilities.L4Backhaul {
			candidates = append(candidates, e)
		}
	}
	return candidates
}

// ─── Wire helpers (private, identical to ICP wire format) ───────────────────

// writeBlobHash writes the blob hash to w as a varint-prefixed string.
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

// readBlobHash reads a varint-prefixed blob hash from r.
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

// Ensure Fetcher satisfies backhaul.L4Fetcher at compile time.
var _ interface { // compile-time interface guard
	FetchFromL4Node(ctx context.Context, blobHash string) (interface{}, error)
} = (*Fetcher)(nil)
