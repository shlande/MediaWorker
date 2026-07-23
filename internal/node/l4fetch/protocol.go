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

	"github.com/shlande/mediaworker/internal/node/dialassist"
	"github.com/shlande/mediaworker/internal/types"
)

const L4GetProtocol = protocol.ID("/edge/l4/get/1.0.0")

var ErrNoL4NodeAvailable = errors.New("no L4 node available")

type PeerLister interface {
	ActivePeers() []types.PeerStoreEntry
}

type FetchFunc func(ctx context.Context, w io.Writer, blobHash string) error

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

type Fetcher struct {
	h       host.Host
	peers   PeerLister
	addrSrc dialassist.AddrSource
	rr      atomic.Uint64
	log     *slog.Logger
}

func NewFetcher(h host.Host, peers PeerLister, addrSrc dialassist.AddrSource) *Fetcher {
	return &Fetcher{
		h:       h,
		peers:   peers,
		addrSrc: addrSrc,
		log:     slog.Default().With("component", "l4fetch"),
	}
}

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

		stream, fetchErr := dialassist.ReseedAndRetry(ctx, f.h, pid, f.addrSrc,
			func(dialCtx context.Context) (network.Stream, error) {
				return f.openStream(dialCtx, pid)
			},
		)
		if fetchErr != nil {
			errs = append(errs, fmt.Errorf("peer %s: %w", pid, fetchErr))
			continue
		}

		if err := writeBlobHash(stream, blobHash); err != nil {
			_ = stream.Close()
			errs = append(errs, fmt.Errorf("peer %s write: %w", pid, err))
			continue
		}

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

func (f *Fetcher) openStream(ctx context.Context, pid peer.ID) (network.Stream, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return f.h.NewStream(dialCtx, pid, L4GetProtocol)
}

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

var _ interface {
	FetchFromL4Node(ctx context.Context, blobHash string) (interface{}, error)
} = (*Fetcher)(nil)
