// Package dialassist provides a shared dial-reseed helper that reseeds
// a libp2p host's in-memory peerstore from a persistent AddrSource when
// NewStream fails (typically because libp2p's internal peerstore expired
// addresses while our BadgerDB PeerEntryStore still holds fresh entries).
//
// Callers that open libp2p streams — l4fetch.Fetcher, edge_router.proxyToPeer,
// and ICP client paths — use this package to avoid "no addresses" failures
// after ~90s of idle.
package dialassist

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// AddrSource provides peer addresses from a persistent store (typically
// *peerstore.PeerEntryStore). It answers "what multiaddr strings does
// our BadgerDB store have for this peer?" so that ReseedAndRetry can
// repopulate the transient libp2p peerstore.
type AddrSource interface {
	AddrsOf(pid peer.ID) ([]string, bool)
}

// ReseedAndRetry attempts dial(ctx). On failure, it reseeds h's libp2p
// peerstore with addresses from src (to work around in-memory peerstore
// expiry) and retries dial once. If the reseed succeeds and the retry
// succeeds, the second stream is returned. Otherwise both the original
// and retry errors are joined together.
//
// src may be nil; in that case the helper is a pass-through (no reseed).
//
// dial is the NewStream call, possibly wrapped in a per-call timeout.
// The same ctx is passed to both attempts.
func ReseedAndRetry(
	ctx context.Context,
	h host.Host,
	pid peer.ID,
	src AddrSource,
	dial func(context.Context) (network.Stream, error),
) (network.Stream, error) {
	stream, err := dial(ctx)
	if err == nil {
		return stream, nil
	}

	// Reseed libp2p peerstore from our persistent store and retry once.
	if src != nil {
		if addrs, ok := src.AddrsOf(pid); ok && len(addrs) > 0 {
			mas := ParseAddrs(addrs)
			if len(mas) > 0 {
				h.Peerstore().AddAddrs(pid, mas, 10*time.Minute)
				if stream2, err2 := dial(ctx); err2 == nil {
					return stream2, nil
				} else {
					return nil, fmt.Errorf("%w (reseed retry: %v)", err, err2)
				}
			}
		}
	}

	return nil, err
}

// ParseAddrs parses a []string of multiaddr representations into
// []multiaddr.Multiaddr. Invalid addrs are skipped with a logged warning.
func ParseAddrs(addrs []string) []multiaddr.Multiaddr {
	mas := make([]multiaddr.Multiaddr, 0, len(addrs))
	for _, a := range addrs {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			slog.Default().Warn("dialassist: skipping unparseable multiaddr",
				"addr", a, "error", err,
			)
			continue
		}
		mas = append(mas, ma)
	}
	return mas
}
