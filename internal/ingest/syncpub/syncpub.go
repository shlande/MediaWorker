// Package syncpub publishes ContentIngestedEvent from the standalone
// ingest-worker to the control-plane SyncBroadcaster over the libp2p
// /edge/control/1.0.0 stream protocol (reverse direction).
//
// The worker joins the private PSK mesh as an infrastructure identity — no
// DHT, no GossipSub, no JWT (plan line 167). PSK is admission: a peer that
// lacks the 32-byte pre-shared key is rejected at the transport layer before
// libp2p security negotiates. The worker never dials anyone other than the
// configured control-plane multiaddr and listens on no addresses
// (libp2p.NoListenAddrs) — it is a pure client of the sync channel.
package syncpub

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/multiformats/go-multiaddr"

	nodesync "github.com/shlande/mediaworker/internal/node/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

const eventType = types.EventContentIngested // "CONTENT_INGESTED"

// retry policy: 3 attempts, 100ms · 2^attempt backoff. Tuned for transient
// stream-open failures; a hard-unreachable CP is caught up-front by the
// connectivity check in main.go (host.Connect → log.Fatal).
const (
	maxRetries     = 3
	baseBackoffMS  = 100
	sendTimeoutSec = 10
)

// publishFailures counts events that exhausted all retries and were logged
// at Error level. Exposed for T20 metrics scraping; atomic for lock-free
// concurrent Publish callers (the IngestPipeline fires events from a
// goroutine per ingest request).
var publishFailures atomic.Uint64

// PublishFailures returns the total number of events that permanently failed
// to reach the control plane since process start.
func PublishFailures() uint64 { return publishFailures.Load() }

// SyncPublisher implements ingest.EventPublisher by opening a
// /edge/control/1.0.0 stream to the control plane for each event and writing
// a varint-prefixed JSON WireMessage{Type:"CONTENT_INGESTED", Payload:evt}.
//
// It satisfies the interface structurally — ingest.EventPublisher is a single
// method `Publish(evt types.ContentIngestedEvent)`. The receiver is a pointer
// so the host is shared across calls.
type SyncPublisher struct {
	host   host.Host
	client *nodesync.Client
	cpPeer peer.ID
}

// NewSyncPublisher builds the libp2p host (no listen addrs, PSK-admitted),
// loads the ingest-worker identity from privKeyPath, decodes the control-plane
// multiaddr, and constructs the nodesync.Client. It does NOT dial the CP —
// callers should run CheckConnectivity before serving traffic.
//
// pskHexEnv is the name of the env var holding the hex-encoded 32-byte PSK
// (matches cmd/edge-node/main.go:158 LIBP2P_PSK convention). An empty/unset
// env var yields an open host — the caller decides whether to log.Fatal.
func NewSyncPublisher(cpMultiaddr string, privKeyPath string, pskHexEnv string) (*SyncPublisher, error) {
	nodeID, err := identity.LoadOrGenerateIdentity(privKeyPath)
	if err != nil {
		return nil, fmt.Errorf("syncpub: load identity: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(nodeID.PrivKey),
		libp2p.NoListenAddrs,
	}

	if pskHexEnv != "" {
		pskHex := os.Getenv(pskHexEnv)
		if pskHex != "" {
			pskBytes, err := hex.DecodeString(pskHex)
			if err != nil {
				return nil, fmt.Errorf("syncpub: decode %s: %w", pskHexEnv, err)
			}
			if len(pskBytes) != 32 {
				return nil, fmt.Errorf("syncpub: %s must be 32 bytes, got %d", pskHexEnv, len(pskBytes))
			}
			opts = append(opts, libp2p.PrivateNetwork(pnet.PSK(pskBytes)))
		}
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("syncpub: create libp2p host: %w", err)
	}

	cpMA, err := multiaddr.NewMultiaddr(cpMultiaddr)
	if err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("syncpub: parse control-plane multiaddr %q: %w", cpMultiaddr, err)
	}
	cpAI, err := peer.AddrInfoFromP2pAddr(cpMA)
	if err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("syncpub: extract peer from %q: %w", cpMultiaddr, err)
	}

	// Pre-seed peerstore so the first SendToControlPlane doesn't need a
	// separate AddAddrs call. Addrs persist for the process lifetime.
	h.Peerstore().AddAddrs(cpAI.ID, cpAI.Addrs, time.Hour)

	client := nodesync.NewClient(h, nil, nil)

	slog.Info("syncpub initialized",
		"self_peer", h.ID().ShortString(),
		"cp_peer", cpAI.ID.ShortString(),
		"cp_addrs", cpAI.Addrs,
	)

	return &SyncPublisher{
		host:   h,
		client: client,
		cpPeer: cpAI.ID,
	}, nil
}

// CheckConnectivity dials the control plane once to fail closed at startup
// rather than silently dropping every subsequent event. Returns an error
// suitable for log.Fatal in main.go.
func (p *SyncPublisher) CheckConnectivity(ctx context.Context) error {
	if err := p.host.Connect(ctx, peer.AddrInfo{
		ID:    p.cpPeer,
		Addrs: p.host.Peerstore().Addrs(p.cpPeer),
	}); err != nil {
		return fmt.Errorf("syncpub: control plane %s unreachable: %w", p.cpPeer.ShortString(), err)
	}
	return nil
}

// Publish sends the ContentIngestedEvent to the control plane. It retries up
// to 3 times with exponential backoff on transient errors. Final failure is
// logged at Error level (with content_id) and the publishFailures counter is
// incremented — events are NEVER silently dropped (plan line 48: PinOrchestrator
// would starve). Publish always returns; the IngestPipeline calls it from a
// goroutine whose return value is ignored (pipeline.go:79 async semantics).
func (p *SyncPublisher) Publish(evt types.ContentIngestedEvent) {
	slog.Info("syncpub.Publish: entered", "content_id", evt.ContentID, "content_type", evt.ContentType, "blob_count", len(evt.Blobs))
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(baseBackoffMS<<attempt) * time.Millisecond
			time.Sleep(backoff)
		}

		ctx, cancel := context.WithTimeout(context.Background(), sendTimeoutSec*time.Second)
		err := p.client.SendToControlPlane(ctx, p.cpPeer, eventType, evt)
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		slog.Warn("syncpub publish attempt failed",
			"attempt", attempt+1,
			"content_id", evt.ContentID,
			"err", err,
		)
	}

	publishFailures.Add(1)
	slog.Error("syncpub publish permanently failed",
		"content_id", evt.ContentID,
		"attempts", maxRetries,
		"err", lastErr,
		"publish_failures_total", publishFailures.Load(),
	)
}

// Close releases the libp2p host. Idempotent.
func (p *SyncPublisher) Close() error {
	if p.host == nil {
		return nil
	}
	return p.host.Close()
}

// Host exposes the underlying libp2p host for tests that need to wire two
// in-memory peers together. Production code should not call this.
func (p *SyncPublisher) Host() host.Host { return p.host }

// CPPeer returns the control-plane peer.ID. Test helper.
func (p *SyncPublisher) CPPeer() peer.ID { return p.cpPeer }
