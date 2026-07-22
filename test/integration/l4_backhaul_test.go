// Package integration_test provides full-pipeline integration tests for the
// MediaWorker L4 stream backhaul protocol — verifying that a non-L4 edge node
// can pull a blob from an L4-capable peer through /edge/l4/get/1.0.0.
//
// These tests use real libp2p hosts, real l4fetch.Fetcher, and a mock data
// plane on the L4 node. The non-L4 node's peerstore is seeded directly (no
// auth exchange dependency), keeping the tests hermetic.
package integration_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/node/backhaul"
	"github.com/shlande/mediaworker/internal/node/l4fetch"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	"github.com/shlande/mediaworker/internal/types"
)

// ═══════════════════════════════════════════════════════════════════════════
// Test: L4 backhaul end-to-end — non-L4 node pulls blob from L4 node via
// real /edge/l4/get/1.0.0 protocol.
// ═══════════════════════════════════════════════════════════════════════════

func TestL4Backhaul_EndToEnd(t *testing.T) {
	blobData := []byte("l4-backhaul-e2e-test-data-xyz")
	blobHash := "l4-backhaul-e2e-blob-hash"
	psk := genTestPSK(t)

	// ── L4 node: build host + gater + peerstore ──────────────────────────

	l4Identity := genTestIdentity(t)
	l4PS, err := peerstore.NewPeerEntryStore(t.TempDir() + "/l4-peerstore")
	if err != nil {
		t.Fatalf("L4 peerstore: %v", err)
	}
	t.Cleanup(func() { _ = l4PS.Close() })

	l4Gater := libp2phost.NewEdgeConnectionGater(l4PS, nil, 1000, 100, nil)
	l4Host, err := libp2phost.NewEdgeHost(l4Identity, []string{"/ip4/127.0.0.1/tcp/0"}, psk, l4Gater)
	if err != nil {
		t.Fatalf("L4 host: %v", err)
	}
	t.Cleanup(func() { _ = l4Host.Close() })

	// ── L4 node: mock data plane with a call counter (proves wire path) ──

	var l4FetchCalls atomic.Int64
	l4DP := &countingDataPlane{
		data:      blobData,
		callCount: &l4FetchCalls,
	}

	// ── L4 node: warm cache (empty — all requests hit the data plane) ────

	l4Cache := newMemBlobCache()

	// ── L4 node: BackhaulManager in L4 mode (nil ICP, nil L4) ────────────

	l4BM := backhaul.NewBackhaulManager(
		l4Cache,
		l4DP,
		nil, // icpFetcher — no sibling ICP for this node
		nil, // l4Fetcher — this IS an L4 node
	)

	// ── L4 node: register /edge/l4/get/1.0.0 handler ─────────────────────

	l4fetch.RegisterHandler(l4Host, func(ctx context.Context, w io.Writer, hash string) error {
		return l4BM.HandleBlobL4(ctx, w, hash)
	})

	// ── Non-L4 node: build host + gater + peerstore ──────────────────────

	nl4Identity := genTestIdentity(t)
	nl4PS, err := peerstore.NewPeerEntryStore(t.TempDir() + "/nl4-peerstore")
	if err != nil {
		t.Fatalf("non-L4 peerstore: %v", err)
	}
	t.Cleanup(func() { _ = nl4PS.Close() })

	nl4Gater := libp2phost.NewEdgeConnectionGater(nl4PS, nil, 1000, 100, nil)
	nl4Host, err := libp2phost.NewEdgeHost(nl4Identity, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nl4Gater)
	if err != nil {
		t.Fatalf("non-L4 host: %v", err)
	}
	t.Cleanup(func() { _ = nl4Host.Close() })

	// ── Seed non-L4 peerstore with L4 node's entry (hermetic) ───────────

	l4Addrs := make([]string, len(l4Host.Addrs()))
	for i, a := range l4Host.Addrs() {
		l4Addrs[i] = a.String()
	}
	_ = nl4PS.Put(
		peerstore.PeerIdFromPeerID(l4Host.ID()),
		types.PeerStoreEntry{
			PeerID: peerstore.PeerIdFromPeerID(l4Host.ID()),
			Addrs:  l4Addrs,
			Capabilities: types.NodeCapabilities{
				L4Backhaul: true,
				PeerICP:    true,
			},
			JWTExp:   time.Now().Unix() + 3600,
			LastSeen: time.Now().Unix(),
			Score:    0,
		},
	)

	// ── Connect the two hosts ────────────────────────────────────────────

	ctx := context.Background()
	nl4Host.Peerstore().AddAddrs(l4Host.ID(), l4Host.Addrs(), time.Hour)
	l4Host.Peerstore().AddAddrs(nl4Host.ID(), nl4Host.Addrs(), time.Hour)
	if err := nl4Host.Connect(ctx, peer.AddrInfo{ID: l4Host.ID(), Addrs: l4Host.Addrs()}); err != nil {
		t.Fatalf("connect non-L4 → L4: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// ── Non-L4 node: BackhaulManager with real l4fetch.Fetcher ───────────

	nl4Cache := newMemBlobCache()
	nl4BM := backhaul.NewBackhaulManager(
		nl4Cache,
		nil, // dataPlane — disabled
		nil, // icpFetcher — no sibling ICP
		l4fetch.NewFetcher(nl4Host, nl4PS),
	)

	// ── ACT: non-L4 node fetches blob via HandleBlobNoL4 ─────────────────

	var buf bytes.Buffer
	if err := nl4BM.HandleBlobNoL4(context.Background(), &buf, blobHash); err != nil {
		t.Fatalf("HandleBlobNoL4: %v", err)
	}

	// ── ASSERT: bytes match ──────────────────────────────────────────────

	if !bytes.Equal(buf.Bytes(), blobData) {
		t.Fatalf("data mismatch: got %q, want %q", buf.Bytes(), blobData)
	}

	// ── ASSERT: L4 fetch path was exercised — the L4 node's data plane
	//    was called (proves the request crossed the wire via /edge/l4/get).

	if n := l4FetchCalls.Load(); n == 0 {
		t.Fatal("L4 data plane was never called — the L4 fetch path was NOT exercised")
	}
	t.Logf("L4 backhaul end-to-end: %d bytes delivered, L4 data-plane called %d time(s)", buf.Len(), l4FetchCalls.Load())
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: non-L4 node with empty peerstore → error contains "L4"
// ═══════════════════════════════════════════════════════════════════════════

func TestL4Backhaul_NoL4Candidate(t *testing.T) {
	blobHash := "l4-backhaul-no-candidate-hash"

	// Non-L4 node with empty peerstore.
	nl4Identity := genTestIdentity(t)
	nl4PS, err := peerstore.NewPeerEntryStore(t.TempDir() + "/nl4-ps")
	if err != nil {
		t.Fatalf("peerstore: %v", err)
	}
	t.Cleanup(func() { _ = nl4PS.Close() })

	nl4Gater := libp2phost.NewEdgeConnectionGater(nl4PS, nil, 1000, 100, nil)
	nl4Host, err := libp2phost.NewEdgeHost(nl4Identity, []string{"/ip4/127.0.0.1/tcp/0"}, genTestPSK(t), nl4Gater)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = nl4Host.Close() })

	nl4Cache := newMemBlobCache()
	nl4BM := backhaul.NewBackhaulManager(
		nl4Cache,
		nil, // dataPlane
		nil, // icpFetcher
		l4fetch.NewFetcher(nl4Host, nl4PS),
	)

	var buf bytes.Buffer
	err = nl4BM.HandleBlobNoL4(context.Background(), &buf, blobHash)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "L4") && !errors.Is(err, l4fetch.ErrNoL4NodeAvailable) {
		t.Fatalf("error should mention L4, got: %v", err)
	}
	t.Logf("no-L4-candidate error (expected): %v", err)

	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer on error, got %d bytes", buf.Len())
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

// countingDataPlane implements backhaul.DataPlane and records each call.
type countingDataPlane struct {
	data      []byte
	callCount *atomic.Int64
}

func (m *countingDataPlane) FetchBlobLocal(_ interface{}, _ string) (io.ReadCloser, error) {
	m.callCount.Add(1)
	return io.NopCloser(bytes.NewReader(m.data)), nil
}
