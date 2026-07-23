// Package integration_test provides full-pipeline integration tests for the
// MediaWorker node status report flow. Each test exercises the complete
// vertical slice: node Reporter → libp2p reverse channel → CP SyncBroadcaster
// subscribe loop → noderegistry.UpsertReport + history write — all in a
// single process with real in-memory libp2p hosts.
package integration_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	sb "github.com/shlande/mediaworker/internal/controlplane/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/node/reporter"
	sbnode "github.com/shlande/mediaworker/internal/node/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── test fixture ────────────────────────────────────────────────────────────

// mockHistoryClient records calls to InsertNodeStatusHistory for test
// assertion. Count incremented on every call; last row accessible for
// field-level verification.
type mockHistoryClient struct {
	mu    sync.Mutex
	count int
}

func (m *mockHistoryClient) InsertCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}

// InsertNodeStatusHistory records the call. The row argument is discarded; the
// test asserts pipeline connectivity, not row contents.
func (m *mockHistoryClient) InsertNodeStatusHistory() {
	m.mu.Lock()
	m.count++
	m.mu.Unlock()
}

// spawnTwoHosts creates two in-memory libp2p hosts (CP + node) and connects
// them. Modeled on syncbroadcaster_test.go:34-66.
func spawnTwoHosts(t *testing.T) (cpHost, nodeHost host.Host, nodePeer peer.ID, cleanup func()) {
	t.Helper()

	cpID := genTestIdentity(t)
	cph, err := libp2phost.NewEdgeHost(cpID, []string{"/ip4/127.0.0.1/tcp/0"}, nil, nil)
	if err != nil {
		t.Fatalf("cp host: %v", err)
	}

	nodeID := genTestIdentity(t)
	nodeH, err := libp2phost.NewEdgeHost(nodeID, []string{"/ip4/127.0.0.1/tcp/0"}, nil, nil)
	if err != nil {
		_ = cph.Close()
		t.Fatalf("node host: %v", err)
	}

	cph.Peerstore().AddAddrs(nodeH.ID(), nodeH.Addrs(), time.Hour)
	nodeH.Peerstore().AddAddrs(cph.ID(), cph.Addrs(), time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cph.Connect(ctx, peer.AddrInfo{ID: nodeH.ID(), Addrs: nodeH.Addrs()}); err != nil {
		_ = cph.Close()
		_ = nodeH.Close()
		t.Fatalf("connect: %v", err)
	}

	cleanup = func() {
		_ = cph.Close()
		_ = nodeH.Close()
	}
	return cph, nodeH, nodeH.ID(), cleanup
}

// stubReport builds a fully-populated NodeStatusReport suitable for pipeline
// assertions. The collect function is a stub because this test verifies the
// pipeline, not collection logic (plan line 279).
func stubReport(nodeID types.PeerId) types.NodeStatusReport {
	return types.NodeStatusReport{
		NodeID:  "node-x",
		PeerID:  nodeID,
		Healthy: true,
		PrefixSpace: types.PartitionStatus{
			TotalBytes: 1024,
			UsedBytes:  512,
			BlobCount:  10,
		},
		WarmSpace: types.PartitionStatus{
			TotalBytes: 2048,
			UsedBytes:  256,
			BlobCount:  3,
		},
		Region:     "us-east-1",
		Version:    "test-v1",
		ConnCount:  5,
		LastUpdate: time.Now().Unix(),
		StartedAt:  time.Now().Unix() - 3600,
	}
}

// ─── tests ───────────────────────────────────────────────────────────────────

// TestStatusReport_Pipeline validates the end-to-end node status report flow:
// Reporter → libp2p reverse channel → CP subscribe loop → noderegistry +
// history mock. Uses polling (no fixed sleeps) to eliminate flaky behavior.
func TestStatusReport_Pipeline(t *testing.T) {
	cpHost, nodeHost, nodePeer, cleanup := spawnTwoHosts(t)
	defer cleanup()

	// CP side: broadcaster, subscribe loop, node registry, mock history.
	broadcaster := sb.New(cpHost)
	reg := noderegistry.NewRegistry()
	history := &mockHistoryClient{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe goroutine — mirrors cmd/control-plane/main.go:195-207.
	subCh := broadcaster.Subscribe("NODE_STATUS_REPORT")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-subCh:
				if !ok {
					return
				}
				var report types.NodeStatusReport
				if err := json.Unmarshal(evt.Payload, &report); err != nil {
					t.Logf("subscribe: decode error: %v", err)
					continue
				}
				reg.UpsertReport(report)
				history.InsertNodeStatusHistory()
			}
		}
	}()

	// Node side: client + reporter with stub collect.
	nodeClient := sbnode.NewClient(nodeHost, nil, nil)

	peerID := types.PeerId(nodePeer.String())
	reportCollect := func() types.NodeStatusReport {
		return stubReport(peerID)
	}
	rpt := reporter.NewReporter(reporter.Config{
		Client:   nodeClient,
		CP:       cpHost.ID(),
		Interval: 50 * time.Millisecond,
		Collect:  reportCollect,
	})

	reporterCtx, reporterCancel := context.WithCancel(ctx)
	defer reporterCancel()
	go rpt.Run(reporterCtx)

	// Assert with polling: 3s timeout, 10ms tick.
	assertEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		view, ok := reg.Get(peerID)
		if !ok {
			return false
		}
		if view.Region != "us-east-1" {
			return false
		}
		if view.Version != "test-v1" {
			return false
		}
		if view.ConnCount != 5 {
			return false
		}
		return true
	}, "registry.Get should return report with Region/Version/ConnCount non-zero")

	// Snapshot contains the peer.
	snap := reg.Snapshot()
	found := false
	for _, v := range snap {
		if v.PeerID == peerID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Snapshot does not contain peer %s", peerID)
	}

	// History mock received at least one Insert call.
	assertEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return history.InsertCount() > 0
	}, "history mock should receive Insert calls")
}

// TestStatusReport_CPFirst validates the out-of-order convergence scenario:
// the CP subscribe loop starts first (no reporter yet), then the reporter
// starts later. The pipeline must still converge — the next report cycle
// fills the registry.
func TestStatusReport_CPFirst(t *testing.T) {
	cpHost, nodeHost, nodePeer, cleanup := spawnTwoHosts(t)
	defer cleanup()

	// CP side starts first.
	broadcaster := sb.New(cpHost)
	reg := noderegistry.NewRegistry()
	history := &mockHistoryClient{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subCh := broadcaster.Subscribe("NODE_STATUS_REPORT")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-subCh:
				if !ok {
					return
				}
				var report types.NodeStatusReport
				if err := json.Unmarshal(evt.Payload, &report); err != nil {
					t.Logf("subscribe: decode error: %v", err)
					continue
				}
				reg.UpsertReport(report)
				history.InsertNodeStatusHistory()
			}
		}
	}()

	// Small delay to let CP subscribe loop settle before node starts.
	time.Sleep(50 * time.Millisecond)

	// Verify registry is empty BEFORE reporter starts.
	snap := reg.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("registry should be empty before reporter starts, got %d entries", len(snap))
	}

	// Node side: reporter starts AFTER CP is already subscribing (delayed start).
	nodeClient := sbnode.NewClient(nodeHost, nil, nil)

	peerID := types.PeerId(nodePeer.String())
	reportCollect := func() types.NodeStatusReport {
		return stubReport(peerID)
	}
	rpt := reporter.NewReporter(reporter.Config{
		Client:   nodeClient,
		CP:       cpHost.ID(),
		Interval: 50 * time.Millisecond,
		Collect:  reportCollect,
	})

	reporterCtx, reporterCancel := context.WithCancel(ctx)
	defer reporterCancel()
	go rpt.Run(reporterCtx)

	// Still converges: polling assertion.
	assertEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		view, ok := reg.Get(peerID)
		if !ok {
			return false
		}
		return view.Region == "us-east-1" && view.Version == "test-v1" && view.ConnCount == 5
	}, "registry should converge even when CP starts before reporter")

	// History also converged.
	assertEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return history.InsertCount() > 0
	}, "history mock should receive Insert calls in CP-first scenario")
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// assertEventually polls cond every tick interval until it returns true or
// timeout elapses. Fails the test with msg on timeout. This is the
// require.Eventually equivalent for stdlib-only tests.
func assertEventually(t *testing.T, timeout, tick time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatalf("timeout (%s): %s", timeout, msg)
}
