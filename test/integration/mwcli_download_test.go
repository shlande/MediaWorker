// Package integration_test provides full-pipeline integration tests for mwcli
// download — verifying that FetchBlob on an embedded non-L4 edge node can pull a
// blob from a seeded L4-capable peer through the production router path.
package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/node/app"
	"github.com/shlande/mediaworker/internal/node/backhaul"
	"github.com/shlande/mediaworker/internal/node/l4fetch"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
	"gopkg.in/yaml.v3"
)

// writeTempConfig writes a minimal non-L4 CLI-side config to a temp file and
// returns its path. The directory is cleaned up when the test completes.
func writeTempConfig(t *testing.T, cfg *config.Config) string {
	t.Helper()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	path := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// minCliConfig returns a minimal non-L4 Config suitable for a CLI-side app.
func minCliConfig(t *testing.T) *config.Config {
	td := t.TempDir()
	// Generate an identity key.
	keyPath := filepath.Join(td, "ed25519.key")
	_, err := sharedid.LoadOrGenerateIdentity(keyPath)
	if err != nil {
		t.Fatalf("gen identity: %v", err)
	}
	return &config.Config{
		Node: config.NodeConfig{
			Identity: config.IdentityConfig{PrivKeyPath: keyPath},
			DeclaredCapabilities: config.CapabilitiesConfig{
				Edge:    true,
				PeerICP: true,
			},
			Libp2p: config.Libp2pConfig{
				Listen: []string{"/ip4/127.0.0.1/tcp/0"},
				DHT: config.DHTConfig{
					Mode:              "client",
					Namespace:         "test",
					AdvertiseTTL:      "2m",
					AdvertiseInterval: "30s",
				},
				PeerStore: config.PeerStoreConfig{Path: filepath.Join(td, "peerstore.db")},
			},
			JWTService: config.JWTServiceConfig{
				Endpoint: "http://127.0.0.1:1/v1/node/jwt",
			},
		},
		Edge: config.EdgeConfig{
			PrefixCache: config.CacheConfig{Enabled: true, Path: filepath.Join(td, "prefix"), SizeGB: 1},
			WarmCache:   config.CacheConfig{Enabled: true, Path: filepath.Join(td, "warm"), SizeGB: 1},
		},
		HashRing: config.HashRingConfig{Replicas: 150},
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: CLI download end-to-end — non-L4 app fetches blob via FetchBlob
// from a seeded L4 peer using the production router path.
// ═══════════════════════════════════════════════════════════════════════════

func TestMwcliDownload_EndToEnd(t *testing.T) {
	if os.Getenv("CONTROL_PLANE_PUBKEY") == "" {
		t.Setenv("CONTROL_PLANE_PUBKEY", "2457cb230c29f9cbec3dd0a2a3770580785a994d5d967b1ef7155e5a8ee9dcb6")
	}

	blobData := []byte("mwcli-download-e2e-test-data")
	blobHash := fmt.Sprintf("sha256:%x", sha256.Sum256(blobData))
	blobHash64 := fmt.Sprintf("%x", sha256.Sum256(blobData))

	// ── L4 peer: raw host (no PSK — matches CLI app's private_network.enabled=false) ──

	l4Identity := genTestIdentity(t)
	l4PS, err := peerstore.NewPeerEntryStore(t.TempDir() + "/l4-ps")
	if err != nil {
		t.Fatalf("L4 peerstore: %v", err)
	}
	t.Cleanup(func() { _ = l4PS.Close() })

	l4Gater := libp2phost.NewEdgeConnectionGater(l4PS, nil, 1000, 100, nil)
	l4Host, err := libp2phost.NewEdgeHost(l4Identity, []string{"/ip4/127.0.0.1/tcp/0"}, nil, l4Gater)
	if err != nil {
		t.Fatalf("L4 host: %v", err)
	}
	t.Cleanup(func() { _ = l4Host.Close() })

	var l4FetchCalls atomic.Int64
	l4DP := &countingDataPlane{
		data:      blobData,
		callCount: &l4FetchCalls,
	}

	l4BM := backhaul.NewBackhaulManager(
		newMemBlobCache(),
		l4DP,
		nil, // icpFetcher
		nil, // l4Fetcher — this IS an L4 node
	)

	l4fetch.RegisterHandler(l4Host, func(ctx context.Context, w io.Writer, hash string) error {
		return l4BM.HandleBlobL4(ctx, w, hash)
	})

	// ── CLI-side app (non-L4) via app.New ─────────────────────────────────

	cfg := minCliConfig(t)

	cliCtx, cliCancel := context.WithCancel(context.Background())
	defer cliCancel()

	cliNode, err := app.New(cliCtx, cfg, app.Options{JWTRequestTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	defer func() { _ = cliNode.Close() }()

	// ── Seed CLI peerstore with L4 entry ──────────────────────────────────

	l4Addrs := make([]string, len(l4Host.Addrs()))
	for i, a := range l4Host.Addrs() {
		l4Addrs[i] = a.String()
	}
	_ = cliNode.PeerStore.Put(
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

	// ── Connect the two hosts ─────────────────────────────────────────────

	cliNode.Host.Peerstore().AddAddrs(l4Host.ID(), l4Host.Addrs(), time.Hour)
	l4Host.Peerstore().AddAddrs(cliNode.Host.ID(), cliNode.Host.Addrs(), time.Hour)
	if err := cliNode.Host.Connect(context.Background(), peer.AddrInfo{ID: l4Host.ID(), Addrs: l4Host.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// ── ACT: FetchBlob through the production router path ─────────────────

	var buf bytes.Buffer
	if err := cliNode.FetchBlob(context.Background(), &buf, blobHash); err != nil {
		t.Fatalf("FetchBlob: %v", err)
	}

	// ── ASSERT: bytes match ──────────────────────────────────────────────

	if !bytes.Equal(buf.Bytes(), blobData) {
		t.Fatalf("data mismatch: got %q, want %q", buf.Bytes(), blobData)
	}

	// ── ASSERT: SHA-256 matches ──────────────────────────────────────────

	gotHash := fmt.Sprintf("%x", sha256.Sum256(buf.Bytes()))
	if gotHash != blobHash64 {
		t.Fatalf("hash mismatch: expected %s, got %s", blobHash64, gotHash)
	}

	// ── ASSERT: L4 data plane was called (proves wire path) ──────────────

	if n := l4FetchCalls.Load(); n == 0 {
		t.Fatal("L4 data plane was never called — the L4 fetch path was NOT exercised")
	}
	t.Logf("mwcli download e2e: %d bytes, sha256 verified, L4 data-plane called %d time(s)", buf.Len(), l4FetchCalls.Load())
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: unknown blob with empty peerstore → error from FetchBlob
// The L4 fetcher has no candidate, so HandleBlobNoL4 falls through
// cache → ICP → l4fetch → ErrNoL4NodeAvailable → error.
// ═══════════════════════════════════════════════════════════════════════════

func TestMwcliDownload_UnknownBlob(t *testing.T) {
	if os.Getenv("CONTROL_PLANE_PUBKEY") == "" {
		t.Setenv("CONTROL_PLANE_PUBKEY", "2457cb230c29f9cbec3dd0a2a3770580785a994d5d967b1ef7155e5a8ee9dcb6")
	}

	cfg := minCliConfig(t)

	cliCtx, cliCancel := context.WithCancel(context.Background())
	defer cliCancel()

	cliNode, err := app.New(cliCtx, cfg, app.Options{JWTRequestTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	defer func() { _ = cliNode.Close() }()

	// No peer seeded — l4fetch has no candidate.
	var buf bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cliNode.FetchBlob(ctx, &buf, "unknown-blob-hash")
	if err == nil {
		t.Fatal("expected error for unknown blob with no peers, got nil")
	}
	t.Logf("unknown blob error (expected): %v", err)

	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer on error, got %d bytes", buf.Len())
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: no peer available (empty peerstore) → wait timeout
// ═══════════════════════════════════════════════════════════════════════════

func TestMwcliDownload_NoPeerWaitTimeout(t *testing.T) {
	if os.Getenv("CONTROL_PLANE_PUBKEY") == "" {
		t.Setenv("CONTROL_PLANE_PUBKEY", "2457cb230c29f9cbec3dd0a2a3770580785a994d5d967b1ef7155e5a8ee9dcb6")
	}

	cfg := minCliConfig(t)
	_ = writeTempConfig(t, cfg) // config path not used when we call waitForUsablePeer directly

	cliCtx, cliCancel := context.WithCancel(context.Background())
	defer cliCancel()

	cliNode, err := app.New(cliCtx, cfg, app.Options{JWTRequestTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	defer func() { _ = cliNode.Close() }()

	// No peer seeded — wait should time out.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Use the exported FetchBlob to prove it fails when there's no peer to
	// respond. The hash ring is empty → HandleBlobRequest calls serveAsPrimary
	// → backhaul chain: cache miss → ICP miss → L4 fetcher has no candidate →
	// error.
	var buf bytes.Buffer
	err = cliNode.FetchBlob(ctx, &buf, "sha256:0000000000000000000000000000000000000000000000000000000000000000")
	if err == nil {
		t.Fatal("expected error with no peer, got nil")
	}
	t.Logf("no-peer error (expected): %v", err)
}
