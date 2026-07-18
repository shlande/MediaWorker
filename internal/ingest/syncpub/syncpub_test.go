package syncpub_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/multiformats/go-multiaddr"

	cpsyncbroadcaster "github.com/shlande/mediaworker/internal/controlplane/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/ingest/syncpub"
	"github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// spawnCPHost spins up a libp2p host that acts as the control-plane
// SyncBroadcaster. It listens on a random localhost port, admits the given
// PSK, and returns the host plus its full /p2p/<peerID> multiaddr.
func spawnCPHost(t *testing.T, psk []byte) (host.Host, multiaddr.Multiaddr) {
	t.Helper()

	id, err := identity.LoadOrGenerateIdentity(filepath.Join(t.TempDir(), "cp.key"))
	if err != nil {
		t.Fatalf("cp identity: %v", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(id.PrivKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	}
	if len(psk) > 0 {
		opts = append(opts, libp2p.PrivateNetwork(pnet.PSK(psk)))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		t.Fatalf("cp host: %v", err)
	}

	if len(h.Addrs()) == 0 {
		h.Close()
		t.Fatalf("cp host has no listen addrs")
	}
	ma, err := multiaddr.NewMultiaddr("/p2p/" + h.ID().String())
	if err != nil {
		h.Close()
		t.Fatalf("build cp multiaddr: %v", err)
	}
	fullMA := h.Addrs()[0].Encapsulate(ma)
	return h, fullMA
}

// runWithPSK drives the end-to-end test: spin up a CP host (with optional
// PSK), build a SyncPublisher pointed at it, fire Publish, and assert the
// event arrives on the CP-side Subscribe channel within 5s.
func runWithPSK(t *testing.T, pskHex string, pskEnvName string) {
	t.Helper()

	var psk []byte
	if pskHex != "" {
		var err error
		psk, err = hex.DecodeString(pskHex)
		if err != nil {
			t.Fatalf("decode psk: %v", err)
		}
	}

	// 1. Spawn CP-side host with SyncBroadcaster.
	cpHost, cpMA := spawnCPHost(t, psk)
	defer cpHost.Close()

	broadcaster := cpsyncbroadcaster.New(cpHost)
	subCh := broadcaster.Subscribe(types.EventContentIngested)

	// 2. Build SyncPublisher pointed at the CP.
	privKeyPath := filepath.Join(t.TempDir(), "ingest-worker.key")

	// If PSK env var name is set, populate it before constructing publisher.
	if pskEnvName != "" && pskHex != "" {
		t.Setenv(pskEnvName, pskHex)
	}

	pub, err := syncpub.NewSyncPublisher(cpMA.String(), privKeyPath, pskEnvName)
	if err != nil {
		t.Fatalf("NewSyncPublisher: %v", err)
	}
	defer pub.Close()

	// 3. Seed the publisher's peerstore so Connect succeeds. The publisher
	//    already pre-seeds via NewSyncPublisher (time.Hour ttl); we just need
	//    to dial.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	if err := pub.CheckConnectivity(dialCtx); err != nil {
		t.Fatalf("CheckConnectivity: %v", err)
	}

	// 4. Publish a known event.
	want := types.ContentIngestedEvent{
		ContentID:   "cnt_test_001",
		ContentType: "dash_video",
		Blobs: []types.BlobDescriptor{
			{BlobHash: "hash_aaa", BlobType: "mp4_init_segment", Size: 1024},
			{BlobHash: "hash_bbb", BlobType: "m4s_media_segment", Size: 4096},
		},
		Roles: []types.BlobRole{
			{BlobHash: "hash_aaa", Role: "init", SortOrder: 0},
			{BlobHash: "hash_bbb", Role: "media", SortOrder: 1, BusinessMeta: map[string]any{"bitrate": 1500000}},
		},
		Timestamp: 1700000000,
	}

	pub.Publish(want)

	// 5. Assert delivery within 5s and payload decode.
	select {
	case evt := <-subCh:
		if evt.Type != types.EventContentIngested {
			t.Fatalf("expected event type %s, got %s", types.EventContentIngested, evt.Type)
		}
		var got types.ContentIngestedEvent
		if err := json.Unmarshal(evt.Payload, &got); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if got.ContentID != want.ContentID {
			t.Fatalf("content_id: want %q got %q", want.ContentID, got.ContentID)
		}
		if got.ContentType != want.ContentType {
			t.Fatalf("content_type: want %q got %q", want.ContentType, got.ContentType)
		}
		if len(got.Blobs) != len(want.Blobs) {
			t.Fatalf("blobs len: want %d got %d", len(want.Blobs), len(got.Blobs))
		}
		if got.Blobs[0].BlobHash != want.Blobs[0].BlobHash {
			t.Fatalf("blobs[0].hash: want %q got %q", want.Blobs[0].BlobHash, got.Blobs[0].BlobHash)
		}
		if len(got.Roles) != len(want.Roles) {
			t.Fatalf("roles len: want %d got %d", len(want.Roles), len(got.Roles))
		}
		if got.Roles[1].Role != "media" {
			t.Fatalf("roles[1].role: want media got %s", got.Roles[1].Role)
		}
		if got.Timestamp != want.Timestamp {
			t.Fatalf("timestamp: want %d got %d", want.Timestamp, got.Timestamp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for CONTENT_INGESTED delivery")
	}
}

// TestSyncPublisher_EndToEnd_WithPSK verifies the happy path: ingest-worker
// joins the PSK mesh and delivers CONTENT_INGESTED to the CP subscriber.
func TestSyncPublisher_EndToEnd_WithPSK(t *testing.T) {
	pskHex := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	runWithPSK(t, pskHex, "LIBP2P_PSK")
}

// TestSyncPublisher_EndToEnd_NoPSK verifies the open-mesh fallback: when
// LIBP2P_PSK env var is empty, both CP and worker run without a PSK. This
// is a degenerate config (production always sets PSK) but must not crash.
func TestSyncPublisher_EndToEnd_NoPSK(t *testing.T) {
	runWithPSK(t, "", "LIBP2P_PSK")
}

// TestSyncPublisher_PSKMismatch_FailsClosed verifies that a PSK mismatch
// between worker and CP causes CheckConnectivity to fail — fail-closed
// behavior prevents the worker from serving traffic it cannot report.
func TestSyncPublisher_PSKMismatch_FailsClosed(t *testing.T) {
	cpPSK := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	workerPSK := "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

	cpHost, cpMA := spawnCPHost(t, mustHex(t, cpPSK))
	defer cpHost.Close()

	// Worker uses a different PSK.
	t.Setenv("LIBP2P_PSK", workerPSK)

	privKeyPath := filepath.Join(t.TempDir(), "ingest-worker.key")
	pub, err := syncpub.NewSyncPublisher(cpMA.String(), privKeyPath, "LIBP2P_PSK")
	if err != nil {
		t.Fatalf("NewSyncPublisher: %v", err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := pub.CheckConnectivity(ctx); err == nil {
		t.Fatal("expected CheckConnectivity to fail with PSK mismatch, got nil")
	} else {
		t.Logf("PSK mismatch correctly rejected: %v", err)
	}
}

// TestSyncPublisher_BadMultiaddr verifies malformed multiaddrs error in
// the constructor rather than at first Publish.
func TestSyncPublisher_BadMultiaddr(t *testing.T) {
	privKeyPath := filepath.Join(t.TempDir(), "ingest-worker.key")
	_, err := syncpub.NewSyncPublisher("not-a-multiaddr", privKeyPath, "LIBP2P_PSK")
	if err == nil {
		t.Fatal("expected error for malformed multiaddr, got nil")
	}
	t.Logf("malformed multiaddr correctly rejected: %v", err)
}

// TestSyncPublisher_PublishFailure_IncrementsCounter verifies that a
// permanently-failing publish (CP offline) increments the publishFailures
// counter — never silently drops (plan line 48).
func TestSyncPublisher_PublishFailure_IncrementsCounter(t *testing.T) {
	// Spawn a CP host, get its multiaddr, then Close it so the worker
	// dials a dead address.
	cpHost, cpMA := spawnCPHost(t, nil)
	cpHost.Close()

	before := syncpub.PublishFailures()

	privKeyPath := filepath.Join(t.TempDir(), "ingest-worker.key")
	pub, err := syncpub.NewSyncPublisher(cpMA.String(), privKeyPath, "LIBP2P_PSK")
	if err != nil {
		t.Fatalf("NewSyncPublisher: %v", err)
	}
	defer pub.Close()

	evt := types.ContentIngestedEvent{
		ContentID:   "cnt_fail_001",
		ContentType: "image",
		Timestamp:   1700000001,
	}

	// Publish should retry 3x then log + counter++. Since the backoff is
	// 100ms + 200ms + 400ms ≈ 700ms, allow generous headroom.
	done := make(chan struct{})
	go func() {
		pub.Publish(evt)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Publish did not return within 15s")
	}

	after := syncpub.PublishFailures()
	if after <= before {
		t.Fatalf("publishFailures counter: expected > %d, got %d", before, after)
	}
	t.Logf("publishFailures: before=%d after=%d (delta=%d)", before, after, after-before)
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("hex decode: %v", err)
	}
	return b
}
