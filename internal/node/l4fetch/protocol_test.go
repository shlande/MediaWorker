package l4fetch

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/node/libp2phost"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Test helpers ──────────────────────────────────────────────────────────

// fakePeerLister is a test double that returns a pre-configured list of entries.
type fakePeerLister struct {
	entries []types.PeerStoreEntry
}

func (f *fakePeerLister) ActivePeers() []types.PeerStoreEntry { return f.entries }

// genTestPSK returns a 32-byte random PSK for private-network tests.
func genTestPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("generate PSK: %v", err)
	}
	return types.PSK(psk)
}

// genTestHost creates a libp2p host with a fresh Ed25519 key bound to 127.0.0.1:0.
func genTestHost(t *testing.T, psk types.PSK) host.Host {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	id := &sharedid.NodeIdentity{PrivKey: priv, PeerID: types.PeerId(pid.String())}
	h, err := libp2phost.NewEdgeHost(id, []string{"/ip4/127.0.0.1/tcp/0"}, psk, nil)
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// connectTestHosts connects h2 to h1 and waits briefly for the connection to settle.
func connectTestHosts(t *testing.T, ctx context.Context, h1, h2 host.Host) {
	t.Helper()
	pi := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}
	if err := h2.Connect(ctx, pi); err != nil {
		t.Fatalf("connect h2 -> h1: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
}

// peerEntryFromHost builds a types.PeerStoreEntry from a host's identity.
func peerEntryFromHost(h host.Host, addrs []string, l4 bool) types.PeerStoreEntry {
	addrsForEntry := addrs
	if addrsForEntry == nil {
		addrStrs := make([]string, len(h.Addrs()))
		for i, a := range h.Addrs() {
			addrStrs[i] = a.String()
		}
		addrsForEntry = addrStrs
	}
	return types.PeerStoreEntry{
		PeerID:       types.PeerId(h.ID().String()),
		Addrs:        addrsForEntry,
		Capabilities: types.NodeCapabilities{L4Backhaul: l4},
		Score:        0,
	}
}

// ─── Tests: Happy path end-to-end ──────────────────────────────────────────

func TestFetchFromL4Node_EndToEnd(t *testing.T) {
	// Given: a server host with L4 handler + a fetcher host that knows about it.
	psk := genTestPSK(t)
	srvHost := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	const blobHash = "sha256:aaaaaaaabbbbbbbbccccccccddddddddeeeeeeeeffffffff0000000011111111"
	wantData := []byte("hello from l4 stream backhaul")

	// Register server-side handler.
	RegisterHandler(srvHost, func(ctx context.Context, w io.Writer, hash string) error {
		if hash != blobHash {
			return fmt.Errorf("unexpected blob hash: %q", hash)
		}
		_, err := w.Write(wantData)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, srvHost, clientHost)

	// Build a fake lister pointing at the server host.
	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{peerEntryFromHost(srvHost, nil, true)},
	}

	fetcher := NewFetcher(clientHost, lister)

	// When: FetchFromL4Node is called.
	result, err := fetcher.FetchFromL4Node(ctx, blobHash)
	if err != nil {
		t.Fatalf("FetchFromL4Node: unexpected error: %v", err)
	}

	// Then: the returned stream contains the exact data.
	rc := result.(io.ReadCloser)
	defer func() { _ = rc.Close() }()

	got, readErr := io.ReadAll(rc)
	if readErr != nil {
		t.Fatalf("read stream: %v", readErr)
	}
	if string(got) != string(wantData) {
		t.Fatalf("data mismatch: got %q, want %q", string(got), string(wantData))
	}
}

func TestFetchFromL4Node_LargeBlob(t *testing.T) {
	// Given: 100KB of blob data for streaming test.
	psk := genTestPSK(t)
	srvHost := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	const blobHash = "sha256:large-blob-100k"
	const blobSize = 100 * 1024
	wantData := make([]byte, blobSize)
	for i := range wantData {
		wantData[i] = byte(i % 251)
	}

	RegisterHandler(srvHost, func(ctx context.Context, w io.Writer, hash string) error {
		if hash != blobHash {
			return fmt.Errorf("unexpected blob hash: %q", hash)
		}
		_, err := w.Write(wantData)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, srvHost, clientHost)

	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{peerEntryFromHost(srvHost, nil, true)},
	}
	fetcher := NewFetcher(clientHost, lister)

	result, err := fetcher.FetchFromL4Node(ctx, blobHash)
	if err != nil {
		t.Fatalf("FetchFromL4Node: unexpected error: %v", err)
	}

	rc := result.(io.ReadCloser)
	defer func() { _ = rc.Close() }()

	got := make([]byte, 0, blobSize)
	buf := make([]byte, 32*1024)
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("read error at %d bytes: %v", len(got), readErr)
		}
	}

	if len(got) != blobSize {
		t.Fatalf("length mismatch: got %d, want %d", len(got), blobSize)
	}
	for i := range wantData {
		if got[i] != wantData[i] {
			t.Fatalf("data mismatch at byte %d: got %d, want %d", i, got[i], wantData[i])
		}
	}
}

// ─── Tests: Server fetch error → client observes error ─────────────────────

func TestFetchFromL4Node_ServerError(t *testing.T) {
	// Given: a server that returns an error for the blob hash.
	//
	// IMPORTANT: FetchFromL4Node returns the stream BEFORE the server processes
	// the blob hash. The server reads the hash, calls the fetch func, which
	// fails and calls stream.Reset(). The error surfaces on the client's first
	// Read — not during FetchFromL4Node. This matches the ICP GET protocol
	// semantics (icp/protocol.go:178-182).
	psk := genTestPSK(t)
	srvHost := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	RegisterHandler(srvHost, func(ctx context.Context, w io.Writer, hash string) error {
		return errors.New("simulated fetch failure")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, srvHost, clientHost)

	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{peerEntryFromHost(srvHost, nil, true)},
	}
	fetcher := NewFetcher(clientHost, lister)

	// When: FetchFromL4Node is called.
	result, err := fetcher.FetchFromL4Node(ctx, "sha256:does-not-matter")
	if err != nil {
		// The server reset may race with FetchFromL4Node's response — either
		// outcome (immediate error or error on Read) is correct.
		t.Logf("server error caught during FetchFromL4Node (reset raced): %v", err)
		return
	}

	rc := result.(io.ReadCloser)
	defer func() { _ = rc.Close() }()

	time.Sleep(150 * time.Millisecond) // Allow server goroutine to process.
	// Then: reading from the stream should fail because the server reset it.
	_, readErr := io.ReadAll(rc)
	if readErr == nil {
		t.Fatal("expected read error (stream reset by server), got clean EOF empty data")
	}
	t.Logf("client observed expected error on read: %v", readErr)
}

func TestFetchFromL4Node_ServerResetObservedByClient(t *testing.T) {
	// Given: a server that returns an error (causing stream.Reset).
	psk := genTestPSK(t)
	srvHost := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	RegisterHandler(srvHost, func(ctx context.Context, w io.Writer, hash string) error {
		return errors.New("blob not available on this L4 node")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, srvHost, clientHost)

	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{peerEntryFromHost(srvHost, nil, true)},
	}
	fetcher := NewFetcher(clientHost, lister)

	// FetchFromL4Node returns the stream BEFORE the server processes it.
	// The server will read the hash, find the blob unavailable, and reset the
	// stream. The client should see a read error (not silent EOF).
	result, err := fetcher.FetchFromL4Node(ctx, "sha256:some-hash")
	if err != nil {
		// Acceptable: stream opening may also fail if the reset races with
		// NewStream. Either FetchFromL4Node returns an error, or the read
		// path will error — both are acceptable outcomes of server reset.
		t.Logf("FetchFromL4Node returned early error (server reset raced): %v", err)
		return
	}

	rc := result.(io.ReadCloser)
	defer func() { _ = rc.Close() }()

	// Try reading — the server should have reset the stream by now.
	time.Sleep(100 * time.Millisecond) // Allow the server goroutine to process.
	_, readErr := io.ReadAll(rc)
	if readErr == nil {
		t.Fatal("expected read error (stream reset by server), got clean EOF empty data")
	}
	t.Logf("client observed expected error: %v", readErr)
}

// ─── Tests: Candidate filtering ────────────────────────────────────────────

func TestFetchFromL4Node_FiltersNonL4Candidates(t *testing.T) {
	// Given: two peers in the lister, one L4, one non-L4.
	psk := genTestPSK(t)
	srvHost := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	const blobHash = "sha256:filter-test"
	wantData := []byte("l4-only-data")

	// Only the L4 host registers the handler.
	RegisterHandler(srvHost, func(ctx context.Context, w io.Writer, hash string) error {
		_, err := w.Write(wantData)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, srvHost, clientHost)

	// Non-L4 peer: a made-up peer ID (won't be reachable, but filtering should
	// exclude it before any connection attempt).
	nonL4Entry := types.PeerStoreEntry{
		PeerID:       types.PeerId("12D3KooWNonL4PeerXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
		Addrs:        []string{"/ip4/127.0.0.1/tcp/1"},
		Capabilities: types.NodeCapabilities{L4Backhaul: false},
		Score:        0,
	}

	l4Entry := peerEntryFromHost(srvHost, nil, true)
	// Put non-L4 first so that if filtering fails, it would try non-L4 first.
	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{nonL4Entry, l4Entry},
	}

	fetcher := NewFetcher(clientHost, lister)
	result, err := fetcher.FetchFromL4Node(ctx, blobHash)
	if err != nil {
		t.Fatalf("expected success via L4 candidate, got error: %v", err)
	}

	rc := result.(io.ReadCloser)
	defer func() { _ = rc.Close() }()

	got, readErr := io.ReadAll(rc)
	if readErr != nil {
		t.Fatalf("read stream: %v", readErr)
	}
	if string(got) != string(wantData) {
		t.Fatalf("data mismatch: got %q, want %q", string(got), string(wantData))
	}
}

func TestFetchFromL4Node_FiltersStaleCandidates(t *testing.T) {
	psk := genTestPSK(t)
	clientHost := genTestHost(t, psk)

	// ActivePeers() filters Stale entries. Our fake lister simulates
	// an ActivePeers() call where all L4 entries are stale → empty result.
	lister := &fakePeerLister{entries: nil}

	fetcher := NewFetcher(clientHost, lister)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := fetcher.FetchFromL4Node(ctx, "sha256:any")
	if !errors.Is(err, ErrNoL4NodeAvailable) {
		t.Fatalf("expected ErrNoL4NodeAvailable for stale-only lister, got: %v", err)
	}
}

// ─── Tests: Round-robin ──────────────────────────────────────────────────

func TestFetchFromL4Node_RoundRobin(t *testing.T) {
	// Given: two L4-capable server hosts.
	psk := genTestPSK(t)
	srv1 := genTestHost(t, psk)
	srv2 := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	// Each server returns its own identity so we can verify which was contacted.
	var srv1Calls, srv2Calls atomic.Int64
	RegisterHandler(srv1, func(ctx context.Context, w io.Writer, hash string) error {
		srv1Calls.Add(1)
		_, err := w.Write([]byte("srv1"))
		return err
	})
	RegisterHandler(srv2, func(ctx context.Context, w io.Writer, hash string) error {
		srv2Calls.Add(1)
		_, err := w.Write([]byte("srv2"))
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, srv1, clientHost)
	connectTestHosts(t, ctx, srv2, clientHost)

	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{
			peerEntryFromHost(srv1, nil, true),
			peerEntryFromHost(srv2, nil, true),
		},
	}

	fetcher := NewFetcher(clientHost, lister)

	// Make 4 calls; with round-robin across 2 candidates we expect both to be hit.
	seen := map[string]int{}
	for i := range 4 {
		result, err := fetcher.FetchFromL4Node(ctx, fmt.Sprintf("sha256:rr-%d", i))
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		rc := result.(io.ReadCloser)
		data, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr != nil {
			t.Fatalf("call %d read: %v", i, readErr)
		}
		seen[string(data)]++
	}

	if seen["srv1"] == 0 || seen["srv2"] == 0 {
		t.Errorf("round-robin didn't hit both servers: srv1=%d calls, srv2=%d calls; seen=%v",
			srv1Calls.Load(), srv2Calls.Load(), seen)
	}
}

// ─── Tests: No candidates ──────────────────────────────────────────────────

func TestFetchFromL4Node_NoL4Candidates(t *testing.T) {
	// Given: a lister with peers but none have L4Backhaul.
	psk := genTestPSK(t)
	clientHost := genTestHost(t, psk)

	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{
			{PeerID: "peer1", Addrs: []string{"/ip4/1.2.3.4/tcp/1"}, Capabilities: types.NodeCapabilities{Edge: true}, Score: 0},
			{PeerID: "peer2", Addrs: []string{"/ip4/5.6.7.8/tcp/1"}, Capabilities: types.NodeCapabilities{PeerICP: true}, Score: 0},
		},
	}

	fetcher := NewFetcher(clientHost, lister)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := fetcher.FetchFromL4Node(ctx, "sha256:any")
	if !errors.Is(err, ErrNoL4NodeAvailable) {
		t.Fatalf("expected ErrNoL4NodeAvailable, got: %v", err)
	}
}

func TestFetchFromL4Node_EmptyLister(t *testing.T) {
	// Given: an empty lister.
	psk := genTestPSK(t)
	clientHost := genTestHost(t, psk)

	lister := &fakePeerLister{}
	fetcher := NewFetcher(clientHost, lister)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := fetcher.FetchFromL4Node(ctx, "sha256:any")
	if !errors.Is(err, ErrNoL4NodeAvailable) {
		t.Fatalf("expected ErrNoL4NodeAvailable for empty lister, got: %v", err)
	}
}

// ─── Tests: All candidates fail ────────────────────────────────────────────

func TestFetchFromL4Node_AllCandidatesUnreachable(t *testing.T) {
	// Given: the lister has L4 peers, but none are reachable (no hosts listening).
	psk := genTestPSK(t)
	clientHost := genTestHost(t, psk)

	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{
			{
				PeerID:       types.PeerId("12D3KooWDeadPeer1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
				Addrs:        []string{"/ip4/127.0.0.1/tcp/65535"},
				Capabilities: types.NodeCapabilities{L4Backhaul: true},
				Score:        0,
			},
			{
				PeerID:       types.PeerId("12D3KooWDeadPeer2XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
				Addrs:        []string{"/ip4/127.0.0.1/tcp/65534"},
				Capabilities: types.NodeCapabilities{L4Backhaul: true},
				Score:        0,
			},
		},
	}

	fetcher := NewFetcher(clientHost, lister)
	// Short timeout — the 10s dial timeout per peer adds up, so give a generous
	// total context but expect failure before exhaustion.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := fetcher.FetchFromL4Node(ctx, "sha256:any")
	if err == nil {
		t.Fatal("expected error when all candidates are unreachable, got nil")
	}
	if errors.Is(err, ErrNoL4NodeAvailable) {
		t.Fatal("expected aggregate error (not ErrNoL4NodeAvailable) when candidates exist but all fail")
	}
	t.Logf("aggregate error (expected): %v", err)
}
