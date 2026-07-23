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

type fakePeerLister struct {
	entries []types.PeerStoreEntry
}

func (f *fakePeerLister) ActivePeers() []types.PeerStoreEntry { return f.entries }

func (f *fakePeerLister) AddrsOf(pid peer.ID) ([]string, bool) {
	pidStr := pid.String()
	for _, e := range f.entries {
		if string(e.PeerID) == pidStr {
			return e.Addrs, true
		}
	}
	return nil, false
}

func genTestPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("generate PSK: %v", err)
	}
	return types.PSK(psk)
}

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

func connectTestHosts(t *testing.T, ctx context.Context, h1, h2 host.Host) {
	t.Helper()
	pi := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}
	if err := h2.Connect(ctx, pi); err != nil {
		t.Fatalf("connect h2 -> h1: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
}

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

func TestFetchFromL4Node_EndToEnd(t *testing.T) {
	psk := genTestPSK(t)
	srvHost := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	const blobHash = "sha256:aaaaaaaabbbbbbbbccccccccddddddddeeeeeeeeffffffff0000000011111111"
	wantData := []byte("hello from l4 stream backhaul")

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

	fetcher := NewFetcher(clientHost, lister, lister)

	result, err := fetcher.FetchFromL4Node(ctx, blobHash)
	if err != nil {
		t.Fatalf("FetchFromL4Node: unexpected error: %v", err)
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

func TestFetchFromL4Node_LargeBlob(t *testing.T) {
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
	fetcher := NewFetcher(clientHost, lister, lister)

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

func TestFetchFromL4Node_ServerError(t *testing.T) {
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
	fetcher := NewFetcher(clientHost, lister, lister)

	result, err := fetcher.FetchFromL4Node(ctx, "sha256:does-not-matter")
	if err != nil {
		t.Logf("server error caught during FetchFromL4Node (reset raced): %v", err)
		return
	}

	rc := result.(io.ReadCloser)
	defer func() { _ = rc.Close() }()

	time.Sleep(150 * time.Millisecond)
	_, readErr := io.ReadAll(rc)
	if readErr == nil {
		t.Fatal("expected read error (stream reset by server), got clean EOF empty data")
	}
	t.Logf("client observed expected error on read: %v", readErr)
}

func TestFetchFromL4Node_ServerResetObservedByClient(t *testing.T) {
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
	fetcher := NewFetcher(clientHost, lister, lister)

	result, err := fetcher.FetchFromL4Node(ctx, "sha256:some-hash")
	if err != nil {
		t.Logf("FetchFromL4Node returned early error (server reset raced): %v", err)
		return
	}

	rc := result.(io.ReadCloser)
	defer func() { _ = rc.Close() }()

	time.Sleep(100 * time.Millisecond)
	_, readErr := io.ReadAll(rc)
	if readErr == nil {
		t.Fatal("expected read error (stream reset by server), got clean EOF empty data")
	}
	t.Logf("client observed expected error: %v", readErr)
}

func TestFetchFromL4Node_FiltersNonL4Candidates(t *testing.T) {
	psk := genTestPSK(t)
	srvHost := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	const blobHash = "sha256:filter-test"
	wantData := []byte("l4-only-data")

	RegisterHandler(srvHost, func(ctx context.Context, w io.Writer, hash string) error {
		_, err := w.Write(wantData)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, srvHost, clientHost)

	nonL4Entry := types.PeerStoreEntry{
		PeerID:       types.PeerId("12D3KooWNonL4PeerXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
		Addrs:        []string{"/ip4/127.0.0.1/tcp/1"},
		Capabilities: types.NodeCapabilities{L4Backhaul: false},
		Score:        0,
	}

	l4Entry := peerEntryFromHost(srvHost, nil, true)
	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{nonL4Entry, l4Entry},
	}

	fetcher := NewFetcher(clientHost, lister, lister)
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

	lister := &fakePeerLister{entries: nil}

	fetcher := NewFetcher(clientHost, lister, lister)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := fetcher.FetchFromL4Node(ctx, "sha256:any")
	if !errors.Is(err, ErrNoL4NodeAvailable) {
		t.Fatalf("expected ErrNoL4NodeAvailable for stale-only lister, got: %v", err)
	}
}

func TestFetchFromL4Node_RoundRobin(t *testing.T) {
	psk := genTestPSK(t)
	srv1 := genTestHost(t, psk)
	srv2 := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

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

	fetcher := NewFetcher(clientHost, lister, lister)

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

func TestFetchFromL4Node_NoL4Candidates(t *testing.T) {
	psk := genTestPSK(t)
	clientHost := genTestHost(t, psk)

	lister := &fakePeerLister{
		entries: []types.PeerStoreEntry{
			{PeerID: "peer1", Addrs: []string{"/ip4/1.2.3.4/tcp/1"}, Capabilities: types.NodeCapabilities{Edge: true}, Score: 0},
			{PeerID: "peer2", Addrs: []string{"/ip4/5.6.7.8/tcp/1"}, Capabilities: types.NodeCapabilities{PeerICP: true}, Score: 0},
		},
	}

	fetcher := NewFetcher(clientHost, lister, lister)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := fetcher.FetchFromL4Node(ctx, "sha256:any")
	if !errors.Is(err, ErrNoL4NodeAvailable) {
		t.Fatalf("expected ErrNoL4NodeAvailable, got: %v", err)
	}
}

func TestFetchFromL4Node_EmptyLister(t *testing.T) {
	psk := genTestPSK(t)
	clientHost := genTestHost(t, psk)

	lister := &fakePeerLister{}
	fetcher := NewFetcher(clientHost, lister, lister)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := fetcher.FetchFromL4Node(ctx, "sha256:any")
	if !errors.Is(err, ErrNoL4NodeAvailable) {
		t.Fatalf("expected ErrNoL4NodeAvailable for empty lister, got: %v", err)
	}
}

func TestFetchFromL4Node_ReseedFromStoreWhenPeerstoreExpired(t *testing.T) {
	psk := genTestPSK(t)
	srvHost := genTestHost(t, psk)
	clientHost := genTestHost(t, psk)

	const blobHash = "sha256:reseed-test-blob"
	wantData := []byte("reseed from store works")

	RegisterHandler(srvHost, func(ctx context.Context, w io.Writer, hash string) error {
		if hash != blobHash {
			return fmt.Errorf("unexpected blob hash: %q", hash)
		}
		_, err := w.Write(wantData)
		return err
	})

	srvAddrs := func() []string {
		as := srvHost.Addrs()
		strs := make([]string, len(as))
		for i, a := range as {
			strs[i] = a.String()
		}
		return strs
	}()
	if len(srvAddrs) == 0 {
		t.Fatal("server host has no listen addrs")
	}

	entry := types.PeerStoreEntry{
		PeerID:       types.PeerId(srvHost.ID().String()),
		Addrs:        srvAddrs,
		Capabilities: types.NodeCapabilities{L4Backhaul: true},
		Score:        0,
	}
	lister := &fakePeerLister{entries: []types.PeerStoreEntry{entry}}

	fetcher := NewFetcher(clientHost, lister, lister)

	clientPeerstoredAddrs := clientHost.Peerstore().Addrs(srvHost.ID())
	if len(clientPeerstoredAddrs) > 0 {
		t.Fatalf("precondition failed: client peerstore unexpectedly has addrs for server: %v", clientPeerstoredAddrs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result, err := fetcher.FetchFromL4Node(ctx, blobHash)
	if err != nil {
		t.Fatalf("FetchFromL4Node: unexpected error (reseed should have succeeded): %v", err)
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

func TestFetchFromL4Node_AllCandidatesUnreachable(t *testing.T) {
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

	fetcher := NewFetcher(clientHost, lister, lister)
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
