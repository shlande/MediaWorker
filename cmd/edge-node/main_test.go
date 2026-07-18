package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/node/backhaul"
	"github.com/shlande/mediaworker/internal/node/cache"
	"github.com/shlande/mediaworker/internal/node/hashring"
	"github.com/shlande/mediaworker/internal/node/icp"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Test helpers ──────────────────────────────────────────────────────────

func t12GenPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("gen psk: %v", err)
	}
	return types.PSK(psk)
}

func t12GenHost(t *testing.T, psk types.PSK) (host.Host, types.PeerId) {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
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
	return h, id.PeerID
}

func t12Connect(t *testing.T, ctx context.Context, from, to host.Host) {
	t.Helper()
	pi := peer.AddrInfo{ID: to.ID(), Addrs: to.Addrs()}
	if err := from.Connect(ctx, pi); err != nil {
		t.Fatalf("connect: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
}

func t12TempPeerStore(t *testing.T) *peerstore.PeerEntryStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "t12-peerstore-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	store, err := peerstore.NewPeerEntryStore(dir)
	if err != nil {
		t.Fatalf("new peer store: %v", err)
	}
	if err := store.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func t12PutPeer(t *testing.T, store *peerstore.PeerEntryStore, id types.PeerId) {
	t.Helper()
	if err := store.Put(id, types.PeerStoreEntry{
		PeerID: id,
		Capabilities: types.NodeCapabilities{
			PeerICP: true,
		},
	}); err != nil {
		t.Fatalf("put peer %s: %v", id, err)
	}
}

func t12TempWarmCache(t *testing.T) *cache.WarmCache {
	t.Helper()
	dir, err := os.MkdirTemp("", "t12-warmcache-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return cache.NewWarmCache(filepath.Join(dir, "warm"), 1<<30, cache.NewMemoryIndex(), nil, nil)
}

// memoryBlobStore implements icp.BlobStore backed by a map.
type t12MemoryBlobStore struct {
	blobs map[string][]byte
}

func (m *t12MemoryBlobStore) Has(h string) bool { _, ok := m.blobs[h]; return ok }

func (m *t12MemoryBlobStore) Get(h string) (io.ReadCloser, error) {
	data, ok := m.blobs[h]
	if !ok {
		return nil, errors.New("blob not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// ─── Tests ─────────────────────────────────────────────────────────────────

// TestBackhaulICPFetcher_SiblingHit verifies the happy-path 3-host scenario:
//   - A (self) has no warm cache, no data plane.
//   - B (sibling) holds the blob and is the ring primary for blobHash.
//   - A's backhaulICPFetcher routes via ring.Get → icp.FetchFromPeer(B).
//   - HandleBlobNoL4 streams bytes from B and writes them into A's warm cache.
func TestBackhaulICPFetcher_SiblingHit(t *testing.T) {
	psk := t12GenPSK(t)
	hostA, pidA := t12GenHost(t, psk)
	hostB, pidB := t12GenHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	t12Connect(t, ctx, hostA, hostB)

	// Ring: {A, B}. Scan for a blobHash whose ring primary is B (so the
	// fetcher will actually place an ICP call to B, not short-circuit on
	// self-target). With 150 virtual nodes per peer and 2 peers, ~50% of
	// hashes land on each peer.
	store := t12TempPeerStore(t)
	t12PutPeer(t, store, pidA)
	t12PutPeer(t, store, pidB)
	ring := hashring.NewHashRing(pidA, store, 150)
	ring.RebuildHashRing()

	const blobBody = "sibling bytes from B"
	blobHash := ""
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("t12-sibling-hit-blob-%d", i)
		if ring.Get(candidate) == pidB {
			blobHash = candidate
			break
		}
	}
	if blobHash == "" {
		t.Skip("could not find a blob hash mapping to B within 1000 tries")
	}

	// B registers ICP handlers and stores the blob (after we know the hash).
	storeB := &t12MemoryBlobStore{blobs: map[string][]byte{blobHash: []byte(blobBody)}}
	icp.RegisterHandlers(hostB, storeB)

	warmA := t12TempWarmCache(t)
	fetcher := backhaulICPFetcher{h: hostA, ring: ring, self: pidA}
	bm := backhaul.NewBackhaulManager(
		backhaulWarmCache{warmA},
		nil, // no data plane
		fetcher,
		nil, // no L4 fetcher
	)

	var buf bytes.Buffer
	if err := bm.HandleBlobNoL4(ctx, &buf, blobHash); err != nil {
		t.Fatalf("HandleBlobNoL4: %v", err)
	}
	if buf.String() != blobBody {
		t.Errorf("body mismatch: got %q want %q", buf.String(), blobBody)
	}

	cached, ok := warmA.Get(blobHash)
	if !ok {
		t.Fatal("expected blob written to A's warm cache")
	}
	if string(cached) != blobBody {
		t.Errorf("cached mismatch: got %q want %q", string(cached), blobBody)
	}
}

// TestBackhaulICPFetcher_EmptyRingFallsBackLocal verifies that an empty ring
// (no peers → ring.Get returns "") results in zero network requests and the
// backhaul falls through to local paths. We assert the ICPFetcher returns
// (nil, false, nil) directly, which is the contract HandleBlobL4/NoL4 use.
func TestBackhaulICPFetcher_EmptyRingFallsBackLocal(t *testing.T) {
	psk := t12GenPSK(t)
	hostA, pidA := t12GenHost(t, psk)

	store := t12TempPeerStore(t) // no peers put → ring empty after rebuild
	ring := hashring.NewHashRing(pidA, store, 150)
	ring.RebuildHashRing()

	if got := ring.Get("any-blob"); got != "" {
		t.Fatalf("expected empty ring to return \"\", got %q", got)
	}

	fetcher := backhaulICPFetcher{h: hostA, ring: ring, self: pidA}

	reader, ok, err := fetcher.FetchFromPeer(context.Background(), "any-blob")
	if err != nil {
		t.Fatalf("expected nil err on empty ring, got %v", err)
	}
	if ok {
		t.Fatal("expected ok=false on empty ring")
	}
	if reader != nil {
		t.Fatal("expected nil reader on empty ring")
	}
}

// TestBackhaulICPFetcher_SelfTargetSkipsNetwork verifies that when ring.Get
// returns the local peer ID, the fetcher short-circuits with
// (nil, false, nil) and does NOT open any libp2p stream to itself.
func TestBackhaulICPFetcher_SelfTargetSkipsNetwork(t *testing.T) {
	psk := t12GenPSK(t)
	hostA, pidA := t12GenHost(t, psk)

	// Ring with self only — every blob maps to self.
	store := t12TempPeerStore(t)
	t12PutPeer(t, store, pidA)
	ring := hashring.NewHashRing(pidA, store, 150)
	ring.RebuildHashRing()

	const blobHash = "t12-self-target-blob"
	if got := ring.Get(blobHash); got != pidA {
		t.Skipf("ring primary for %q is %q, not self=%q; cannot verify self-skip",
			blobHash, got, pidA)
	}

	// Use a host that has NO ICP handlers registered. If the fetcher were to
	// accidentally open a stream to itself, the stream would error. We assert
	// no error and (nil, false, nil) — i.e. no stream was opened.
	fetcher := backhaulICPFetcher{h: hostA, ring: ring, self: pidA}

	reader, ok, err := fetcher.FetchFromPeer(context.Background(), blobHash)
	if err != nil {
		t.Fatalf("expected nil err on self-target, got %v", err)
	}
	if ok {
		t.Fatal("expected ok=false on self-target")
	}
	if reader != nil {
		t.Fatal("expected nil reader on self-target")
	}
}
