package icp

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/node/libp2phost"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── In-memory BlobStore for tests ─────────────────────────────────────────

type memoryBlobStore struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

func newMemoryBlobStore() *memoryBlobStore {
	return &memoryBlobStore{blobs: make(map[string][]byte)}
}

func (m *memoryBlobStore) Has(blobHash string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.blobs[blobHash]
	return ok
}

func (m *memoryBlobStore) Get(blobHash string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.blobs[blobHash]
	if !ok {
		return nil, fmt.Errorf("blob %s not found", blobHash)
	}
	return io.NopCloser(&byteSliceReader{data: data, pos: 0}), nil
}

func (m *memoryBlobStore) Put(blobHash string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[blobHash] = data
}

type byteSliceReader struct {
	data []byte
	pos  int
}

func (r *byteSliceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *byteSliceReader) Close() error { return nil }

// ─── Test helpers ──────────────────────────────────────────────────────────

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

// ─── Tests: HEAD ───────────────────────────────────────────────────────────

func TestFetchFromPeerHead_Hit(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	store := newMemoryBlobStore()
	store.Put("blob-abc", []byte("hello"))
	RegisterHandlers(h1, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, h1, h2)

	has, err := FetchFromPeerHead(ctx, h2, h1.ID(), "blob-abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !has {
		t.Fatal("expected HEAD to report HIT, got MISS")
	}
}

func TestFetchFromPeerHead_Miss(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	store := newMemoryBlobStore()
	RegisterHandlers(h1, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, h1, h2)

	has, err := FetchFromPeerHead(ctx, h2, h1.ID(), "blob-xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if has {
		t.Fatal("expected HEAD to report MISS, got HIT")
	}
}

func TestFetchFromPeerHead_Timeout(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	// host1 has no handler → incoming HEAD streams hang → 10ms timeout fires.

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, h1, h2)

	has, err := FetchFromPeerHead(ctx, h2, h1.ID(), "blob-abc")
	if err == nil {
		t.Fatal("expected error (timeout), got nil")
	}
	if has {
		t.Fatal("expected false on timeout, got true")
	}
}

// ─── Tests: GET streaming ─────────────────────────────────────────────────

func TestFetchFromPeerGet_Streaming(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	blobData := make([]byte, 1024)
	for i := range blobData {
		blobData[i] = byte(i % 256)
	}

	store := newMemoryBlobStore()
	store.Put("blob-1k", blobData)
	RegisterHandlers(h1, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, h1, h2)

	// Verify streaming by reading in small chunks — if data were fully
	// buffered, the chunk size would not matter.
	stream, err := FetchFromPeerGet(ctx, h2, h1.ID(), "blob-1k")
	if err != nil {
		t.Fatalf("FetchFromPeerGet: %v", err)
	}
	defer func() { _ = stream.Close() }()

	var received []byte
	chunk := make([]byte, 64)
	for {
		n, readErr := stream.Read(chunk)
		if n > 0 {
			received = append(received, chunk[:n]...)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("read error: %v", readErr)
		}
	}

	if len(received) != len(blobData) {
		t.Fatalf("length mismatch: got %d, want %d", len(received), len(blobData))
	}
	for i := range blobData {
		if received[i] != blobData[i] {
			t.Fatalf("data mismatch at byte %d: got %d, want %d", i, received[i], blobData[i])
		}
	}
}

// ─── Tests: Combined HEAD + GET ────────────────────────────────────────────

func TestFetchFromPeer_Combined(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	blobData := []byte("hello from combined test")
	store := newMemoryBlobStore()
	store.Put("blob-abc", blobData)
	RegisterHandlers(h1, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, h1, h2)

	stream, has, err := FetchFromPeer(ctx, h2, h1.ID(), "blob-abc")
	if err != nil {
		t.Fatalf("FetchFromPeer: %v", err)
	}
	if !has {
		t.Fatal("expected HIT, got MISS")
	}
	if stream == nil {
		t.Fatal("expected non-nil stream on HIT")
	}
	defer func() { _ = stream.Close() }()

	received, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(received) != string(blobData) {
		t.Fatalf("data mismatch: got %q, want %q", received, blobData)
	}
}

func TestFetchFromPeer_Combined_Miss(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	store := newMemoryBlobStore()
	RegisterHandlers(h1, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, h1, h2)

	stream, has, err := FetchFromPeer(ctx, h2, h1.ID(), "blob-xyz")
	if err != nil {
		t.Fatalf("unexpected error on MISS: %v", err)
	}
	if has {
		t.Fatal("expected MISS, got HIT")
	}
	if stream != nil {
		t.Fatal("expected nil stream on MISS")
	}
}

// ─── Tests: Large blob and missing blob ────────────────────────────────────

func TestHandleBlobGet_LargeBlob(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	const blobSize = 100 * 1024 // 100KB
	blobData := make([]byte, blobSize)
	for i := range blobData {
		blobData[i] = byte(i % 256)
	}

	store := newMemoryBlobStore()
	store.Put("blob-100k", blobData)
	RegisterHandlers(h1, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, h1, h2)

	stream, err := FetchFromPeerGet(ctx, h2, h1.ID(), "blob-100k")
	if err != nil {
		t.Fatalf("FetchFromPeerGet: %v", err)
	}
	defer func() { _ = stream.Close() }()

	received := make([]byte, 0, blobSize)
	buf := make([]byte, 32*1024)
	for {
		n, readErr := stream.Read(buf)
		if n > 0 {
			received = append(received, buf[:n]...)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("read error at %d bytes: %v", len(received), readErr)
		}
	}

	if len(received) != blobSize {
		t.Fatalf("length mismatch: got %d, want %d", len(received), blobSize)
	}
	for i := range blobData {
		if received[i] != blobData[i] {
			t.Fatalf("data mismatch at byte %d: got %d, want %d", i, received[i], blobData[i])
		}
	}
}

func TestHandleBlobGet_Missing(t *testing.T) {
	psk := genTestPSK(t)
	h1 := genTestHost(t, psk)
	h2 := genTestHost(t, psk)

	store := newMemoryBlobStore()
	RegisterHandlers(h1, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectTestHosts(t, ctx, h1, h2)

	stream, err := FetchFromPeerGet(ctx, h2, h1.ID(), "blob-missing")
	if err != nil {
		// Server reset the stream — acceptable.
		return
	}
	defer func() { _ = stream.Close() }()

	_, readErr := io.ReadAll(stream)
	if readErr == nil {
		t.Fatal("expected read error for missing blob, got success")
	}
}
