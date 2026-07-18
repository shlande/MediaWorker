package routing

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/node/icp"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	sharedid "github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Scheduler tests ──────────────────────────────────────────────────────

func TestScheduler_ResolveDNS(t *testing.T) {
	s := NewScheduler([]NodeEndpoint{
		{PeerID: "peer-1", Addr: "10.0.0.1:9001", Region: "beijing", Healthy: true},
		{PeerID: "peer-2", Addr: "10.0.0.2:9001", Region: "guangzhou", Healthy: true},
		{PeerID: "peer-3", Addr: "10.0.0.3:9001", Region: "singapore", Healthy: true},
	})

	// Given 3 healthy nodes across 3 regions
	// When resolving DNS for "beijing"
	addr, err := s.ResolveDNS("beijing")
	// Then the first healthy beijing node is returned
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "10.0.0.1:9001" {
		t.Errorf("expected 10.0.0.1:9001, got %s", addr)
	}

	// When resolving for "singapore"
	addr, err = s.ResolveDNS("singapore")
	// Then the singapore node is returned
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "10.0.0.3:9001" {
		t.Errorf("expected 10.0.0.3:9001, got %s", addr)
	}

	if s.HealthyNodes() != 3 {
		t.Errorf("expected 3 healthy nodes, got %d", s.HealthyNodes())
	}
}

func TestScheduler_ResolveDNS_UnhealthyAll(t *testing.T) {
	s := NewScheduler([]NodeEndpoint{
		{PeerID: "peer-1", Addr: "10.0.0.1:9001", Region: "beijing", Healthy: false},
		{PeerID: "peer-2", Addr: "10.0.0.2:9001", Region: "guangzhou", Healthy: false},
	})

	// Given all nodes are unhealthy
	// When resolving for any region
	addr, err := s.ResolveDNS("beijing")
	// Then an error is returned
	if err == nil {
		t.Fatal("expected error for all-unhealthy scenario")
	}
	if addr != "" {
		t.Errorf("expected empty addr, got %s", addr)
	}
}

func TestScheduler_HTTP302(t *testing.T) {
	s := NewScheduler([]NodeEndpoint{
		{PeerID: "peer-1", Addr: "10.0.0.1:9001", Region: "beijing", Healthy: false},
		{PeerID: "peer-2", Addr: "10.0.0.2:9001", Region: "beijing", Healthy: true},
	})

	// Given primary 10.0.0.1:9001 is unhealthy, backup 10.0.0.2:9001 is healthy
	// When a request arrives for the primary
	req := httptest.NewRequest(http.MethodGet, "http://10.0.0.1:9001/some/path", nil)
	rec := httptest.NewRecorder()
	s.HandleHTTP302(rec, req, "10.0.0.1:9001")

	// Then we get a 302 redirect to backup
	if rec.Code != http.StatusFound {
		t.Errorf("expected 302 Found, got %d", rec.Code)
	}
	loc := rec.Header().Get("Location")
	if loc != "http://10.0.0.2:9001/some/path" {
		t.Errorf("expected redirect to backup, got %q", loc)
	}
}

func TestScheduler_HTTP302_PrimaryHealthy(t *testing.T) {
	s := NewScheduler([]NodeEndpoint{
		{PeerID: "peer-1", Addr: "10.0.0.1:9001", Region: "beijing", Healthy: true},
		{PeerID: "peer-2", Addr: "10.0.0.2:9001", Region: "beijing", Healthy: true},
	})

	// Given the primary is healthy
	// When a request arrives for the primary
	req := httptest.NewRequest(http.MethodGet, "http://10.0.0.1:9001/path", nil)
	rec := httptest.NewRecorder()
	s.HandleHTTP302(rec, req, "10.0.0.1:9001")

	// Then no redirect is issued — caller handles it normally
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK (no-op), got %d", rec.Code)
	}
}

func TestScheduler_MarkUnhealthyHealthy(t *testing.T) {
	s := NewScheduler([]NodeEndpoint{
		{PeerID: "peer-1", Addr: "10.0.0.1:9001", Region: "beijing", Healthy: true},
	})

	// Given a healthy node
	// When marking it unhealthy
	s.MarkUnhealthy("peer-1")
	addr, err := s.ResolveDNS("beijing")
	// Then resolve fails
	if err == nil {
		t.Fatal("expected error after mark unhealthy")
	}
	_ = addr

	// When marking it healthy again
	s.MarkHealthy("peer-1")
	addr, err = s.ResolveDNS("beijing")
	// Then resolve succeeds
	if err != nil {
		t.Fatalf("expected success after re-mark healthy: %v", err)
	}
	if addr != "10.0.0.1:9001" {
		t.Errorf("expected 10.0.0.1:9001, got %s", addr)
	}
}

func TestScheduler_HTTP302_AllUnhealthy(t *testing.T) {
	s := NewScheduler([]NodeEndpoint{
		{PeerID: "peer-1", Addr: "10.0.0.1:9001", Region: "beijing", Healthy: false},
	})

	// Given no healthy nodes at all
	req := httptest.NewRequest(http.MethodGet, "http://10.0.0.1:9001/path", nil)
	rec := httptest.NewRecorder()
	s.HandleHTTP302(rec, req, "10.0.0.1:9001")

	// Then we get 503 Service Unavailable
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

// ─── Mock implementations ────────────────────────────────────────────────

// mockPrimaryChecker implements PrimaryChecker for tests.
type mockPrimaryChecker struct {
	mu        sync.RWMutex
	primaries map[string]types.PeerId // blobHash → primary PeerId
	isPrimary bool
	selfPeer  types.PeerId
}

func (m *mockPrimaryChecker) Get(blobHash string) types.PeerId {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if primary, ok := m.primaries[blobHash]; ok {
		return primary
	}
	return m.selfPeer // default to self
}

func (m *mockPrimaryChecker) IsPrimary(blobHash string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if primary, ok := m.primaries[blobHash]; ok {
		return primary == m.selfPeer
	}
	return m.isPrimary
}

// mockBackhaul implements BlobRouterBackhaul for tests.
type mockBackhaul struct {
	mu          sync.Mutex
	l4Called    int
	noL4Called  int
	l4Data      []byte
	noL4Data    []byte
	l4Err       error
	noL4Err     error
}

func (m *mockBackhaul) HandleBlobL4(_ context.Context, w io.Writer, _ string) error {
	m.mu.Lock()
	m.l4Called++
	m.mu.Unlock()
	if m.l4Err != nil {
		return m.l4Err
	}
	_, err := w.Write(m.l4Data)
	return err
}

func (m *mockBackhaul) HandleBlobNoL4(_ context.Context, w io.Writer, _ string) error {
	m.mu.Lock()
	m.noL4Called++
	m.mu.Unlock()
	if m.noL4Err != nil {
		return m.noL4Err
	}
	_, err := w.Write(m.noL4Data)
	return err
}

// mockPrefixCache implements PrefixCache for tests.
type mockPrefixCache struct {
	mu    sync.RWMutex
	store map[string][]byte
}

func newMockPrefixCache() *mockPrefixCache {
	return &mockPrefixCache{store: make(map[string][]byte)}
}

func (m *mockPrefixCache) Get(blobHash string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.store[blobHash]
	return data, ok
}

// ─── EdgeRouter tests ────────────────────────────────────────────────────

func TestEdgeRouter_IsPrimary(t *testing.T) {
	self := types.PeerId("self-peer")
	ring := &mockPrimaryChecker{
		primaries: map[string]types.PeerId{
			"blob-abc": self,
			"blob-xyz": "other-peer",
		},
		selfPeer: self,
	}
	bh := &mockBackhaul{}
	er := NewEdgeRouter(ring, bh, self, false, nil)

	// Given blob-abc primary is self
	// When checking isPrimaryNode("blob-abc")
	if !er.isPrimaryNode("blob-abc") {
		t.Error("expected self to be primary for blob-abc")
	}

	// Given blob-xyz primary is other-peer
	// When checking isPrimaryNode("blob-xyz")
	if er.isPrimaryNode("blob-xyz") {
		t.Error("expected self not to be primary for blob-xyz")
	}
}

func TestEdgeRouter_ServeAsPrimary_L4(t *testing.T) {
	self := types.PeerId("self-peer")
	ring := &mockPrimaryChecker{isPrimary: true, selfPeer: self}
	bh := &mockBackhaul{l4Data: []byte("from L4 data plane")}
	er := NewEdgeRouter(ring, bh, self, true, nil)

	// Given a primary node with L4 capability
	// When handling a blob request
	var buf bytes.Buffer
	err := er.HandleBlobRequest(context.Background(), &buf, "blob-test")

	// Then HandleBlobL4 is called
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "from L4 data plane" {
		t.Errorf("expected 'from L4 data plane', got %q", buf.String())
	}
	if bh.l4Called != 1 {
		t.Errorf("expected 1 L4 call, got %d", bh.l4Called)
	}
	if bh.noL4Called != 0 {
		t.Errorf("expected 0 noL4 calls, got %d", bh.noL4Called)
	}
}

func TestEdgeRouter_ServeAsPrimary_NoL4(t *testing.T) {
	self := types.PeerId("self-peer")
	ring := &mockPrimaryChecker{isPrimary: true, selfPeer: self}
	bh := &mockBackhaul{noL4Data: []byte("from L4 peer proxy")}
	er := NewEdgeRouter(ring, bh, self, false, nil)

	// Given a primary node without L4 capability
	// When handling a blob request
	var buf bytes.Buffer
	err := er.HandleBlobRequest(context.Background(), &buf, "blob-test")

	// Then HandleBlobNoL4 is called
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "from L4 peer proxy" {
		t.Errorf("expected 'from L4 peer proxy', got %q", buf.String())
	}
	if bh.noL4Called != 1 {
		t.Errorf("expected 1 noL4 call, got %d", bh.noL4Called)
	}
	if bh.l4Called != 0 {
		t.Errorf("expected 0 L4 calls, got %d", bh.l4Called)
	}
}

func TestEdgeRouter_HandlePrefixPull(t *testing.T) {
	self := types.PeerId("self-peer")
	ring := &mockPrimaryChecker{isPrimary: true, selfPeer: self}
	bh := &mockBackhaul{}
	er := NewEdgeRouter(ring, bh, self, false, nil)
	cache := newMockPrefixCache()
	cache.store["prefix-001"] = []byte("prefix blob data")
	er.SetPrefixCache(cache)

	// Given prefix blob "prefix-001" is cached
	// When pulling it through HandlePrefixPull
	var buf bytes.Buffer
	err := er.HandlePrefixPull(&buf, "prefix-001")

	// Then data is returned directly from cache
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "prefix blob data" {
		t.Errorf("expected 'prefix blob data', got %q", buf.String())
	}
}

func TestEdgeRouter_HandlePrefixPull_Miss(t *testing.T) {
	self := types.PeerId("self-peer")
	ring := &mockPrimaryChecker{isPrimary: true, selfPeer: self}
	bh := &mockBackhaul{}
	er := NewEdgeRouter(ring, bh, self, false, nil)
	cache := newMockPrefixCache()
	er.SetPrefixCache(cache)

	// Given prefix blob "prefix-missing" is NOT cached
	// When pulling via HandlePrefixPull
	var buf bytes.Buffer
	err := er.HandlePrefixPull(&buf, "prefix-missing")

	// Then an error is returned
	if err == nil {
		t.Fatal("expected error for prefix cache miss")
	}
}

// ─── Proxy-to-peer test (real libp2p hosts with PSK) ────────────────────

// genTestPSK creates a random 32-byte PSK for test hosts.
func genTestPSK(t *testing.T) types.PSK {
	t.Helper()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatalf("generate PSK: %v", err)
	}
	return types.PSK(psk)
}

// genTestHost creates a libp2p host with a fresh Ed25519 key.
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
	t.Cleanup(func() { h.Close() })
	return h
}

// connectHosts establishes a connection from h2 to h1 and ensures h2's
// peerstore has h1's addresses for subsequent NewStream calls.
func connectHosts(t *testing.T, ctx context.Context, h1, h2 host.Host) {
	t.Helper()
	// Explicitly populate the peerstore so NewStream can dial.
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)
	pi := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}
	if err := h2.Connect(ctx, pi); err != nil {
		t.Fatalf("connect h2 -> h1: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
}

// memoryBlobStore is a simple in-memory store matching the icp.BlobStore interface.
type memoryBlobStore struct {
	mu    sync.RWMutex
	blobs map[string][]byte
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
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestEdgeRouter_ProxyToPeer(t *testing.T) {
	psk := genTestPSK(t)

	// Primary node (h1) — has the blob, handles GET.
	h1 := genTestHost(t, psk)
	store := &memoryBlobStore{blobs: map[string][]byte{
		"blob-abc": []byte("hello from primary node!"),
	}}
	icp.RegisterHandlers(h1, store)

	// Non-primary node (h2) — will proxy requests to h1.
	h2 := genTestHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectHosts(t, ctx, h1, h2)

	// Given h2 is the non-primary node and h1 is primary for blob-abc
	self := types.PeerId(h2.ID())
	primary := types.PeerId(h1.ID())
	ring := &mockPrimaryChecker{
		primaries: map[string]types.PeerId{
			"blob-abc": primary,
		},
		selfPeer: self,
	}
	bh := &mockBackhaul{}
	er := NewEdgeRouter(ring, bh, self, false, h2)

	// Verify h2 knows h1's addresses for dialing.
	cachedAddrs := h2.Peerstore().Addrs(h1.ID())
	if len(cachedAddrs) == 0 {
		t.Fatal("h2 peerstore has no addresses for h1 — proxy will fail")
	}

	// When h2 handles a blob request for blob-abc
	var buf bytes.Buffer
	err := er.HandleBlobRequest(ctx, &buf, "blob-abc")

	// Then it proxies to h1 via libp2p stream and gets the data back
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "hello from primary node!" {
		t.Errorf("expected 'hello from primary node!', got %q", buf.String())
	}
}

// TestEdgeRouter_ProxyFallbackToLocal asserts availability-first behavior:
// when proxyToPeer fails (target unreachable / no stream handler), the
// request must fall back to serveAsPrimary on the local node, the bytes
// from the local backhaul must be served, and a Warn log must be emitted.
func TestEdgeRouter_ProxyFallbackToLocal(t *testing.T) {
	psk := genTestPSK(t)

	// Primary node (h1) — deliberately does NOT register ICP handlers,
	// so any stream h2 opens for /edge/blob/get/1.0.0 will be rejected.
	h1 := genTestHost(t, psk)

	// Non-primary node (h2) — router will proxy to h1, fail, fall back.
	h2 := genTestHost(t, psk)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connectHosts(t, ctx, h1, h2)

	self := types.PeerId(h2.ID())
	primary := types.PeerId(h1.ID())
	ring := &mockPrimaryChecker{
		primaries: map[string]types.PeerId{
			"blob-fallback": primary,
		},
		selfPeer: self,
	}
	// Local backhaul has the bytes — fallback path must serve them.
	localPayload := []byte("served from local fallback path")
	bh := &mockBackhaul{noL4Data: localPayload}
	er := NewEdgeRouter(ring, bh, self, false, h2)

	// Capture slog.Warn output to verify the warn was emitted.
	var logBuf bytes.Buffer
	prevDefault := slog.Default()
	defer slog.SetDefault(prevDefault)
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	// Given h1 is primary but has NO ICP stream handler registered
	// When h2 handles a blob request for blob-fallback
	var buf bytes.Buffer
	err := er.HandleBlobRequest(ctx, &buf, "blob-fallback")

	// Then proxyToPeer fails (stream rejected) and serveAsPrimary is called
	if err != nil {
		t.Fatalf("expected nil error after fallback, got: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), localPayload) {
		t.Errorf("expected local payload %q, got %q", localPayload, buf.String())
	}
	if bh.noL4Called != 1 {
		t.Errorf("expected 1 local noL4 call after fallback, got %d", bh.noL4Called)
	}

	// And a Warn log mentioning the fallback was emitted.
	logOut := logBuf.String()
	if !strings.Contains(logOut, "proxy to peer failed, falling back to local") {
		t.Errorf("expected warn log about fallback, got: %q", logOut)
	}
	if !strings.Contains(logOut, "blob-fallback") {
		t.Errorf("expected warn log to include blobHash, got: %q", logOut)
	}
}
