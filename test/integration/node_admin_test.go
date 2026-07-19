// Node local admin API surface test (plan todo 49): assembles the adminapi
// Server with the SAME Register*Routes calls main.go uses, backed by real
// components where cheap (pinstore/planlog/warmcache/config file on temp
// dirs) and light fakes for the narrow libp2p-facing interfaces, then drives
// it over a REAL socket (srv.Serve on a free loopback port). The matrix
// covers routing + auth + assembly only — per-handler field semantics are
// covered by each handler's own unit tests and are intentionally NOT
// re-asserted here.
package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/node/adminapi"
	"github.com/shlande/mediaworker/internal/node/cache"
	"github.com/shlande/mediaworker/internal/node/netstats"
	"github.com/shlande/mediaworker/internal/node/pinstore"
	"github.com/shlande/mediaworker/internal/node/planlog"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Light fakes for the narrow interfaces (libp2p-facing) ───

type fakeHostView struct{ addrs []string }

func (f fakeHostView) Addrs() []ma.Multiaddr {
	out := make([]ma.Multiaddr, 0, len(f.addrs))
	for _, s := range f.addrs {
		if a, err := ma.NewMultiaddr(s); err == nil {
			out = append(out, a)
		}
	}
	return out
}

type fakeConnSource struct{}

func (fakeConnSource) Conns() []network.Conn { return nil }

type fakeDHTView struct{ size int }

func (f fakeDHTView) RoutingTableSize() int { return f.size }

type fakeRingView struct {
	size int
	pct  float64
	ok   bool
}

func (f fakeRingView) Size() int                    { return f.size }
func (f fakeRingView) PositionPct() (float64, bool) { return f.pct, f.ok }

type fakePeerStoreView struct{ entries []types.PeerStoreEntry }

func (f fakePeerStoreView) List() []types.PeerStoreEntry { return f.entries }

// ─── Server assembly mirroring main.go section 22b ───

const nodeAdminToken = "integration-admin-token"

type nodeAdminFixture struct {
	addr string
}

func minimalNodeYAML(token string) string {
	return `
node:
  identity:
    priv_key_path: "/data/key"
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
  jwt_service:
    endpoint: "https://cp.example.com/v1/node/jwt"
    refresh_interval: "5m"
    refresh_before_expiry: "5m"
hash_ring:
  replicas: 150
admin_api:
  listen: "127.0.0.1:8081"
  token: "` + token + `"
`
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

// assembleNodeAdmin builds the admin server exactly like main.go's
// consolidated block: every Register*Routes with the same dep wiring. l4
// toggles the backhaul assembly (L4Enabled) and warm-cache presence (flush
// 409 path on non-L4).
func assembleNodeAdmin(t *testing.T, l4 bool) *nodeAdminFixture {
	t.Helper()
	dir := t.TempDir()

	pinStore, err := pinstore.NewPinStore(
		filepath.Join(dir, "pin.db"),
		filepath.Join(dir, "prefix"),
		1<<30,
		func(string) ([]byte, error) { return nil, errors.New("no fetch in test") },
	)
	if err != nil {
		t.Fatalf("NewPinStore: %v", err)
	}
	t.Cleanup(func() { _ = pinStore.Close() })

	pLog := planlog.New()
	pLog.Add(planlog.Record{Seq: 1, ReceivedAt: time.Now(), Pins: 2, Unpins: 1, Applied: true})

	var warm *cache.WarmCache
	if l4 {
		warm = cache.NewWarmCache(filepath.Join(dir, "warm"), 1<<20, cache.NewMemoryIndex(), nil, nil)
	}
	// Mirror main.go's typed-nil guard: a nil *WarmCache must reach the
	// handlers as a nil interface (they branch on dep != nil).
	var warmReader adminapi.WarmCacheReader
	var warmFlusher adminapi.WarmCacheFlusher
	if warm != nil {
		warmReader = warm
		warmFlusher = warm
	}

	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(minimalNodeYAML(nodeAdminToken)), 0o644); err != nil {
		t.Fatal(err)
	}
	runningCfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	durations := &config.RefreshDurations{}
	durations.Store(runningCfg.Node.JWTService.ParsedRefreshInterval, runningCfg.Node.JWTService.ParsedRefreshBeforeExpiry)

	srv := adminapi.NewServer(nodeAdminToken)
	srv.Handle("GET /v1/healthz", func(w http.ResponseWriter, r *http.Request) {
		adminapi.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	adminapi.RegisterStatusRoutes(srv, adminapi.StatusDeps{
		PeerID:        "12D3KooWTestPeer",
		Capabilities:  types.NodeCapabilities{Edge: true, L4Backhaul: l4},
		L4Mode:        l4,
		StartedAt:     time.Now(),
		RefreshBefore: 5 * time.Minute,
	})
	adminapi.RegisterCacheRoutes(srv, pinStore, warmReader)
	adminapi.RegisterPinsRoutes(srv, pinStore, pLog)
	adminapi.RegisterNetworkRoutes(srv, adminapi.NetworkDeps{
		Host:    fakeHostView{addrs: []string{"/ip4/127.0.0.1/tcp/9001"}},
		Conns:   fakeConnSource{},
		DHT:     fakeDHTView{size: 3},
		DHTMode: "server",
		Ring:    fakeRingView{size: 2, pct: 0.5, ok: true},
		Peers: fakePeerStoreView{entries: []types.PeerStoreEntry{{
			PeerID:       "12D3KooWPeerA",
			Capabilities: types.NodeCapabilities{Edge: true},
			Score:        1.5,
			LastSeen:     1_700_000_000,
		}}},
		Stats: netstats.New(),
	})
	adminapi.RegisterBackhaulRoutes(srv, adminapi.BackhaulDeps{L4Enabled: l4})
	adminapi.NewReloader(cfgPath, runningCfg, durations).RegisterReloadRoutes(srv)
	adminapi.RegisterFlushRoutes(srv, warmFlusher)

	ctx, cancel := context.WithCancel(context.Background())
	addr := freeAddr(t)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, addr) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})

	// Wait for the listener to come up.
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("admin server did not come up within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	return &nodeAdminFixture{addr: addr}
}

func (f *nodeAdminFixture) do(t *testing.T, method, path, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+f.addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("X-Admin-Token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

// ─── The matrix ───

type routeCase struct {
	name   string
	method string
	path   string
	body   string
	want   int
}

func nodeAdminRoutes() []routeCase {
	return []routeCase{
		{"healthz", http.MethodGet, "/v1/healthz", "", http.StatusOK},
		{"status", http.MethodGet, "/v1/status", "", http.StatusOK},
		{"cache", http.MethodGet, "/v1/cache", "", http.StatusOK},
		{"pins", http.MethodGet, "/v1/pins", "", http.StatusOK},
		{"pin retry unknown", http.MethodPost, "/v1/pins/no-such-blob/retry", "", http.StatusNotFound},
		{"pin plans recent", http.MethodGet, "/v1/pin-plans/recent", "", http.StatusOK},
		{"network", http.MethodGet, "/v1/network", "", http.StatusOK},
		{"peers", http.MethodGet, "/v1/peers", "", http.StatusOK},
		{"backhaul l4", http.MethodGet, "/v1/backhaul", "", http.StatusOK},
		{"reload", http.MethodPost, "/v1/admin/reload-config", "", http.StatusOK},
		{"flush warm", http.MethodPost, "/v1/admin/flush-cache", `{"partitions":["warm"]}`, http.StatusAccepted},
	}
}

// Given the full L4 assembly, when every endpoint is hit without a token,
// then ALL return 401; with the right token, ALL return their success status.
func TestNodeAdmin_RouteAndAuthMatrix(t *testing.T) {
	fx := assembleNodeAdmin(t, true)

	for _, rc := range nodeAdminRoutes() {
		t.Run(rc.name, func(t *testing.T) {
			code, _ := fx.do(t, rc.method, rc.path, "", rc.body)
			if code != http.StatusUnauthorized {
				t.Fatalf("no token: %s %s = %d, want 401", rc.method, rc.path, code)
			}

			code, data := fx.do(t, rc.method, rc.path, nodeAdminToken, rc.body)
			if code != rc.want {
				t.Fatalf("with token: %s %s = %d, want %d (body %s)", rc.method, rc.path, code, rc.want, data)
			}
		})
	}
}

// Given the L4 assembly, when key endpoints respond, then their contract
// top-level fields are present (existence only — semantics live in unit tests).
func TestNodeAdmin_FieldPresenceSpotChecks(t *testing.T) {
	fx := assembleNodeAdmin(t, true)

	checks := []struct {
		path string
		keys []string
	}{
		{"/v1/status", []string{"peer_id", "capabilities", "jwt"}},
		{"/v1/cache", []string{"prefix", "warm", "eviction_counters"}},
		{"/v1/pins", []string{"pins", "summary"}},
		{"/v1/network", []string{"listen_addrs", "conn", "dht", "nat", "hash_ring"}},
		{"/v1/backhaul", []string{"bandwidth", "accounts"}},
	}
	for _, c := range checks {
		code, data := fx.do(t, http.MethodGet, c.path, nodeAdminToken, "")
		if code != http.StatusOK {
			t.Fatalf("%s: status %d", c.path, code)
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal(data, &body); err != nil {
			t.Fatalf("%s: decode: %v", c.path, err)
		}
		for _, k := range c.keys {
			if _, ok := body[k]; !ok {
				t.Fatalf("%s: missing key %q in %s", c.path, k, data)
			}
		}
	}
}

// Given the non-L4 assembly, when backhaul and flush are hit, then backhaul
// is 409 (node is not L4) and flush is 409 (warm cache not wired).
func TestNodeAdmin_NonL4Assembly(t *testing.T) {
	fx := assembleNodeAdmin(t, false)

	code, _ := fx.do(t, http.MethodGet, "/v1/backhaul", nodeAdminToken, "")
	if code != http.StatusConflict {
		t.Fatalf("non-L4 backhaul = %d, want 409", code)
	}

	code, _ = fx.do(t, http.MethodPost, "/v1/admin/flush-cache", nodeAdminToken, `{"partitions":["warm"]}`)
	if code != http.StatusConflict {
		t.Fatalf("non-L4 flush (warm nil) = %d, want 409", code)
	}

	// The shared read-only surface stays 200 on non-L4 too.
	for _, path := range []string{"/v1/status", "/v1/cache", "/v1/network", "/v1/peers"} {
		code, _ = fx.do(t, http.MethodGet, path, nodeAdminToken, "")
		if code != http.StatusOK {
			t.Fatalf("non-L4 %s = %d, want 200", path, code)
		}
	}
}
