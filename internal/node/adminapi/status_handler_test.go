package adminapi

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Fakes ──────────────────────────────────────────────────────────────────

type fakeStatusJWT struct {
	current  types.CapabilityJWT
	degraded bool
	lastAt   time.Time
	lastOK   bool
	fails24h int
}

func (f *fakeStatusJWT) CurrentJWT() types.CapabilityJWT { return f.current }
func (f *fakeStatusJWT) IsDegraded() bool                { return f.degraded }
func (f *fakeStatusJWT) RefreshStats() (time.Time, bool, int) {
	return f.lastAt, f.lastOK, f.fails24h
}

type fakeScorer struct{ n int }

func (f fakeScorer) GraylistedCount() int { return f.n }

type fakeNetwork struct{ total, in, out int }

func (f fakeNetwork) ConnCounts() (int, int, int) { return f.total, f.in, f.out }

type fakeStatusBackhaul struct {
	hitRate float64
	p95     int64
}

func (f fakeStatusBackhaul) WarmCacheHitRate() float64 { return f.hitRate }
func (f fakeStatusBackhaul) TTFBP95Ms() int64          { return f.p95 }

// fakeConn embeds the network.Conn interface (nil) and overrides only Stat —
// the sole method Libp2pNetworkReporter's classification loop calls.
type fakeConn struct {
	network.Conn
	dir network.Direction
}

func (c fakeConn) Stat() network.ConnStats {
	return network.ConnStats{Stats: network.Stats{Direction: c.dir}}
}

type fakeConnSource struct{ conns []network.Conn }

func (s fakeConnSource) Conns() []network.Conn { return s.conns }

// ─── Helpers ────────────────────────────────────────────────────────────────

var testNow = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

func mustCapabilityJWT(t *testing.T, pub ed25519.PublicKey, priv ed25519.PrivateKey, exp int64) types.CapabilityJWT {
	t.Helper()
	token, err := sjwt.SignJWT(types.NodeJWTPayload{
		NodeID: "node-1", PeerID: "peer-1",
		Capabilities: types.NodeCapabilities{Edge: true},
		Iat:          exp - 3600, Exp: exp,
	}, priv)
	if err != nil {
		t.Fatalf("SignJWT: %v", err)
	}
	_ = pub
	return token
}

func statusDeps(token types.CapabilityJWT, cpPub ed25519.PublicKey) StatusDeps {
	return StatusDeps{
		PeerID: "12D3KooWTestPeer",
		Capabilities: types.NodeCapabilities{
			Edge: true, L4Backhaul: true, PeerICP: true,
		},
		L4Mode:             true,
		Region:             "cn",
		Version:            "v1.2.3",
		StartedAt:          testNow.Add(-2 * time.Hour),
		RefreshBefore:      5 * time.Minute,
		ControlPlanePubKey: cpPub,
		JWTClient: &fakeStatusJWT{
			current:  token,
			lastAt:   testNow.Add(-30 * time.Minute),
			lastOK:   true,
			fails24h: 2,
		},
		Scorer:   fakeScorer{n: 3},
		Network:  fakeNetwork{total: 10, in: 4, out: 6},
		Backhaul: fakeStatusBackhaul{hitRate: 0.83, p95: 142},
		Now:      func() time.Time { return testNow },
	}
}

func doStatus(t *testing.T, srv *Server, token string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	var body map[string]any
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
	}
	return rec.Code, body
}

// ─── Tests ──────────────────────────────────────────────────────────────────

// Given fully-wired deps (real signed JWT, all mocks), When GET /v1/status
// runs, Then every contract field is present with the mocked values.
func TestStatus_AllFields(t *testing.T) {
	cpPub, cpPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	// Use real clock: VerifyJWTAnyPeerID (which the handler calls) checks
	// against time.Now() — not the injected testNow clock.
	exp := time.Now().Add(time.Hour).Unix()
	token := mustCapabilityJWT(t, cpPub, cpPriv, exp)

	srv := NewServer("secret")
	RegisterStatusRoutes(srv, statusDeps(token, cpPub))

	code, body := doStatus(t, srv, "secret")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}

	if body["peer_id"] != "12D3KooWTestPeer" {
		t.Errorf("peer_id = %v", body["peer_id"])
	}
	caps, ok := body["capabilities"].([]any)
	if !ok || len(caps) != 3 || caps[0] != "edge" || caps[1] != "l4_backhaul" || caps[2] != "peer_icp" {
		t.Errorf("capabilities = %v, want [edge l4_backhaul peer_icp]", body["capabilities"])
	}
	if body["mode"] != "l4" {
		t.Errorf("mode = %v, want l4", body["mode"])
	}
	if body["region"] != "cn" || body["version"] != "v1.2.3" {
		t.Errorf("region/version = %v/%v", body["region"], body["version"])
	}
	if body["uptime_sec"] != float64(7200) {
		t.Errorf("uptime_sec = %v, want 7200", body["uptime_sec"])
	}
	if body["healthy"] != true {
		t.Errorf("healthy = %v, want true", body["healthy"])
	}

	jwt, _ := body["jwt"].(map[string]any)
	if jwt["exp"] != float64(exp) {
		t.Errorf("jwt.exp = %v, want %d", jwt["exp"], exp)
	}
	if jwt["refresh_before"] != float64(300) {
		t.Errorf("jwt.refresh_before = %v, want 300", jwt["refresh_before"])
	}
	if jwt["last_refresh_ok"] != true || jwt["refresh_fail_count_24h"] != float64(2) {
		t.Errorf("jwt refresh fields = %v", jwt)
	}
	if jwt["last_refresh_at"] == nil {
		t.Error("jwt.last_refresh_at should be present")
	}

	sv, _ := body["score_view"].(map[string]any)
	if sv["graylisted_peers"] != float64(3) {
		t.Errorf("graylisted_peers = %v, want 3 (from mock scorer)", sv["graylisted_peers"])
	}

	conn, _ := body["conn"].(map[string]any)
	if conn["total"] != float64(10) || conn["inbound"] != float64(4) || conn["outbound"] != float64(6) {
		t.Errorf("conn = %v, want 10/4/6", conn)
	}

	chr, _ := body["cache_hit_rate"].(map[string]any)
	if chr["warm"] != 0.83 {
		t.Errorf("cache_hit_rate.warm = %v, want 0.83", chr["warm"])
	}
	if chr["prefix"] != float64(0) {
		t.Errorf("cache_hit_rate.prefix = %v, want 0 (计数点未接入)", chr["prefix"])
	}

	if body["ttfb_p95_ms"] != float64(142) {
		t.Errorf("ttfb_p95_ms = %v, want 142", body["ttfb_p95_ms"])
	}
	if body["relay_bytes_24h"] != float64(0) {
		t.Errorf("relay_bytes_24h = %v, want 0 (计数点未接入)", body["relay_bytes_24h"])
	}
}

// Given a JWT client holding NO token, When GET /v1/status runs, Then
// jwt.exp is null and the handler does not panic.
func TestStatus_NoJWT_ExpNull(t *testing.T) {
	deps := statusDeps("", nil)
	deps.JWTClient = &fakeStatusJWT{current: ""}

	srv := NewServer("secret")
	RegisterStatusRoutes(srv, deps)

	code, body := doStatus(t, srv, "secret")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (no panic)", code)
	}
	jwt, _ := body["jwt"].(map[string]any)
	v, present := jwt["exp"]
	if !present || v != nil {
		t.Errorf("jwt.exp = %v (present=%v), want explicit null", v, present)
	}
	// Never attempted a refresh → last_refresh_at null too.
	if v := jwt["last_refresh_at"]; v != nil {
		t.Errorf("jwt.last_refresh_at = %v, want null", v)
	}
	// healthy still computed from IsDegraded (false here) → true.
	if body["healthy"] != true {
		t.Errorf("healthy = %v, want true (not degraded)", body["healthy"])
	}
}

// Given a degraded JWT client, When GET /v1/status runs, Then healthy=false.
func TestStatus_Degraded_NotHealthy(t *testing.T) {
	deps := statusDeps("", nil)
	deps.JWTClient = &fakeStatusJWT{degraded: true}

	srv := NewServer("secret")
	RegisterStatusRoutes(srv, deps)

	_, body := doStatus(t, srv, "secret")
	if body["healthy"] != false {
		t.Errorf("healthy = %v, want false when degraded", body["healthy"])
	}
}

// Given an expired JWT, When GET /v1/status runs, Then jwt.exp is null
// (VerifyJWTAnyPeerID rejects expired tokens).
func TestStatus_ExpiredJWT_ExpNull(t *testing.T) {
	cpPub, cpPriv, _ := ed25519.GenerateKey(rand.Reader)
	// Wall-clock relative: VerifyJWTAnyPeerID compares against time.Now(),
	// not the injected testNow clock.
	token := mustCapabilityJWT(t, cpPub, cpPriv, time.Now().Add(-time.Hour).Unix())
	deps := statusDeps(token, cpPub)
	deps.JWTClient = &fakeStatusJWT{current: token}

	srv := NewServer("secret")
	RegisterStatusRoutes(srv, deps)

	_, body := doStatus(t, srv, "secret")
	jwt, _ := body["jwt"].(map[string]any)
	if v := jwt["exp"]; v != nil {
		t.Errorf("jwt.exp = %v, want null for expired JWT", v)
	}
}

// Given zero-traffic backhaul stats, When GET /v1/status runs, Then
// cache_hit_rate.warm is exactly 0, not NaN/absent.
func TestStatus_ZeroTraffic_HitRateZero(t *testing.T) {
	deps := statusDeps("", nil)
	deps.Backhaul = fakeStatusBackhaul{hitRate: 0, p95: 0}

	srv := NewServer("secret")
	RegisterStatusRoutes(srv, deps)

	_, body := doStatus(t, srv, "secret")
	chr, _ := body["cache_hit_rate"].(map[string]any)
	warm, ok := chr["warm"].(float64)
	if !ok || warm != 0 || math.IsNaN(warm) {
		t.Errorf("cache_hit_rate.warm = %v, want 0 (not NaN, not absent)", chr["warm"])
	}
	if body["ttfb_p95_ms"] != float64(0) {
		t.Errorf("ttfb_p95_ms = %v, want 0 with no samples", body["ttfb_p95_ms"])
	}
}

// Given an L2 (non-L4) mode dep, When GET /v1/status runs, Then mode=edge.
func TestStatus_ModeEdge(t *testing.T) {
	deps := statusDeps("", nil)
	deps.L4Mode = false

	srv := NewServer("secret")
	RegisterStatusRoutes(srv, deps)

	_, body := doStatus(t, srv, "secret")
	if body["mode"] != "edge" {
		t.Errorf("mode = %v, want edge", body["mode"])
	}
}

// Given a wrong admin token, When GET /v1/status runs, Then 401.
func TestStatus_BadToken_401(t *testing.T) {
	srv := NewServer("secret")
	RegisterStatusRoutes(srv, statusDeps("", nil))

	code, _ := doStatus(t, srv, "wrong")
	if code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", code)
	}
}

// Given mixed-direction libp2p conns, When the adapter counts them, Then
// inbound/outbound/total classify correctly (DirUnknown → total only).
func TestLibp2pNetworkReporter_ClassifiesDirections(t *testing.T) {
	src := fakeConnSource{conns: []network.Conn{
		fakeConn{dir: network.DirInbound},
		fakeConn{dir: network.DirInbound},
		fakeConn{dir: network.DirOutbound},
		fakeConn{dir: network.DirUnknown},
	}}
	rep := Libp2pNetworkReporter(src)

	total, in, out := rep.ConnCounts()
	if total != 4 || in != 2 || out != 1 {
		t.Errorf("ConnCounts = %d/%d/%d, want 4/2/1", total, in, out)
	}
}
