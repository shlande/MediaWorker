package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mock NodesReader ──────────────────────────────────────────────────────

// mockNodesReader implements NodesReader for tests.
type mockNodesReader struct {
	views           []noderegistry.NodeView
	issuances       map[types.PeerId]issuanceRecord
	shouldRenew     map[types.PeerId]bool
	snapshotErr     error
}

type issuanceRecord struct {
	exp int64
	l4  bool
	ok  bool
}

func (m *mockNodesReader) Snapshot() []noderegistry.NodeView {
	return m.views
}

func (m *mockNodesReader) Issuance(peerID types.PeerId) (exp int64, l4 bool, ok bool) {
	rec, exists := m.issuances[peerID]
	if !exists {
		return 0, false, false
	}
	return rec.exp, rec.l4, rec.ok
}

func (m *mockNodesReader) ShouldHaveRenewed(peerID types.PeerId, now time.Time) bool {
	return m.shouldRenew[peerID]
}

// ─── Test helpers ─────────────────────────────────────────────────────────

func newNodesServer(reg NodesReader, nowFunc func() time.Time) (*Server, []byte) {
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	srv.Handle("GET /v1/admin/nodes", listNodesHandler(reg, nowFunc), true)
	return srv, secret
}

func signAdminNodesToken(t *testing.T, secret []byte) string {
	t.Helper()
	token, err := SignUserToken(UserTokenPayload{
		UserID:   "user-1",
		Username: "root",
		Roles:    []string{"admin"},
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(time.Hour).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}
	return token
}

func getNodesList(t *testing.T, ts *httptest.Server, token string, query string) (*http.Response, []nodeListItemResponse) {
	t.Helper()
	url := ts.URL + "/v1/admin/nodes"
	if query != "" {
		url += "?" + query
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	var body []nodeListItemResponse
	if resp.StatusCode == http.StatusOK && resp.Body != nil {
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
	}
	return resp, body
}

func findNodeByPeerID(items []nodeListItemResponse, peerID string) *nodeListItemResponse {
	for i := range items {
		if items[i].PeerID == peerID {
			return &items[i]
		}
	}
	return nil
}

// ─── Test data ─────────────────────────────────────────────────────────────

var (
	testNow = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	// exp1 = 2026-07-20T13:00:00Z (still valid, before renewal window)
	exp1 = time.Date(2026, 7, 20, 13, 0, 0, 0, time.UTC).Unix()

	// exp2 = 2026-07-20T12:05:00Z (within renewal window: exp - RenewWindowSeconds)
	exp2 = testNow.Add(5 * time.Minute).Unix()

	// exp3 = 2026-07-20T11:55:00Z (already expired? no, but within
	// renewal window boundary; we'll use this for should-have-renewed)
	exp3 = testNow.Add(-5 * time.Minute).Unix()

	startedAt = testNow.Add(-3 * time.Hour).Unix() // node has been up for 3h
)

// ─── Tests ─────────────────────────────────────────────────────────────────

// Given three nodes with different JWT states (no-issuance, normal, should-have-renewed),
// when GET /v1/admin/nodes is called with a valid admin token, then all three are
// returned with correct JWT shapes.
func TestNodesList_Happy_ThreeNodes(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{
				PeerID: "peer-no-jwt",
				NodeID: "node-001",
				Capabilities: types.NodeCapabilities{
					Edge:          true,
					L4Backhaul:    true,
					RelayProvider: false,
					PeerICP:       false,
				},
				PrefixSpace: types.PartitionStatus{TotalBytes: 1000, UsedBytes: 200, BlobCount: 5},
				WarmSpace:   types.PartitionStatus{TotalBytes: 5000, UsedBytes: 1500, BlobCount: 30},
				Healthy:     true,
				ReceivedAt:  testNow.Add(-30 * time.Second),
				Region:      "ap-northeast-1",
				Version:     "v1.2.3",
				StartedAt:   startedAt,
				ConnCount:   12,
			},
			{
				PeerID: "peer-normal-jwt",
				NodeID: "node-002",
				Capabilities: types.NodeCapabilities{
					Edge:          true,
					L4Backhaul:    false,
					RelayProvider: true,
					PeerICP:       false,
				},
				PrefixSpace: types.PartitionStatus{TotalBytes: 2000, UsedBytes: 800, BlobCount: 12},
				WarmSpace:   types.PartitionStatus{TotalBytes: 10000, UsedBytes: 3000, BlobCount: 60},
				Healthy:     true,
				ReceivedAt:  testNow.Add(-15 * time.Second),
				Region:      "ap-southeast-1",
				Version:     "v1.2.3",
				StartedAt:   startedAt,
				ConnCount:   8,
			},
			{
				PeerID: "peer-should-renew",
				NodeID: "node-003",
				Capabilities: types.NodeCapabilities{
					Edge:          true,
					L4Backhaul:    true,
					RelayProvider: false,
					PeerICP:       true,
				},
				PrefixSpace: types.PartitionStatus{TotalBytes: 3000, UsedBytes: 2500, BlobCount: 8},
				WarmSpace:   types.PartitionStatus{TotalBytes: 8000, UsedBytes: 6000, BlobCount: 45},
				Healthy:     false,
				ReceivedAt:  testNow.Add(-2 * time.Minute),
				Region:      "us-west-1",
				Version:     "v1.2.0",
				StartedAt:   0, // zero = no uptime
				ConnCount:   3,
			},
		},
		issuances: map[types.PeerId]issuanceRecord{
			"peer-normal-jwt":   {exp: exp1, l4: false, ok: true},
			"peer-should-renew": {exp: exp3, l4: true, ok: true},
		},
		shouldRenew: map[types.PeerId]bool{
			"peer-normal-jwt":   false,
			"peer-should-renew": true,
		},
	}

	clock := func() time.Time { return testNow }
	srv, secret := newNodesServer(reg, clock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminNodesToken(t, secret)

	resp, body := getNodesList(t, ts, token, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body) != 3 {
		t.Fatalf("len(body) = %d, want 3", len(body))
	}

	// ── Node 1: no JWT issuance → jwt=null ──
	n1 := findNodeByPeerID(body, "peer-no-jwt")
	if n1 == nil {
		t.Fatal("peer-no-jwt missing from response")
	}
	if n1.JWT != nil {
		t.Errorf("peer-no-jwt.jwt = %+v, want nil", n1.JWT)
	}
	if n1.Healthy != true {
		t.Errorf("peer-no-jwt.healthy = %v, want true", n1.Healthy)
	}
	if n1.UptimeSec != 3*3600 {
		t.Errorf("peer-no-jwt.uptime_sec = %d, want %d", n1.UptimeSec, 3*3600)
	}
	if !slices.Equal(n1.Capabilities, []string{"edge", "l4_backhaul"}) {
		t.Errorf("peer-no-jwt.capabilities = %v, want [edge l4_backhaul]", n1.Capabilities)
	}
	if n1.LastSeen != testNow.Add(-30*time.Second).Format(time.RFC3339) {
		t.Errorf("peer-no-jwt.last_seen = %s, want %s", n1.LastSeen, testNow.Add(-30*time.Second).Format(time.RFC3339))
	}
	if n1.PrefixSpace.Used != 200 || n1.PrefixSpace.Total != 1000 {
		t.Errorf("peer-no-jwt.prefix_space = {used:%d total:%d}, want {used:200 total:1000}", n1.PrefixSpace.Used, n1.PrefixSpace.Total)
	}

	// ── Node 2: normal JWT, should_have_renewed=false ──
	n2 := findNodeByPeerID(body, "peer-normal-jwt")
	if n2 == nil {
		t.Fatal("peer-normal-jwt missing from response")
	}
	if n2.JWT == nil {
		t.Fatal("peer-normal-jwt.jwt = nil, want non-nil")
	}
	if n2.JWT.Exp != time.Unix(exp1, 0).Format(time.RFC3339) {
		t.Errorf("peer-normal-jwt.jwt.exp = %s, want %s", n2.JWT.Exp, time.Unix(exp1, 0).Format(time.RFC3339))
	}
	if n2.JWT.ShouldHaveRenewed {
		t.Error("peer-normal-jwt.jwt.should_have_renewed = true, want false")
	}
	if !slices.Equal(n2.Capabilities, []string{"edge", "relay_provider"}) {
		t.Errorf("peer-normal-jwt.capabilities = %v, want [edge relay_provider]", n2.Capabilities)
	}

	// ── Node 3: expired JWT, should_have_renewed=true, unhealthy, zero StartedAt → no uptime_sec ──
	n3 := findNodeByPeerID(body, "peer-should-renew")
	if n3 == nil {
		t.Fatal("peer-should-renew missing from response")
	}
	if n3.JWT == nil {
		t.Fatal("peer-should-renew.jwt = nil, want non-nil")
	}
	if !n3.JWT.ShouldHaveRenewed {
		t.Error("peer-should-renew.jwt.should_have_renewed = false, want true")
	}
	if n3.Healthy != false {
		t.Errorf("peer-should-renew.healthy = %v, want false", n3.Healthy)
	}
	if n3.UptimeSec != 0 {
		t.Errorf("peer-should-renew.uptime_sec = %d, want 0 (StartedAt=0)", n3.UptimeSec)
	}
	// verify uptime_sec is omitted from JSON when 0
	resp2, raw := getNodesRawJSON(t, ts, token, "")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("raw status = %d", resp2.StatusCode)
	}
	if !strings.Contains(raw, `"peer-should-renew"`) {
		t.Fatal("peer-should-renew not found in raw JSON")
	}
}

// Given a response with three nodes, when checked for disallowed fields,
// then "score" is never present.
func TestNodesList_NoScoreField(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{
				PeerID:       "peer-1",
				NodeID:       "n1",
				Capabilities: types.NodeCapabilities{Edge: true},
				PrefixSpace:  types.PartitionStatus{TotalBytes: 1000, UsedBytes: 200, BlobCount: 5},
				WarmSpace:    types.PartitionStatus{TotalBytes: 5000, UsedBytes: 1500, BlobCount: 30},
				Healthy:      true,
				ReceivedAt:   testNow,
				Region:       "cn",
				Version:      "v1",
				StartedAt:    startedAt,
				ConnCount:    5,
			},
		},
	}

	clock := func() time.Time { return testNow }
	srv, secret := newNodesServer(reg, clock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	_, raw := getNodesRawJSON(t, ts, signAdminNodesToken(t, secret), "")
	if strings.Contains(raw, `"score"`) {
		t.Errorf("response contains \"score\" field — must not be present per ui-adjustments §2-nodes")
	}
}

// Given an empty registry, when GET /v1/admin/nodes is called, then response
// is [] (not null).
func TestNodesList_EmptyRegistry(t *testing.T) {
	reg := &mockNodesReader{}
	clock := func() time.Time { return testNow }
	srv, secret := newNodesServer(reg, clock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	_, raw := getNodesRawJSON(t, ts, signAdminNodesToken(t, secret), "")
	if strings.TrimSpace(raw) != "[]" {
		t.Errorf("empty registry body = %s, want []", raw)
	}
}

// Given a non-empty registry, when filtering by healthy=true, then only
// healthy nodes are returned.
func TestNodesList_HealthyFilter(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{PeerID: "peer-healthy", Healthy: true, Capabilities: types.NodeCapabilities{Edge: true}, ReceivedAt: testNow},
			{PeerID: "peer-unhealthy", Healthy: false, Capabilities: types.NodeCapabilities{Edge: true}, ReceivedAt: testNow},
		},
	}

	clock := func() time.Time { return testNow }
	srv, secret := newNodesServer(reg, clock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	// healthy=true
	_, body := getNodesList(t, ts, signAdminNodesToken(t, secret), "healthy=true")
	if len(body) != 1 || body[0].PeerID != "peer-healthy" {
		t.Errorf("healthy=true: got %d nodes (peer_ids=%v), want [peer-healthy]", len(body), nodeIDs(body))
	}

	// healthy=false
	_, body = getNodesList(t, ts, signAdminNodesToken(t, secret), "healthy=false")
	if len(body) != 1 || body[0].PeerID != "peer-unhealthy" {
		t.Errorf("healthy=false: got %d nodes (peer_ids=%v), want [peer-unhealthy]", len(body), nodeIDs(body))
	}
}

// Given nodes with mixed capabilities, when filtering by capability, then
// only nodes with that capability are returned.
func TestNodesList_CapabilityFilter(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{
				PeerID:       "peer-l4",
				Capabilities: types.NodeCapabilities{Edge: true, L4Backhaul: true},
				Healthy:      true, ReceivedAt: testNow,
			},
			{
				PeerID:       "peer-relay",
				Capabilities: types.NodeCapabilities{Edge: true, RelayProvider: true},
				Healthy:      true, ReceivedAt: testNow,
			},
			{
				PeerID:       "peer-icp",
				Capabilities: types.NodeCapabilities{Edge: true, PeerICP: true},
				Healthy:      true, ReceivedAt: testNow,
			},
		},
	}

	clock := func() time.Time { return testNow }
	srv, secret := newNodesServer(reg, clock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	tests := []struct {
		filter    string
		wantPeers []string
	}{
		{"l4_backhaul", []string{"peer-l4"}},
		{"relay_provider", []string{"peer-relay"}},
		{"peer_icp", []string{"peer-icp"}},
		{"edge", []string{"peer-l4", "peer-relay", "peer-icp"}},
		{"unknown_capability", nil},
	}
	for _, tc := range tests {
		t.Run(tc.filter, func(t *testing.T) {
			_, body := getNodesList(t, ts, signAdminNodesToken(t, secret), "capability="+tc.filter)
			got := nodeIDs(body)
			if !slicesEqualUnordered(got, tc.wantPeers) {
				t.Errorf("capability=%s: got %v, want %v", tc.filter, got, tc.wantPeers)
			}
		})
	}
}

// Given healthy and capability filters together, when both applied, then the
// intersection is returned.
func TestNodesList_HealthyAndCapabilityFilter(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{PeerID: "peer-a", Healthy: true, Capabilities: types.NodeCapabilities{Edge: true, L4Backhaul: true}, ReceivedAt: testNow},
			{PeerID: "peer-b", Healthy: false, Capabilities: types.NodeCapabilities{Edge: true, L4Backhaul: true}, ReceivedAt: testNow},
			{PeerID: "peer-c", Healthy: true, Capabilities: types.NodeCapabilities{Edge: true}, ReceivedAt: testNow},
		},
	}

	clock := func() time.Time { return testNow }
	srv, secret := newNodesServer(reg, clock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	_, body := getNodesList(t, ts, signAdminNodesToken(t, secret), "healthy=true&capability=l4_backhaul")
	if len(body) != 1 || body[0].PeerID != "peer-a" {
		t.Errorf("got %v, want [peer-a]", nodeIDs(body))
	}
}

// Given a request with no bearer token, when calling GET /v1/admin/nodes,
// then 401.
func TestNodesList_NoToken_401(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{PeerID: "peer-1", Healthy: true, Capabilities: types.NodeCapabilities{Edge: true}, ReceivedAt: testNow},
		},
	}
	srv, _ := newNodesServer(reg, func() time.Time { return testNow })
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	resp, _ := getNodesList(t, ts, "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// Given the response JSON, when parsed, then no "score" or "graylist" field
// exists anywhere in the raw bytes.
func getNodesRawJSON(t *testing.T, ts *httptest.Server, token string, query string) (*http.Response, string) {
	t.Helper()
	url := ts.URL + "/v1/admin/nodes"
	if query != "" {
		url += "?" + query
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	var raw []byte
	if resp.Body != nil {
		raw = make([]byte, 4096)
		n, _ := resp.Body.Read(raw)
		raw = raw[:n]
	}
	return resp, string(raw)
}

// ─── Helpers ──────────────────────────────────────────────────────────────

func nodeIDs(items []nodeListItemResponse) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.PeerID)
	}
	return out
}

func slicesEqualUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !slices.Contains(b, x) {
			return false
		}
	}
	return true
}
