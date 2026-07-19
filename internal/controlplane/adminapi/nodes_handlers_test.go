package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mock NodesReader ──────────────────────────────────────────────────────

type mockNodesReader struct {
	views       []noderegistry.NodeView
	byPeerID    map[types.PeerId]noderegistry.NodeView
	issuances   map[types.PeerId]issuanceRecord
	shouldRenew map[types.PeerId]bool
}

type issuanceRecord struct {
	exp int64
	l4  bool
	ok  bool
}

func (m *mockNodesReader) Snapshot() []noderegistry.NodeView {
	return m.views
}

func (m *mockNodesReader) Get(peerID types.PeerId) (noderegistry.NodeView, bool) {
	v, ok := m.byPeerID[peerID]
	return v, ok
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

// ─── Mock history / pin-plan readers ───────────────────────────────────────

type dummyHistoryReader struct{}

func (*dummyHistoryReader) GetNodeStatusHistory(ctx context.Context, peerID string, limit int) ([]metadata.NodeStatusHistoryRow, error) {
	return nil, nil
}

type mockHistoryReader struct {
	rows []metadata.NodeStatusHistoryRow
	err  error
}

func (m *mockHistoryReader) GetNodeStatusHistory(ctx context.Context, peerID string, limit int) ([]metadata.NodeStatusHistoryRow, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.rows, nil
}

type dummyPinPlanLogReader struct{}

func (*dummyPinPlanLogReader) RecentByNode(nodeID string, limit int) []pinstrategy.DispatchRecord {
	return nil
}

type mockPinPlanLogReader struct {
	records []pinstrategy.DispatchRecord
}

func (m *mockPinPlanLogReader) RecentByNode(nodeID string, limit int) []pinstrategy.DispatchRecord {
	return m.records
}

// ─── slog helpers ───────────────────────────────────────────────────────────

func slogLogger() *slog.Logger       { return slog.New(slog.NewTextHandler(ioWriter{}, nil)) }
func discardLogger() *slog.Logger    { return slog.New(slog.NewTextHandler(discardWriter{}, nil)) }

type ioWriter struct{}
func (ioWriter) Write(p []byte) (n int, err error) { return len(p), nil }

type discardWriter struct{}
func (discardWriter) Write(p []byte) (n int, err error) { return len(p), nil }

// ─── Test helpers ───────────────────────────────────────────────────────────

func newNodesServer(reg NodesReader, nowFunc func() time.Time) (*Server, []byte) {
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterNodesRoutes(srv, reg, (*dummyHistoryReader)(nil), (*dummyPinPlanLogReader)(nil), slogLogger(), nowFunc)
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

func findNodeByPeerID(items []nodeListItemResponse, peerID string) *nodeListItemResponse {
	for i := range items {
		if items[i].PeerID == peerID {
			return &items[i]
		}
	}
	return nil
}

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

// ─── Detail test helpers ───────────────────────────────────────────────────

func newDetailServer(reg NodesReader, hr NodeHistoryReader, pl PinPlanLogReader) (*Server, []byte) {
	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterNodesRoutes(srv, reg, hr, pl, discardLogger(), func() time.Time { return testNow })
	return srv, secret
}

func getNodeDetail(t *testing.T, ts *httptest.Server, token string, peerID string) (*http.Response, nodeDetailResponse) {
	t.Helper()
	url := fmt.Sprintf("%s/v1/admin/nodes/%s", ts.URL, peerID)
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

	var body nodeDetailResponse
	if resp.StatusCode == http.StatusOK && resp.Body != nil {
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
	}
	return resp, body
}

func getNodeDetailRawJSON(t *testing.T, ts *httptest.Server, token string, peerID string) (*http.Response, string) {
	t.Helper()
	url := fmt.Sprintf("%s/v1/admin/nodes/%s", ts.URL, peerID)
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
		raw = make([]byte, 8192)
		n, _ := resp.Body.Read(raw)
		raw = raw[:n]
	}
	return resp, string(raw)
}

func ptrInt64(v int64) *int64    { return &v }
func ptrInt32(v int32) *int32    { return &v }
func ptrString(v string) *string { return &v }

// ─── Test data ──────────────────────────────────────────────────────────────

var (
	testNow = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	exp1    = time.Date(2026, 7, 20, 13, 0, 0, 0, time.UTC).Unix()
	exp2    = testNow.Add(5 * time.Minute).Unix()
	exp3    = testNow.Add(-5 * time.Minute).Unix()
	startedAt = testNow.Add(-3 * time.Hour).Unix()
)

// ─── List endpoint tests ───────────────────────────────────────────────────

func TestNodesList_Happy_ThreeNodes(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{
				PeerID: "peer-no-jwt", NodeID: "node-001",
				Capabilities: types.NodeCapabilities{Edge: true, L4Backhaul: true},
				PrefixSpace:  types.PartitionStatus{TotalBytes: 1000, UsedBytes: 200, BlobCount: 5},
				WarmSpace:    types.PartitionStatus{TotalBytes: 5000, UsedBytes: 1500, BlobCount: 30},
				Healthy: true, ReceivedAt: testNow.Add(-30 * time.Second),
				Region: "ap-northeast-1", Version: "v1.2.3", StartedAt: startedAt, ConnCount: 12,
			},
			{
				PeerID: "peer-normal-jwt", NodeID: "node-002",
				Capabilities: types.NodeCapabilities{Edge: true, RelayProvider: true},
				PrefixSpace:  types.PartitionStatus{TotalBytes: 2000, UsedBytes: 800, BlobCount: 12},
				WarmSpace:    types.PartitionStatus{TotalBytes: 10000, UsedBytes: 3000, BlobCount: 60},
				Healthy: true, ReceivedAt: testNow.Add(-15 * time.Second),
				Region: "ap-southeast-1", Version: "v1.2.3", StartedAt: startedAt, ConnCount: 8,
			},
			{
				PeerID: "peer-should-renew", NodeID: "node-003",
				Capabilities: types.NodeCapabilities{Edge: true, L4Backhaul: true, PeerICP: true},
				PrefixSpace:  types.PartitionStatus{TotalBytes: 3000, UsedBytes: 2500, BlobCount: 8},
				WarmSpace:    types.PartitionStatus{TotalBytes: 8000, UsedBytes: 6000, BlobCount: 45},
				Healthy: false, ReceivedAt: testNow.Add(-2 * time.Minute),
				Region: "us-west-1", Version: "v1.2.0", StartedAt: 0, ConnCount: 3,
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

	n1 := findNodeByPeerID(body, "peer-no-jwt")
	if n1 == nil {
		t.Fatal("peer-no-jwt missing")
	}
	if n1.JWT != nil {
		t.Errorf("peer-no-jwt.jwt = %+v, want nil", n1.JWT)
	}
	if n1.UptimeSec != 3*3600 {
		t.Errorf("uptime_sec = %d, want %d", n1.UptimeSec, 3*3600)
	}

	n2 := findNodeByPeerID(body, "peer-normal-jwt")
	if n2 == nil || n2.JWT == nil || n2.JWT.ShouldHaveRenewed {
		t.Error("peer-normal-jwt.jwt.should_have_renewed = true, want false")
	}

	n3 := findNodeByPeerID(body, "peer-should-renew")
	if n3 == nil || n3.JWT == nil || !n3.JWT.ShouldHaveRenewed {
		t.Error("peer-should-renew.jwt.should_have_renewed = false, want true")
	}
	if n3.UptimeSec != 0 {
		t.Errorf("uptime_sec = %d, want 0", n3.UptimeSec)
	}
}

func TestNodesList_NoScoreField(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{PeerID: "peer-1", NodeID: "n1", Capabilities: types.NodeCapabilities{Edge: true},
				PrefixSpace: types.PartitionStatus{TotalBytes: 1000, UsedBytes: 200, BlobCount: 5},
				WarmSpace: types.PartitionStatus{TotalBytes: 5000, UsedBytes: 1500, BlobCount: 30},
				Healthy: true, ReceivedAt: testNow, Region: "cn", Version: "v1", StartedAt: startedAt, ConnCount: 5},
		},
	}
	clock := func() time.Time { return testNow }
	srv, secret := newNodesServer(reg, clock)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	_, raw := getNodesRawJSON(t, ts, signAdminNodesToken(t, secret), "")
	if strings.Contains(raw, `"score"`) {
		t.Error("response contains \"score\"")
	}
}

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
	_, body := getNodesList(t, ts, signAdminNodesToken(t, secret), "healthy=true")
	if len(body) != 1 || body[0].PeerID != "peer-healthy" {
		t.Errorf("healthy=true: got %v, want [peer-healthy]", nodeIDs(body))
	}
	_, body = getNodesList(t, ts, signAdminNodesToken(t, secret), "healthy=false")
	if len(body) != 1 || body[0].PeerID != "peer-unhealthy" {
		t.Errorf("healthy=false: got %v, want [peer-unhealthy]", nodeIDs(body))
	}
}

func TestNodesList_CapabilityFilter(t *testing.T) {
	reg := &mockNodesReader{
		views: []noderegistry.NodeView{
			{PeerID: "peer-l4", Capabilities: types.NodeCapabilities{Edge: true, L4Backhaul: true}, Healthy: true, ReceivedAt: testNow},
			{PeerID: "peer-relay", Capabilities: types.NodeCapabilities{Edge: true, RelayProvider: true}, Healthy: true, ReceivedAt: testNow},
			{PeerID: "peer-icp", Capabilities: types.NodeCapabilities{Edge: true, PeerICP: true}, Healthy: true, ReceivedAt: testNow},
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
			if !slicesEqualUnordered(nodeIDs(body), tc.wantPeers) {
				t.Errorf("got %v, want %v", nodeIDs(body), tc.wantPeers)
			}
		})
	}
}

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

// ─── Detail endpoint tests ─────────────────────────────────────────────────

func TestNodeDetail_HappyFullData(t *testing.T) {
	reg := &mockNodesReader{
		byPeerID: map[types.PeerId]noderegistry.NodeView{
			"peer-ok": {
				PeerID: "peer-ok", NodeID: "node-ok",
				Capabilities: types.NodeCapabilities{Edge: true, L4Backhaul: true},
				PrefixSpace:  types.PartitionStatus{TotalBytes: 1000, UsedBytes: 200, BlobCount: 5},
				WarmSpace:    types.PartitionStatus{TotalBytes: 8000, UsedBytes: 3000, BlobCount: 45},
				ColdSpace:    nil,
				Healthy:      true, ReceivedAt: testNow,
				Region: "ap-northeast-1", Version: "v1.2.3", StartedAt: startedAt, ConnCount: 5,
			},
		},
		issuances:   map[types.PeerId]issuanceRecord{"peer-ok": {exp: exp1, l4: true, ok: true}},
		shouldRenew: map[types.PeerId]bool{"peer-ok": false},
	}

	ts := time.Date(2026, 7, 20, 11, 55, 0, 0, time.UTC)
	hr := &mockHistoryReader{rows: []metadata.NodeStatusHistoryRow{{
		ID: 1, PeerID: "peer-ok", NodeID: ptrString("node-ok"), Healthy: true,
		PrefixUsed: ptrInt64(200), PrefixTotal: ptrInt64(1000),
		WarmUsed: ptrInt64(3000), WarmTotal: ptrInt64(8000),
		ConnCount: ptrInt32(5), Region: ptrString("ap-northeast-1"), Version: ptrString("v1.2.3"),
		ReportedAt: ts, ReceivedAt: ts,
	}}}
	pl := &mockPinPlanLogReader{records: []pinstrategy.DispatchRecord{
		{Seq: 1, TargetNode: "node-ok", ContentID: "content-1", Pins: 3, Trigger: pinstrategy.TriggerAuto, SentAt: testNow.Add(-1 * time.Minute)},
	}}

	srv, secret := newDetailServer(reg, hr, pl)
	ts2 := httptest.NewServer(srv.mux)
	t.Cleanup(ts2.Close)
	token := signAdminNodesToken(t, secret)

	resp, body := getNodeDetail(t, ts2, token, "peer-ok")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if body.ColdSpace != nil {
		t.Errorf("cold_space = %+v, want nil", body.ColdSpace)
	}
	if len(body.RecentReports) != 1 {
		t.Fatalf("len(recent_reports) = %d, want 1", len(body.RecentReports))
	}
	if *body.RecentReports[0].PrefixUsed != 200 {
		t.Errorf("prefix_used = %d, want 200", *body.RecentReports[0].PrefixUsed)
	}
	if len(body.RecentPinPlans) != 1 {
		t.Fatalf("len(recent_pin_plans) = %d, want 1", len(body.RecentPinPlans))
	}
	if body.RecentPinPlans[0].ContentID != "content-1" {
		t.Errorf("content_id = %s, want content-1", body.RecentPinPlans[0].ContentID)
	}
}

func TestNodeDetail_HistoryErrorDegrades(t *testing.T) {
	reg := &mockNodesReader{
		byPeerID: map[types.PeerId]noderegistry.NodeView{
			"peer-ok": {PeerID: "peer-ok", NodeID: "node-ok", Capabilities: types.NodeCapabilities{Edge: true},
				Healthy: true, ReceivedAt: testNow, Region: "na", Version: "v1", StartedAt: startedAt},
		},
	}
	hr := &mockHistoryReader{err: fmt.Errorf("pg unavailable")}
	pl := &dummyPinPlanLogReader{}

	srv, secret := newDetailServer(reg, hr, pl)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminNodesToken(t, secret)

	resp, body := getNodeDetail(t, ts, token, "peer-ok")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body.RecentReports) != 0 {
		t.Errorf("recent_reports len = %d, want 0", len(body.RecentReports))
	}
}

func TestNodeDetail_UnknownPeer_404(t *testing.T) {
	reg := &mockNodesReader{byPeerID: map[types.PeerId]noderegistry.NodeView{}}
	srv, secret := newDetailServer(reg, &dummyHistoryReader{}, &dummyPinPlanLogReader{})
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	resp, _ := getNodeDetail(t, ts, signAdminNodesToken(t, secret), "nonexistent")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestNodeDetail_ColdSpacePresent(t *testing.T) {
	reg := &mockNodesReader{
		byPeerID: map[types.PeerId]noderegistry.NodeView{
			"peer-cold": {
				PeerID: "peer-cold", NodeID: "node-cold",
				ColdSpace:    &types.PartitionStatus{TotalBytes: 10000, UsedBytes: 5000, BlobCount: 20},
				Capabilities: types.NodeCapabilities{Edge: true},
				Healthy: true, ReceivedAt: testNow, Region: "us", Version: "v1", StartedAt: startedAt,
			},
		},
	}
	srv, secret := newDetailServer(reg, &dummyHistoryReader{}, &dummyPinPlanLogReader{})
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	resp, body := getNodeDetail(t, ts, signAdminNodesToken(t, secret), "peer-cold")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if body.ColdSpace == nil || body.ColdSpace.Used != 5000 || body.ColdSpace.Total != 10000 {
		t.Errorf("cold_space = %+v, want {used:5000 total:10000}", body.ColdSpace)
	}
}

func TestNodeDetail_NoToken_401(t *testing.T) {
	reg := &mockNodesReader{
		byPeerID: map[types.PeerId]noderegistry.NodeView{
			"peer-ok": {PeerID: "peer-ok", NodeID: "n1", Capabilities: types.NodeCapabilities{Edge: true}, Healthy: true, ReceivedAt: testNow},
		},
	}
	srv, _ := newDetailServer(reg, &dummyHistoryReader{}, &dummyPinPlanLogReader{})
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	resp, _ := getNodeDetail(t, ts, "", "peer-ok")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestNodeDetail_NoWhitelistOrScore(t *testing.T) {
	reg := &mockNodesReader{
		byPeerID: map[types.PeerId]noderegistry.NodeView{
			"peer-ok": {PeerID: "peer-ok", NodeID: "n1", Capabilities: types.NodeCapabilities{Edge: true},
				Healthy: true, ReceivedAt: testNow, Region: "cn", Version: "v1", StartedAt: startedAt},
		},
	}
	srv, secret := newDetailServer(reg, &dummyHistoryReader{}, &dummyPinPlanLogReader{})
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	_, raw := getNodeDetailRawJSON(t, ts, signAdminNodesToken(t, secret), "peer-ok")
	for _, banned := range []string{`"score"`, `"whitelist"`, `"graylist"`} {
		if strings.Contains(raw, banned) {
			t.Errorf("detail contains %s field", banned)
		}
	}
}
