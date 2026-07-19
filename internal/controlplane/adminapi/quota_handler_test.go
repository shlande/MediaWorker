package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/storage/quota"
	"github.com/shlande/mediaworker/internal/types"
)

type quotaNoopBroadcaster struct{}

func (quotaNoopBroadcaster) Broadcast(string, any) error { return nil }

type fakeNodeSnapshotter struct {
	views []noderegistry.NodeView
}

func (f fakeNodeSnapshotter) Snapshot() []noderegistry.NodeView { return f.views }

func freshView(peer types.PeerId) noderegistry.NodeView {
	return noderegistry.NodeView{PeerID: peer, NodeID: string(peer), ReceivedAt: time.Now()}
}

func quotaTestServer(t *testing.T, qa *quota.QuotaAllocator, nodes nodeSnapshotter) *httptest.Server {
	t.Helper()
	srv := NewServer(testAuthSecret)
	RegisterQuotaRoutes(srv, qa, nodes)
	return httptest.NewServer(srv.mux)
}

func adminToken(t *testing.T) string {
	t.Helper()
	token, err := SignUserToken(UserTokenPayload{
		UserID:   "uid-1",
		Username: "admin",
		Roles:    []string{"admin"},
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(time.Hour).Unix(),
	}, testAuthSecret)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return token
}

func getQuota(t *testing.T, ts *httptest.Server) (int, quotaResponse) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/quota", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+adminToken(t))
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("GET quota: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body quotaResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return resp.StatusCode, body
}

// Given 2 accounts × 2 online nodes with a completed rebalance, when GETting
// quota, then global_qps is summed across accounts and each node's
// base_share is summed across accounts.
func TestQuotaHandlerTwoAccountsTwoNodes(t *testing.T) {
	qa := quota.NewQuotaAllocator(quotaNoopBroadcaster{})
	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 100})
	qa.SetGlobalLimit("baidu:acct_02", types.RateLimitConfig{QPS: 50})
	for _, key := range qa.AccountKeys() {
		qa.RegisterNode(key, "node-a")
		qa.RegisterNode(key, "node-b")
	}
	qa.Rebalance(context.Background())

	nodes := fakeNodeSnapshotter{views: []noderegistry.NodeView{freshView("node-a"), freshView("node-b")}}
	ts := quotaTestServer(t, qa, nodes)
	defer ts.Close()

	status, body := getQuota(t, ts)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.GlobalQPS != 150 {
		t.Errorf("global_qps = %v, want 150", body.GlobalQPS)
	}
	if body.NodeCount != 2 {
		t.Errorf("node_count = %d, want 2", body.NodeCount)
	}
	if want := 150.0 * 0.8 / 2; body.BaseShare != want {
		t.Errorf("base_share = %v, want %v", body.BaseShare, want)
	}

	// Per node: 100*0.8/2 + 50*0.8/2 = 40 + 20 = 60.
	if len(body.Allocations) != 2 {
		t.Fatalf("allocations len = %d, want 2", len(body.Allocations))
	}
	for _, row := range body.Allocations {
		if row.BaseShare != 60 {
			t.Errorf("allocations[%s].base_share = %v, want 60", row.PeerID, row.BaseShare)
		}
	}
	if body.Allocations[0].PeerID > body.Allocations[1].PeerID {
		t.Errorf("allocations not sorted by peer_id: %v", body.Allocations)
	}
}

// Given accounts but zero online nodes, when GETting quota, then node_count
// is 0 and base_share uses the max(node_count,1) guard instead of dividing
// by zero.
func TestQuotaHandlerZeroNodesGuard(t *testing.T) {
	qa := quota.NewQuotaAllocator(quotaNoopBroadcaster{})
	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 100})

	ts := quotaTestServer(t, qa, fakeNodeSnapshotter{})
	defer ts.Close()

	status, body := getQuota(t, ts)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.NodeCount != 0 {
		t.Errorf("node_count = %d, want 0", body.NodeCount)
	}
	if want := 100.0 * 0.8; body.BaseShare != want {
		t.Errorf("base_share = %v, want %v (guard: divide by max(0,1)=1)", body.BaseShare, want)
	}
	if len(body.Allocations) != 0 {
		t.Errorf("allocations len = %d, want 0", len(body.Allocations))
	}
}

// Given a stale (offline) node view, when GETting quota, then the stale node
// is not counted as online.
func TestQuotaHandlerStaleNodeNotOnline(t *testing.T) {
	qa := quota.NewQuotaAllocator(quotaNoopBroadcaster{})
	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 100})

	stale := noderegistry.NodeView{PeerID: "node-old", ReceivedAt: time.Now().Add(-5 * time.Minute)}
	nodes := fakeNodeSnapshotter{views: []noderegistry.NodeView{stale, freshView("node-live")}}
	ts := quotaTestServer(t, qa, nodes)
	defer ts.Close()

	_, body := getQuota(t, ts)
	if body.NodeCount != 1 {
		t.Errorf("node_count = %d, want 1 (stale view excluded)", body.NodeCount)
	}
}

// Given zero accounts, when GETting quota, then the empty state is
// well-formed: global_qps 0 and allocations is an empty array (not null).
func TestQuotaHandlerEmptyState(t *testing.T) {
	qa := quota.NewQuotaAllocator(quotaNoopBroadcaster{})
	ts := quotaTestServer(t, qa, nil)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/quota", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+adminToken(t))
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("GET quota: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if raw["global_qps"] != 0.0 {
		t.Errorf("global_qps = %v, want 0", raw["global_qps"])
	}
	alloc, ok := raw["allocations"].([]any)
	if !ok {
		t.Fatalf("allocations = %v, want JSON array (not null/missing)", raw["allocations"])
	}
	if len(alloc) != 0 {
		t.Errorf("allocations len = %d, want 0", len(alloc))
	}
}

// Given no bearer token, when GETting quota, then the middleware rejects
// with 401.
func TestQuotaHandlerRequiresAuth(t *testing.T) {
	qa := quota.NewQuotaAllocator(quotaNoopBroadcaster{})
	ts := quotaTestServer(t, qa, nil)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/v1/admin/quota")
	if err != nil {
		t.Fatalf("GET quota: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}
