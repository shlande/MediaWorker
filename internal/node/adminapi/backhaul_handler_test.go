package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/driver/mock"
	"github.com/shlande/mediaworker/internal/types"
)

type bhStats struct {
	successRate float64
	p95Ms       int64
	utilization float64
}

func (f *bhStats) Stats24h(time.Time) (float64, int64, int64) {
	return f.successRate, f.p95Ms, 0
}
func (f *bhStats) BackhaulUtilization() float64 { return f.utilization }

type bhLinkpool struct {
	entries int
	hitRate float64
}

func (f *bhLinkpool) Len() int         { return f.entries }
func (f *bhLinkpool) HitRate() float64 { return f.hitRate }

type bhAccountPool struct {
	accounts []*accountpool.Account
}

func (f *bhAccountPool) SnapshotAccounts() []*accountpool.Account { return f.accounts }

type bhCircuitBreaker struct {
	state int
}

func (f *bhCircuitBreaker) State() int  { return f.state }
func (f *bhCircuitBreaker) ForceOpen()  {}
func (f *bhCircuitBreaker) ForceClose() {}

func newBackhaulTestAccount(vendor types.Vendor, id, health string, cbState int, qps float64, inflight int32) *accountpool.Account {
	a := &accountpool.Account{
		Vendor:    vendor,
		AccountID: id,
		Driver: mock.NewMockDriver(vendor, mock.MockDriverConfig{
			RateLimit: types.RateLimitConfig{QPS: qps, Burst: 2, ConcurrentLimit: 8},
		}),
		CB: &bhCircuitBreaker{state: cbState},
	}
	a.Health.Store(types.HealthState{State: health})
	a.Concurrent.Store(inflight)
	return a
}

func doBackhaulGet(t *testing.T, s *Server, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/backhaul", nil)
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)
	return rr
}

func decodeBackhaulBody(t *testing.T, rr *httptest.ResponseRecorder) backhaulResponse {
	t.Helper()
	var resp backhaulResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode backhaul response: %v", err)
	}
	return resp
}

func backhaulFullDeps() BackhaulDeps {
	return BackhaulDeps{
		L4Enabled:            true,
		BackhaulCapacityMbps: 500,
		Stats:                &bhStats{successRate: 0.97, p95Ms: 140, utilization: 1250},
		Linkpool:             &bhLinkpool{entries: 42, hitRate: 0.9},
		Pool: &bhAccountPool{accounts: []*accountpool.Account{
			newBackhaulTestAccount(types.Vendor115, "acct_01", "healthy", accountpool.StateClosed, 1.0, 3),
			newBackhaulTestAccount(types.VendorBaidu, "acct_02", "degraded", accountpool.StateHalfOpen, 2.0, 0),
		}},
	}
}

// TestBackhaulHandler_NonL4_409 verifies non-L4 nodes get 409 with the exact
// contract error wording.
func TestBackhaulHandler_NonL4_409(t *testing.T) {
	s := NewServer(testToken)
	deps := backhaulFullDeps()
	deps.L4Enabled = false
	RegisterBackhaulRoutes(s, deps)

	rr := doBackhaulGet(t, s, testToken)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	body := strings.TrimSpace(rr.Body.String())
	want := `{"error":"node is not L4; backhaul unavailable"}`
	if body != want {
		t.Fatalf("body = %s, want %s", body, want)
	}
}

// TestBackhaulHandler_L4FullFields verifies every response field on an L4
// node with two pool accounts.
func TestBackhaulHandler_L4FullFields(t *testing.T) {
	s := NewServer(testToken)
	RegisterBackhaulRoutes(s, backhaulFullDeps())

	rr := doBackhaulGet(t, s, testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	resp := decodeBackhaulBody(t, rr)

	if resp.Bandwidth.UsedBps != 10000 { // 1250 bytes/s * 8
		t.Fatalf("bandwidth.used_bps = %v, want 10000", resp.Bandwidth.UsedBps)
	}
	if resp.Bandwidth.CapacityBps == nil || *resp.Bandwidth.CapacityBps != 500_000_000 {
		t.Fatalf("bandwidth.capacity_bps = %v, want 500000000", resp.Bandwidth.CapacityBps)
	}
	if resp.SuccessRate24h != 0.97 {
		t.Fatalf("success_rate_24h = %v, want 0.97", resp.SuccessRate24h)
	}
	if resp.LatencyP95Ms != 140 {
		t.Fatalf("latency_p95_ms = %d, want 140", resp.LatencyP95Ms)
	}
	if resp.Linkpool == nil {
		t.Fatal("linkpool is nil, want non-nil")
	}
	if resp.Linkpool.Entries != 42 || resp.Linkpool.HitRate != 0.9 {
		t.Fatalf("linkpool = %+v, want {42, 0.9}", resp.Linkpool)
	}

	if len(resp.Accounts) != 2 {
		t.Fatalf("accounts len = %d, want 2", len(resp.Accounts))
	}
	a0 := resp.Accounts[0]
	if a0.BackendID != "115:acct_01" {
		t.Fatalf("accounts[0].backend_id = %q, want 115:acct_01", a0.BackendID)
	}
	if a0.Health != "healthy" || a0.Circuit != "closed" {
		t.Fatalf("accounts[0] health/circuit = %q/%q, want healthy/closed", a0.Health, a0.Circuit)
	}
	if a0.QPS.Used != nil {
		t.Fatalf("accounts[0].qps.used = %v, want null (令牌桶无回读，v1 不报 used)", *a0.QPS.Used)
	}
	if a0.QPS.Limit != 1.0 {
		t.Fatalf("accounts[0].qps.limit = %v, want 1.0", a0.QPS.Limit)
	}
	if a0.Inflight != 3 {
		t.Fatalf("accounts[0].inflight = %d, want 3", a0.Inflight)
	}
	a1 := resp.Accounts[1]
	if a1.BackendID != "baidu:acct_02" || a1.Health != "degraded" || a1.Circuit != "half_open" {
		t.Fatalf("accounts[1] = %+v, want baidu:acct_02/degraded/half_open", a1)
	}
}

// TestBackhaulHandler_NilPool_AccountsEmpty verifies an L4 node whose pool
// snapshot has not arrived yet reports accounts as an empty array, not null.
func TestBackhaulHandler_NilPool_AccountsEmpty(t *testing.T) {
	s := NewServer(testToken)
	deps := backhaulFullDeps()
	deps.Pool = nil
	RegisterBackhaulRoutes(s, deps)

	rr := doBackhaulGet(t, s, testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"accounts":[]`) {
		t.Fatalf("body = %s, want accounts as []", rr.Body.String())
	}
}

// TestBackhaulHandler_CapacityZero_Null verifies capacity_bps is null when
// the operator did not declare backhaul_capacity_mbps.
func TestBackhaulHandler_CapacityZero_Null(t *testing.T) {
	s := NewServer(testToken)
	deps := backhaulFullDeps()
	deps.BackhaulCapacityMbps = 0
	RegisterBackhaulRoutes(s, deps)

	rr := doBackhaulGet(t, s, testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	resp := decodeBackhaulBody(t, rr)
	if resp.Bandwidth.CapacityBps != nil {
		t.Fatalf("capacity_bps = %v, want null", *resp.Bandwidth.CapacityBps)
	}
}

// TestBackhaulHandler_NilStatsAndLinkpool verifies zero-value degradation
// when the L4 node's stats / linkpool are not wired yet.
func TestBackhaulHandler_NilStatsAndLinkpool(t *testing.T) {
	s := NewServer(testToken)
	deps := backhaulFullDeps()
	deps.Stats = nil
	deps.Linkpool = nil
	RegisterBackhaulRoutes(s, deps)

	rr := doBackhaulGet(t, s, testToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	resp := decodeBackhaulBody(t, rr)
	if resp.Bandwidth.UsedBps != 0 || resp.SuccessRate24h != 0 || resp.LatencyP95Ms != 0 {
		t.Fatalf("stats fields = %+v/%v/%d, want zeros", resp.Bandwidth, resp.SuccessRate24h, resp.LatencyP95Ms)
	}
	if resp.Linkpool != nil {
		t.Fatalf("linkpool = %+v, want null", resp.Linkpool)
	}
	if len(resp.Accounts) != 2 {
		t.Fatalf("accounts len = %d, want 2", len(resp.Accounts))
	}
}

// TestBackhaulHandler_CircuitMapping verifies CB state int → wire string.
func TestBackhaulHandler_CircuitMapping(t *testing.T) {
	cases := []struct {
		name string
		cb   accountpool.CircuitBreaker
		want string
	}{
		{"closed", &bhCircuitBreaker{state: accountpool.StateClosed}, "closed"},
		{"half_open", &bhCircuitBreaker{state: accountpool.StateHalfOpen}, "half_open"},
		{"open", &bhCircuitBreaker{state: accountpool.StateOpen}, "open"},
		{"nil_breaker", nil, "closed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := circuitStateString(tc.cb); got != tc.want {
				t.Fatalf("circuitStateString = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBackhaulHandler_BadToken_401 verifies the backhaul handler requires auth.
func TestBackhaulHandler_BadToken_401(t *testing.T) {
	s := NewServer(testToken)
	RegisterBackhaulRoutes(s, backhaulFullDeps())

	rr := doBackhaulGet(t, s, "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d, want 401", rr.Code)
	}

	rr = doBackhaulGet(t, s, "wrong-token")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d, want 401", rr.Code)
	}
}
