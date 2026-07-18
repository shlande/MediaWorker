package quota

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── BorrowableLimiter tests ─────────────────────────────────────────────

func TestBorrowableLimiter_Allow_baseHasTokens(t *testing.T) {
	base := rate.NewLimiter(10, 10) // plenty of tokens
	bl := NewBorrowableLimiter(base)

	if !bl.Allow() {
		t.Fatal("expected Allow()=true when base has tokens")
	}
}

func TestBorrowableLimiter_Allow_baseExhausted_usesBorrowed(t *testing.T) {
	base := rate.NewLimiter(1.0, 10) // burst=10, maxBorrow = 10*3/10 = 3
	bl := NewBorrowableLimiter(base)

	// Consume all base tokens.
	for i := 0; i < 10; i++ {
		if !bl.Allow() {
			t.Fatalf("expected Allow()=true for base token %d", i)
		}
	}

	// Base is now exhausted. Grant up to maxBorrow tokens.
	bl.Grant(5, time.Now().Add(time.Hour))

	// Should have 3 borrowed tokens (maxBorrow).
	for i := 0; i < 3; i++ {
		if !bl.Allow() {
			t.Fatalf("expected Allow()=true for borrowed token %d", i)
		}
	}

	// Borrowed tokens exhausted too (3 out of 5 capped by maxBorrow).
	if bl.Allow() {
		t.Fatal("expected Allow()=false after borrowed tokens exhausted")
	}
}

func TestBorrowableLimiter_Allow_borrowedExpired(t *testing.T) {
	base := rate.NewLimiter(1, 5)
	bl := NewBorrowableLimiter(base)

	// Consume all base tokens.
	for i := 0; i < 5; i++ {
		bl.Allow()
	}

	// Grant with an already-expired deadline.
	bl.Grant(5, time.Now().Add(-time.Second))

	if bl.Allow() {
		t.Fatal("expected Allow()=false when borrowed tokens are expired")
	}
}

func TestBorrowableLimiter_Allow_noBorrowed(t *testing.T) {
	base := rate.NewLimiter(1, 5)
	bl := NewBorrowableLimiter(base)

	// Consume all base tokens.
	for i := 0; i < 5; i++ {
		bl.Allow()
	}

	// No borrowed tokens granted.
	if bl.Allow() {
		t.Fatal("expected Allow()=false when base exhausted and no borrowed tokens")
	}
}

func TestBorrowableLimiter_Grant_capsAtMaxBorrow(t *testing.T) {
	// Burst=10 → maxBorrow = 10*3/10 = 3
	base := rate.NewLimiter(1, 10)
	bl := NewBorrowableLimiter(base)

	// Consume all burst tokens to force borrow path.
	for i := 0; i < 10; i++ {
		bl.Allow()
	}

	// Grant 100, but maxBorrow caps at 3.
	bl.Grant(100, time.Now().Add(time.Hour))

	// Should have exactly 3 borrowed tokens.
	for i := 0; i < 3; i++ {
		if !bl.Allow() {
			t.Fatalf("expected Allow()=true for borrowed token %d", i)
		}
	}
	if bl.Allow() {
		t.Fatal("expected Allow()=false after maxBorrow borrowed tokens consumed")
	}
}

func TestBorrowableLimiter_Revoke(t *testing.T) {
	base := rate.NewLimiter(1, 5)
	bl := NewBorrowableLimiter(base)

	// Consume all base tokens.
	for i := 0; i < 5; i++ {
		bl.Allow()
	}

	bl.Grant(5, time.Now().Add(time.Hour))
	bl.Revoke()

	if bl.Allow() {
		t.Fatal("expected Allow()=false after Revoke")
	}
}

func TestBorrowableLimiter_SetLimit(t *testing.T) {
	base := rate.NewLimiter(100, 1) // burst=1
	bl := NewBorrowableLimiter(base)

	// Consume the initial token.
	bl.Allow()

	// base is now empty. Grant borrowed tokens.
	bl.Grant(3, time.Now().Add(time.Hour))

	// SetLimit on base should set the rate limit.
	bl.SetLimit(rate.Limit(1))

	// With 0 base tokens and 3 borrowed tokens, we should be able to use those.
	allowed := 0
	for i := 0; i < 5; i++ {
		if bl.Allow() {
			allowed++
		}
	}
	if allowed == 0 {
		t.Fatal("expected borrowed tokens to be usable after SetLimit")
	}
}

func TestBorrowableLimiter_compilesToLimiterInterface(t *testing.T) {
	// Compile-time assertion already exists in borrowable.go.
	// This test verifies that *BorrowableLimiter satisfies accountpool.Limiter
	// by attempting an interface conversion.
	bl := NewBorrowableLimiter(rate.NewLimiter(1, 1))
	var _ accountpool.Limiter = bl
}

// ─── Mock Broadcaster ─────────────────────────────────────────────────────

// mockBroadcaster records broadcast calls for test assertions.
type mockBroadcaster struct {
	mu     sync.Mutex
	events []broadcastCall
}

type broadcastCall struct {
	eventType string
	payload   any
}

func (mb *mockBroadcaster) Broadcast(eventType string, payload any) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.events = append(mb.events, broadcastCall{eventType: eventType, payload: payload})
	return nil
}

func (mb *mockBroadcaster) calls() []broadcastCall {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	c := make([]broadcastCall, len(mb.events))
	copy(c, mb.events)
	return c
}

// ─── QuotaAllocator tests ─────────────────────────────────────────────────

func TestQuotaAllocator_SetGlobalLimit(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})

	qa.mu.RLock()
	got, ok := qa.globalLimit["115:acct_01"]
	qa.mu.RUnlock()
	if !ok {
		t.Fatal("expected global limit to be set")
	}
	if got.QPS != 1.0 {
		t.Fatalf("expected QPS=1.0, got %f", got.QPS)
	}
}

func TestQuotaAllocator_Rebalance_threeNodes(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})
	qa.RegisterNode("115:acct_01", "node_A")
	qa.RegisterNode("115:acct_01", "node_B")
	qa.RegisterNode("115:acct_01", "node_C")

	qa.Rebalance(context.Background())

	qa.mu.RLock()
	allocA := qa.allocation["115:acct_01"]["node_A"]
	allocB := qa.allocation["115:acct_01"]["node_B"]
	allocC := qa.allocation["115:acct_01"]["node_C"]
	qa.mu.RUnlock()

	// baseShare = 1.0 * 0.8 / 3 = 0.2667; all loads default to 0.
	expected := 0.8 / 3.0
	tolerance := 0.01
	if diff := abs(allocA - expected); diff > tolerance {
		t.Fatalf("expected node_A share ≈%.4f, got %.4f", expected, allocA)
	}
	if diff := abs(allocB - expected); diff > tolerance {
		t.Fatalf("expected node_B share ≈%.4f, got %.4f", expected, allocB)
	}
	if diff := abs(allocC - expected); diff > tolerance {
		t.Fatalf("expected node_C share ≈%.4f, got %.4f", expected, allocC)
	}

	// Should have broadcast QUOTA_UPDATE.
	calls := mb.calls()
	if len(calls) < 1 {
		t.Fatal("expected at least 1 broadcast call")
	}
	if calls[0].eventType != types.EventQuotaUpdate {
		t.Fatalf("expected event QUOTA_UPDATE, got %s", calls[0].eventType)
	}
}

func TestQuotaAllocator_Rebalance_loadAdjustment(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})
	qa.RegisterNode("115:acct_01", "node_A")
	qa.RegisterNode("115:acct_01", "node_B")

	// node_A reports 100% load, node_B reports 0%.
	qa.ReportNodeLoad("115:acct_01", "node_A", 1.0)
	qa.ReportNodeLoad("115:acct_01", "node_B", 0.0)

	qa.Rebalance(context.Background())

	qa.mu.RLock()
	allocA := qa.allocation["115:acct_01"]["node_A"]
	allocB := qa.allocation["115:acct_01"]["node_B"]
	qa.mu.RUnlock()

	// baseShare = 1.0 * 0.8 / 2 = 0.4
	// node_A: 0.4 * (1.0 - 1.0*0.5) = 0.2
	// node_B: 0.4 * (1.0 - 0.0*0.5) = 0.4
	tolerance := 0.01
	if diff := abs(allocA - 0.2); diff > tolerance {
		t.Fatalf("expected node_A share 0.2, got %.4f", allocA)
	}
	if diff := abs(allocB - 0.4); diff > tolerance {
		t.Fatalf("expected node_B share 0.4, got %.4f", allocB)
	}
}

func TestQuotaAllocator_Rebalance_noNodes(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})

	// No nodes registered; Rebalance should not crash.
	qa.Rebalance(context.Background())
}

func TestQuotaAllocator_Rebalance_noAccounts(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	// No limits set; Rebalance should not crash.
	qa.Rebalance(context.Background())
}

func TestQuotaAllocator_HandleBorrowRequests_approve(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})
	qa.RegisterNode("115:acct_01", "node_A")
	qa.ReportActualUsage("115:acct_01", 0.5) // 50% usage < 80% threshold

	qa.Rebalance(context.Background())

	requests := []BorrowRequest{
		{AccountKey: "115:acct_01", Requested: 0.3},
	}

	qa.HandleBorrowRequests("node_A", requests)

	calls := mb.calls()
	borrowCalls := 0
	for _, c := range calls {
		if c.eventType == types.EventQuotaBorrow {
			borrowCalls++
		}
	}
	if borrowCalls != 1 {
		t.Fatalf("expected 1 QUOTA_BORROW broadcast, got %d", borrowCalls)
	}
}

func TestQuotaAllocator_HandleBorrowRequests_deny_whenUsageHigh(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})
	qa.RegisterNode("115:acct_01", "node_A")
	qa.ReportActualUsage("115:acct_01", 0.9) // 90% usage > 80% threshold

	qa.Rebalance(context.Background())

	requests := []BorrowRequest{
		{AccountKey: "115:acct_01", Requested: 0.3},
	}

	qa.HandleBorrowRequests("node_A", requests)

	calls := mb.calls()
	for _, c := range calls {
		if c.eventType == types.EventQuotaBorrow {
			t.Fatal("expected no QUOTA_BORROW broadcast when usage >= 80%")
		}
	}
}

func TestQuotaAllocator_HandleBorrowRequests_noGlobalLimit(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	requests := []BorrowRequest{
		{AccountKey: "nonexistent", Requested: 0.3},
	}

	// Should not panic if there's no global limit for the account.
	qa.HandleBorrowRequests("node_A", requests)
}

func TestQuotaAllocator_RegisterNode(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})

	// Register a node.
	qa.RegisterNode("115:acct_01", "node_A")
	qa.RegisterNode("115:acct_01", "node_B")

	qa.mu.RLock()
	alloc := qa.allocation["115:acct_01"]
	qa.mu.RUnlock()

	if _, ok := alloc["node_A"]; !ok {
		t.Fatal("expected node_A to be registered")
	}
	if _, ok := alloc["node_B"]; !ok {
		t.Fatal("expected node_B to be registered")
	}

	// Re-register should be a no-op.
	qa.RegisterNode("115:acct_01", "node_A")

	qa.mu.RLock()
	if len(qa.allocation["115:acct_01"]) != 2 {
		t.Fatalf("expected 2 nodes after double register, got %d", len(qa.allocation["115:acct_01"]))
	}
	qa.mu.RUnlock()
}

func TestQuotaAllocator_ReportNodeLoad(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})
	qa.RegisterNode("115:acct_01", "node_A")

	currentAlloc := qa.ReportNodeLoad("115:acct_01", "node_A", 0.5)
	// load should be recorded.
	qa.mu.RLock()
	load := qa.nodeLoads["115:acct_01"]["node_A"]
	qa.mu.RUnlock()
	if load != 0.5 {
		t.Fatalf("expected load=0.5, got %f", load)
	}
	_ = currentAlloc // unused but valid
}

func TestQuotaAllocator_ReportNodeLoad_clamped(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5})
	qa.RegisterNode("115:acct_01", "node_A")

	// Negative load should be clamped to 0.
	qa.ReportNodeLoad("115:acct_01", "node_A", -0.5)
	qa.mu.RLock()
	load := qa.nodeLoads["115:acct_01"]["node_A"]
	qa.mu.RUnlock()
	if load != 0.0 {
		t.Fatalf("expected load=0.0, got %f", load)
	}

	// Over-1.0 load should be clamped to 1.0.
	qa.ReportNodeLoad("115:acct_01", "node_A", 2.0)
	qa.mu.RLock()
	load = qa.nodeLoads["115:acct_01"]["node_A"]
	qa.mu.RUnlock()
	if load != 1.0 {
		t.Fatalf("expected load=1.0, got %f", load)
	}
}

func TestQuotaAllocator_Run_cancels(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancelled

	// Run should return promptly.
	done := make(chan struct{})
	go func() {
		qa.Run(ctx, time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s after cancellation")
	}
}

func TestQuotaAllocator_ReportActualUsage(t *testing.T) {
	mb := &mockBroadcaster{}
	qa := NewQuotaAllocator(mb)

	qa.ReportActualUsage("115:acct_01", 0.7)

	qa.mu.RLock()
	usage := qa.actualUsage["115:acct_01"]
	qa.mu.RUnlock()
	if usage != 0.7 {
		t.Fatalf("expected usage=0.7, got %f", usage)
	}
}

func TestBorrowableLimiter_concurrent(t *testing.T) {
	base := rate.NewLimiter(100, 100)
	bl := NewBorrowableLimiter(base)
	// maxBorrow = 100*3/10 = 30
	bl.Grant(100, time.Now().Add(time.Hour))

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if bl.Allow() {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	// We have 100 base + at most 30 borrowed tokens (maxBorrow).
	if allowed.Load() > 131 {
		t.Fatalf("expected at most ~130 allowed tokens, got %d", allowed.Load())
	}
}

// ─── Helper ────────────────────────────────────────────────────────────────

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
