package quota

import (
	"testing"

	"github.com/shlande/mediaworker/internal/types"
)

type noopBroadcaster struct{}

func (noopBroadcaster) Broadcast(string, any) error { return nil }

// Given two accounts with QPS limits, when reading GlobalQPS, then the sum
// of both limits is returned.
func TestQuotaGlobalQPSSumsAccounts(t *testing.T) {
	qa := NewQuotaAllocator(noopBroadcaster{})
	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 100})
	qa.SetGlobalLimit("baidu:acct_02", types.RateLimitConfig{QPS: 50.5})

	if got, want := qa.GlobalQPS(), 150.5; got != want {
		t.Errorf("GlobalQPS = %v, want %v", got, want)
	}
}

// Given registered nodes, when mutating the returned Allocations map, then
// the allocator's internal state is unchanged (deep copy isolation).
func TestQuotaAllocationsReturnsDeepCopy(t *testing.T) {
	qa := NewQuotaAllocator(noopBroadcaster{})
	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 100})
	qa.RegisterNode("115:acct_01", "node-a")

	snap := qa.Allocations()
	snap["115:acct_01"]["node-a"] = 9999
	snap["injected"] = map[string]float64{"x": 1}
	delete(snap, "115:acct_01")

	fresh := qa.Allocations()
	if got := fresh["115:acct_01"]["node-a"]; got != 0.0 {
		t.Errorf("internal allocation mutated via returned map: got %v, want 0.0", got)
	}
	if _, ok := fresh["injected"]; ok {
		t.Error("injected account key leaked into allocator")
	}
}

// Given limits set at runtime, when listing AccountKeys, then all keys are
// present and mutating the slice does not affect the allocator.
func TestQuotaAccountKeysListsAllLimits(t *testing.T) {
	qa := NewQuotaAllocator(noopBroadcaster{})
	if got := len(qa.AccountKeys()); got != 0 {
		t.Fatalf("AccountKeys len = %d, want 0 for fresh allocator", got)
	}
	qa.SetGlobalLimit("115:acct_01", types.RateLimitConfig{QPS: 10})
	qa.SetGlobalLimit("quark:acct_09", types.RateLimitConfig{QPS: 20})

	keys := qa.AccountKeys()
	if len(keys) != 2 {
		t.Fatalf("AccountKeys len = %d, want 2", len(keys))
	}
	seen := map[string]bool{}
	for _, k := range keys {
		seen[k] = true
	}
	for _, want := range []string{"115:acct_01", "quark:acct_09"} {
		if !seen[want] {
			t.Errorf("AccountKeys missing %q (got %v)", want, keys)
		}
	}

	keys[0] = "mutated"
	for _, k := range qa.AccountKeys() {
		if k == "mutated" {
			t.Error("mutating returned slice leaked into allocator")
		}
	}
}
