package config

import (
	"testing"
	"time"
)

func TestRefreshDurations_StoreLoadRoundTrip(t *testing.T) {
	// Given/When: a stored cadence pair
	d := &RefreshDurations{}
	d.Store(10*time.Minute, 7*time.Minute)

	// Then: Load returns exactly what was stored
	iv, be := d.Load()
	if iv != 10*time.Minute || be != 7*time.Minute {
		t.Fatalf("Load = (%v, %v), want (10m, 7m)", iv, be)
	}
}

func TestRefreshDurations_ZeroValueLoadsZero(t *testing.T) {
	d := &RefreshDurations{}
	iv, be := d.Load()
	if iv != 0 || be != 0 {
		t.Fatalf("zero-value Load = (%v, %v), want (0, 0) — callers own the fallback", iv, be)
	}
}

// diffFixture builds a minimal normalized Config with the given knobs.
func diffFixture(refresh, before time.Duration, token string, replicas int) *Config {
	c := &Config{}
	c.Node.JWTService.ParsedRefreshInterval = refresh
	c.Node.JWTService.ParsedRefreshBeforeExpiry = before
	c.AdminAPI.Token = token
	c.HashRing.Replicas = replicas
	return c
}

func TestDiffForReload_NoChanges(t *testing.T) {
	// Given identical configs
	running := diffFixture(5*time.Minute, 5*time.Minute, "tok", 150)
	fresh := diffFixture(5*time.Minute, 5*time.Minute, "tok", 150)

	// When diffed
	c := DiffForReload(running, fresh)

	// Then: nothing applicable, nothing refused
	if c.RefreshInterval != nil || c.RefreshBeforeExpiry != nil || c.AdminToken != nil {
		t.Fatalf("expected no applicable changes, got %+v", c)
	}
	if len(c.NotApplied) != 0 {
		t.Fatalf("expected no not_applied entries, got %+v", c.NotApplied)
	}
}

func TestDiffForReload_WhitelistChanges(t *testing.T) {
	// Given changes confined to the three whitelisted fields
	running := diffFixture(5*time.Minute, 5*time.Minute, "tok-a", 150)
	fresh := diffFixture(10*time.Minute, 7*time.Minute, "tok-b", 150)

	// When diffed
	c := DiffForReload(running, fresh)

	// Then: all three surface with their new values; nothing refused
	if c.RefreshInterval == nil || *c.RefreshInterval != 10*time.Minute {
		t.Fatalf("RefreshInterval = %v, want 10m", c.RefreshInterval)
	}
	if c.RefreshBeforeExpiry == nil || *c.RefreshBeforeExpiry != 7*time.Minute {
		t.Fatalf("RefreshBeforeExpiry = %v, want 7m", c.RefreshBeforeExpiry)
	}
	if c.AdminToken == nil || *c.AdminToken != "tok-b" {
		t.Fatalf("AdminToken = %v, want tok-b", c.AdminToken)
	}
	if len(c.NotApplied) != 0 {
		t.Fatalf("expected no not_applied entries, got %+v", c.NotApplied)
	}
}

func TestDiffForReload_ReplicasRefused(t *testing.T) {
	// Given a hash_ring.replicas change
	running := diffFixture(5*time.Minute, 5*time.Minute, "tok", 150)
	fresh := diffFixture(5*time.Minute, 5*time.Minute, "tok", 300)

	// When diffed
	c := DiffForReload(running, fresh)

	// Then: replicas lands in not_applied with a reason
	if len(c.NotApplied) != 1 {
		t.Fatalf("expected 1 not_applied entry, got %+v", c.NotApplied)
	}
	if c.NotApplied[0].Field != ReloadFieldHashRingReplicas {
		t.Fatalf("field = %q, want %q", c.NotApplied[0].Field, ReloadFieldHashRingReplicas)
	}
	if c.NotApplied[0].Reason == "" {
		t.Fatal("reason must explain the refusal")
	}
}

func TestDiffForReload_RestartRequiredFields(t *testing.T) {
	// Given changes to listen address, identity key and a cache path
	running := diffFixture(5*time.Minute, 5*time.Minute, "tok", 150)
	running.Node.Libp2p.Listen = []string{"/ip4/0.0.0.0/tcp/9001"}
	running.Node.Identity.PrivKeyPath = "/data/a.key"
	running.Edge.WarmCache.Path = "/data/warm"
	fresh := diffFixture(5*time.Minute, 5*time.Minute, "tok", 150)
	fresh.Node.Libp2p.Listen = []string{"/ip4/0.0.0.0/tcp/9002"}
	fresh.Node.Identity.PrivKeyPath = "/data/b.key"
	fresh.Edge.WarmCache.Path = "/data/warm2"

	// When diffed
	c := DiffForReload(running, fresh)

	// Then: each is refused individually with a restart reason
	want := map[string]bool{
		"node.libp2p.listen":          false,
		"node.identity.priv_key_path": false,
		"edge.warm_cache.path":        false,
	}
	for _, na := range c.NotApplied {
		if _, ok := want[na.Field]; ok {
			want[na.Field] = true
		}
		if na.Reason == "" {
			t.Fatalf("entry %+v has empty reason", na)
		}
	}
	for field, seen := range want {
		if !seen {
			t.Fatalf("expected not_applied entry for %s, got %+v", field, c.NotApplied)
		}
	}
	// And: the catch-all must NOT double-report them
	for _, na := range c.NotApplied {
		if na.Field == reloadFieldOther {
			t.Fatalf("classified fields leaked into catch-all: %+v", c.NotApplied)
		}
	}
}

func TestDiffForReload_OtherFieldCollapsesToCatchAll(t *testing.T) {
	// Given a change outside the whitelist (node.region)
	running := diffFixture(5*time.Minute, 5*time.Minute, "tok", 150)
	fresh := diffFixture(5*time.Minute, 5*time.Minute, "tok", 150)
	fresh.Node.Region = "eu-west"

	// When diffed
	c := DiffForReload(running, fresh)

	// Then: a single catch-all not_applied entry, no applicable change
	if c.RefreshInterval != nil || c.RefreshBeforeExpiry != nil || c.AdminToken != nil {
		t.Fatalf("unexpected applicable change: %+v", c)
	}
	if len(c.NotApplied) != 1 || c.NotApplied[0].Field != reloadFieldOther {
		t.Fatalf("expected single catch-all entry, got %+v", c.NotApplied)
	}
}

func TestAdvanceReloadBaseline_AppliedFieldsAdvance(t *testing.T) {
	// Given a diff that applied token + interval but refused replicas
	running := diffFixture(5*time.Minute, 5*time.Minute, "tok-a", 150)
	fresh := diffFixture(10*time.Minute, 5*time.Minute, "tok-b", 300)
	c := DiffForReload(running, fresh)

	// When the baseline advances
	next := AdvanceReloadBaseline(running, fresh, c)

	// Then: applied fields take fresh values, refused keep running values
	if next.AdminAPI.Token != "tok-b" {
		t.Fatalf("baseline token = %q, want tok-b (applied)", next.AdminAPI.Token)
	}
	if next.Node.JWTService.ParsedRefreshInterval != 10*time.Minute {
		t.Fatalf("baseline interval = %v, want 10m (applied)", next.Node.JWTService.ParsedRefreshInterval)
	}
	if next.HashRing.Replicas != 150 {
		t.Fatalf("baseline replicas = %d, want 150 (refused field stays at runtime value)", next.HashRing.Replicas)
	}
	if running.AdminAPI.Token != "tok-a" {
		t.Fatal("running baseline must not be mutated")
	}
}

func TestAdvanceReloadBaseline_EnablesRevertDetection(t *testing.T) {
	// Given a token applied then reverted in yaml
	running := diffFixture(5*time.Minute, 5*time.Minute, "tok-a", 150)
	rotated := diffFixture(5*time.Minute, 5*time.Minute, "tok-b", 150)
	first := DiffForReload(running, rotated)
	baseline := AdvanceReloadBaseline(running, rotated, first)

	// When yaml reverts to the original token
	reverted := diffFixture(5*time.Minute, 5*time.Minute, "tok-a", 150)
	second := DiffForReload(baseline, reverted)

	// Then: the revert is detected as an applicable change (not swallowed)
	if second.AdminToken == nil || *second.AdminToken != "tok-a" {
		t.Fatalf("revert not detected: %+v", second)
	}
}
