package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// janitorYAML is the minimal valid janitor config used across tests.
const janitorYAML = `
metadata:
  pg_dsn: "postgres://user:pass@localhost:5432/db?sslmode=disable"

storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "baidu_01"
      client_id: "cid"
      client_secret: "csec"
      region: "cn"
      enabled: true
`

func writeJanitorYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "janitor.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write janitor yaml: %v", err)
	}
	return path
}

// TestLoadJanitorConfig_Defaults verifies that an empty GC stanza yields
// the documented defaults: Interval=1h, MinAge=24h, Grace=24h,
// BatchLimit=500, DryRun=true (via EffectiveDryRun), Once=false.
func TestLoadJanitorConfig_Defaults(t *testing.T) {
	path := writeJanitorYAML(t, janitorYAML)
	cfg, err := LoadJanitorConfig(path)
	if err != nil {
		t.Fatalf("LoadJanitorConfig: %v", err)
	}

	if cfg.GC.Interval != DefaultJanitorInterval {
		t.Errorf("Interval = %q, want %q", cfg.GC.Interval, DefaultJanitorInterval)
	}
	if cfg.GC.MinAge != DefaultJanitorMinAge {
		t.Errorf("MinAge = %q, want %q", cfg.GC.MinAge, DefaultJanitorMinAge)
	}
	if cfg.GC.Grace != DefaultJanitorGrace {
		t.Errorf("Grace = %q, want %q", cfg.GC.Grace, DefaultJanitorGrace)
	}
	if cfg.GC.BatchLimit != DefaultJanitorBatchLimit {
		t.Errorf("BatchLimit = %d, want %d", cfg.GC.BatchLimit, DefaultJanitorBatchLimit)
	}
	if !cfg.GC.EffectiveDryRun() {
		t.Errorf("EffectiveDryRun = false, want true (default)")
	}
	if cfg.GC.EffectiveOnce() {
		t.Errorf("EffectiveOnce = true, want false (default)")
	}

	if cfg.GC.ParsedInterval != time.Hour {
		t.Errorf("ParsedInterval = %v, want %v", cfg.GC.ParsedInterval, time.Hour)
	}
	if cfg.GC.ParsedMinAge != 24*time.Hour {
		t.Errorf("ParsedMinAge = %v, want %v", cfg.GC.ParsedMinAge, 24*time.Hour)
	}
	if cfg.GC.ParsedGrace != 24*time.Hour {
		t.Errorf("ParsedGrace = %v, want %v", cfg.GC.ParsedGrace, 24*time.Hour)
	}
}

// TestLoadJanitorConfig_Overrides verifies explicit YAML values override
// defaults and the *bool DryRun/Once fields are populated correctly.
func TestLoadJanitorConfig_Overrides(t *testing.T) {
	yaml := `
metadata:
  pg_dsn: "postgres://u:p@h:5432/db"
storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "b1"
      enabled: true
gc:
  interval: "30m"
  min_age: "6h"
  grace: "12h"
  batch_limit: 100
  dry_run: false
  once: true
`
	path := writeJanitorYAML(t, yaml)
	cfg, err := LoadJanitorConfig(path)
	if err != nil {
		t.Fatalf("LoadJanitorConfig: %v", err)
	}

	if cfg.GC.Interval != "30m" {
		t.Errorf("Interval = %q, want 30m", cfg.GC.Interval)
	}
	if cfg.GC.MinAge != "6h" {
		t.Errorf("MinAge = %q, want 6h", cfg.GC.MinAge)
	}
	if cfg.GC.Grace != "12h" {
		t.Errorf("Grace = %q, want 12h", cfg.GC.Grace)
	}
	if cfg.GC.BatchLimit != 100 {
		t.Errorf("BatchLimit = %d, want 100", cfg.GC.BatchLimit)
	}
	if cfg.GC.EffectiveDryRun() {
		t.Errorf("EffectiveDryRun = true, want false (explicit override)")
	}
	if !cfg.GC.EffectiveOnce() {
		t.Errorf("EffectiveOnce = false, want true (explicit override)")
	}
	if cfg.GC.DryRun == nil {
		t.Errorf("DryRun pointer = nil, want non-nil (explicit yaml value)")
	}
	if cfg.GC.Once == nil {
		t.Errorf("Once pointer = nil, want non-nil (explicit yaml value)")
	}

	if cfg.GC.ParsedInterval != 30*time.Minute {
		t.Errorf("ParsedInterval = %v, want %v", cfg.GC.ParsedInterval, 30*time.Minute)
	}
}

// TestLoadJanitorConfig_DryRunExplicitTrue verifies `dry_run: true` in YAML
// produces a non-nil pointer with value true (distinguishable from omitted).
func TestLoadJanitorConfig_DryRunExplicitTrue(t *testing.T) {
	yaml := `
metadata:
  pg_dsn: "postgres://u:p@h:5432/db"
storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "b1"
      enabled: true
gc:
  dry_run: true
`
	path := writeJanitorYAML(t, yaml)
	cfg, err := LoadJanitorConfig(path)
	if err != nil {
		t.Fatalf("LoadJanitorConfig: %v", err)
	}
	if cfg.GC.DryRun == nil {
		t.Fatalf("DryRun pointer = nil, want non-nil")
	}
	if !*cfg.GC.DryRun {
		t.Errorf("*DryRun = false, want true")
	}
	if !cfg.GC.EffectiveDryRun() {
		t.Errorf("EffectiveDryRun = false, want true")
	}
}

// TestLoadJanitorConfig_InvalidDuration verifies invalid duration strings
// produce a startup error (plan line 225: "invalid interval config → startup error").
func TestLoadJanitorConfig_InvalidDuration(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{
			name: "bad interval",
			yaml: `
metadata:
  pg_dsn: "postgres://u:p@h:5432/db"
storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "b1"
      enabled: true
gc:
  interval: "not-a-duration"
`,
		},
		{
			name: "bad min_age",
			yaml: `
metadata:
  pg_dsn: "postgres://u:p@h:5432/db"
storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "b1"
      enabled: true
gc:
  min_age: "abc"
`,
		},
		{
			name: "bad grace",
			yaml: `
metadata:
  pg_dsn: "postgres://u:p@h:5432/db"
storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "b1"
      enabled: true
gc:
  grace: "12x"
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeJanitorYAML(t, tc.yaml)
			_, err := LoadJanitorConfig(path)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), "config: gc.") {
				t.Errorf("error %q should mention config: gc.<field>", err.Error())
			}
		})
	}
}

// TestLoadJanitorConfig_ZeroDurationRejected verifies zero/negative durations
// are rejected (would cause an infinite-ticker or mark-nothing bug).
func TestLoadJanitorConfig_ZeroDurationRejected(t *testing.T) {
	yaml := `
metadata:
  pg_dsn: "postgres://u:p@h:5432/db"
storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "b1"
      enabled: true
gc:
  interval: "0s"
`
	path := writeJanitorYAML(t, yaml)
	_, err := LoadJanitorConfig(path)
	if err == nil {
		t.Fatalf("expected error for zero interval, got nil")
	}
	if !strings.Contains(err.Error(), "must be positive") {
		t.Errorf("error %q should say 'must be positive'", err.Error())
	}
}

// TestLoadJanitorConfig_MissingPGDSN verifies a missing pg_dsn fails loading.
func TestLoadJanitorConfig_MissingPGDSN(t *testing.T) {
	yaml := `
storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "b1"
      enabled: true
`
	path := writeJanitorYAML(t, yaml)
	_, err := LoadJanitorConfig(path)
	if err == nil {
		t.Fatalf("expected error for missing pg_dsn, got nil")
	}
	if !strings.Contains(err.Error(), "pg_dsn is required") {
		t.Errorf("error %q should mention pg_dsn is required", err.Error())
	}
}

// TestLoadJanitorConfig_NoEnabledAccounts verifies at least one enabled
// cloud account is required (the resolver needs to resolve backend_ids even
// in dry-run mode to log them).
func TestLoadJanitorConfig_NoEnabledAccounts(t *testing.T) {
	yaml := `
metadata:
  pg_dsn: "postgres://u:p@h:5432/db"
storage:
  cloud_accounts:
    - vendor: baidu
      account_id: "b1"
      enabled: false
`
	path := writeJanitorYAML(t, yaml)
	_, err := LoadJanitorConfig(path)
	if err == nil {
		t.Fatalf("expected error for no enabled accounts, got nil")
	}
	if !strings.Contains(err.Error(), "at least one enabled account") {
		t.Errorf("error %q should mention at least one enabled account", err.Error())
	}
}

// TestLoadJanitorConfig_NoAccounts verifies an empty cloud_accounts list
// also fails the enabled-account check.
func TestLoadJanitorConfig_NoAccounts(t *testing.T) {
	yaml := `
metadata:
  pg_dsn: "postgres://u:p@h:5432/db"
storage:
  cloud_accounts: []
`
	path := writeJanitorYAML(t, yaml)
	_, err := LoadJanitorConfig(path)
	if err == nil {
		t.Fatalf("expected error for empty cloud_accounts, got nil")
	}
}

// TestLoadJanitorConfig_FileError verifies a missing config file fails loading.
func TestLoadJanitorConfig_FileError(t *testing.T) {
	_, err := LoadJanitorConfig("/nonexistent/path/janitor.yaml")
	if err == nil {
		t.Fatalf("expected error for missing file, got nil")
	}
	if !strings.Contains(err.Error(), "read janitor config") {
		t.Errorf("error %q should mention read janitor config", err.Error())
	}
}
