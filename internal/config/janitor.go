// Package config — janitor: standalone GC service configuration.
//
// The janitor is an independent binary (NOT merged into control-plane or
// ingest-worker — plan line 221) that periodically runs the two-phase
// soft-delete garbage collector (T13's internal/storage/gc.Collector).
//
// DryRun defaults to true: a freshly-deployed janitor with an empty/missing
// gc stanza only LOGS what it would delete — never calls Driver.Remove and
// never DELETEs PG rows. An explicit `gc.dry_run: false` in YAML (or
// `-dry-run=false` on the CLI) is required to actually delete. This is the
// primary safety guard against accidental mass-deletion on misconfiguration
// (plan line 221 — "DryRun 语义不得被任何代码路径绕过").
//
// DryRun is a *bool (pointer) so we can distinguish "operator omitted
// dry_run" (nil → default true) from "operator wrote dry_run: false"
// (non-nil false). This mirrors the T6/T7 pattern for capability flags
// where Go's bool zero-value is ambiguous with an explicit "off".
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// JanitorConfig is the root configuration for the standalone janitor GC
// service (cmd/janitor).
type JanitorConfig struct {
	Metadata JanitorMetadataConfig `yaml:"metadata"`
	Storage  IngestStorageConfig   `yaml:"storage"` // reuse ingest-worker shape (cloud_accounts/vendor_profiles/rate_limits)
	GC       JanitorGCConfig       `yaml:"gc"`
}

// JanitorMetadataConfig holds the Postgres DSN for the metadata service.
// Identical to IngestMetadataConfig but kept as a distinct type so the
// janitor config tree is self-documenting and independent of future
// ingest-worker metadata changes.
type JanitorMetadataConfig struct {
	PGDSN string `yaml:"pg_dsn"`
}

// JanitorGCConfig controls the two-phase garbage collector.
type JanitorGCConfig struct {
	// Interval is the time between GC cycles in interval (long-run) mode.
	// Default "1h". Parsed into ParsedInterval (yaml:"-").
	Interval string `yaml:"interval"`

	// MinAge is the minimum age a blob must reach before MarkOrphans will
	// consider it (guards the in-flight ingest transaction window).
	// Default "24h". Parsed into ParsedMinAge (yaml:"-").
	MinAge string `yaml:"min_age"`

	// Grace is the window between soft-mark and hard-delete. A blob rescued
	// (content_blob reference appeared) during this window has its deleted_at
	// reset to NULL and is not deleted. Default "24h".
	// Parsed into ParsedGrace (yaml:"-").
	Grace string `yaml:"grace"`

	// BatchLimit caps the number of blob_hashes Sweep processes per cycle.
	// Default 500.
	BatchLimit int `yaml:"batch_limit"`

	// DryRun, when true, makes Sweep only LOG `would delete blob=... locations=N
	// backends=[...]` — Driver.Remove is never called and no PG DELETE is
	// issued. Default true. Plan line 221: "DryRun 语义不得被任何代码路径绕过".
	//
	// Pointer type: nil (field omitted in YAML) → default true. Explicit
	// `dry_run: false` → non-nil false. Explicit `dry_run: true` → non-nil
	// true. Use EffectiveDryRun() to read.
	DryRun *bool `yaml:"dry_run"`

	// Once selects single-run mode: run one GC cycle (phase 1 + phase 2) then
	// exit (exit 0 on success / 1 on error). Default false (interval mode).
	// Pointer for the same reason as DryRun — but Once=false is the safe
	// default, so a plain bool would also work. Using *bool for consistency.
	Once *bool `yaml:"once"`

	// Parsed fields — populated by LoadJanitorConfig, not unmarshaled from YAML.
	ParsedInterval time.Duration `yaml:"-"`
	ParsedMinAge   time.Duration `yaml:"-"`
	ParsedGrace    time.Duration `yaml:"-"`
}

// Default values for JanitorGCConfig. Centralised here so the loader and
// tests share the same source of truth.
const (
	DefaultJanitorInterval   = "1h"
	DefaultJanitorMinAge     = "24h"
	DefaultJanitorGrace      = "24h"
	DefaultJanitorBatchLimit = 500
	DefaultJanitorDryRun     = true
	DefaultJanitorOnce       = false
)

// EffectiveDryRun returns the resolved DryRun value, applying the default
// (true) when the YAML omitted the field. This is the ONLY safe accessor —
// never read DryRun directly.
func (gc *JanitorGCConfig) EffectiveDryRun() bool {
	if gc.DryRun == nil {
		return DefaultJanitorDryRun
	}
	return *gc.DryRun
}

// EffectiveOnce returns the resolved Once value, applying the default (false)
// when the YAML omitted the field.
func (gc *JanitorGCConfig) EffectiveOnce() bool {
	if gc.Once == nil {
		return DefaultJanitorOnce
	}
	return *gc.Once
}

// LoadJanitorConfig reads a YAML file at path, unmarshals it into
// JanitorConfig, applies defaults to the GC stanza, parses duration strings,
// validates required fields, and returns the parsed result.
//
// Validation:
//   - metadata.pg_dsn must be non-empty (janitor cannot operate without PG).
//   - At least one enabled cloud account is REQUIRED — Sweep must be able to
//     resolve backend_id → driver even in dry-run mode (the dry-run path
//     still enumerates locations + drivers for the "would delete" log line).
//     An empty pool makes the resolver return ok=false for every backend_id,
//     silently swallowing real deletable blobs; reject at config-load time.
//   - Duration strings (Interval/MinAge/Grace) must parse via time.ParseDuration.
//     Invalid → startup error (plan line 225: "invalid interval config → startup error").
func LoadJanitorConfig(path string) (*JanitorConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read janitor config: %w", err)
	}

	var cfg JanitorConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse janitor config: %w", err)
	}

	if cfg.Metadata.PGDSN == "" {
		return nil, fmt.Errorf("config: metadata.pg_dsn is required")
	}

	// Apply defaults to the GC stanza (string durations + batch_limit only —
	// DryRun/Once are *bool and resolved via Effective* accessors at use).
	applyJanitorGCDefaults(&cfg.GC)

	// Parse durations.
	if cfg.GC.ParsedInterval, err = time.ParseDuration(cfg.GC.Interval); err != nil {
		return nil, fmt.Errorf("config: gc.interval %q: %w", cfg.GC.Interval, err)
	}
	if cfg.GC.ParsedInterval <= 0 {
		return nil, fmt.Errorf("config: gc.interval must be positive, got %s", cfg.GC.Interval)
	}
	if cfg.GC.ParsedMinAge, err = time.ParseDuration(cfg.GC.MinAge); err != nil {
		return nil, fmt.Errorf("config: gc.min_age %q: %w", cfg.GC.MinAge, err)
	}
	if cfg.GC.ParsedMinAge <= 0 {
		return nil, fmt.Errorf("config: gc.min_age must be positive, got %s", cfg.GC.MinAge)
	}
	if cfg.GC.ParsedGrace, err = time.ParseDuration(cfg.GC.Grace); err != nil {
		return nil, fmt.Errorf("config: gc.grace %q: %w", cfg.GC.Grace, err)
	}
	if cfg.GC.ParsedGrace <= 0 {
		return nil, fmt.Errorf("config: gc.grace must be positive, got %s", cfg.GC.Grace)
	}

	// Validate at least one enabled cloud account — required even in dry-run.
	enabledCount := 0
	for _, acct := range cfg.Storage.CloudAccounts {
		if acct.Enabled {
			enabledCount++
		}
	}
	if enabledCount == 0 {
		return nil, fmt.Errorf("config: storage.cloud_accounts must have at least one enabled account")
	}

	return &cfg, nil
}

// applyJanitorGCDefaults fills empty/zero GC fields with the documented
// defaults. Mirrors the IngestWorkerConfig defaulting pattern. Does NOT
// touch DryRun/Once (*bool) — those are resolved via Effective* accessors.
func applyJanitorGCDefaults(gc *JanitorGCConfig) {
	if gc.Interval == "" {
		gc.Interval = DefaultJanitorInterval
	}
	if gc.MinAge == "" {
		gc.MinAge = DefaultJanitorMinAge
	}
	if gc.Grace == "" {
		gc.Grace = DefaultJanitorGrace
	}
	if gc.BatchLimit <= 0 {
		gc.BatchLimit = DefaultJanitorBatchLimit
	}
}
