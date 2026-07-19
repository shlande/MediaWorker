package config

import (
	"reflect"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Hot-reload whitelist (node admin POST /v1/admin/reload-config)
// ---------------------------------------------------------------------------

// Field names reported by the reload endpoint. Only the three whitelisted
// fields are hot-applied; everything else lands in NotApplied.
const (
	ReloadFieldJWTRefreshInterval     = "jwt_service.refresh_interval"
	ReloadFieldJWTRefreshBeforeExpiry = "jwt_service.refresh_before_expiry"
	ReloadFieldAdminToken             = "admin_api.token"
	ReloadFieldHashRingReplicas       = "hash_ring.replicas"

	// reloadFieldOther is the catch-all entry for any change outside the
	// classified fields (whitelist + named restart-required fields).
	reloadFieldOther = "(other fields)"
)

// reloadReasonRestart is the reason attached to fields that cannot be applied
// without a process restart.
const reloadReasonRestart = "requires restart"

// ReloadNotApplied is one field the reload endpoint refused to apply.
type ReloadNotApplied struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// ReloadReport is the response body of POST /v1/admin/reload-config.
type ReloadReport struct {
	Applied    []string           `json:"applied"`
	NotApplied []ReloadNotApplied `json:"not_applied"`
}

// RefreshDurations shares the JWT refresh cadence between the running refresh
// loop (reads every round) and the config reload endpoint (writes on hot
// reload). Zero value is ready to use; callers apply their own <=0 fallbacks.
type RefreshDurations struct {
	interval     atomic.Int64 // nanoseconds
	beforeExpiry atomic.Int64 // nanoseconds
}

// Store atomically publishes a new cadence pair.
func (d *RefreshDurations) Store(interval, beforeExpiry time.Duration) {
	d.interval.Store(int64(interval))
	d.beforeExpiry.Store(int64(beforeExpiry))
}

// Load atomically reads the current cadence pair.
func (d *RefreshDurations) Load() (interval, beforeExpiry time.Duration) {
	return time.Duration(d.interval.Load()), time.Duration(d.beforeExpiry.Load())
}

// ReloadChanges classifies the delta between the running config and a freshly
// loaded one. Whitelisted fields carry their new values for the applier;
// refused and restart-required changes are already rendered as NotApplied.
type ReloadChanges struct {
	RefreshInterval     *time.Duration // non-nil when the normalized value changed
	RefreshBeforeExpiry *time.Duration
	AdminToken          *string
	NotApplied          []ReloadNotApplied
}

// DiffForReload compares running against fresh within the reload domain:
// whitelisted fields surface as applicable changes, hash_ring.replicas and
// the named restart-required fields (listen addresses, identity key, cache
// paths) are refused individually, and any remaining change collapses into a
// single catch-all NotApplied entry (no full hot reload).
//
// Duration comparison uses the normalized Parsed* values so a cosmetic yaml
// change ("5m" -> "300s") is not reported as a diff.
func DiffForReload(running, fresh *Config) *ReloadChanges {
	c := &ReloadChanges{}

	if running.Node.JWTService.ParsedRefreshInterval != fresh.Node.JWTService.ParsedRefreshInterval {
		v := fresh.Node.JWTService.ParsedRefreshInterval
		c.RefreshInterval = &v
	}
	if running.Node.JWTService.ParsedRefreshBeforeExpiry != fresh.Node.JWTService.ParsedRefreshBeforeExpiry {
		v := fresh.Node.JWTService.ParsedRefreshBeforeExpiry
		c.RefreshBeforeExpiry = &v
	}
	if running.AdminAPI.Token != fresh.AdminAPI.Token {
		v := fresh.AdminAPI.Token
		c.AdminToken = &v
	}

	if running.HashRing.Replicas != fresh.HashRing.Replicas {
		c.NotApplied = append(c.NotApplied, ReloadNotApplied{
			Field:  ReloadFieldHashRingReplicas,
			Reason: reloadReasonRestart + ": ring rebuild is not hot-reloadable",
		})
	}
	for _, f := range restartRequiredChanges(running, fresh) {
		c.NotApplied = append(c.NotApplied, ReloadNotApplied{Field: f, Reason: reloadReasonRestart})
	}

	restRunning, restFresh := *running, *fresh
	scrubReloadClassified(&restRunning)
	scrubReloadClassified(&restFresh)
	if !reflect.DeepEqual(restRunning, restFresh) {
		c.NotApplied = append(c.NotApplied, ReloadNotApplied{
			Field:  reloadFieldOther,
			Reason: "outside hot-reload whitelist; " + reloadReasonRestart,
		})
	}

	return c
}

// restartRequiredChanges lists the named fields that changed but need a
// process restart: listen addresses, identity key path, cache paths.
func restartRequiredChanges(running, fresh *Config) []string {
	var out []string
	if !reflect.DeepEqual(running.Node.Libp2p.Listen, fresh.Node.Libp2p.Listen) {
		out = append(out, "node.libp2p.listen")
	}
	if running.AdminAPI.Listen != fresh.AdminAPI.Listen {
		out = append(out, "admin_api.listen")
	}
	if running.Node.Identity.PrivKeyPath != fresh.Node.Identity.PrivKeyPath {
		out = append(out, "node.identity.priv_key_path")
	}
	if running.Edge.PrefixCache.Path != fresh.Edge.PrefixCache.Path {
		out = append(out, "edge.prefix_cache.path")
	}
	if running.Edge.WarmCache.Path != fresh.Edge.WarmCache.Path {
		out = append(out, "edge.warm_cache.path")
	}
	return out
}

// scrubReloadClassified zeroes every field DiffForReload has already
// classified (whitelist + refused + restart-required) so the DeepEqual
// catch-all only fires on truly unhandled changes.
func scrubReloadClassified(c *Config) {
	c.Node.JWTService.RefreshInterval = ""
	c.Node.JWTService.RefreshBeforeExpiry = ""
	c.Node.JWTService.ParsedRefreshInterval = 0
	c.Node.JWTService.ParsedRefreshBeforeExpiry = 0
	c.AdminAPI.Token = ""
	c.HashRing.Replicas = 0
	c.Node.Libp2p.Listen = nil
	c.AdminAPI.Listen = ""
	c.Node.Identity.PrivKeyPath = ""
	c.Edge.PrefixCache.Path = ""
	c.Edge.WarmCache.Path = ""
}

// AdvanceReloadBaseline returns a copy of running with the applied whitelist
// fields taken from fresh (refused/restart/other fields keep running values),
// so the next DiffForReload compares against the effective runtime state —
// including correct detection of a later yaml revert of an applied field.
func AdvanceReloadBaseline(running, fresh *Config, c *ReloadChanges) *Config {
	next := *running
	if c.RefreshInterval != nil {
		next.Node.JWTService.ParsedRefreshInterval = *c.RefreshInterval
		next.Node.JWTService.RefreshInterval = fresh.Node.JWTService.RefreshInterval
	}
	if c.RefreshBeforeExpiry != nil {
		next.Node.JWTService.ParsedRefreshBeforeExpiry = *c.RefreshBeforeExpiry
		next.Node.JWTService.RefreshBeforeExpiry = fresh.Node.JWTService.RefreshBeforeExpiry
	}
	if c.AdminToken != nil {
		next.AdminAPI.Token = *c.AdminToken
	}
	return &next
}
