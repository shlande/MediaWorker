// Package config provides YAML-based configuration loading for the MediaWorker
// edge node. It defines the complete configuration struct tree matching the
// YAML structure specified in docs/distribution/network.md §2.3.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/shlande/mediaworker/internal/types"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

// Config is the root configuration for a MediaWorker edge node.
type Config struct {
	Node     NodeConfig     `yaml:"node"`
	Edge     EdgeConfig     `yaml:"edge"`
	Access   AccessConfig   `yaml:"access_layer"`
	HashRing HashRingConfig `yaml:"hash_ring"`
}

// ---------------------------------------------------------------------------
// Node identity & capabilities
// ---------------------------------------------------------------------------

// NodeConfig groups identity, declared capabilities, libp2p host settings and
// JWT service connection parameters.
type NodeConfig struct {
	Identity             IdentityConfig     `yaml:"identity"`
	DeclaredCapabilities CapabilitiesConfig `yaml:"declared_capabilities"`
	Libp2p               Libp2pConfig       `yaml:"libp2p"`
	JWTService           JWTServiceConfig   `yaml:"jwt_service"`
}

// IdentityConfig holds the path to the node's Ed25519 private key.
type IdentityConfig struct {
	PrivKeyPath string `yaml:"priv_key_path"`
}

// CapabilitiesConfig declares the capabilities the node requests. Actual
// authorization depends on the JWT signed by the control plane.
type CapabilitiesConfig struct {
	Edge          bool `yaml:"edge"`
	L4Backhaul    bool `yaml:"l4_backhaul"`
	RelayProvider bool `yaml:"relay_provider"`
	PeerICP       bool `yaml:"peer_icp"`
}

// ---------------------------------------------------------------------------
// libp2p host
// ---------------------------------------------------------------------------

// Libp2pConfig groups all libp2p-related settings.
type Libp2pConfig struct {
	Listen         []string             `yaml:"listen"`
	PrivateNetwork PrivateNetworkConfig `yaml:"private_network"`
	DHT            DHTConfig            `yaml:"dht"`
	NATTraversal   NATTraversalConfig   `yaml:"nat_traversal"`
	PeerStore      PeerStoreConfig      `yaml:"peer_store"`
	ConnGater      ConnGaterConfig      `yaml:"conn_gater"`
}

// PrivateNetworkConfig controls PSK-based private network admission.
type PrivateNetworkConfig struct {
	Enabled      bool `yaml:"enabled"`
	ForcePnetEnv bool `yaml:"force_pnet_env"`
}

// DHTConfig controls the private DHT discovery settings.
//
// AdvertiseTTL / AdvertiseInterval are string durations parsed into the
// corresponding Parsed fields by LoadConfig. ParsedAdvertiseInterval defaults
// to 5m when empty/zero/invalid; ParsedAdvertiseTTL stays zero when the
// string is empty (caller decides fallback) but is parsed when set.
type DHTConfig struct {
	Mode              string   `yaml:"mode"`               // "server" or "client"
	Namespace         string   `yaml:"namespace"`          // fixed lookup namespace
	AdvertiseTTL      string   `yaml:"advertise_ttl"`      // e.g. "15m"
	AdvertiseInterval string   `yaml:"advertise_interval"` // e.g. "5m"
	BootstrapPeers    []string `yaml:"bootstrap_peers"`    // multiaddr + /p2p/ suffix

	// Parsed fields populated by LoadConfig; safe to read after LoadConfig returns.
	ParsedAdvertiseTTL      time.Duration `yaml:"-"`
	ParsedAdvertiseInterval time.Duration `yaml:"-"`
}

// NATTraversalConfig controls AutoNAT, AutoRelay and DCUtR behaviour.
//
// The fields are *bool so we can distinguish "field omitted" (nil → preserve
// current host behaviour, which enables all three) from "field explicitly
// false" (disable that specific NAT traversal feature). LoadConfig normalises
// nil pointers to true via normaliseNATTraversal.
//
// Effective accessors: AutoNATEffective(), AutoRelayEffective(), DCUtREffective()
// — callers MUST use these rather than dereferencing the pointers directly.
type NATTraversalConfig struct {
	AutoNAT   *bool `yaml:"autonat"`
	AutoRelay *bool `yaml:"auto_relay"`
	DCUtR     *bool `yaml:"dcutr"`
}

// AutoNATEffective returns the resolved AutoNAT setting: true when the field
// is omitted (nil) — preserving the pre-T15 host behaviour — or when the
// operator explicitly sets it to true. False only when explicitly set false.
func (n NATTraversalConfig) AutoNATEffective() bool {
	if n.AutoNAT == nil {
		return true
	}
	return *n.AutoNAT
}

// AutoRelayEffective returns the resolved AutoRelay setting: true when the
// field is omitted (nil) — preserving the pre-T15 host behaviour — or when
// the operator explicitly sets it to true. False only when explicitly set false.
func (n NATTraversalConfig) AutoRelayEffective() bool {
	if n.AutoRelay == nil {
		return true
	}
	return *n.AutoRelay
}

// DCUtREffective returns the resolved DCUtR setting: true when the field is
// omitted (nil) — preserving the pre-T15 host behaviour — or when the
// operator explicitly sets it to true. False only when explicitly set false.
func (n NATTraversalConfig) DCUtREffective() bool {
	if n.DCUtR == nil {
		return true
	}
	return *n.DCUtR
}

// PeerStoreConfig controls the persistent BadgerDB peer store.
//
// GCInterval is a string duration parsed into ParsedGCInterval by LoadConfig
// (default 1h when empty/zero/invalid).
type PeerStoreConfig struct {
	Path       string `yaml:"path"`
	GCInterval string `yaml:"gc_interval"` // e.g. "1h"

	// Parsed field populated by LoadConfig; safe to read after LoadConfig returns.
	ParsedGCInterval time.Duration `yaml:"-"`
}

// ConnGaterConfig controls connection gating limits.
type ConnGaterConfig struct {
	IPRateLimit   int      `yaml:"ip_rate_limit"`
	CIDRAllowlist []string `yaml:"cidr_allowlist"`
}

// ---------------------------------------------------------------------------
// JWT service
// ---------------------------------------------------------------------------

// JWTServiceConfig holds the control-plane JWT signing endpoint and refresh
// parameters.
//
// RefreshInterval / RefreshBeforeExpiry are string durations parsed into the
// corresponding *Parsed fields by LoadConfig (default 5m when empty/zero/invalid).
// Callers should prefer the Parsed fields over re-parsing the strings.
type JWTServiceConfig struct {
	Endpoint            string `yaml:"endpoint"`
	RefreshInterval     string `yaml:"refresh_interval"`      // e.g. "5m"
	RefreshBeforeExpiry string `yaml:"refresh_before_expiry"` // e.g. "5m"

	// Parsed fields populated by LoadConfig; safe to read after LoadConfig returns.
	ParsedRefreshInterval     time.Duration `yaml:"-"`
	ParsedRefreshBeforeExpiry time.Duration `yaml:"-"`
}

const (
	defaultJWTRefreshInterval     = 5 * time.Minute
	defaultJWTRefreshBeforeExpiry = 5 * time.Minute

	defaultDHTAdvertiseInterval = 5 * time.Minute
	defaultPeerStoreGCInterval  = time.Hour
)

// ---------------------------------------------------------------------------
// Edge cache
// ---------------------------------------------------------------------------

// EdgeConfig describes the edge cache configuration.
//
// cold_cache was removed in T17 — the on-disk cold-store was never wired (see
// cmd/edge-node/main.go history). Operators with stale YAML still load fine:
// LoadConfig emits a deprecated-key Warn via scanDeprecatedConfigKeys.
type EdgeConfig struct {
	PrefixCache CacheConfig `yaml:"prefix_cache"`
	WarmCache   CacheConfig `yaml:"warm_cache"`
}

// CacheConfig describes a single on-disk cache tier.
type CacheConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
	SizeGB  int    `yaml:"size_gb"`
}

// ---------------------------------------------------------------------------
// Access layer (data plane & fetch segment)
// ---------------------------------------------------------------------------

// AccessConfig groups data-plane configuration for the edge node.
//
// The multi-account pool fields that used to live here (vendor_profiles,
// rate_limits, health_check, cloud_accounts, fetch_segment_server,
// fetch_segment_client) were removed in T17 — they were never consumed by
// any production code path on the edge node. The ingest-worker and janitor
// have their own copies under IngestStorageConfig (which IS consumed).
// Operators with stale YAML still load fine: LoadConfig emits a
// deprecated-key Warn via scanDeprecatedConfigKeys for each removed field.
type AccessConfig struct {
	DataPlane DataPlaneConfig `yaml:"data_plane"`
}

// DataPlaneConfig controls the local data-plane (driver backends for L4 nodes).
//
// subscribe_control / drivers / rate_limit_local were removed in T17 — they
// were never consumed by any production code path. Operators with stale YAML
// still load fine: LoadConfig emits a deprecated-key Warn.
type DataPlaneConfig struct {
	Enabled  bool           `yaml:"enabled"`
	LinkPool LinkPoolConfig `yaml:"link_pool"`
}

// LinkPoolConfig controls the max number of cached driver-link entries.
type LinkPoolConfig struct {
	MaxEntries int `yaml:"max_entries"`
}

// ---------------------------------------------------------------------------
// Vendor profiles, rate limits, health check & cloud accounts
// ---------------------------------------------------------------------------
//
// The standalone types below (VendorProfileConfig, RateLimitConfigYAML,
// HealthCheckConfig, CloudAccountConfig) are still used by the ingest-worker
// and janitor config trees (see ingest.go / janitor.go). They are NOT used by
// the edge-node AccessConfig anymore — the AccessConfig copies were removed
// in T17 (see AccessConfig docstring above).

// VendorProfileConfig is the YAML representation of a vendor capability profile.
// Weight, BaseLatencyMs and BandwidthMbps are used by the AccountPool selection
// logic to score and rank candidates for read/upload.
type VendorProfileConfig struct {
	Weight        float64 `yaml:"weight"`
	BaseLatencyMs int     `yaml:"base_latency_ms"`
	BandwidthMbps int     `yaml:"bandwidth_mbps"`
}

// RateLimitConfigYAML is the YAML representation of per-vendor rate-limit
// parameters. QPS is the steady-state tokens/second; Burst is the token-bucket
// capacity; Concurrent is the maximum number of in-flight download connections.
type RateLimitConfigYAML struct {
	QPS        float64 `yaml:"qps"`        // tokens per second
	Burst      int     `yaml:"burst"`      // token bucket burst capacity
	Concurrent int     `yaml:"concurrent"` // max concurrent connections
}

// HealthCheckConfig controls the interval at which the account-pool health
// check worker probes each cloud-drive account.
type HealthCheckConfig struct {
	Interval string `yaml:"interval"` // e.g. "30s"
}

// CloudAccountConfig represents a single cloud-drive account in the node's
// local configuration. Credentials (ClientSecret, RefreshToken) are stored
// in plain-text YAML; production deployments should source these from a
// secrets vault injected at deploy time.
type CloudAccountConfig struct {
	Vendor       string `yaml:"vendor"`
	AccountID    string `yaml:"account_id"`
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
	RefreshToken string `yaml:"refresh_token"`
	RedirectURI  string `yaml:"redirect_uri"`
	Region       string `yaml:"region"`
	Enabled      bool   `yaml:"enabled"`
}

// ---------------------------------------------------------------------------
// Hash ring
// ---------------------------------------------------------------------------

// HashRingConfig controls the consistent-hash ring parameters.
type HashRingConfig struct {
	Replicas int `yaml:"replicas"`
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

// deprecatedConfigKeys is the set of YAML keys removed from the edge-node
// Config tree in T17. They are still tolerated in operator YAML (we never
// call Decoder.KnownFields(true), per plan line 248) but each occurrence
// emits a slog.Warn at load time so operators notice and clean up.
//
// Paths are dotted; nested keys use the form "parent.child". A path matches
// if the leaf key exists at the dotted path in the parsed YAML tree. We do
// not error — old configs must continue to load (plan line 248).
var deprecatedConfigKeys = []string{
	"edge.cold_cache",
	"access_layer.fetch_segment_server",
	"access_layer.fetch_segment_client",
	"access_layer.vendor_profiles",
	"access_layer.rate_limits",
	"access_layer.health_check",
	"access_layer.cloud_accounts",
	"access_layer.data_plane.subscribe_control",
	"access_layer.data_plane.drivers",
	"access_layer.data_plane.rate_limit_local",
}

// scanDeprecatedConfigKeys re-parses data as a generic YAML tree and emits a
// slog.Warn for each deprecated key path that is present. Errors during the
// generic re-parse are silently ignored — the structured parse already
// succeeded (or will report a real error), and the deprecation scan is a
// best-effort operator visibility feature, not a correctness gate.
func scanDeprecatedConfigKeys(data []byte, paths []string) {
	var root map[string]any
	if err := yaml.Unmarshal(data, &root); err != nil {
		return
	}
	for _, path := range paths {
		if yamlPathExists(root, path) {
			slog.Warn("deprecated config key ignored", "key", path)
		}
	}
}

// yamlPathExists walks a generic YAML map tree following a dotted path and
// reports whether the leaf key exists. Intermediate segments must be maps;
// a missing intermediate or a non-map intermediate returns false.
func yamlPathExists(root map[string]any, path string) bool {
	segs := splitDotted(path)
	cur := any(root)
	for _, s := range segs {
		m, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		v, ok := m[s]
		if !ok {
			return false
		}
		cur = v
	}
	return true
}

func splitDotted(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

// LoadConfig reads a YAML file at path, unmarshals it into Config and returns
// the parsed result. It returns an error if the file cannot be read or the
// YAML is invalid. Deprecated YAML keys (removed in T17) are tolerated and
// emit a slog.Warn per occurrence — they do NOT cause a load failure.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	scanDeprecatedConfigKeys(data, deprecatedConfigKeys)

	// Basic required-field validation.
	if cfg.Node.Identity.PrivKeyPath == "" {
		return nil, fmt.Errorf("config: node.identity.priv_key_path is required")
	}
	if len(cfg.Node.Libp2p.Listen) == 0 {
		return nil, fmt.Errorf("config: node.libp2p.listen must have at least one address")
	}
	if cfg.Node.Libp2p.DHT.Namespace == "" {
		return nil, fmt.Errorf("config: node.libp2p.dht.namespace is required")
	}
	if cfg.Node.JWTService.Endpoint == "" {
		return nil, fmt.Errorf("config: node.jwt_service.endpoint is required")
	}

	normalizeJWTRefreshDurations(&cfg.Node.JWTService)
	normaliseDHTDurations(&cfg.Node.Libp2p.DHT)
	normalisePeerStoreGCInterval(&cfg.Node.Libp2p.PeerStore)

	return &cfg, nil
}

// normaliseDHTDurations parses AdvertiseTTL / AdvertiseInterval strings into
// the Parsed* fields. AdvertiseInterval defaults to 5m for empty/zero/invalid
// values. AdvertiseTTL is parsed when set; empty/invalid falls back to 0
// (callers handle the zero case). Invalid values do NOT fail config loading —
// they fall back to defaults and are logged by the caller (matches the
// "invalid duration → use default" contract from T7 §c).
func normaliseDHTDurations(d *DHTConfig) {
	d.ParsedAdvertiseInterval = parseDurationOrDefault(d.AdvertiseInterval, defaultDHTAdvertiseInterval)
	if d.AdvertiseTTL == "" {
		d.ParsedAdvertiseTTL = 0
		return
	}
	if v, err := time.ParseDuration(d.AdvertiseTTL); err == nil && v > 0 {
		d.ParsedAdvertiseTTL = v
	} else {
		d.ParsedAdvertiseTTL = 0
	}
}

// normalisePeerStoreGCInterval parses PeerStore.GCInterval into ParsedGCInterval
// with a 1h default for empty/zero/invalid values.
func normalisePeerStoreGCInterval(p *PeerStoreConfig) {
	p.ParsedGCInterval = parseDurationOrDefault(p.GCInterval, defaultPeerStoreGCInterval)
}

// normalizeJWTRefreshDurations parses RefreshInterval / RefreshBeforeExpiry
// strings (e.g. "5m") into the Parsed* fields, applying 5m defaults for
// empty/zero/invalid values. Invalid values do NOT fail config loading — they
// fall back to defaults and are logged by the caller (matches the plan's
// "invalid duration → use default" contract from T7 §c).
func normalizeJWTRefreshDurations(j *JWTServiceConfig) {
	j.ParsedRefreshInterval = parseDurationOrDefault(j.RefreshInterval, defaultJWTRefreshInterval)
	j.ParsedRefreshBeforeExpiry = parseDurationOrDefault(j.RefreshBeforeExpiry, defaultJWTRefreshBeforeExpiry)
}

func parseDurationOrDefault(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// LoadVendorProfiles reads a YAML file at path, unmarshals the top-level
// vendor_profiles key, and returns a map keyed by types.Vendor.  If a vendor
// key in the YAML is not a known types.Vendor constant it is silently skipped.
// When the YAML file does not exist or the key is absent, nil is returned
// (allowing callers to fall back to built-in defaults).
func LoadVendorProfiles(path string) (map[types.Vendor]types.VendorProfile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil //nolint:nilnil — caller falls back to defaults
		}
		return nil, fmt.Errorf("read vendor_profiles file: %w", err)
	}

	var raw struct {
		VendorProfiles map[string]VendorProfileConfig `yaml:"vendor_profiles"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse vendor_profiles: %w", err)
	}
	if raw.VendorProfiles == nil {
		return nil, nil
	}

	out := make(map[types.Vendor]types.VendorProfile, len(raw.VendorProfiles))
	for vendorStr, vpc := range raw.VendorProfiles {
		v := types.Vendor(vendorStr)
		out[v] = types.VendorProfile{
			Vendor:        v,
			Weight:        vpc.Weight,
			BaseLatencyMs: vpc.BaseLatencyMs,
			BandwidthMbps: vpc.BandwidthMbps,
		}
	}
	return out, nil
}
