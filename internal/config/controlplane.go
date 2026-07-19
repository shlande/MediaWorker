// Package config provides YAML-based configuration loading for both the edge
// node and the control plane. This file defines the control plane config tree.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Top-level control plane config
// ---------------------------------------------------------------------------

// ControlPlaneConfig is the root configuration for a MediaWorker control plane.
type ControlPlaneConfig struct {
	JWT             JWTHTTPConfig              `yaml:"jwt_http"`
	JWTPolicy       JWTPolicyConfig            `yaml:"jwt_policy"`
	L4Whitelist     L4WhitelistConfig          `yaml:"l4_whitelist"`
	PinOrchestrator PinOrchestratorConfig      `yaml:"pin_orchestrator"`
	DHTBootstrap    DHTBootstrapConfig         `yaml:"dht_bootstrap"`
	SyncBroadcaster SyncBroadcasterConfig      `yaml:"sync_broadcaster"`
	Metadata        MetadataConfig             `yaml:"metadata"`
	Identity        ControlPlaneIdentityConfig `yaml:"identity"`
	AdminAPI        AdminAPIConfig             `yaml:"admin_api"`
}

// ---------------------------------------------------------------------------
// JWT issuance policy
// ---------------------------------------------------------------------------

// JWTPolicyDefaultCapabilities holds the policy-default capability grants used
// when a node does not declare its own capabilities (or declares none). These
// values are intersected with the node's declared capabilities when present.
//
// L4Backhaul is intentionally absent here: L4 is whitelist-only and decided
// solely by the L4 whitelist check in the JWT service.
type JWTPolicyDefaultCapabilities struct {
	Edge          bool `yaml:"edge"`
	PeerICP       bool `yaml:"peer_icp"`
	RelayProvider bool `yaml:"relay_provider"`
}

// JWTPolicyConfig controls JWT issuance parameters: TTL, refresh window,
// bandwidth quota, and the default capabilities granted when a node does not
// declare its own. All fields are optional; zero/empty values are normalised
// to sensible defaults by LoadControlPlaneConfig via applyJWTPolicyDefaults.
type JWTPolicyConfig struct {
	TTL                  string                       `yaml:"ttl"`
	RefreshBeforeSeconds int                          `yaml:"refresh_before_seconds"`
	BandwidthQuotaBytes  int64                        `yaml:"bandwidth_quota_bytes"`
	DefaultCapabilities  JWTPolicyDefaultCapabilities `yaml:"default_capabilities"`
}

const (
	defaultJWTPolicyTTL                  = "1h"
	defaultJWTPolicyRefreshBeforeSeconds = 300
	defaultJWTPolicyBandwidthQuotaBytes  = 50_000_000
)

// applyJWTPolicyDefaults normalises zero/empty fields of p to sensible defaults
// in-place. A nil receiver is treated as an empty config so callers can pass a
// freshly allocated zero-value struct safely.
func applyJWTPolicyDefaults(p *JWTPolicyConfig) {
	if p == nil {
		return
	}
	if p.TTL == "" {
		p.TTL = defaultJWTPolicyTTL
	}
	if p.RefreshBeforeSeconds == 0 {
		p.RefreshBeforeSeconds = defaultJWTPolicyRefreshBeforeSeconds
	}
	if p.BandwidthQuotaBytes == 0 {
		p.BandwidthQuotaBytes = defaultJWTPolicyBandwidthQuotaBytes
	}
	// Go bool has no "unset" state, so an all-false DefaultCapabilities is
	// ambiguous with an explicit "grant nothing". We treat all-false as
	// "use defaults" (edge+peer_icp=true, relay=false) to preserve
	// bit-for-bit backward compat for configs that omit the stanza entirely.
	if !p.DefaultCapabilities.Edge && !p.DefaultCapabilities.PeerICP && !p.DefaultCapabilities.RelayProvider {
		p.DefaultCapabilities.Edge = true
		p.DefaultCapabilities.PeerICP = true
	}
}

// ---------------------------------------------------------------------------
// Admin API server (control-plane management plane)
// ---------------------------------------------------------------------------

// AdminAPIConfig controls the control-plane admin HTTP server
// (internal/controlplane/adminapi). The server is opt-in: a fully-empty
// admin_api stanza (and no ADMIN_TOKEN_SECRET in the environment) leaves
// Listen empty, which consumers treat as "admin server disabled".
type AdminAPIConfig struct {
	Listen                 string `yaml:"listen"`                   // default "127.0.0.1:8082"; empty = admin server disabled
	TokenSecret            string `yaml:"token_secret"`             // empty -> env ADMIN_TOKEN_SECRET; still empty -> startup error when enabled
	PrometheusURL          string `yaml:"prometheus_url"`           // optional
	AlertWebhookToken      string `yaml:"alert_webhook_token"`      // optional; empty = webhook endpoint not mounted
	QuotaRebalanceInterval string `yaml:"quota_rebalance_interval"` // default "60s"
}

const (
	defaultAdminAPIListen                 = "127.0.0.1:8082"
	defaultAdminAPIQuotaRebalanceInterval = "60s"

	// adminAPITokenSecretEnv is the environment fallback for
	// admin_api.token_secret. The secret is never logged.
	adminAPITokenSecretEnv = "ADMIN_TOKEN_SECRET"
)

// applyAdminAPIDefaults normalises zero/empty fields of a to sensible defaults
// in-place, in the style of applyJWTPolicyDefaults. A nil receiver is treated
// as an empty config.
//
// Enablement rule: the stanza is opt-in. Any explicitly-configured field (or
// an env-provided token secret) activates the admin server, and an empty
// Listen then falls back to the default address. A completely empty stanza
// keeps Listen empty so existing configs without admin_api stay disabled.
func applyAdminAPIDefaults(a *AdminAPIConfig) {
	if a == nil {
		return
	}
	if a.TokenSecret == "" {
		a.TokenSecret = os.Getenv(adminAPITokenSecretEnv)
	}
	configured := a.TokenSecret != "" ||
		a.PrometheusURL != "" ||
		a.AlertWebhookToken != "" ||
		a.QuotaRebalanceInterval != ""
	if a.Listen == "" && configured {
		a.Listen = defaultAdminAPIListen
	}
	if a.QuotaRebalanceInterval == "" {
		a.QuotaRebalanceInterval = defaultAdminAPIQuotaRebalanceInterval
	}
}

// ---------------------------------------------------------------------------
// JWT HTTP endpoint
// ---------------------------------------------------------------------------

// JWTHTTPConfig controls the JWT signing HTTP server run by the control plane.
type JWTHTTPConfig struct {
	Listen       string `yaml:"listen"`        // e.g. ":8443"
	ReadTimeout  string `yaml:"read_timeout"`  // e.g. "10s"
	WriteTimeout string `yaml:"write_timeout"` // e.g. "10s"
}

// ---------------------------------------------------------------------------
// L4 whitelist
// ---------------------------------------------------------------------------

// L4WhitelistConfig controls the persistent PeerId whitelist for L4 capability.
type L4WhitelistConfig struct {
	DBPath string `yaml:"db_path"` // path to BadgerDB whitelist store
}

// ---------------------------------------------------------------------------
// Pin orchestrator
// ---------------------------------------------------------------------------

// PinOrchestratorConfig controls the pin-plan computation engine.
type PinOrchestratorConfig struct {
	RebalanceInterval string `yaml:"rebalance_interval"` // e.g. "10m"
	TopContentsLimit  int    `yaml:"top_contents_limit"` // e.g. 5000
}

// ---------------------------------------------------------------------------
// DHT bootstrap
// ---------------------------------------------------------------------------

// DHTBootstrapConfig controls the DHT bootstrap node(s) run by the control plane.
type DHTBootstrapConfig struct {
	ListenAddrs       []string `yaml:"listen_addrs"`       // multiaddrs to listen on
	Namespace         string   `yaml:"namespace"`          // DHT lookup namespace
	AdvertiseTTL      string   `yaml:"advertise_ttl"`      // e.g. "15m"
	AdvertiseInterval string   `yaml:"advertise_interval"` // e.g. "5m"
	BootstrapPeers    []string `yaml:"bootstrap_peers"`    // fallback bootstrap multiaddrs
}

// ---------------------------------------------------------------------------
// Sync broadcaster (control plane → nodes pub/sub channel)
// ---------------------------------------------------------------------------

// SyncBroadcasterConfig controls the GossipSub-based command broadcast channel.
type SyncBroadcasterConfig struct {
	ProtocolID  string `yaml:"protocol_id"`  // pub/sub topic / protocol identifier
	SendTimeout string `yaml:"send_timeout"` // per-message send timeout, e.g. "30s"
}

// ---------------------------------------------------------------------------
// Metadata database
// ---------------------------------------------------------------------------

// MetadataConfig controls database connectivity for content metadata.
//
// popularity_query_interval was removed in T17 — synchronous query already
// satisfies current scale, so the periodic background poll was never wired.
// Operators with stale YAML still load fine: LoadControlPlaneConfig emits a
// deprecated-key Warn via scanDeprecatedConfigKeys.
type MetadataConfig struct {
	PGDSN string `yaml:"pg_dsn"` // Postgres DSN
}

// ---------------------------------------------------------------------------
// Control plane identity
// ---------------------------------------------------------------------------

// ControlPlaneIdentityConfig holds the paths to the control plane's Ed25519
// private keys:
//   - PrivKeyPath: JWT signing key (PEM PKCS#8 format, crypto/ed25519).
//   - Libp2pPrivKeyPath: libp2p identity key (protobuf format, libp2p crypto.PrivKey).
//
// These formats are incompatible; keeping them in separate files avoids
// a startup crash on first run.
type ControlPlaneIdentityConfig struct {
	PrivKeyPath       string `yaml:"priv_key_path"`
	Libp2pPrivKeyPath string `yaml:"libp2p_priv_key_path"`
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

// deprecatedControlPlaneKeys is the set of YAML keys removed from the
// ControlPlaneConfig tree in T17. Same tolerance contract as
// deprecatedConfigKeys in config.go: tolerated in operator YAML, emit a
// slog.Warn per occurrence, never error.
var deprecatedControlPlaneKeys = []string{
	"metadata.popularity_query_interval",
}

// LoadControlPlaneConfig reads a YAML file at path, unmarshals it into
// ControlPlaneConfig and returns the parsed result. It returns an error
// if the file cannot be read, the YAML is invalid, or a required field
// is empty. Deprecated YAML keys (removed in T17) are tolerated and emit
// a slog.Warn per occurrence — they do NOT cause a load failure.
func LoadControlPlaneConfig(path string) (*ControlPlaneConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read control-plane config file: %w", err)
	}

	var cfg ControlPlaneConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse control-plane config file: %w", err)
	}

	scanDeprecatedConfigKeys(data, deprecatedControlPlaneKeys)

	applyJWTPolicyDefaults(&cfg.JWTPolicy)
	applyAdminAPIDefaults(&cfg.AdminAPI)

	// Required-field validation.
	if cfg.JWT.Listen == "" {
		return nil, fmt.Errorf("config: jwt_http.listen is required")
	}
	if cfg.AdminAPI.Listen != "" && cfg.AdminAPI.TokenSecret == "" {
		return nil, fmt.Errorf("config: admin_api.token_secret is required when admin_api.listen is set (or set env ADMIN_TOKEN_SECRET)")
	}
	if cfg.DHTBootstrap.Namespace == "" {
		return nil, fmt.Errorf("config: dht_bootstrap.namespace is required")
	}
	if cfg.Metadata.PGDSN == "" {
		return nil, fmt.Errorf("config: metadata.pg_dsn is required")
	}
	if cfg.Identity.PrivKeyPath == "" {
		return nil, fmt.Errorf("config: identity.priv_key_path is required")
	}
	if cfg.Identity.Libp2pPrivKeyPath == "" {
		return nil, fmt.Errorf("config: identity.libp2p_priv_key_path is required")
	}

	return &cfg, nil
}
