// Package config provides YAML-based configuration loading for the MediaWorker
// edge node. It defines the complete configuration struct tree matching the
// YAML structure specified in docs/distribution/network.md §2.3.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

// Config is the root configuration for a MediaWorker edge node.
type Config struct {
	Node       NodeConfig       `yaml:"node"`
	Edge       EdgeConfig       `yaml:"edge"`
	Access     AccessConfig     `yaml:"access_layer"`
	HashRing   HashRingConfig   `yaml:"hash_ring"`
}

// ---------------------------------------------------------------------------
// Node identity & capabilities
// ---------------------------------------------------------------------------

// NodeConfig groups identity, declared capabilities, libp2p host settings and
// JWT service connection parameters.
type NodeConfig struct {
	Identity             IdentityConfig        `yaml:"identity"`
	DeclaredCapabilities CapabilitiesConfig    `yaml:"declared_capabilities"`
	Libp2p               Libp2pConfig          `yaml:"libp2p"`
	JWTService           JWTServiceConfig      `yaml:"jwt_service"`
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
	Listen         []string              `yaml:"listen"`
	PrivateNetwork PrivateNetworkConfig  `yaml:"private_network"`
	DHT            DHTConfig             `yaml:"dht"`
	NATTraversal   NATTraversalConfig    `yaml:"nat_traversal"`
	PeerStore      PeerStoreConfig       `yaml:"peer_store"`
	ConnGater      ConnGaterConfig       `yaml:"conn_gater"`
}

// PrivateNetworkConfig controls PSK-based private network admission.
type PrivateNetworkConfig struct {
	Enabled      bool `yaml:"enabled"`
	ForcePnetEnv bool `yaml:"force_pnet_env"`
}

// DHTConfig controls the private DHT discovery settings.
type DHTConfig struct {
	Mode              string   `yaml:"mode"`              // "server" or "client"
	Namespace         string   `yaml:"namespace"`         // fixed lookup namespace
	AdvertiseTTL      string   `yaml:"advertise_ttl"`     // e.g. "15m"
	AdvertiseInterval string   `yaml:"advertise_interval"` // e.g. "5m"
	BootstrapPeers    []string `yaml:"bootstrap_peers"`   // multiaddr + /p2p/ suffix
}

// NATTraversalConfig controls AutoNAT, AutoRelay and DCUtR behaviour.
type NATTraversalConfig struct {
	AutoNAT   bool `yaml:"autonat"`
	AutoRelay bool `yaml:"auto_relay"`
	DCUtR     bool `yaml:"dcutr"`
}

// PeerStoreConfig controls the persistent BadgerDB peer store.
type PeerStoreConfig struct {
	Path       string `yaml:"path"`
	GCInterval string `yaml:"gc_interval"` // e.g. "1h"
}

// ConnGaterConfig controls connection gating limits.
type ConnGaterConfig struct {
	IPRateLimit    int      `yaml:"ip_rate_limit"`
	CIDRAllowlist  []string `yaml:"cidr_allowlist"`
}

// ---------------------------------------------------------------------------
// JWT service
// ---------------------------------------------------------------------------

// JWTServiceConfig holds the control-plane JWT signing endpoint and refresh
// parameters.
type JWTServiceConfig struct {
	Endpoint           string `yaml:"endpoint"`
	RefreshInterval    string `yaml:"refresh_interval"`     // e.g. "5m"
	RefreshBeforeExpiry string `yaml:"refresh_before_expiry"` // e.g. "5m"
}

// ---------------------------------------------------------------------------
// Edge cache
// ---------------------------------------------------------------------------

// EdgeConfig describes the three-tier edge cache configuration.
type EdgeConfig struct {
	PrefixCache CacheConfig `yaml:"prefix_cache"`
	WarmCache   CacheConfig `yaml:"warm_cache"`
	ColdCache   CacheConfig `yaml:"cold_cache"`
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

// AccessConfig groups data-plane and fetch-segment configuration.
type AccessConfig struct {
	DataPlane           DataPlaneConfig           `yaml:"data_plane"`
	FetchSegmentServer  FetchSegmentServerConfig  `yaml:"fetch_segment_server"`
	FetchSegmentClient  FetchSegmentClientConfig  `yaml:"fetch_segment_client"`
}

// DataPlaneConfig controls the local data-plane (driver backends for L4 nodes).
type DataPlaneConfig struct {
	Enabled          bool          `yaml:"enabled"`
	SubscribeControl bool          `yaml:"subscribe_control"`
	Drivers          []string      `yaml:"drivers"`
	LinkPool         LinkPoolConfig `yaml:"link_pool"`
	RateLimitLocal   bool          `yaml:"rate_limit_local"`
}

// LinkPoolConfig controls the max number of cached driver-link entries.
type LinkPoolConfig struct {
	MaxEntries int `yaml:"max_entries"`
}

// FetchSegmentServerConfig controls exposing FetchSegment for sibling peers.
type FetchSegmentServerConfig struct {
	Enabled bool `yaml:"enabled"`
}

// FetchSegmentClientConfig controls fetching segments from sibling peers.
type FetchSegmentClientConfig struct {
	Enabled bool `yaml:"enabled"`
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

// LoadConfig reads a YAML file at path, unmarshals it into Config and returns
// the parsed result. It returns an error if the file cannot be read or the
// YAML is invalid.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

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

	return &cfg, nil
}