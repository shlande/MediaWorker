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
	JWT             JWTHTTPConfig             `yaml:"jwt_http"`
	L4Whitelist     L4WhitelistConfig         `yaml:"l4_whitelist"`
	PinOrchestrator PinOrchestratorConfig     `yaml:"pin_orchestrator"`
	DHTBootstrap    DHTBootstrapConfig        `yaml:"dht_bootstrap"`
	SyncBroadcaster SyncBroadcasterConfig     `yaml:"sync_broadcaster"`
	Metadata        MetadataConfig            `yaml:"metadata"`
	Identity        ControlPlaneIdentityConfig `yaml:"identity"`
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
	ProtocolID string `yaml:"protocol_id"` // pub/sub topic / protocol identifier
	SendTimeout string `yaml:"send_timeout"` // per-message send timeout, e.g. "30s"
}

// ---------------------------------------------------------------------------
// Metadata database
// ---------------------------------------------------------------------------

// MetadataConfig controls database connectivity for content metadata.
type MetadataConfig struct {
	PGDSN                   string `yaml:"pg_dsn"`                     // Postgres DSN
	PopularityQueryInterval string `yaml:"popularity_query_interval"` // e.g. "10m"
}

// ---------------------------------------------------------------------------
// Control plane identity
// ---------------------------------------------------------------------------

// ControlPlaneIdentityConfig holds the paths to the control plane's Ed25519
// private keys:
//   - PrivKeyPath: JWT signing key (PEM PKCS#8 format, crypto/ed25519).
//   - Libp2pPrivKeyPath: libp2p identity key (protobuf format, libp2p crypto.PrivKey).
// These formats are incompatible; keeping them in separate files avoids
// a startup crash on first run.
type ControlPlaneIdentityConfig struct {
	PrivKeyPath       string `yaml:"priv_key_path"`
	Libp2pPrivKeyPath string `yaml:"libp2p_priv_key_path"`
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

// LoadControlPlaneConfig reads a YAML file at path, unmarshals it into
// ControlPlaneConfig and returns the parsed result. It returns an error
// if the file cannot be read, the YAML is invalid, or a required field
// is empty.
func LoadControlPlaneConfig(path string) (*ControlPlaneConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read control-plane config file: %w", err)
	}

	var cfg ControlPlaneConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse control-plane config file: %w", err)
	}

	// Required-field validation.
	if cfg.JWT.Listen == "" {
		return nil, fmt.Errorf("config: jwt_http.listen is required")
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