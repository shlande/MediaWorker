// Package config provides YAML-based configuration loading. This file
// defines the ingest-worker configuration struct tree, separate from the
// edge-node Config and control-plane ControlPlaneConfig.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
)

// IngestWorkerConfig is the root configuration for the standalone ingest-worker
// HTTP service.
type IngestWorkerConfig struct {
	HTTP         IngestHTTPConfig         `yaml:"http"`
	Metadata     IngestMetadataConfig     `yaml:"metadata"`
	Storage      IngestStorageConfig      `yaml:"storage"`
	Ingest       IngestSectionConfig      `yaml:"ingest"`
	ControlPlane IngestControlPlaneConfig `yaml:"control_plane"`
}

// IngestControlPlaneConfig wires the ingest-worker into the libp2p sync mesh so
// it can publish ContentIngestedEvent directly to the control-plane
// SyncBroadcaster (T8: 事件回路接通). The worker is an infrastructure identity
// — it joins the private PSK mesh as an admission-only peer (no DHT, no
// GossipSub, no JWT, plan line 167).
type IngestControlPlaneConfig struct {
	// Multiaddr is the full libp2p multiaddr of the control-plane bootstrap
	// host, including the /p2p/<peerID> suffix. Example:
	//   "/ip4/10.0.0.5/tcp/4001/p2p/12D3KooW..."
	Multiaddr string `yaml:"multiaddr"`

	// PrivKeyPath is the filesystem path to the ingest-worker's own libp2p
	// identity (protobuf-encoded Ed25519 key, 0600 perms). Loaded via
	// identity.LoadOrGenerateIdentity — a missing key is generated in place.
	PrivKeyPath string `yaml:"priv_key_path"`
}

// IngestHTTPConfig controls the HTTP server.
type IngestHTTPConfig struct {
	Listen         string `yaml:"listen"`           // e.g. ":8080"
	MaxUploadBytes int64  `yaml:"max_upload_bytes"` // max upload body size (default: 10 GiB)
}

// IngestMetadataConfig holds the Postgres DSN for the metadata service.
type IngestMetadataConfig struct {
	PGDSN string `yaml:"pg_dsn"`
}

// IngestStorageConfig holds the cloud account configuration for upload-only
// access (no libp2p, no DHT — the ingest worker is a standalone HTTP service).
type IngestStorageConfig struct {
	CloudAccounts  []CloudAccountConfig           `yaml:"cloud_accounts"`
	VendorProfiles map[string]VendorProfileConfig `yaml:"vendor_profiles"`
	RateLimits     map[string]RateLimitConfigYAML `yaml:"rate_limits"`
}

// ToAccountPoolConfig adapts IngestStorageConfig (YAML config tree) into the
// accountpool.StorageConfig shape (vendor-neutral, no config import in
// accountpool). Used by both ingest-worker and janitor to build their account
// pools via accountpool.BuildFromConfig (T14 — shared constructor extraction).
func (s IngestStorageConfig) ToAccountPoolConfig() accountpool.StorageConfig {
	accounts := make([]accountpool.CloudAccount, 0, len(s.CloudAccounts))
	for _, a := range s.CloudAccounts {
		accounts = append(accounts, accountpool.CloudAccount{
			Vendor:       a.Vendor,
			AccountID:    a.AccountID,
			ClientID:     a.ClientID,
			ClientSecret: a.ClientSecret,
			Region:       a.Region,
			Enabled:      a.Enabled,
		})
	}
	profiles := make(map[string]accountpool.VendorProfile, len(s.VendorProfiles))
	for k, v := range s.VendorProfiles {
		profiles[k] = accountpool.VendorProfile{Weight: v.Weight}
	}
	limits := make(map[string]accountpool.RateLimit, len(s.RateLimits))
	for k, r := range s.RateLimits {
		limits[k] = accountpool.RateLimit{
			QPS:        r.QPS,
			Burst:      r.Burst,
			Concurrent: r.Concurrent,
		}
	}
	return accountpool.StorageConfig{
		CloudAccounts:  accounts,
		VendorProfiles: profiles,
		RateLimits:     limits,
	}
}

// IngestSectionConfig controls the content ingestion processing.
type IngestSectionConfig struct {
	FFmpegPath string `yaml:"ffmpeg_path"` // e.g. "/usr/bin/ffmpeg"
	WorkDir    string `yaml:"work_dir"`    // temp directory for processing
	Redundancy int    `yaml:"redundancy"`  // K-upload redundancy (default: 2)
}

// LoadIngestWorkerConfig reads a YAML file at path, unmarshals it into
// IngestWorkerConfig, validates required fields, and returns the parsed result.
func LoadIngestWorkerConfig(path string) (*IngestWorkerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ingest-worker config: %w", err)
	}

	var cfg IngestWorkerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse ingest-worker config: %w", err)
	}

	if cfg.HTTP.Listen == "" {
		return nil, fmt.Errorf("config: http.listen is required")
	}
	if cfg.Metadata.PGDSN == "" {
		return nil, fmt.Errorf("config: metadata.pg_dsn is required")
	}
	if cfg.Ingest.FFmpegPath == "" {
		return nil, fmt.Errorf("config: ingest.ffmpeg_path is required")
	}
	if cfg.Ingest.WorkDir == "" {
		return nil, fmt.Errorf("config: ingest.work_dir is required")
	}
	if cfg.Ingest.Redundancy <= 0 {
		cfg.Ingest.Redundancy = 2
	}
	if cfg.HTTP.MaxUploadBytes <= 0 {
		cfg.HTTP.MaxUploadBytes = 10 << 30
	}

	if cfg.ControlPlane.Multiaddr == "" {
		return nil, fmt.Errorf("config: control_plane.multiaddr is required (full multiaddr incl /p2p/<peerID>)")
	}
	if cfg.ControlPlane.PrivKeyPath == "" {
		return nil, fmt.Errorf("config: control_plane.priv_key_path is required (ingest-worker libp2p identity)")
	}

	return &cfg, nil
}
