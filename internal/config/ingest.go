// Package config provides YAML-based configuration loading. This file
// defines the ingest-worker configuration struct tree, separate from the
// edge-node Config and control-plane ControlPlaneConfig.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// IngestWorkerConfig is the root configuration for the standalone ingest-worker
// HTTP service.
type IngestWorkerConfig struct {
	HTTP     IngestHTTPConfig      `yaml:"http"`
	Metadata IngestMetadataConfig  `yaml:"metadata"`
	Storage  IngestStorageConfig   `yaml:"storage"`
	Ingest   IngestSectionConfig   `yaml:"ingest"`
}

// IngestHTTPConfig controls the HTTP server.
type IngestHTTPConfig struct {
	Listen string `yaml:"listen"` // e.g. ":8080"
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

	return &cfg, nil
}
