package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

// ---------------------------------------------------------------------------
// Happy path: L4 node
// ---------------------------------------------------------------------------

const l4ConfigYAML = `
node:
  identity:
    priv_key_path: "/data/identity/ed25519.key"
  declared_capabilities:
    edge: true
    l4_backhaul: true
    relay_provider: true
    peer_icp: true
  libp2p:
    listen:
      - "/ip4/0.0.0.0/tcp/9001"
      - "/ip4/0.0.0.0/udp/9001/quic"
    private_network:
      enabled: true
      force_pnet_env: true
    dht:
      mode: server
      namespace: "edge"
      advertise_ttl: "15m"
      advertise_interval: "5m"
      bootstrap_peers:
        - "/dnsaddr/dht-bootstrap-01.example.com/tcp/9001/p2p/QmBootstrap01"
        - "/dnsaddr/dht-bootstrap-02.example.com/tcp/9001/p2p/QmBootstrap02"
        - "/dnsaddr/dht-bootstrap-03.example.com/tcp/9001/p2p/QmBootstrap03"
    nat_traversal:
      autonat: true
      auto_relay: true
      dcutr: true
    peer_store:
      path: "/data/identity/peerstore.db"
      gc_interval: "1h"
    conn_gater:
      ip_rate_limit: 50
  jwt_service:
    endpoint: "https://control-plane.example.com/v1/node/jwt"
    refresh_interval: "5m"
    refresh_before_expiry: "5m"
edge:
  prefix_cache: { enabled: true, path: "/data/prefix", size_gb: 2000 }
  warm_cache:   { enabled: true, path: "/data/warm",   size_gb: 50000 }
  # cold_cache: removed in T17 (deprecated — LoadConfig emits Warn if present)
access_layer:
  data_plane:
    enabled: true
    location_endpoint: "https://control-plane.example.com"
    # subscribe_control: removed in T17 (deprecated)
    # drivers: removed in T17 (deprecated)
    link_pool: { max_entries: 10000 }
    # rate_limit_local: removed in T17 (deprecated)
  # fetch_segment_server: removed in T17 (deprecated)
  # fetch_segment_client: removed in T17 (deprecated)
  # vendor_profiles: removed in T17 (deprecated — see ingest-worker.yaml for the live tree)
  # rate_limits: removed in T17 (deprecated — see ingest-worker.yaml for the live tree)
  # health_check: removed in T17 (deprecated)
  # cloud_accounts: removed in T17 (deprecated — see ingest-worker.yaml for the live tree)
hash_ring:
  replicas: 150
`

func TestLoadConfig_L4Node(t *testing.T) {
	path := writeTempYAML(t, l4ConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Node identity
	if cfg.Node.Identity.PrivKeyPath != "/data/identity/ed25519.key" {
		t.Errorf("PrivKeyPath = %q, want %q", cfg.Node.Identity.PrivKeyPath, "/data/identity/ed25519.key")
	}

	// Capabilities
	if !cfg.Node.DeclaredCapabilities.L4Backhaul {
		t.Error("L4Backhaul expected true")
	}
	if !cfg.Node.DeclaredCapabilities.RelayProvider {
		t.Error("RelayProvider expected true")
	}

	// libp2p
	if len(cfg.Node.Libp2p.Listen) != 2 {
		t.Errorf("listen addresses = %d, want 2", len(cfg.Node.Libp2p.Listen))
	}
	if !cfg.Node.Libp2p.PrivateNetwork.Enabled {
		t.Error("private_network.enabled expected true")
	}
	if cfg.Node.Libp2p.DHT.Mode != "server" {
		t.Errorf("DHT mode = %q, want %q", cfg.Node.Libp2p.DHT.Mode, "server")
	}
	if cfg.Node.Libp2p.DHT.Namespace != "edge" {
		t.Errorf("DHT namespace = %q, want %q", cfg.Node.Libp2p.DHT.Namespace, "edge")
	}
	if len(cfg.Node.Libp2p.DHT.BootstrapPeers) != 3 {
		t.Errorf("bootstrap peers = %d, want 3", len(cfg.Node.Libp2p.DHT.BootstrapPeers))
	}
	if !cfg.Node.Libp2p.NATTraversal.AutoRelayEffective() {
		t.Error("AutoRelay expected true")
	}
	if cfg.Node.Libp2p.ConnGater.IPRateLimit != 50 {
		t.Errorf("IPRateLimit = %d, want 50", cfg.Node.Libp2p.ConnGater.IPRateLimit)
	}

	// JWT
	if cfg.Node.JWTService.Endpoint != "https://control-plane.example.com/v1/node/jwt" {
		t.Errorf("JWT endpoint = %q", cfg.Node.JWTService.Endpoint)
	}
	if cfg.Node.JWTService.ParsedRefreshInterval != 5*time.Minute {
		t.Errorf("ParsedRefreshInterval = %v, want 5m", cfg.Node.JWTService.ParsedRefreshInterval)
	}
	if cfg.Node.JWTService.ParsedRefreshBeforeExpiry != 5*time.Minute {
		t.Errorf("ParsedRefreshBeforeExpiry = %v, want 5m", cfg.Node.JWTService.ParsedRefreshBeforeExpiry)
	}

	// Edge caches
	if !cfg.Edge.PrefixCache.Enabled || cfg.Edge.PrefixCache.SizeGB != 2000 {
		t.Error("prefix_cache misconfigured")
	}

	// Access layer
	if !cfg.Access.DataPlane.Enabled {
		t.Error("data_plane.enabled expected true")
	}
	if cfg.Access.DataPlane.LocationEndpoint != "https://control-plane.example.com" {
		t.Errorf("LocationEndpoint = %q, want %q", cfg.Access.DataPlane.LocationEndpoint, "https://control-plane.example.com")
	}

	// Hash ring
	if cfg.HashRing.Replicas != 150 {
		t.Errorf("HashRing.Replicas = %d, want 150", cfg.HashRing.Replicas)
	}
}

// ---------------------------------------------------------------------------
// Happy path: edge (non-L4) node
// ---------------------------------------------------------------------------

const edgeConfigYAML = `
node:
  identity:
    priv_key_path: "/data/identity/ed25519.key"
  declared_capabilities:
    edge: true
    l4_backhaul: false
    relay_provider: false
    peer_icp: true
  libp2p:
    listen:
      - "/ip4/0.0.0.0/tcp/9001"
      - "/ip4/0.0.0.0/udp/9001/quic"
    private_network:
      enabled: true
      force_pnet_env: true
    dht:
      mode: client
      namespace: "edge"
      advertise_ttl: "15m"
      advertise_interval: "5m"
      bootstrap_peers:
        - "/dnsaddr/dht-bootstrap-01.example.com/tcp/9001/p2p/QmBootstrap01"
        - "/dnsaddr/dht-bootstrap-02.example.com/tcp/9001/p2p/QmBootstrap02"
        - "/dnsaddr/dht-bootstrap-03.example.com/tcp/9001/p2p/QmBootstrap03"
    nat_traversal:
      autonat: true
      auto_relay: true
      dcutr: true
    peer_store:
      path: "/data/identity/peerstore.db"
      gc_interval: "1h"
    conn_gater:
      ip_rate_limit: 50
  jwt_service:
    endpoint: "https://control-plane.example.com/v1/node/jwt"
    refresh_interval: "5m"
    refresh_before_expiry: "5m"
edge:
  prefix_cache: { enabled: true, path: "/data/prefix", size_gb: 2000 }
  warm_cache:   { enabled: true, path: "/data/warm",   size_gb: 50000 }
access_layer:
  data_plane:
    enabled: false
  # fetch_segment_client: removed in T17 (deprecated)
hash_ring:
  replicas: 150
`

func TestLoadConfig_EdgeNode(t *testing.T) {
	path := writeTempYAML(t, edgeConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// L4 should be false for pure edge node
	if cfg.Node.DeclaredCapabilities.L4Backhaul {
		t.Error("L4Backhaul expected false for edge node")
	}
	if cfg.Node.DeclaredCapabilities.RelayProvider {
		t.Error("RelayProvider expected false for edge node")
	}

	// DHT mode = client
	if cfg.Node.Libp2p.DHT.Mode != "client" {
		t.Errorf("DHT mode = %q, want %q", cfg.Node.Libp2p.DHT.Mode, "client")
	}

	// data_plane.enabled = false
	if cfg.Access.DataPlane.Enabled {
		t.Error("data_plane.enabled expected false for edge node")
	}
}

// Given node.region in YAML, When LoadConfig runs, Then Node.Region is populated.
func TestLoadConfig_NodeRegion(t *testing.T) {
	yamlDoc := `
node:
  region: "cn"
  identity:
    priv_key_path: "/data/identity/ed25519.key"
  libp2p:
    listen:
      - "/ip4/0.0.0.0/tcp/9001"
    dht:
      namespace: "edge"
  jwt_service:
    endpoint: "https://control-plane.example.com/v1/node/jwt"
`
	cfg, err := LoadConfig(writeTempYAML(t, yamlDoc))
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Node.Region != "cn" {
		t.Errorf("Node.Region = %q, want %q", cfg.Node.Region, "cn")
	}
}

// Given YAML without node.region, When LoadConfig runs, Then Node.Region stays
// empty (empty = unknown).
func TestLoadConfig_NodeRegionAbsent_DefaultsEmpty(t *testing.T) {
	cfg, err := LoadConfig(writeTempYAML(t, l4ConfigYAML))
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Node.Region != "" {
		t.Errorf("Node.Region = %q, want empty for absent key", cfg.Node.Region)
	}
}

// ---------------------------------------------------------------------------
// Extended config: vendor_profiles, rate_limits, health_check, cloud_accounts
// ---------------------------------------------------------------------------
//
// The AccessConfig-side tests for vendor_profiles / rate_limits / health_check
// / cloud_accounts were removed in T17 — those fields were deleted from
// AccessConfig (never consumed by edge-node production code). The standalone
// LoadVendorProfiles function (used by ingest-worker) is still covered by the
// TestLoadVendorProfiles_* tests below.

func TestLoadVendorProfiles_FromStandaloneFile(t *testing.T) {
	// LoadVendorProfiles reads a standalone YAML file whose top-level key
	// is vendor_profiles.  This test provides exactly that shape.
	const vendorProfilesYAML = `vendor_profiles:
  "115":       { weight: 3.0, base_latency_ms: 100, bandwidth_mbps: 50 }
  baidu:       { weight: 2.0, base_latency_ms: 200, bandwidth_mbps: 80 }
  quark:       { weight: 1.0, base_latency_ms: 300, bandwidth_mbps: 30 }
  onedrive:    { weight: 2.0, base_latency_ms: 80,  bandwidth_mbps: 40 }
  aliyundrive: { weight: 2.5, base_latency_ms: 90,  bandwidth_mbps: 40 }
`
	path := writeTempYAML(t, vendorProfilesYAML)

	profiles, err := LoadVendorProfiles(path)
	if err != nil {
		t.Fatalf("LoadVendorProfiles failed: %v", err)
	}

	const vendCount = 5
	if len(profiles) != vendCount {
		t.Fatalf("VendorProfiles count = %d, want %d", len(profiles), vendCount)
	}

	v115 := profiles["115"]
	if v115.Weight != 3.0 {
		t.Errorf("115 Weight = %v, want 3.0", v115.Weight)
	}
	if v115.BaseLatencyMs != 100 {
		t.Errorf("115 BaseLatencyMs = %d, want 100", v115.BaseLatencyMs)
	}
	if v115.BandwidthMbps != 50 {
		t.Errorf("115 BandwidthMbps = %d, want 50", v115.BandwidthMbps)
	}
}

func TestLoadVendorProfiles_MissingFile(t *testing.T) {
	profiles, err := LoadVendorProfiles("/nonexistent/path/vendor_profiles.yaml")
	if err != nil {
		t.Fatalf("LoadVendorProfiles for missing file: %v", err)
	}
	if profiles != nil {
		t.Fatal("expected nil profiles for missing file")
	}
}

func TestLoadVendorProfiles_MissingKey(t *testing.T) {
	const yaml = `other_key: true`
	path := writeTempYAML(t, yaml)
	profiles, err := LoadVendorProfiles(path)
	if err != nil {
		t.Fatalf("LoadVendorProfiles: %v", err)
	}
	if profiles != nil {
		t.Fatal("expected nil when vendor_profiles key absent")
	}
}

func TestLoadConfig_MissingPrivKey(t *testing.T) {
	const yaml = `
node:
  identity:
    priv_key_path: ""
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
  jwt_service:
    endpoint: "https://example.com/jwt"
`
	path := writeTempYAML(t, yaml)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for missing priv_key_path, got nil")
	}
}

func TestLoadConfig_MissingListen(t *testing.T) {
	const yaml = `
node:
  identity:
    priv_key_path: "/data/key"
  libp2p:
    listen: []
    dht:
      namespace: "edge"
  jwt_service:
    endpoint: "https://example.com/jwt"
`
	path := writeTempYAML(t, yaml)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for empty listen, got nil")
	}
}

func TestLoadConfig_MissingJWTEndpoint(t *testing.T) {
	const yaml = `
node:
  identity:
    priv_key_path: "/data/key"
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
  jwt_service:
    endpoint: ""
`
	path := writeTempYAML(t, yaml)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for missing jwt endpoint, got nil")
	}
}

// ---------------------------------------------------------------------------
// access_layer.data_plane.location_endpoint validation
// ---------------------------------------------------------------------------

// minimalValidNodeYAML is the smallest node config that passes all required
// field checks; data-plane stanzas are appended per test.
const minimalValidNodeYAML = `
node:
  identity:
    priv_key_path: "/data/key"
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
  jwt_service:
    endpoint: "https://cp.example.com/v1/node/jwt"
`

func TestDataPlaneLocationEndpoint_RequiredWhenEnabled(t *testing.T) {
	// Given: data_plane.enabled=true with an empty location_endpoint
	path := writeTempYAML(t, minimalValidNodeYAML+`
access_layer:
  data_plane:
    enabled: true
    location_endpoint: ""
`)

	// When: the config is loaded
	_, err := LoadConfig(path)

	// Then: loading fails and the message names the missing field
	if err == nil {
		t.Fatal("expected error for empty location_endpoint, got nil")
	}
	if !strings.Contains(err.Error(), "access_layer.data_plane.location_endpoint") {
		t.Fatalf("error must name the missing field, got: %v", err)
	}
}

func TestDataPlaneLocationEndpoint_AbsentWhenEnabled(t *testing.T) {
	// Given: data_plane.enabled=true with the key absent entirely
	path := writeTempYAML(t, minimalValidNodeYAML+`
access_layer:
  data_plane:
    enabled: true
`)

	// When/Then: same failure as the explicit-empty case
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for absent location_endpoint, got nil")
	}
	if !strings.Contains(err.Error(), "access_layer.data_plane.location_endpoint") {
		t.Fatalf("error must name the missing field, got: %v", err)
	}
}

func TestDataPlaneLocationEndpoint_NotRequiredWhenDisabled(t *testing.T) {
	// Given: data_plane.enabled=false (default) with no location_endpoint
	path := writeTempYAML(t, minimalValidNodeYAML)

	// When: the config is loaded
	cfg, err := LoadConfig(path)

	// Then: loading succeeds with an empty endpoint (non-L4 node)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Access.DataPlane.LocationEndpoint != "" {
		t.Fatalf("LocationEndpoint = %q, want empty", cfg.Access.DataPlane.LocationEndpoint)
	}
}

func TestDataPlaneLocationEndpoint_ParsedWhenSet(t *testing.T) {
	// Given: data_plane.enabled=true with a valid location_endpoint
	path := writeTempYAML(t, minimalValidNodeYAML+`
access_layer:
  data_plane:
    enabled: true
    location_endpoint: "https://control-plane.example.com"
`)

	// When: the config is loaded
	cfg, err := LoadConfig(path)

	// Then: the endpoint round-trips into DataPlaneConfig
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Access.DataPlane.LocationEndpoint != "https://control-plane.example.com" {
		t.Fatalf("LocationEndpoint = %q", cfg.Access.DataPlane.LocationEndpoint)
	}
}

// ---------------------------------------------------------------------------
// Failure: nonexistent file
// ---------------------------------------------------------------------------

func TestLoadConfig_NonexistentFile(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}
}

// ---------------------------------------------------------------------------
// Failure: invalid YAML
// ---------------------------------------------------------------------------

func TestLoadConfig_InvalidYAML(t *testing.T) {
	path := writeTempYAML(t, `node: { identity: { priv_key_path: "/key" } `) // unbalanced brace
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

// ---------------------------------------------------------------------------
// Ingest-worker config: MaxUploadBytes normalization
// ---------------------------------------------------------------------------

const ingestWorkerConfigBase = `
http:
  listen: ":8080"
metadata:
  pg_dsn: "postgres://localhost:5432/test"
ingest:
  ffmpeg_path: "/usr/bin/ffmpeg"
  work_dir: "/tmp/work"
  redundancy: 2
control_plane:
  multiaddr: "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWFakePeerIDForTestingOnlyXYZ"
  priv_key_path: "/tmp/ingest-worker.key"
`

func TestLoadIngestWorkerConfig_MaxUploadBytes_Explicit(t *testing.T) {
	path := writeTempYAML(t, `http:
  listen: ":8080"
  max_upload_bytes: 1073741824
metadata:
  pg_dsn: "postgres://localhost:5432/test"
ingest:
  ffmpeg_path: "/usr/bin/ffmpeg"
  work_dir: "/tmp/work"
  redundancy: 2
control_plane:
  multiaddr: "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWFakePeerIDForTestingOnlyXYZ"
  priv_key_path: "/tmp/ingest-worker.key"
`)
	cfg, err := LoadIngestWorkerConfig(path)
	if err != nil {
		t.Fatalf("LoadIngestWorkerConfig failed: %v", err)
	}
	if cfg.HTTP.MaxUploadBytes != 1073741824 {
		t.Errorf("MaxUploadBytes = %d, want 1073741824 (1 GiB)", cfg.HTTP.MaxUploadBytes)
	}
}

func TestLoadIngestWorkerConfig_MaxUploadBytes_Default(t *testing.T) {
	path := writeTempYAML(t, ingestWorkerConfigBase)
	cfg, err := LoadIngestWorkerConfig(path)
	if err != nil {
		t.Fatalf("LoadIngestWorkerConfig failed: %v", err)
	}
	if cfg.HTTP.MaxUploadBytes != 10<<30 {
		t.Errorf("MaxUploadBytes = %d, want %d (10 GiB)", cfg.HTTP.MaxUploadBytes, 10<<30)
	}
}

func TestLoadIngestWorkerConfig_MaxUploadBytes_ZeroNegative(t *testing.T) {
	tests := []struct {
		name    string
		maxYAML string
	}{
		{"zero", `max_upload_bytes: 0`},
		{"negative", `max_upload_bytes: -1`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTempYAML(t, `http:
  listen: ":8080"
  `+tt.maxYAML+`
metadata:
  pg_dsn: "postgres://localhost:5432/test"
ingest:
  ffmpeg_path: "/usr/bin/ffmpeg"
  work_dir: "/tmp/work"
  redundancy: 2
control_plane:
  multiaddr: "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWFakePeerIDForTestingOnlyXYZ"
  priv_key_path: "/tmp/ingest-worker.key"
`)
			cfg, err := LoadIngestWorkerConfig(path)
			if err != nil {
				t.Fatalf("LoadIngestWorkerConfig failed: %v", err)
			}
			if cfg.HTTP.MaxUploadBytes != 10<<30 {
				t.Errorf("MaxUploadBytes = %d, want %d (10 GiB default)", cfg.HTTP.MaxUploadBytes, 10<<30)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Ingest-worker config: control_plane validation (T8)
// ---------------------------------------------------------------------------

func TestLoadIngestWorkerConfig_ControlPlane_Required(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing_multiaddr",
			yaml: `http:
  listen: ":8080"
metadata:
  pg_dsn: "postgres://localhost:5432/test"
ingest:
  ffmpeg_path: "/usr/bin/ffmpeg"
  work_dir: "/tmp/work"
control_plane:
  priv_key_path: "/tmp/ingest-worker.key"
`,
			want: "control_plane.multiaddr is required",
		},
		{
			name: "missing_priv_key_path",
			yaml: `http:
  listen: ":8080"
metadata:
  pg_dsn: "postgres://localhost:5432/test"
ingest:
  ffmpeg_path: "/usr/bin/ffmpeg"
  work_dir: "/tmp/work"
control_plane:
  multiaddr: "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWFake"
`,
			want: "control_plane.priv_key_path is required",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTempYAML(t, tt.yaml)
			_, err := LoadIngestWorkerConfig(path)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestLoadIngestWorkerConfig_ControlPlane_Populated(t *testing.T) {
	path := writeTempYAML(t, `http:
  listen: ":8080"
metadata:
  pg_dsn: "postgres://localhost:5432/test"
ingest:
  ffmpeg_path: "/usr/bin/ffmpeg"
  work_dir: "/tmp/work"
control_plane:
  multiaddr: "/ip4/10.0.0.5/tcp/4001/p2p/12D3KooWRealPeerID"
  priv_key_path: "/var/lib/mediaworker/ingest-worker.key"
`)
	cfg, err := LoadIngestWorkerConfig(path)
	if err != nil {
		t.Fatalf("LoadIngestWorkerConfig failed: %v", err)
	}
	if cfg.ControlPlane.Multiaddr != "/ip4/10.0.0.5/tcp/4001/p2p/12D3KooWRealPeerID" {
		t.Errorf("Multiaddr = %q", cfg.ControlPlane.Multiaddr)
	}
	if cfg.ControlPlane.PrivKeyPath != "/var/lib/mediaworker/ingest-worker.key" {
		t.Errorf("PrivKeyPath = %q", cfg.ControlPlane.PrivKeyPath)
	}
}

// ---------------------------------------------------------------------------
// JWT refresh duration parsing & defaults
// ---------------------------------------------------------------------------

func TestLoadConfig_JWTRefreshDurationDefaults(t *testing.T) {
	cases := []struct {
		name       string
		yamlBody   string
		wantInter  time.Duration
		wantBefore time.Duration
	}{
		{
			name: "explicit values",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
    refresh_interval: "10m"
    refresh_before_expiry: "2m"
`,
			wantInter:  10 * time.Minute,
			wantBefore: 2 * time.Minute,
		},
		{
			name: "missing stanza falls back to 5m defaults",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantInter:  5 * time.Minute,
			wantBefore: 5 * time.Minute,
		},
		{
			name: "invalid value falls back to 5m default",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
    refresh_interval: "not-a-duration"
    refresh_before_expiry: ""
`,
			wantInter:  5 * time.Minute,
			wantBefore: 5 * time.Minute,
		},
		{
			name: "zero/negative falls back to default",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
    refresh_interval: "0s"
    refresh_before_expiry: "-5m"
`,
			wantInter:  5 * time.Minute,
			wantBefore: 5 * time.Minute,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempYAML(t, tc.yamlBody)
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if cfg.Node.JWTService.ParsedRefreshInterval != tc.wantInter {
				t.Errorf("ParsedRefreshInterval = %v, want %v",
					cfg.Node.JWTService.ParsedRefreshInterval, tc.wantInter)
			}
			if cfg.Node.JWTService.ParsedRefreshBeforeExpiry != tc.wantBefore {
				t.Errorf("ParsedRefreshBeforeExpiry = %v, want %v",
					cfg.Node.JWTService.ParsedRefreshBeforeExpiry, tc.wantBefore)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T15: DHT advertise_interval + peer_store.gc_interval parsing & defaults
// ---------------------------------------------------------------------------

func TestLoadConfig_DHTAdvertiseIntervalDefaults(t *testing.T) {
	cases := []struct {
		name      string
		yamlBody  string
		wantInter time.Duration
		wantTTL   time.Duration
	}{
		{
			name: "explicit values",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
      mode: "client"
      advertise_ttl: "20m"
      advertise_interval: "2m"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantInter: 2 * time.Minute,
			wantTTL:   20 * time.Minute,
		},
		{
			name: "missing interval falls back to 5m default",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
      mode: "client"
      advertise_ttl: "20m"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantInter: 5 * time.Minute,
			wantTTL:   20 * time.Minute,
		},
		{
			name: "invalid interval falls back to 5m default",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
      mode: "client"
      advertise_ttl: "20m"
      advertise_interval: "not-a-duration"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantInter: 5 * time.Minute,
			wantTTL:   20 * time.Minute,
		},
		{
			name: "zero/negative interval falls back to default",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
      mode: "client"
      advertise_ttl: "20m"
      advertise_interval: "0s"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantInter: 5 * time.Minute,
			wantTTL:   20 * time.Minute,
		},
		{
			name: "invalid ttl falls back to zero (caller handles)",
			yamlBody: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
      mode: "client"
      advertise_ttl: "garbage"
      advertise_interval: "5m"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantInter: 5 * time.Minute,
			wantTTL:   0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempYAML(t, tc.yamlBody)
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if cfg.Node.Libp2p.DHT.ParsedAdvertiseInterval != tc.wantInter {
				t.Errorf("ParsedAdvertiseInterval = %v, want %v",
					cfg.Node.Libp2p.DHT.ParsedAdvertiseInterval, tc.wantInter)
			}
			if cfg.Node.Libp2p.DHT.ParsedAdvertiseTTL != tc.wantTTL {
				t.Errorf("ParsedAdvertiseTTL = %v, want %v",
					cfg.Node.Libp2p.DHT.ParsedAdvertiseTTL, tc.wantTTL)
			}
		})
	}
}

func TestLoadConfig_PeerStoreGCIntervalDefaults(t *testing.T) {
	cases := []struct {
		name   string
		yaml   string
		wantGC time.Duration
	}{
		{
			name: "explicit value",
			yaml: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
    peer_store:
      path: "/data/ps.db"
      gc_interval: "30m"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantGC: 30 * time.Minute,
		},
		{
			name: "missing gc_interval falls back to 1h default",
			yaml: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
    peer_store:
      path: "/data/ps.db"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantGC: time.Hour,
		},
		{
			name: "invalid gc_interval falls back to 1h default",
			yaml: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
    peer_store:
      path: "/data/ps.db"
      gc_interval: "garbage"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantGC: time.Hour,
		},
		{
			name: "zero/negative gc_interval falls back to 1h default",
			yaml: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
    peer_store:
      path: "/data/ps.db"
      gc_interval: "0s"
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantGC: time.Hour,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempYAML(t, tc.yaml)
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if cfg.Node.Libp2p.PeerStore.ParsedGCInterval != tc.wantGC {
				t.Errorf("ParsedGCInterval = %v, want %v",
					cfg.Node.Libp2p.PeerStore.ParsedGCInterval, tc.wantGC)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T15: NATTraversalConfig *bool effective accessors
// ---------------------------------------------------------------------------

func TestLoadConfig_NATTraversalEffective(t *testing.T) {
	cases := []struct {
		name      string
		yaml      string
		wantAuto  bool
		wantRelay bool
		wantDCUtR bool
	}{
		{
			name: "all omitted → effective true (preserves pre-T15 behaviour)",
			yaml: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantAuto: true, wantRelay: true, wantDCUtR: true,
		},
		{
			name: "all explicit true",
			yaml: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
    nat_traversal:
      autonat: true
      auto_relay: true
      dcutr: true
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantAuto: true, wantRelay: true, wantDCUtR: true,
		},
		{
			name: "all explicit false",
			yaml: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
    nat_traversal:
      autonat: false
      auto_relay: false
      dcutr: false
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantAuto: false, wantRelay: false, wantDCUtR: false,
		},
		{
			name: "mixed — autonat off, others on",
			yaml: `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
    nat_traversal:
      autonat: false
      auto_relay: true
      dcutr: true
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
`,
			wantAuto: false, wantRelay: true, wantDCUtR: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempYAML(t, tc.yaml)
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			n := cfg.Node.Libp2p.NATTraversal
			if got := n.AutoNATEffective(); got != tc.wantAuto {
				t.Errorf("AutoNATEffective = %v, want %v", got, tc.wantAuto)
			}
			if got := n.AutoRelayEffective(); got != tc.wantRelay {
				t.Errorf("AutoRelayEffective = %v, want %v", got, tc.wantRelay)
			}
			if got := n.DCUtREffective(); got != tc.wantDCUtR {
				t.Errorf("DCUtREffective = %v, want %v", got, tc.wantDCUtR)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T15: edge.*.enabled cache gates — config-side parsing
// ---------------------------------------------------------------------------

func TestLoadConfig_CacheEnabledDefaults(t *testing.T) {
	const yaml = `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
edge:
  prefix_cache: { path: "/data/prefix", size_gb: 100 }
  warm_cache:   { path: "/data/warm",   size_gb: 50 }
  # cold_cache: removed in T17 (deprecated)
`
	path := writeTempYAML(t, yaml)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Edge.PrefixCache.Enabled {
		t.Errorf("PrefixCache.Enabled = true, want false when omitted")
	}
	if cfg.Edge.WarmCache.Enabled {
		t.Errorf("WarmCache.Enabled = true, want false when omitted")
	}
}

// ---------------------------------------------------------------------------
// T17: deprecated-key Warn scanner
// ---------------------------------------------------------------------------

// captureSlogWarns swaps the default slog logger for a text handler writing to
// a buffer, runs fn, then restores the original logger. Returns the buffered
// text. Each call is serialized via the package-level slogLoggerMu so parallel
// tests don't clobber each other's default logger.
var slogLoggerMu sync.Mutex

func captureSlogWarns(t *testing.T, fn func()) string {
	t.Helper()
	slogLoggerMu.Lock()
	defer slogLoggerMu.Unlock()

	var buf bytes.Buffer
	prev := slog.Default()
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(prev)

	fn()
	return buf.String()
}

// TestLoadConfig_DeprecatedKeysEmitWarns loads an edge-node YAML that includes
// every deprecated key removed in T17 and asserts that LoadConfig (a) still
// succeeds, (b) emits a slog.Warn for each deprecated key path. This is the
// "old configs still load (with warnings) instead of breaking" contract.
func TestLoadConfig_DeprecatedKeysEmitWarns(t *testing.T) {
	const yaml = `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
edge:
  prefix_cache: { path: "/data/prefix", size_gb: 100 }
  warm_cache:   { path: "/data/warm",   size_gb: 50 }
  cold_cache:   { enabled: true, path: "/data/cold", size_gb: 200 }
access_layer:
  data_plane:
    enabled: false
    subscribe_control: true
    drivers: ["115", "baidu"]
    rate_limit_local: true
  fetch_segment_server: { enabled: true }
  fetch_segment_client: { enabled: true }
  vendor_profiles:
    baidu: { weight: 2.0 }
  rate_limits:
    baidu: { qps: 2.0, burst: 4, concurrent: 8 }
  health_check: { interval: "30s" }
  cloud_accounts:
    - vendor: baidu
      account_id: "baidu_acct_01"
      enabled: true
`
	path := writeTempYAML(t, yaml)

	out := captureSlogWarns(t, func() {
		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("LoadConfig with deprecated keys failed: %v", err)
		}
		if cfg.Access.DataPlane.Enabled {
			t.Error("DataPlane.Enabled should be false")
		}
	})

	deprecatedKeys := []string{
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
	for _, k := range deprecatedKeys {
		want := "key=" + k
		if !strings.Contains(out, want) {
			t.Errorf("expected slog.Warn for deprecated key %q in output:\n%s", k, out)
		}
	}
}

// TestLoadConfig_CleanYAMLEmitsNoWarns loads a minimal edge-node YAML with no
// deprecated keys and asserts that LoadConfig emits zero slog.Warn lines. This
// is the plan's "QA happy = clean YAML → no Warn" gate (plan line 252).
func TestLoadConfig_CleanYAMLEmitsNoWarns(t *testing.T) {
	const yaml = `
node:
  identity: { priv_key_path: "/data/key" }
  declared_capabilities: { edge: true }
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht: { namespace: "edge", mode: "client" }
  jwt_service:
    endpoint: "http://cp/v1/node/jwt"
edge:
  prefix_cache: { path: "/data/prefix", size_gb: 100 }
  warm_cache:   { path: "/data/warm",   size_gb: 50 }
access_layer:
  data_plane: { enabled: false }
`
	path := writeTempYAML(t, yaml)

	out := captureSlogWarns(t, func() {
		_, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("LoadConfig clean YAML failed: %v", err)
		}
	})
	if strings.Contains(out, "level=WARN") {
		t.Errorf("expected zero WARN lines for clean YAML, got:\n%s", out)
	}
}
