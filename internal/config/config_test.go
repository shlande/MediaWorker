package config

import (
	"os"
	"path/filepath"
	"strings"
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
  cold_cache:   { enabled: true, path: "/data/cold",   size_gb: 100000 }
access_layer:
  data_plane:
    enabled: true
    subscribe_control: true
    drivers: ["115", "baidu", "quark", "onedrive", "aliyundrive"]
    link_pool: { max_entries: 10000 }
    rate_limit_local: true
  fetch_segment_server:
    enabled: true
  vendor_profiles:
    "115":       { weight: 3.0, base_latency_ms: 100, bandwidth_mbps: 50 }
    baidu:       { weight: 2.0, base_latency_ms: 200, bandwidth_mbps: 80 }
    quark:       { weight: 1.0, base_latency_ms: 300, bandwidth_mbps: 30 }
    onedrive:    { weight: 2.0, base_latency_ms: 80,  bandwidth_mbps: 40 }
    aliyundrive: { weight: 2.5, base_latency_ms: 90,  bandwidth_mbps: 40 }
  rate_limits:
    "115":       { qps: 1.0,  burst: 2,  concurrent: 5  }
    baidu:       { qps: 2.0,  burst: 4,  concurrent: 8  }
    quark:       { qps: 0.5,  burst: 1,  concurrent: 5  }
    onedrive:    { qps: 10.0, burst: 20, concurrent: 16 }
    aliyundrive: { qps: 5.0,  burst: 10, concurrent: 10 }
  health_check:
    interval: "30s"
  cloud_accounts:
    - vendor: baidu
      account_id: "baidu_acct_01"
      client_id: "baidu_client_id_sample"
      client_secret: "baidu_client_secret_sample"
      refresh_token: "baidu_refresh_token_sample"
      redirect_uri: "http://localhost:8080/auth/callback/baidu"
      region: "cn"
      enabled: true
    - vendor: onedrive
      account_id: "onedrive_acct_01"
      client_id: "onedrive_client_id_sample"
      client_secret: "onedrive_client_secret_sample"
      refresh_token: "onedrive_refresh_token_sample"
      redirect_uri: "http://localhost:8080/auth/callback/onedrive"
      region: "global"
      enabled: true
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
	if !cfg.Edge.ColdCache.Enabled || cfg.Edge.ColdCache.SizeGB != 100000 {
		t.Error("cold_cache misconfigured")
	}

	// Access layer
	if !cfg.Access.DataPlane.Enabled {
		t.Error("data_plane.enabled expected true")
	}
	if len(cfg.Access.DataPlane.Drivers) != 5 {
		t.Errorf("drivers = %d, want 5", len(cfg.Access.DataPlane.Drivers))
	}
	if !cfg.Access.FetchSegmentServer.Enabled {
		t.Error("fetch_segment_server.enabled expected true")
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
  fetch_segment_client:
    enabled: true
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

	// fetch_segment_server should be zero-value (not present in YAML)
	if cfg.Access.FetchSegmentServer.Enabled {
		t.Error("fetch_segment_server should be disabled for edge node")
	}

	// fetch_segment_client.enabled = true
	if !cfg.Access.FetchSegmentClient.Enabled {
		t.Error("fetch_segment_client.enabled expected true")
	}

	// No cold_cache in edge node config
	if cfg.Edge.ColdCache.Enabled {
		t.Error("cold_cache should be disabled for edge node (not in YAML)")
	}
}

// ---------------------------------------------------------------------------
// Extended config: vendor_profiles, rate_limits, health_check, cloud_accounts
// ---------------------------------------------------------------------------

func TestLoadConfig_VendorProfiles(t *testing.T) {
	path := writeTempYAML(t, l4ConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Verify all 5 vendor profiles loaded
	const vendCount = 5
	if len(cfg.Access.VendorProfiles) != vendCount {
		t.Fatalf("VendorProfiles count = %d, want %d", len(cfg.Access.VendorProfiles), vendCount)
	}

	tests := []struct {
		vendor    string
		wantW     float64
		wantLat   int
		wantBW    int
	}{
		{"115",       3.0, 100, 50},
		{"baidu",     2.0, 200, 80},
		{"quark",     1.0, 300, 30},
		{"onedrive",  2.0,  80, 40},
		{"aliyundrive", 2.5, 90, 40},
	}

	for _, tt := range tests {
		p, ok := cfg.Access.VendorProfiles[tt.vendor]
		if !ok {
			t.Errorf("VendorProfiles[%q] missing", tt.vendor)
			continue
		}
		if p.Weight != tt.wantW {
			t.Errorf("VendorProfiles[%q].Weight = %v, want %v", tt.vendor, p.Weight, tt.wantW)
		}
		if p.BaseLatencyMs != tt.wantLat {
			t.Errorf("VendorProfiles[%q].BaseLatencyMs = %d, want %d", tt.vendor, p.BaseLatencyMs, tt.wantLat)
		}
		if p.BandwidthMbps != tt.wantBW {
			t.Errorf("VendorProfiles[%q].BandwidthMbps = %d, want %d", tt.vendor, p.BandwidthMbps, tt.wantBW)
		}
	}
}

func TestLoadConfig_RateLimits(t *testing.T) {
	path := writeTempYAML(t, l4ConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	const vendCount = 5
	if len(cfg.Access.RateLimits) != vendCount {
		t.Fatalf("RateLimits count = %d, want %d", len(cfg.Access.RateLimits), vendCount)
	}

	tests := []struct {
		vendor string
		wantQPS float64
		wantBurst int
		wantConcurrent int
	}{
		{"115",      1.0,  2,  5},
		{"baidu",    2.0,  4,  8},
		{"quark",    0.5,  1,  5},
		{"onedrive", 10.0, 20, 16},
		{"aliyundrive", 5.0, 10, 10},
	}

	for _, tt := range tests {
		r, ok := cfg.Access.RateLimits[tt.vendor]
		if !ok {
			t.Errorf("RateLimits[%q] missing", tt.vendor)
			continue
		}
		if r.QPS != tt.wantQPS {
			t.Errorf("RateLimits[%q].QPS = %v, want %v", tt.vendor, r.QPS, tt.wantQPS)
		}
		if r.Burst != tt.wantBurst {
			t.Errorf("RateLimits[%q].Burst = %d, want %d", tt.vendor, r.Burst, tt.wantBurst)
		}
		if r.Concurrent != tt.wantConcurrent {
			t.Errorf("RateLimits[%q].Concurrent = %d, want %d", tt.vendor, r.Concurrent, tt.wantConcurrent)
		}
	}
}

func TestLoadConfig_HealthCheck(t *testing.T) {
	path := writeTempYAML(t, l4ConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Access.HealthCheck.Interval != "30s" {
		t.Errorf("HealthCheck.Interval = %q, want %q", cfg.Access.HealthCheck.Interval, "30s")
	}
}

func TestLoadConfig_CloudAccounts(t *testing.T) {
	path := writeTempYAML(t, l4ConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	const acctCount = 2
	if len(cfg.Access.CloudAccounts) != acctCount {
		t.Fatalf("CloudAccounts count = %d, want %d", len(cfg.Access.CloudAccounts), acctCount)
	}

	// baidu account
	if cfg.Access.CloudAccounts[0].Vendor != "baidu" {
		t.Errorf("CloudAccounts[0].Vendor = %q, want %q", cfg.Access.CloudAccounts[0].Vendor, "baidu")
	}
	if cfg.Access.CloudAccounts[0].AccountID != "baidu_acct_01" {
		t.Errorf("CloudAccounts[0].AccountID = %q", cfg.Access.CloudAccounts[0].AccountID)
	}
	if !cfg.Access.CloudAccounts[0].Enabled {
		t.Error("CloudAccounts[0].Enabled expected true")
	}

	// onedrive account
	if cfg.Access.CloudAccounts[1].Vendor != "onedrive" {
		t.Errorf("CloudAccounts[1].Vendor = %q, want %q", cfg.Access.CloudAccounts[1].Vendor, "onedrive")
	}
	if cfg.Access.CloudAccounts[1].Region != "global" {
		t.Errorf("CloudAccounts[1].Region = %q", cfg.Access.CloudAccounts[1].Region)
	}
}

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
		name      string
		yamlBody  string
		wantInter time.Duration
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
  cold_cache:   { path: "/data/cold",   size_gb: 200, enabled: false }
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
	if cfg.Edge.ColdCache.Enabled {
		t.Errorf("ColdCache.Enabled = true, want false when explicit false")
	}
}