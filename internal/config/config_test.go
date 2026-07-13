package config

import (
	"os"
	"path/filepath"
	"testing"
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
	if cfg.Node.Libp2p.NATTraversal.AutoRelay != true {
		t.Error("AutoRelay expected true")
	}
	if cfg.Node.Libp2p.ConnGater.IPRateLimit != 50 {
		t.Errorf("IPRateLimit = %d, want 50", cfg.Node.Libp2p.ConnGater.IPRateLimit)
	}

	// JWT
	if cfg.Node.JWTService.Endpoint != "https://control-plane.example.com/v1/node/jwt" {
		t.Errorf("JWT endpoint = %q", cfg.Node.JWTService.Endpoint)
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
// Failure: missing required field
// ---------------------------------------------------------------------------

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