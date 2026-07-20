package config

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeTempControlPlaneYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "control-plane.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp control-plane config: %v", err)
	}
	return path
}

// ---------------------------------------------------------------------------
// Happy path: full config
// ---------------------------------------------------------------------------

const validControlPlaneYAML = `
jwt_http:
  listen: ":8443"
  read_timeout: "10s"
  write_timeout: "10s"
l4_whitelist:
  db_path: "/data/l4-whitelist.db"
pin_orchestrator:
  rebalance_interval: "10m"
  top_contents_limit: 5000
dht_bootstrap:
  listen_addrs:
    - "/ip4/0.0.0.0/tcp/9001"
  namespace: "edge"
  advertise_ttl: "15m"
  advertise_interval: "5m"
  bootstrap_peers:
    - "/dnsaddr/bootstrap-01.example.com/tcp/9001/p2p/QmPeer"
sync_broadcaster:
  protocol_id: "/edge/control/1.0.0"
  send_timeout: "30s"
metadata:
  pg_dsn: "postgres://user:pass@localhost:5432/mw?sslmode=disable"
  # popularity_query_interval: removed in T17 (deprecated)
identity:
  priv_key_path: "/data/controlplane/ed25519-jwt.key"
  libp2p_priv_key_path: "/data/controlplane/ed25519-libp2p.key"
`

func TestLoadControlPlaneConfig_Valid(t *testing.T) {
	path := writeTempControlPlaneYAML(t, validControlPlaneYAML)

	cfg, err := LoadControlPlaneConfig(path)
	if err != nil {
		t.Fatalf("LoadControlPlaneConfig failed: %v", err)
	}

	// JWT HTTP
	if cfg.JWT.Listen != ":8443" {
		t.Errorf("JWT.Listen = %q, want %q", cfg.JWT.Listen, ":8443")
	}
	if cfg.JWT.ReadTimeout != "10s" {
		t.Errorf("JWT.ReadTimeout = %q, want %q", cfg.JWT.ReadTimeout, "10s")
	}
	if cfg.JWT.WriteTimeout != "10s" {
		t.Errorf("JWT.WriteTimeout = %q, want %q", cfg.JWT.WriteTimeout, "10s")
	}

	// L4 whitelist
	if cfg.L4Whitelist.DBPath != "/data/l4-whitelist.db" {
		t.Errorf("L4Whitelist.DBPath = %q", cfg.L4Whitelist.DBPath)
	}

	// Pin orchestrator
	if cfg.PinOrchestrator.RebalanceInterval != "10m" {
		t.Errorf("PinOrchestrator.RebalanceInterval = %q", cfg.PinOrchestrator.RebalanceInterval)
	}
	if cfg.PinOrchestrator.TopContentsLimit != 5000 {
		t.Errorf("PinOrchestrator.TopContentsLimit = %d", cfg.PinOrchestrator.TopContentsLimit)
	}

	// DHT bootstrap
	if len(cfg.DHTBootstrap.ListenAddrs) != 1 {
		t.Errorf("DHTBootstrap.ListenAddrs = %d, want 1", len(cfg.DHTBootstrap.ListenAddrs))
	}
	if cfg.DHTBootstrap.Namespace != "edge" {
		t.Errorf("DHTBootstrap.Namespace = %q", cfg.DHTBootstrap.Namespace)
	}
	if cfg.DHTBootstrap.AdvertiseTTL != "15m" {
		t.Errorf("DHTBootstrap.AdvertiseTTL = %q", cfg.DHTBootstrap.AdvertiseTTL)
	}
	if cfg.DHTBootstrap.AdvertiseInterval != "5m" {
		t.Errorf("DHTBootstrap.AdvertiseInterval = %q", cfg.DHTBootstrap.AdvertiseInterval)
	}
	if len(cfg.DHTBootstrap.BootstrapPeers) != 1 {
		t.Errorf("DHTBootstrap.BootstrapPeers = %d, want 1", len(cfg.DHTBootstrap.BootstrapPeers))
	}

	// Sync broadcaster
	if cfg.SyncBroadcaster.ProtocolID != "/edge/control/1.0.0" {
		t.Errorf("SyncBroadcaster.ProtocolID = %q", cfg.SyncBroadcaster.ProtocolID)
	}
	if cfg.SyncBroadcaster.SendTimeout != "30s" {
		t.Errorf("SyncBroadcaster.SendTimeout = %q", cfg.SyncBroadcaster.SendTimeout)
	}

	// Metadata
	if cfg.Metadata.PGDSN != "postgres://user:pass@localhost:5432/mw?sslmode=disable" {
		t.Errorf("Metadata.PGDSN = %q", cfg.Metadata.PGDSN)
	}

	// Identity
	if cfg.Identity.PrivKeyPath != "/data/controlplane/ed25519-jwt.key" {
		t.Errorf("Identity.PrivKeyPath = %q", cfg.Identity.PrivKeyPath)
	}
	if cfg.Identity.Libp2pPrivKeyPath != "/data/controlplane/ed25519-libp2p.key" {
		t.Errorf("Identity.Libp2pPrivKeyPath = %q", cfg.Identity.Libp2pPrivKeyPath)
	}
}

// ---------------------------------------------------------------------------
// Failure: missing required field
// ---------------------------------------------------------------------------

func TestLoadControlPlaneConfig_MissingJWTListen(t *testing.T) {
	const yaml = `
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
`
	path := writeTempControlPlaneYAML(t, yaml)
	_, err := LoadControlPlaneConfig(path)
	if err == nil {
		t.Fatal("expected error for missing jwt_http.listen, got nil")
	}
}

// TestLoadControlPlaneConfig_JWTRateLimitInterval: Given a jwt_http stanza
// carrying rate_limit_interval, when the config loads, then the value
// round-trips verbatim; and given a stanza omitting the knob, when the
// config loads, then the field stays empty so the consumer applies the
// code default (F4a: default fixed in cpjwt.DefaultRateLimitInterval).
func TestLoadControlPlaneConfig_JWTRateLimitInterval(t *testing.T) {
	const yamlWith = `
jwt_http:
  listen: ":8443"
  rate_limit_interval: "2m"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
`
	cfg, err := LoadControlPlaneConfig(writeTempControlPlaneYAML(t, yamlWith))
	if err != nil {
		t.Fatalf("load with rate_limit_interval: %v", err)
	}
	if cfg.JWT.RateLimitInterval != "2m" {
		t.Errorf("JWT.RateLimitInterval = %q, want %q", cfg.JWT.RateLimitInterval, "2m")
	}

	const yamlWithout = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
`
	cfg, err = LoadControlPlaneConfig(writeTempControlPlaneYAML(t, yamlWithout))
	if err != nil {
		t.Fatalf("load without rate_limit_interval: %v", err)
	}
	if cfg.JWT.RateLimitInterval != "" {
		t.Errorf("JWT.RateLimitInterval = %q, want empty (consumer applies code default)", cfg.JWT.RateLimitInterval)
	}
}

func TestLoadControlPlaneConfig_MissingDHTNamespace(t *testing.T) {
	const yaml = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: ""
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
`
	path := writeTempControlPlaneYAML(t, yaml)
	_, err := LoadControlPlaneConfig(path)
	if err == nil {
		t.Fatal("expected error for missing dht_bootstrap.namespace, got nil")
	}
}

func TestLoadControlPlaneConfig_MissingPGDSN(t *testing.T) {
	const yaml = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: ""
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
`
	path := writeTempControlPlaneYAML(t, yaml)
	_, err := LoadControlPlaneConfig(path)
	if err == nil {
		t.Fatal("expected error for missing metadata.pg_dsn, got nil")
	}
}

func TestLoadControlPlaneConfig_MissingPrivKey(t *testing.T) {
	const yaml = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: ""
  libp2p_priv_key_path: "/key2"
`
	path := writeTempControlPlaneYAML(t, yaml)
	_, err := LoadControlPlaneConfig(path)
	if err == nil {
		t.Fatal("expected error for missing identity.priv_key_path, got nil")
	}
}

func TestLoadControlPlaneConfig_MissingLibp2pPrivKey(t *testing.T) {
	const yaml = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: ""
`
	path := writeTempControlPlaneYAML(t, yaml)
	_, err := LoadControlPlaneConfig(path)
	if err == nil {
		t.Fatal("expected error for missing identity.libp2p_priv_key_path, got nil")
	}
}

// ---------------------------------------------------------------------------
// Failure: nonexistent file
// ---------------------------------------------------------------------------

func TestLoadControlPlaneConfig_NonexistentFile(t *testing.T) {
	_, err := LoadControlPlaneConfig("/nonexistent/control-plane.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}
}

// ---------------------------------------------------------------------------
// Failure: invalid YAML
// ---------------------------------------------------------------------------

func TestLoadControlPlaneConfig_InvalidYAML(t *testing.T) {
	path := writeTempControlPlaneYAML(t, `jwt_http: { listen: ":8443" `) // unbalanced brace
	_, err := LoadControlPlaneConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

// ---------------------------------------------------------------------------
// Identity helpers
// ---------------------------------------------------------------------------

func TestLoadOrGenerateControlPlaneKey_GenerateAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ed25519.key")

	// First call generates a new key.
	key1, err := LoadOrGenerateControlPlaneKey(path)
	if err != nil {
		t.Fatalf("LoadOrGenerateControlPlaneKey (generate) failed: %v", err)
	}
	if len(key1) != ed25519.PrivateKeySize {
		t.Errorf("private key length = %d, want %d", len(key1), ed25519.PrivateKeySize)
	}

	// Verify the file was created.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("key file was not created")
	}

	// Second call loads existing key.
	key2, err := LoadOrGenerateControlPlaneKey(path)
	if err != nil {
		t.Fatalf("LoadOrGenerateControlPlaneKey (load) failed: %v", err)
	}
	if len(key2) != ed25519.PrivateKeySize {
		t.Errorf("loaded key length = %d, want %d", len(key2), ed25519.PrivateKeySize)
	}

	// Keys must be identical (same seed).
	if !key1.Equal(key2) {
		t.Error("loaded key differs from generated key")
	}
}

func TestSaveControlPlaneKey_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.key")

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	if err := SaveControlPlaneKey(path, priv); err != nil {
		t.Fatalf("SaveControlPlaneKey: %v", err)
	}

	loaded, err := LoadOrGenerateControlPlaneKey(path)
	if err != nil {
		t.Fatalf("LoadOrGenerateControlPlaneKey: %v", err)
	}

	if !priv.Equal(loaded) {
		t.Error("loaded key does not match saved key")
	}
}

func TestLoadOrGenerateControlPlaneKey_InvalidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.key")
	if err := os.WriteFile(path, []byte("not a pem key"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadOrGenerateControlPlaneKey(path)
	if err == nil {
		t.Fatal("expected error for invalid key file, got nil")
	}
}

// ---------------------------------------------------------------------------
// Admin API config (todo 10)
// ---------------------------------------------------------------------------

const controlPlaneYAMLWithAdminAPI = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
admin_api:
  token_secret: "cfg-secret"
`

// Given an admin_api stanza with only token_secret, when the config loads,
// then listen and quota_rebalance_interval fall back to their defaults.
func TestLoadControlPlaneConfig_AdminAPIDefaults(t *testing.T) {
	t.Setenv("ADMIN_TOKEN_SECRET", "")
	path := writeTempControlPlaneYAML(t, controlPlaneYAMLWithAdminAPI)

	cfg, err := LoadControlPlaneConfig(path)
	if err != nil {
		t.Fatalf("LoadControlPlaneConfig failed: %v", err)
	}
	if cfg.AdminAPI.Listen != "127.0.0.1:8082" {
		t.Errorf("AdminAPI.Listen = %q, want %q", cfg.AdminAPI.Listen, "127.0.0.1:8082")
	}
	if cfg.AdminAPI.TokenSecret != "cfg-secret" {
		t.Errorf("AdminAPI.TokenSecret = %q, want config value", cfg.AdminAPI.TokenSecret)
	}
	if cfg.AdminAPI.QuotaRebalanceInterval != "60s" {
		t.Errorf("AdminAPI.QuotaRebalanceInterval = %q, want %q", cfg.AdminAPI.QuotaRebalanceInterval, "60s")
	}
}

// Given admin_api.listen set but no token_secret in YAML, when the env var
// ADMIN_TOKEN_SECRET is set, then the secret resolves from the environment.
func TestLoadControlPlaneConfig_AdminAPIEnvSecretFallback(t *testing.T) {
	t.Setenv("ADMIN_TOKEN_SECRET", "env-secret")
	const yaml = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
admin_api:
  listen: ":9090"
  quota_rebalance_interval: "30s"
`
	path := writeTempControlPlaneYAML(t, yaml)

	cfg, err := LoadControlPlaneConfig(path)
	if err != nil {
		t.Fatalf("LoadControlPlaneConfig failed: %v", err)
	}
	if cfg.AdminAPI.TokenSecret != "env-secret" {
		t.Errorf("AdminAPI.TokenSecret = %q, want env value", cfg.AdminAPI.TokenSecret)
	}
	if cfg.AdminAPI.Listen != ":9090" {
		t.Errorf("AdminAPI.Listen = %q, want explicit :9090", cfg.AdminAPI.Listen)
	}
	if cfg.AdminAPI.QuotaRebalanceInterval != "30s" {
		t.Errorf("AdminAPI.QuotaRebalanceInterval = %q, want explicit 30s", cfg.AdminAPI.QuotaRebalanceInterval)
	}
}

// Given admin_api enabled (listen set) with no secret in YAML or env, when
// the config loads, then validation fails naming admin_api.token_secret.
func TestLoadControlPlaneConfig_AdminAPIMissingSecret(t *testing.T) {
	t.Setenv("ADMIN_TOKEN_SECRET", "")
	const yaml = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
admin_api:
  listen: "127.0.0.1:8082"
`
	path := writeTempControlPlaneYAML(t, yaml)

	_, err := LoadControlPlaneConfig(path)
	if err == nil {
		t.Fatal("expected error for missing admin_api.token_secret, got nil")
	}
	if !strings.Contains(err.Error(), "admin_api.token_secret") {
		t.Errorf("error %q does not name admin_api.token_secret", err)
	}
}

// Given no admin_api stanza at all (and no env secret), when the config
// loads, then the admin server stays disabled (Listen empty) with defaults
// applied elsewhere — existing configs keep working.
func TestLoadControlPlaneConfig_AdminAPIAbsentDisabled(t *testing.T) {
	t.Setenv("ADMIN_TOKEN_SECRET", "")
	path := writeTempControlPlaneYAML(t, validControlPlaneYAML)

	cfg, err := LoadControlPlaneConfig(path)
	if err != nil {
		t.Fatalf("LoadControlPlaneConfig failed: %v", err)
	}
	if cfg.AdminAPI.Listen != "" {
		t.Errorf("AdminAPI.Listen = %q, want empty (disabled)", cfg.AdminAPI.Listen)
	}
	if cfg.AdminAPI.QuotaRebalanceInterval != "60s" {
		t.Errorf("AdminAPI.QuotaRebalanceInterval = %q, want default 60s", cfg.AdminAPI.QuotaRebalanceInterval)
	}
}

// Given explicit optional admin_api fields, when the config loads, then they
// round-trip unchanged.
func TestLoadControlPlaneConfig_AdminAPIOptionalFields(t *testing.T) {
	t.Setenv("ADMIN_TOKEN_SECRET", "")
	const yaml = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
admin_api:
  token_secret: "cfg-secret"
  prometheus_url: "http://prom:9090"
  alert_webhook_token: "hook-token"
`
	path := writeTempControlPlaneYAML(t, yaml)

	cfg, err := LoadControlPlaneConfig(path)
	if err != nil {
		t.Fatalf("LoadControlPlaneConfig failed: %v", err)
	}
	if cfg.AdminAPI.PrometheusURL != "http://prom:9090" {
		t.Errorf("AdminAPI.PrometheusURL = %q", cfg.AdminAPI.PrometheusURL)
	}
	if cfg.AdminAPI.AlertWebhookToken != "hook-token" {
		t.Errorf("AdminAPI.AlertWebhookToken = %q", cfg.AdminAPI.AlertWebhookToken)
	}
}

// ---------------------------------------------------------------------------
// T17: deprecated-key Warn scanner (control plane)
// ---------------------------------------------------------------------------

// TestLoadControlPlaneConfig_DeprecatedKeysEmitWarns loads a control-plane
// YAML that includes the removed popularity_query_interval key and asserts
// that LoadControlPlaneConfig still succeeds AND emits a slog.Warn for it.
func TestLoadControlPlaneConfig_DeprecatedKeysEmitWarns(t *testing.T) {
	const yaml = `
jwt_http:
  listen: ":8443"
dht_bootstrap:
  namespace: "edge"
metadata:
  pg_dsn: "postgres://localhost/mw"
  popularity_query_interval: "10m"
identity:
  priv_key_path: "/key"
  libp2p_priv_key_path: "/key2"
`
	path := writeTempControlPlaneYAML(t, yaml)

	out := captureSlogWarns(t, func() {
		cfg, err := LoadControlPlaneConfig(path)
		if err != nil {
			t.Fatalf("LoadControlPlaneConfig with deprecated key failed: %v", err)
		}
		if cfg.Metadata.PGDSN != "postgres://localhost/mw" {
			t.Errorf("PGDSN mismatch: %q", cfg.Metadata.PGDSN)
		}
	})

	want := "key=metadata.popularity_query_interval"
	if !strings.Contains(out, want) {
		t.Errorf("expected slog.Warn for deprecated key %q in output:\n%s", want, out)
	}
}

// TestLoadControlPlaneConfig_CleanYAMLEmitsNoWarns asserts that a control-plane
// YAML with no deprecated keys produces zero slog.Warn lines.
func TestLoadControlPlaneConfig_CleanYAMLEmitsNoWarns(t *testing.T) {
	path := writeTempControlPlaneYAML(t, validControlPlaneYAML)

	out := captureSlogWarns(t, func() {
		_, err := LoadControlPlaneConfig(path)
		if err != nil {
			t.Fatalf("LoadControlPlaneConfig clean YAML failed: %v", err)
		}
	})
	if strings.Contains(out, "level=WARN") {
		t.Errorf("expected zero WARN lines for clean control-plane YAML, got:\n%s", out)
	}
}
