package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/config"
)

const reloadBaseYAML = `
node:
  identity:
    priv_key_path: "/data/key"
  libp2p:
    listen: ["/ip4/0.0.0.0/tcp/9001"]
    dht:
      namespace: "edge"
  jwt_service:
    endpoint: "https://cp.example.com/v1/node/jwt"
    refresh_interval: "5m"
    refresh_before_expiry: "5m"
hash_ring:
  replicas: 150
admin_api:
  listen: "127.0.0.1:8081"
  token: "token-a"
`

// reloadFixture builds a Server (token-a), a Reloader over a temp yaml, and
// the shared durations, with a probe route to test token rotation.
type reloadFixture struct {
	srv       *Server
	reloader  *Reloader
	durations *config.RefreshDurations
	path      string
}

func newReloadFixture(t *testing.T, yaml string) *reloadFixture {
	t.Helper()
	t.Setenv("NODE_ADMIN_TOKEN", "")
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	running, err := config.LoadConfig(path)
	if err != nil {
		t.Fatalf("load fixture config: %v", err)
	}
	durations := &config.RefreshDurations{}
	durations.Store(running.Node.JWTService.ParsedRefreshInterval, running.Node.JWTService.ParsedRefreshBeforeExpiry)

	srv := NewServer(running.AdminAPI.Token)
	srv.Handle("GET /v1/ping", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"pong": "1"})
	})
	rl := NewReloader(path, running, durations)
	rl.RegisterReloadRoutes(srv)
	return &reloadFixture{srv: srv, reloader: rl, durations: durations, path: path}
}

func (f *reloadFixture) post(t *testing.T, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/reload-config", nil)
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	rr := httptest.NewRecorder()
	f.srv.mux.ServeHTTP(rr, req)
	return rr
}

func (f *reloadFixture) get(t *testing.T, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/ping", nil)
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	rr := httptest.NewRecorder()
	f.srv.mux.ServeHTTP(rr, req)
	return rr
}

func (f *reloadFixture) rewrite(t *testing.T, yaml string) {
	t.Helper()
	if err := os.WriteFile(f.path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
}

func decodeReport(t *testing.T, rr *httptest.ResponseRecorder) config.ReloadReport {
	t.Helper()
	var report config.ReloadReport
	if err := json.Unmarshal(rr.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v (body %q)", err, rr.Body.String())
	}
	return report
}

// Given yaml changes to all three whitelisted fields, when reload fires, then
// the durations holder and the server token are live-updated and the report
// lists all three fields as applied.
func TestReload_HotAppliesWhitelist(t *testing.T) {
	f := newReloadFixture(t, reloadBaseYAML)

	f.rewrite(t, strings.NewReplacer(
		`refresh_interval: "5m"`, `refresh_interval: "10m"`,
		`refresh_before_expiry: "5m"`, `refresh_before_expiry: "7m"`,
		`token: "token-a"`, `token: "token-b"`,
	).Replace(reloadBaseYAML))

	rr := f.post(t, "token-a")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rr.Code, rr.Body.String())
	}
	report := decodeReport(t, rr)
	for _, field := range []string{
		config.ReloadFieldJWTRefreshInterval,
		config.ReloadFieldJWTRefreshBeforeExpiry,
		config.ReloadFieldAdminToken,
	} {
		found := false
		for _, a := range report.Applied {
			if a == field {
				found = true
			}
		}
		if !found {
			t.Fatalf("applied %v missing %q", report.Applied, field)
		}
	}

	iv, be := f.durations.Load()
	if iv != 10*time.Minute || be != 7*time.Minute {
		t.Fatalf("durations = (%v, %v), want (10m, 7m)", iv, be)
	}
	if rr := f.get(t, "token-a"); rr.Code != http.StatusUnauthorized {
		t.Fatalf("old token: status = %d, want 401 after reload", rr.Code)
	}
	if rr := f.get(t, "token-b"); rr.Code != http.StatusOK {
		t.Fatalf("new token: status = %d, want 200 after reload", rr.Code)
	}
}

// Given a yaml that no longer parses, when reload fires, then the response is
// 422 and the running durations/token are unchanged.
func TestReload_BadYAML_422_ConfigUnchanged(t *testing.T) {
	f := newReloadFixture(t, reloadBaseYAML)

	f.rewrite(t, `node: { identity: { priv_key_path: "/key" } `) // unbalanced brace

	rr := f.post(t, "token-a")
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rr.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if !strings.HasPrefix(body["error"], "reload config:") {
		t.Fatalf("error = %q, want reload context prefix", body["error"])
	}

	iv, be := f.durations.Load()
	if iv != 5*time.Minute || be != 5*time.Minute {
		t.Fatalf("durations changed to (%v, %v) despite failed reload", iv, be)
	}
	if rr := f.get(t, "token-a"); rr.Code != http.StatusOK {
		t.Fatalf("token-a: status = %d, want 200 (config unchanged)", rr.Code)
	}
}

// Given a hash_ring.replicas change, when reload fires, then the response is
// 200 with replicas in not_applied (with reason) and nothing applied.
func TestReload_ReplicasRefused(t *testing.T) {
	f := newReloadFixture(t, reloadBaseYAML)

	f.rewrite(t, strings.Replace(reloadBaseYAML, "replicas: 150", "replicas: 300", 1))

	rr := f.post(t, "token-a")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	report := decodeReport(t, rr)
	if len(report.Applied) != 0 {
		t.Fatalf("applied = %v, want empty", report.Applied)
	}
	if len(report.NotApplied) != 1 || report.NotApplied[0].Field != config.ReloadFieldHashRingReplicas {
		t.Fatalf("not_applied = %+v, want the replicas entry", report.NotApplied)
	}
	if report.NotApplied[0].Reason == "" {
		t.Fatal("replicas refusal must carry a reason")
	}
}

// Given the admin token middleware, when reload is posted without a token,
// then the response is 401 like every other admin route.
func TestReload_RequiresAdminToken(t *testing.T) {
	f := newReloadFixture(t, reloadBaseYAML)
	if rr := f.post(t, ""); rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
}

// Given a token applied via reload, when yaml later reverts the token, then a
// second reload applies the revert too (baseline tracks effective runtime).
func TestReload_TokenRevertIsApplied(t *testing.T) {
	f := newReloadFixture(t, reloadBaseYAML)

	f.rewrite(t, strings.Replace(reloadBaseYAML, `token: "token-a"`, `token: "token-b"`, 1))
	if rr := f.post(t, "token-a"); rr.Code != http.StatusOK {
		t.Fatalf("first reload: status = %d", rr.Code)
	}
	if rr := f.get(t, "token-b"); rr.Code != http.StatusOK {
		t.Fatalf("token-b after first reload: status = %d", rr.Code)
	}

	f.rewrite(t, reloadBaseYAML) // revert to token-a
	if rr := f.post(t, "token-b"); rr.Code != http.StatusOK {
		t.Fatalf("second reload: status = %d", rr.Code)
	}
	if rr := f.get(t, "token-a"); rr.Code != http.StatusOK {
		t.Fatalf("token-a after revert reload: status = %d, want 200", rr.Code)
	}
}
