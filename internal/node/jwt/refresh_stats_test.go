package jwt

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

func newStatsClient(t *testing.T, endpoint string) *JWTClient {
	t.Helper()
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}
	return NewJWTClient(nodePriv, nodePeerID, endpoint, types.NodeCapabilities{Edge: true})
}

// startStatsCPServer starts a mock control-plane JWT endpoint and returns its URL.
func startStatsCPServer(t *testing.T) string {
	t.Helper()
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(), cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/node/jwt", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req types.JWTRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		resp, err := svc.HandleJWTRequest(req, r.RemoteAddr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp) // best-effort: test fixture
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() { _ = http.Serve(listener, mux) }()
	return "http://" + listener.Addr().String() + "/v1/node/jwt"
}

// Given a fresh client, When RefreshStats is called, Then every value is zero.
func TestRefreshStats_initialZero(t *testing.T) {
	c := newStatsClient(t, "http://127.0.0.1:1/v1/node/jwt")
	lastAt, lastOK, fails := c.RefreshStats()
	if !lastAt.IsZero() || lastOK || fails != 0 {
		t.Errorf("fresh client stats = (%v, %v, %d), want all zero", lastAt, lastOK, fails)
	}
}

// Given a successful refresh against a live CP, When RefreshStats is read,
// Then the attempt is recorded as OK with zero failures.
func TestRefreshStats_recordsSuccess(t *testing.T) {
	c := newStatsClient(t, startStatsCPServer(t))
	if _, err := c.RequestJWT(context.Background()); err != nil {
		t.Fatalf("RequestJWT: %v", err)
	}
	lastAt, lastOK, fails := c.RefreshStats()
	if !lastOK {
		t.Error("lastOK = false after successful refresh, want true")
	}
	if fails != 0 {
		t.Errorf("failCount24h = %d after success, want 0", fails)
	}
	if lastAt.IsZero() {
		t.Error("lastAt must be set after an attempt")
	}
}

// Given an unreachable endpoint, When a refresh attempt fails, Then
// RefreshStats records the failed attempt.
func TestRefreshStats_recordsFailure(t *testing.T) {
	c := newStatsClient(t, "http://127.0.0.1:1/v1/node/jwt")
	if _, err := c.RequestJWT(context.Background()); err == nil {
		t.Fatal("expected refresh to fail against dead endpoint")
	}
	lastAt, lastOK, fails := c.RefreshStats()
	if lastOK {
		t.Error("lastOK = true after failed refresh, want false")
	}
	if fails != 1 {
		t.Errorf("failCount24h = %d, want 1", fails)
	}
	if lastAt.IsZero() {
		t.Error("lastAt must be set after an attempt")
	}
}

// Given failures 25h and 23h in the past, When RefreshStats reads the trailing
// 24h window, Then the 25h-old failure is pruned and only the 23h-old counts.
func TestRefreshStats_slidingWindow(t *testing.T) {
	c := newStatsClient(t, "http://127.0.0.1:1/v1/node/jwt")
	base := time.Now()
	fake := base
	c.now = func() time.Time { return fake }

	fake = base.Add(-25 * time.Hour)
	c.recordRefresh(false)
	fake = base.Add(-23 * time.Hour)
	c.recordRefresh(false)

	fake = base
	lastAt, lastOK, fails := c.RefreshStats()
	if fails != 1 {
		t.Errorf("failCount24h = %d, want 1 (25h-old outside window, 23h-old counted)", fails)
	}
	if lastOK {
		t.Error("lastOK = true, want false (last attempt was a failure)")
	}
	if want := base.Add(-23 * time.Hour); !lastAt.Equal(want) {
		t.Errorf("lastAt = %v, want %v", lastAt, want)
	}
	if len(c.refreshFailTimestamps) != 1 {
		t.Errorf("fail timestamps len = %d after lazy prune on read, want 1", len(c.refreshFailTimestamps))
	}
}

// Given a failure 25h ago followed by a success now, When RefreshStats is
// read, Then lastOK flips to true and the stale failure drops out of the window.
func TestRefreshStats_successAfterStaleFailure(t *testing.T) {
	c := newStatsClient(t, "http://127.0.0.1:1/v1/node/jwt")
	base := time.Now()
	fake := base
	c.now = func() time.Time { return fake }

	fake = base.Add(-25 * time.Hour)
	c.recordRefresh(false)
	fake = base
	c.recordRefresh(true)

	_, lastOK, fails := c.RefreshStats()
	if !lastOK {
		t.Error("lastOK = false after success, want true")
	}
	if fails != 0 {
		t.Errorf("failCount24h = %d, want 0 (25h-old failure outside window)", fails)
	}
}

// Given retry exhaustion against a dead endpoint, When RequestJWTWithRetry
// gives up, Then every attempt (1 initial + 10 retries) is counted.
func TestRefreshStats_retryRecordsEachAttempt(t *testing.T) {
	c := newStatsClient(t, "http://127.0.0.1:1/v1/node/jwt")
	c.retryBackoff = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.RequestJWTWithRetry(ctx); err == nil {
		t.Fatal("expected retry exhaustion against dead endpoint")
	}
	_, lastOK, fails := c.RefreshStats()
	if fails != 11 {
		t.Errorf("failCount24h = %d, want 11 (per-attempt tracking)", fails)
	}
	if lastOK {
		t.Error("lastOK = true after retry exhaustion, want false")
	}
}
