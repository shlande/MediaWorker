package jwt_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	cpmetrics "github.com/shlande/mediaworker/internal/controlplane/metrics"
	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

func defaultTestPolicyT20() config.JWTPolicyConfig {
	return config.JWTPolicyConfig{}
}

// TestRegisterMetricsHandler_MountsAndGetMetrics (T20) confirms that calling
// RegisterMetricsHandler mounts GET /metrics on the JWT server's mux and that
// the metrics endpoint returns 200 with the expected metric names. Also
// verifies that JWT issuance increments cp_jwt_issued_total — the
// "after one JWT → counter +1" gate from plan line 279.
func TestRegisterMetricsHandler_MountsAndGetMetrics(t *testing.T) {
	_, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	auditLog := jwt.NewAuditLog(io.Discard)

	server := jwt.NewJWTHTTPServer(jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, defaultTestPolicyT20()))

	metrics := cpmetrics.NewMetrics()
	server.RegisterMetricsHandler(metrics)

	listenAddr := pickFreeAddrT20(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = server.Serve(ctx, listenAddr, 0, 0)
	}()
	waitForServerT20(t, listenAddr)

	resp, err := http.Get("http://" + listenAddr + "/metrics")
	if err != nil {
		t.Fatalf("get /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	jwtResp, err := issueJWTRequestT20(t, "http://"+listenAddr+"/v1/node/jwt", privKey)
	if err != nil {
		t.Fatalf("issue JWT: %v", err)
	}
	if jwtResp.JWT == "" {
		t.Fatal("empty JWT in response")
	}

	resp2, err := http.Get("http://" + listenAddr + "/metrics")
	if err != nil {
		t.Fatalf("get /metrics (2): %v", err)
	}
	defer resp2.Body.Close()
	body, err := io.ReadAll(resp2.Body)
	if err != nil {
		t.Fatalf("read /metrics body: %v", err)
	}
	if !bytes.Contains(body, []byte("cp_jwt_issued_total")) {
		t.Errorf("cp_jwt_issued_total not present in /metrics body")
	}
	if !bytes.Contains(body, []byte(`outcome="success"`)) {
		t.Errorf("expected outcome=\"success\" label in /metrics body")
	}
}

// TestRegisterMetricsHandler_NoRegistrationMeans404 (T20) confirms that
// without RegisterMetricsHandler, /metrics is NOT mounted (existing
// JWT-only behaviour preserved).
func TestRegisterMetricsHandler_NoRegistrationMeans404(t *testing.T) {
	_, privKey, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	whitelist := jwt.NewPeerIdSet()
	rateLimiter := jwt.NewRateLimiter(1 * time.Millisecond)
	auditLog := jwt.NewAuditLog(io.Discard)

	server := jwt.NewJWTHTTPServer(jwt.NewJWTService(privKey, whitelist, rateLimiter, auditLog, defaultTestPolicyT20()))

	listenAddr := pickFreeAddrT20(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = server.Serve(ctx, listenAddr, 0, 0)
	}()
	waitForServerT20(t, listenAddr)

	resp, err := http.Get("http://" + listenAddr + "/metrics")
	if err != nil {
		t.Fatalf("get /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 (route not mounted), got %d", resp.StatusCode)
	}
}

func issueJWTRequestT20(t *testing.T, url string, privKey ed25519.PrivateKey) (*types.JWTResponse, error) {
	t.Helper()
	peerID, err := sjwt.GeneratePeerID(privKey)
	if err != nil {
		return nil, fmt.Errorf("generate peer id: %w", err)
	}
	signedPeerID := sjwt.SignPeerID(privKey, peerID)

	body, _ := json.Marshal(types.JWTRequest{
		PeerID:       peerID,
		SignedPeerID: signedPeerID,
	})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody))
	}
	var jwtResp types.JWTResponse
	if err := json.NewDecoder(resp.Body).Decode(&jwtResp); err != nil {
		return nil, err
	}
	return &jwtResp, nil
}

func pickFreeAddrT20(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitForServerT20(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server at %s never became ready", addr)
}
