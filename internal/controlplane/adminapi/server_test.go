package adminapi

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func signedToken(t *testing.T, secret []byte, roles []string) string {
	t.Helper()
	token, err := SignUserToken(UserTokenPayload{
		UserID:   "user-1",
		Username: "root",
		Roles:    roles,
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(time.Hour).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}
	return token
}

func newAuthedServer(t *testing.T, secret []byte, h http.Handler) *httptest.Server {
	t.Helper()
	s := NewServer(secret)
	s.Handle("GET /v1/protected", h, true)
	ts := httptest.NewServer(s.mux)
	t.Cleanup(ts.Close)
	return ts
}

func get(t *testing.T, url, authHeader string) (int, map[string]string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return resp.StatusCode, body
}

// ---------------------------------------------------------------------------
// Bearer middleware: 401 / 403 / 200 matrix
// ---------------------------------------------------------------------------

// Given an auth=true route, when no Authorization header is sent, then 401.
func TestServer_NoToken_401(t *testing.T) {
	ts := newAuthedServer(t, testSecret(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must not run without a token")
	}))

	status, body := get(t, ts.URL+"/v1/protected", "")
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
	if body["error"] == "" {
		t.Errorf("body = %v, want {\"error\": ...}", body)
	}
}

// Given an auth=true route, when the token is malformed or signed with the
// wrong secret, then 401.
func TestServer_BadToken_401(t *testing.T) {
	cases := map[string]string{
		"malformed":    "Bearer not-a-jwt",
		"wrong scheme": "Basic dXNlcjpwYXNz",
		"wrong secret": "Bearer " + signedToken(t, []byte("another-secret"), []string{"admin"}),
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			ts := newAuthedServer(t, testSecret(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Error("handler must not run with a bad token")
			}))
			status, body := get(t, ts.URL+"/v1/protected", header)
			if status != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", status)
			}
			if body["error"] == "" {
				t.Errorf("body = %v, want {\"error\": ...}", body)
			}
		})
	}
}

// Given a structurally valid but expired token, when calling an auth route,
// then 401.
func TestServer_ExpiredToken_401(t *testing.T) {
	token, err := SignUserToken(UserTokenPayload{
		UserID: "user-1", Username: "root", Roles: []string{"admin"},
		Iat: time.Now().Add(-2 * time.Hour).Unix(),
		Exp: time.Now().Add(-time.Hour).Unix(),
	}, testSecret())
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	ts := newAuthedServer(t, testSecret(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must not run with an expired token")
	}))
	status, _ := get(t, ts.URL+"/v1/protected", "Bearer "+token)
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
}

// Given a valid token without the admin role, when calling an auth route,
// then 403 with {"error":"forbidden"}.
func TestServer_NonAdmin_403(t *testing.T) {
	token := signedToken(t, testSecret(), []string{"operator"})
	ts := newAuthedServer(t, testSecret(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must not run for a non-admin role")
	}))

	status, body := get(t, ts.URL+"/v1/protected", "Bearer "+token)
	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", status)
	}
	if body["error"] != "forbidden" {
		t.Errorf("body[error] = %q, want %q", body["error"], "forbidden")
	}
}

// Given a valid admin token, when calling an auth route, then 200 and the
// handler observes the identity via UserFromCtx.
func TestServer_Admin_200_UserFromCtx(t *testing.T) {
	token := signedToken(t, testSecret(), []string{"operator", "admin"})
	ts := newAuthedServer(t, testSecret(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, username, roles, ok := UserFromCtx(r.Context())
		if !ok {
			t.Error("UserFromCtx: ok = false, want true")
		}
		if userID != "user-1" || username != "root" {
			t.Errorf("UserFromCtx = (%q, %q), want (user-1, root)", userID, username)
		}
		if len(roles) != 2 || roles[0] != "operator" || roles[1] != "admin" {
			t.Errorf("roles = %v, want [operator admin]", roles)
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))

	status, body := get(t, ts.URL+"/v1/protected", "Bearer "+token)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body["status"] != "ok" {
		t.Errorf("body = %v, want status=ok", body)
	}
}

// Given an auth=false route, when no token is sent, then the handler runs.
func TestServer_NoAuthRoute_200(t *testing.T) {
	s := NewServer(testSecret())
	s.Handle("POST /v1/auth/login", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"token": "x"})
	}), false)
	ts := httptest.NewServer(s.mux)
	t.Cleanup(ts.Close)

	resp, err := http.Post(ts.URL+"/v1/auth/login", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// UserFromCtx on a bare context reports ok=false.
func TestUserFromCtx_Missing(t *testing.T) {
	if _, _, _, ok := UserFromCtx(context.Background()); ok {
		t.Error("ok = true on empty context, want false")
	}
}

// ---------------------------------------------------------------------------
// ParsePage edges
// ---------------------------------------------------------------------------

func TestParsePage(t *testing.T) {
	cases := []struct {
		name         string
		query        string
		wantPage     int
		wantPageSize int
	}{
		{"defaults", "", 1, 20},
		{"page zero", "page=0", 1, 20},
		{"page negative", "page=-3", 1, 20},
		{"page non-numeric", "page=abc", 1, 20},
		{"page_size zero", "page_size=0", 1, 20},
		{"page_size negative", "page_size=-1", 1, 20},
		{"page_size over max", "page_size=500", 1, 100},
		{"page_size at max", "page_size=100", 1, 100},
		{"valid both", "page=3&page_size=50", 3, 50},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/x?"+tc.query, nil)
			page, pageSize := ParsePage(r)
			if page != tc.wantPage || pageSize != tc.wantPageSize {
				t.Errorf("ParsePage(?%s) = (%d, %d), want (%d, %d)",
					tc.query, page, pageSize, tc.wantPage, tc.wantPageSize)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// WriteError body contract
// ---------------------------------------------------------------------------

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, http.StatusTeapot, "boom")
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"boom"}` {
		t.Errorf("body = %s, want {\"error\":\"boom\"}", got)
	}
}

// ---------------------------------------------------------------------------
// Serve: lifecycle + graceful drain
// ---------------------------------------------------------------------------

// waitForListener dials addr in a retry loop until the server accepts (or
// timeout). This prevents connection-refused races when tests fire HTTP
// requests before the goroutine running Serve has bound the listener.
func waitForListener(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("waitForListener(%s) timed out after %v", addr, timeout)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

// Given a running Server, when ctx is cancelled, then Serve returns nil.
func TestServer_ServeShutdown(t *testing.T) {
	s := NewServer(testSecret())
	s.Handle("GET /ping", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"pong": "1"})
	}), false)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	addr := freeAddr(t)
	go func() { done <- s.Serve(ctx, addr) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/ping")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not come up within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

// Given an in-flight long request, when shutdown begins, then the request is
// drained (not cut off) and still completes with 200.
func TestServer_ServeDrainsInFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	s := NewServer(testSecret())
	s.Handle("GET /slow", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		WriteJSON(w, http.StatusOK, map[string]string{"slow": "done"})
	}), false)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	addr := freeAddr(t)
	go func() { done <- s.Serve(ctx, addr) }()

	waitForListener(t, addr, 2*time.Second)

	respCh := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + addr + "/slow")
		if err != nil {
			respCh <- -1
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		respCh <- resp.StatusCode
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("slow handler never started")
	}

	cancel()
	time.Sleep(100 * time.Millisecond) // shutdown is now draining
	close(release)

	select {
	case status := <-respCh:
		if status != http.StatusOK {
			t.Fatalf("in-flight request status = %d, want 200 (drained, not cut off)", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after drain")
	}
}
