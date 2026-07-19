package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/shlande/mediaworker/internal/storage/metadata"
)

// ─── Fake user store ───────────────────────────────────────────────────────

type fakeAuthUser struct {
	userID       string
	passwordHash string
	roles        []string
	disabled     bool
}

type fakeUserStore struct {
	users       map[string]fakeAuthUser
	createCalls int
}

func newFakeUserStore() *fakeUserStore {
	return &fakeUserStore{users: map[string]fakeAuthUser{}}
}

func (f *fakeUserStore) GetUserByUsername(_ context.Context, username string) (string, string, []string, bool, error) {
	u, ok := f.users[username]
	if !ok {
		return "", "", nil, false, fmt.Errorf("%w: %q", metadata.ErrUserNotFound, username)
	}
	return u.userID, u.passwordHash, u.roles, u.disabled, nil
}

func (f *fakeUserStore) CountUsers(_ context.Context) (int, error) {
	return len(f.users), nil
}

func (f *fakeUserStore) CreateUser(_ context.Context, username, passwordHash string, roles []string) error {
	f.createCalls++
	f.users[username] = fakeAuthUser{userID: "uid-" + username, passwordHash: passwordHash, roles: roles}
	return nil
}

func (f *fakeUserStore) addUser(t *testing.T, username, password string, roles []string, disabled bool) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	f.users[username] = fakeAuthUser{userID: "uid-" + username, passwordHash: string(hash), roles: roles, disabled: disabled}
}

// ─── Helpers ───────────────────────────────────────────────────────────────

var testAuthSecret = []byte("test-secret-32-bytes-padded-exactly!")

func authTestServer(store AdminUserStore) *httptest.Server {
	srv := NewServer(testAuthSecret)
	RegisterAuthRoutes(srv, store)
	return httptest.NewServer(srv.mux)
}

func postJSON(t *testing.T, client *http.Client, url string, body string, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

// ─── Tests ─────────────────────────────────────────────────────────────────

// Given a seeded admin, when logging in with the bootstrap password and
// calling me/logout with the issued token, then the full chain succeeds.
func TestAuthSeedLoginMeLogoutChain(t *testing.T) {
	t.Setenv(bootstrapPasswordEnv, "s3cret-bootstrap")
	store := newFakeUserStore()
	if err := SeedAdminIfEmpty(context.Background(), store); err != nil {
		t.Fatalf("SeedAdminIfEmpty: %v", err)
	}
	if store.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", store.createCalls)
	}

	ts := authTestServer(store)
	defer ts.Close()

	resp := postJSON(t, ts.Client(), ts.URL+"/v1/auth/login", `{"username":"admin","password":"s3cret-bootstrap"}`, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d, want 200; body: %s", resp.StatusCode, readBody(t, resp))
	}
	var login loginResponse
	if err := json.Unmarshal(readBody(t, resp), &login); err != nil {
		t.Fatalf("decode login body: %v", err)
	}
	if login.Token == "" {
		t.Fatal("login token empty")
	}
	if len(login.Roles) != 1 || login.Roles[0] != "admin" {
		t.Errorf("roles = %v, want [admin]", login.Roles)
	}
	if _, err := time.Parse(time.RFC3339, login.ExpiresAt); err != nil {
		t.Errorf("expires_at %q not RFC3339: %v", login.ExpiresAt, err)
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/auth/me", nil)
	if err != nil {
		t.Fatalf("new me request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+login.Token)
	meResp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("me: %v", err)
	}
	if meResp.StatusCode != http.StatusOK {
		t.Fatalf("me status = %d, want 200; body: %s", meResp.StatusCode, readBody(t, meResp))
	}
	var me meResponse
	if err := json.Unmarshal(readBody(t, meResp), &me); err != nil {
		t.Fatalf("decode me body: %v", err)
	}
	if me.Username != "admin" {
		t.Errorf("me.username = %q, want %q", me.Username, "admin")
	}
	if me.UserID == "" {
		t.Error("me.user_id empty")
	}

	logoutResp := postJSON(t, ts.Client(), ts.URL+"/v1/auth/logout", `{}`, login.Token)
	if logoutResp.StatusCode != http.StatusNoContent {
		t.Fatalf("logout status = %d, want 204", logoutResp.StatusCode)
	}
	_ = readBody(t, logoutResp)
}

// Given wrong-password and unknown-user logins, when both fail, then the
// 401 response bodies are byte-identical (anti-enumeration).
func TestAuthLoginFailuresAreIndistinguishable(t *testing.T) {
	store := newFakeUserStore()
	store.addUser(t, "admin", "correct-pass", []string{"admin"}, false)
	ts := authTestServer(store)
	defer ts.Close()

	wrongPass := postJSON(t, ts.Client(), ts.URL+"/v1/auth/login", `{"username":"admin","password":"wrong-pass"}`, "")
	if wrongPass.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password status = %d, want 401", wrongPass.StatusCode)
	}
	wrongPassBody := readBody(t, wrongPass)

	unknownUser := postJSON(t, ts.Client(), ts.URL+"/v1/auth/login", `{"username":"ghost","password":"wrong-pass"}`, "")
	if unknownUser.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown user status = %d, want 401", unknownUser.StatusCode)
	}
	unknownUserBody := readBody(t, unknownUser)

	if !bytes.Equal(wrongPassBody, unknownUserBody) {
		t.Errorf("401 bodies differ (enumeration leak):\nwrong-pass: %s\nunknown:   %s", wrongPassBody, unknownUserBody)
	}
	if !strings.Contains(string(wrongPassBody), `"error":"invalid credentials"`) {
		t.Errorf("401 body = %s, want invalid credentials", wrongPassBody)
	}
}

// Given a disabled account, when logging in with the correct password, then
// the response is 403.
func TestAuthLoginDisabledUser403(t *testing.T) {
	store := newFakeUserStore()
	store.addUser(t, "disabled-admin", "correct-pass", []string{"admin"}, true)
	ts := authTestServer(store)
	defer ts.Close()

	resp := postJSON(t, ts.Client(), ts.URL+"/v1/auth/login", `{"username":"disabled-admin","password":"correct-pass"}`, "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = readBody(t, resp)
}

// Given a malformed login body, when posting, then the response is 400.
func TestAuthLoginMalformedBody400(t *testing.T) {
	store := newFakeUserStore()
	ts := authTestServer(store)
	defer ts.Close()

	resp := postJSON(t, ts.Client(), ts.URL+"/v1/auth/login", `{not json`, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	_ = readBody(t, resp)
}

// Given a non-empty user table, when seeding, then no user is created
// (idempotent across restarts).
func TestAuthSeedSkippedWhenUsersExist(t *testing.T) {
	store := newFakeUserStore()
	store.addUser(t, "someone", "pw", []string{"admin"}, false)

	if err := SeedAdminIfEmpty(context.Background(), store); err != nil {
		t.Fatalf("SeedAdminIfEmpty: %v", err)
	}
	if store.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (table not empty)", store.createCalls)
	}
}

// Given an empty user table and no bootstrap env, when seeding, then a
// random password is generated, Warn-logged once, and the seeded admin can
// log in with it.
func TestAuthSeedRandomPasswordLoggedOnce(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	t.Setenv(bootstrapPasswordEnv, "")
	store := newFakeUserStore()
	if err := SeedAdminIfEmpty(context.Background(), store); err != nil {
		t.Fatalf("SeedAdminIfEmpty: %v", err)
	}
	if store.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", store.createCalls)
	}

	out := logBuf.String()
	if !strings.Contains(out, "generated random initial password") {
		t.Fatalf("startup log missing random-password warning, got: %s", out)
	}
	marker := "password="
	idx := strings.Index(out, marker)
	if idx < 0 {
		t.Fatalf("log line carries no password field: %s", out)
	}
	rest := out[idx+len(marker):]
	end := strings.IndexAny(rest, " \t\n\"")
	if end < 0 {
		end = len(rest)
	}
	logged := rest[:end]
	if logged == "" {
		t.Fatal("logged password empty")
	}

	ts := authTestServer(store)
	defer ts.Close()
	resp := postJSON(t, ts.Client(), ts.URL+"/v1/auth/login",
		fmt.Sprintf(`{"username":"admin","password":%q}`, logged), "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login with logged password status = %d, want 200; body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = readBody(t, resp)
}
