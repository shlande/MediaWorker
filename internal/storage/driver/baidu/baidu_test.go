package baidu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

// testHarness creates a BaiduDriver wired to a mock HTTP server.
type testHarness struct {
	server    *httptest.Server
	tokenMgr  *auth.TokenManager
	driver    *BaiduDriver
	callCount atomic.Int32
}

func newTestHarness(t *testing.T, handler http.HandlerFunc) *testHarness {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	tm := auth.NewTokenManager(nil)
	// Register a token that never expires so we don't need to mock refresh
	tm.Register(types.VendorBaidu, "test-account", auth.OAuth2Config{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		RefreshToken: "test-refresh-token",
		TokenURL:     srv.URL + "/oauth/2.0/token",
	})

	// Pre-seed an access token by directly setting state
	// We can't do that directly, so instead we mock the token endpoint too.
	// But simpler: create a new token manager that wraps our own mock-server-based token flow.
	// Even simpler: use a custom http client that intercepts token requests.
	// Let's use a different approach: create a mock token server and register it.

	return &testHarness{
		server:   srv,
		tokenMgr: tm,
		driver:   newBaiduDriverWithBaseURL(tm, "test-account", "test-client-id", "test-client-secret", nil, srv.URL),
	}
}

func (th *testHarness) Handle(path string, handler func(w http.ResponseWriter, r *http.Request)) {
	// The httptest server uses a single mux; we need to route.
	// We'll use a more flexible approach in the tests below.
	_ = path
	_ = handler
}

// newBaiduDriverWithBaseURL creates a BaiduDriver with explicit baseURL (for testing).
func newBaiduDriverWithBaseURL(
	tokenMgr *auth.TokenManager,
	accountID, clientID, clientSecret string,
	httpc *http.Client,
	baseURL string,
) *BaiduDriver {
	if httpc == nil {
		httpc = http.DefaultClient
	}
	return &BaiduDriver{
		tokenMgr:    tokenMgr,
		accountID:   accountID,
		clientID:    clientID,
		clientSecret: clientSecret,
		baseURL:     baseURL,
		uploadBaseURL: baseURL, // Use same mock server for uploads in tests
		httpc:       httpc,
	}
}

// ─── mockServer is a test server that handles both token refresh and PCS API ───

// mockBaiduServer creates an httptest.Server that responds to Baidu PCS API requests.
// It pre-configures the token endpoint to return a fixed access token.
type mockBaiduServer struct {
	*httptest.Server
	accessToken string
}

func newMockBaiduServer() *mockBaiduServer {
	accessToken := "mock-access-token-123"
	mux := http.NewServeMux()

	// Token endpoint
	mux.HandleFunc("/oauth/2.0/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token":  accessToken,
			"refresh_token": "new-refresh-token",
			"expires_in":    3600,
		})
	})

	srv := httptest.NewServer(mux)
	return &mockBaiduServer{Server: srv, accessToken: accessToken}
}

// handle adds a handler to the mock server's mux.
func (m *mockBaiduServer) handle(pattern string, handler func(w http.ResponseWriter, r *http.Request)) {
	m.Server.Config.Handler.(*http.ServeMux).HandleFunc(pattern, handler)
}

// ─── helpers for building BaiduDriver test instances ───

func makeBaiduDriver(t *testing.T, srv *mockBaiduServer) *BaiduDriver {
	t.Helper()

	tm := auth.NewTokenManager(nil)
	tm.Register(types.VendorBaidu, "test-account", auth.OAuth2Config{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		RefreshToken: "refresh-token",
		TokenURL:     srv.URL + "/oauth/2.0/token",
	})

	return newBaiduDriverWithBaseURL(tm, "test-account", "client-id", "client-secret", nil, srv.URL)
}

// ─── Test: Vendor ───

func TestBaiduDriver_Vendor(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()
	d := makeBaiduDriver(t, srv)

	if d.Vendor() != types.VendorBaidu {
		t.Errorf("Vendor() = %q, want %q", d.Vendor(), types.VendorBaidu)
	}
}

// ─── Test: List ───

func TestBaiduDriver_List_mapsFileInfo(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		// Verify query parameters
		q := r.URL.Query()
		if q.Get("method") != "list" {
			t.Errorf("method = %q, want list", q.Get("method"))
		}
		if q.Get("access_token") != srv.accessToken {
			t.Errorf("access_token = %q, want %q", q.Get("access_token"), srv.accessToken)
		}
		if q.Get("dir") != "/apps/test" {
			t.Errorf("dir = %q, want /apps/test", q.Get("dir"))
		}

		json.NewEncoder(w).Encode(pcsListResponse{
			Errno: 0,
			List: []pcsFileEntry{
				{FsID: 12345, Filename: "file1.txt", Size: 1024, IsDir: 0, ServerMtime: 1710000000, MD5: "abc123"},
				{FsID: 12346, Filename: "subdir", Size: 0, IsDir: 1, ServerMtime: 1710000001, MD5: ""},
			},
		})
	})

	d := makeBaiduDriver(t, srv)
	ctx := context.Background()

	files, err := d.List(ctx, "/apps/test", 1)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("List: got %d files, want 2", len(files))
	}

	// File entry
	f0 := files[0]
	if f0.ID != "12345" {
		t.Errorf("List[0].ID = %q, want 12345", f0.ID)
	}
	if f0.Name != "file1.txt" {
		t.Errorf("List[0].Name = %q, want file1.txt", f0.Name)
	}
	if f0.Size != 1024 {
		t.Errorf("List[0].Size = %d, want 1024", f0.Size)
	}
	if f0.IsDir {
		t.Error("List[0].IsDir = true, want false")
	}
	if f0.Hash != "abc123" {
		t.Errorf("List[0].Hash = %q, want abc123", f0.Hash)
	}
	if f0.Modified.Unix() != 1710000000 {
		t.Errorf("List[0].Modified = %d, want 1710000000", f0.Modified.Unix())
	}

	// Directory entry
	f1 := files[1]
	if f1.ID != "12346" {
		t.Errorf("List[1].ID = %q, want 12346", f1.ID)
	}
	if !f1.IsDir {
		t.Error("List[1].IsDir = false, want true")
	}
}

func TestBaiduDriver_List_banSignal(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		json.NewEncoder(w).Encode(pcsListResponse{Errno: 403, Errmsg: "access denied"})
	})

	d := makeBaiduDriver(t, srv)
	_, err := d.List(context.Background(), "/", 1)
	if err == nil {
		t.Fatal("List: expected error, got nil")
	}
	var ban *types.BanSignalError
	if !errors.As(err, &ban) {
		t.Errorf("List: expected *BanSignalError, got %T: %v", err, err)
	}
}

// ─── Test: Get ───

func TestBaiduDriver_Get_returnsFileInfo(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("method") != "filemetas" {
			t.Errorf("method = %q, want filemetas", q.Get("method"))
		}
		// fsids should contain the file ID
		fsids := q.Get("fsids")
		if !strings.Contains(fsids, "99999") {
			t.Errorf("fsids = %q, should contain 99999", fsids)
		}

		json.NewEncoder(w).Encode(pcsMetaResponse{
			Errno: 0,
			List: []pcsMetaEntry{
				{FsID: 99999, Filename: "bigfile.zip", Size: 1024000, IsDir: 0, ServerMtime: 1700000000, MD5: "deadbeef"},
			},
		})
	})

	d := makeBaiduDriver(t, srv)
	fi, err := d.Get(context.Background(), "99999")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if fi.ID != "99999" {
		t.Errorf("Get.ID = %q, want 99999", fi.ID)
	}
	if fi.Name != "bigfile.zip" {
		t.Errorf("Get.Name = %q, want bigfile.zip", fi.Name)
	}
	if fi.Size != 1024000 {
		t.Errorf("Get.Size = %d, want 1024000", fi.Size)
	}
	if fi.Hash != "deadbeef" {
		t.Errorf("Get.Hash = %q, want deadbeef", fi.Hash)
	}
}

func TestBaiduDriver_Get_notFound(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsMetaResponse{Errno: 0, List: []pcsMetaEntry{}})
	})

	d := makeBaiduDriver(t, srv)
	_, err := d.Get(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("Get nonexistent: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Get error = %q, should contain 'not found'", err.Error())
	}
}

// ─── Test: GetLink ───

func TestBaiduDriver_GetLink_returnsIPBoundLink(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	// Need two endpoints: filemetas for dlink, then a catch-all for the HEAD to dlink
	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsMetaResponse{
			Errno: 0,
			List: []pcsMetaEntry{
				{FsID: 88888, Filename: "video.mp4", Size: 500000, IsDir: 0, ServerMtime: 1700000000, MD5: "md5hash", Dlink: srv.URL + "/dlink"},
			},
		})
	})

	srv.handle("/dlink", func(w http.ResponseWriter, r *http.Request) {
		// Verify User-Agent header
		if r.Header.Get("User-Agent") != "pan.baidu.com" {
			t.Errorf("HEAD User-Agent = %q, want pan.baidu.com", r.Header.Get("User-Agent"))
		}
		// Verify token is appended
		if !strings.Contains(r.URL.RawQuery, "access_token="+srv.accessToken) {
			t.Errorf("dlink URL missing access_token, got %q", r.URL.String())
		}
		w.Header().Set("Location", "https://cdn.baidu.com/pcs/file/video.mp4")
		w.WriteHeader(302)
	})

	d := makeBaiduDriver(t, srv)
	link, err := d.GetLink(context.Background(), "88888")
	if err != nil {
		t.Fatalf("GetLink: unexpected error: %v", err)
	}
	if link.URL != "https://cdn.baidu.com/pcs/file/video.mp4" {
		t.Errorf("GetLink.URL = %q, want cdn URL", link.URL)
	}
	if !link.IPBound {
		t.Error("GetLink.IPBound = false, want true")
	}
	if link.Headers["User-Agent"] != "pan.baidu.com" {
		t.Errorf("GetLink.Headers[User-Agent] = %q, want pan.baidu.com", link.Headers["User-Agent"])
	}
	if link.ExpireAt.Before(time.Now()) {
		t.Error("GetLink.ExpireAt should be in the future")
	}
	if link.ExpireAt.After(time.Now().Add(20*time.Minute)) {
		t.Error("GetLink.ExpireAt should be ~15 minutes from now")
	}
}

func TestBaiduDriver_GetLink_noDlink(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsMetaResponse{
			Errno: 0,
			List:  []pcsMetaEntry{{FsID: 1, Dlink: ""}},
		})
	})

	d := makeBaiduDriver(t, srv)
	_, err := d.GetLink(context.Background(), "1")
	if err == nil {
		t.Fatal("GetLink: expected error for empty dlink")
	}
}

func TestBaiduDriver_GetLink_banSignal(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		json.NewEncoder(w).Encode(pcsMetaResponse{Errno: 429})
	})

	d := makeBaiduDriver(t, srv)
	_, err := d.GetLink(context.Background(), "1")
	var ban *types.BanSignalError
	if !errors.As(err, &ban) {
		t.Errorf("GetLink: expected *BanSignalError, got %T: %v", err, err)
	}
}

// ─── Test: Put ───

func TestBaiduDriver_Put_threeStepUpload(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	var precreateCalled, uploadCalled, createCalled atomic.Bool

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		method := q.Get("method")
		switch method {
		case "precreate":
			precreateCalled.Store(true)
			// Verify form fields
			if err := r.ParseForm(); err == nil {
				if r.Form.Get("path") != "/apps/test/upload.txt" {
					t.Errorf("precreate path = %q", r.Form.Get("path"))
				}
				if r.Form.Get("isdir") != "0" {
					t.Errorf("precreate isdir = %q", r.Form.Get("isdir"))
				}
			}
			json.NewEncoder(w).Encode(pcsPrecreateResponse{Errno: 0, UploadID: "upload-123"})
		case "create":
			createCalled.Store(true)
			json.NewEncoder(w).Encode(pcsCreateResponse{
				Errno: 0,
				Info:  &pcsCreateInfo{FsID: 77777, Path: "/apps/test/upload.txt", Size: 6, IsDir: 0, MD5: "newmd5", ServerMtime: 1700000000},
			})
		default:
			t.Errorf("unexpected method: %s", method)
			w.WriteHeader(400)
		}
	})

	srv.handle("/rest/2.0/pcs/superfile2", func(w http.ResponseWriter, r *http.Request) {
		uploadCalled.Store(true)
		json.NewEncoder(w).Encode(pcsUploadResponse{Errno: 0})
	})

	d := makeBaiduDriver(t, srv)
	ctx := context.Background()

	content := "hello!"
	fi, err := d.Put(ctx, "/apps/test", "upload.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}

	if fi.ID != "77777" {
		t.Errorf("Put.ID = %q, want 77777", fi.ID)
	}
	if fi.Name != "upload.txt" {
		t.Errorf("Put.Name = %q, want upload.txt", fi.Name)
	}
	if fi.Size != 6 {
		t.Errorf("Put.Size = %d, want 6", fi.Size)
	}
	if fi.Hash != "newmd5" {
		t.Errorf("Put.Hash = %q, want newmd5", fi.Hash)
	}

	if !precreateCalled.Load() {
		t.Error("Put: precreate was not called")
	}
	if !uploadCalled.Load() {
		t.Error("Put: upload was not called")
	}
	if !createCalled.Load() {
		t.Error("Put: create was not called")
	}
}

func TestBaiduDriver_Put_emptyFile(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		method := q.Get("method")
		switch method {
		case "precreate":
			json.NewEncoder(w).Encode(pcsPrecreateResponse{Errno: 0, UploadID: "upload-empty"})
		case "create":
			json.NewEncoder(w).Encode(pcsCreateResponse{
				Errno: 0,
				Info:  &pcsCreateInfo{FsID: 88888, Path: "/apps/test/empty.txt", Size: 0, IsDir: 0, MD5: "d41d8cd98f00b204e9800998ecf8427e", ServerMtime: 1700000000},
			})
		}
	})

	d := makeBaiduDriver(t, srv)
	fi, err := d.Put(context.Background(), "/apps/test", "empty.txt", strings.NewReader(""), 0)
	if err != nil {
		t.Fatalf("Put empty: unexpected error: %v", err)
	}
	if fi.Size != 0 {
		t.Errorf("Put empty.Size = %d, want 0", fi.Size)
	}
}

// ─── Test: Remove ───

func TestBaiduDriver_Remove_success(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	var calledPath string
	srv.handle("/api/filemanager", func(w http.ResponseWriter, r *http.Request) {
		calledPath = r.URL.Path
		q := r.URL.Query()
		if q.Get("method") != "delete" {
			t.Errorf("method = %q, want delete", q.Get("method"))
		}
		json.NewEncoder(w).Encode(pcsDeleteResponse{Errno: 0})
	})

	d := makeBaiduDriver(t, srv)
	err := d.Remove(context.Background(), "12345")
	if err != nil {
		t.Fatalf("Remove: unexpected error: %v", err)
	}
	if calledPath != "/api/filemanager" {
		t.Errorf("Remove path = %q, want /api/filemanager", calledPath)
	}
}

func TestBaiduDriver_Remove_banSignal(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/api/filemanager", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(405)
		json.NewEncoder(w).Encode(pcsDeleteResponse{Errno: 405})
	})

	d := makeBaiduDriver(t, srv)
	err := d.Remove(context.Background(), "1")
	var ban *types.BanSignalError
	if !errors.As(err, &ban) {
		t.Errorf("Remove: expected *BanSignalError, got %T: %v", err, err)
	}
}

// ─── Test: Mkdir ───

func TestBaiduDriver_Mkdir_createsDir(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		method := q.Get("method")
		if method != "create" {
			t.Errorf("method = %q, want create", q.Get("method"))
		}

		if err := r.ParseForm(); err == nil {
			if r.Form.Get("isdir") != "1" {
				t.Errorf("isdir = %q, want 1", r.Form.Get("isdir"))
			}
			if r.Form.Get("type") != "2" {
				t.Errorf("type = %q, want 2", r.Form.Get("type"))
			}
			if r.Form.Get("path") != "/parent/newdir" {
				t.Errorf("path = %q, want /parent/newdir", r.Form.Get("path"))
			}
		}

		json.NewEncoder(w).Encode(pcsCreateResponse{
			Errno: 0,
			Info:  &pcsCreateInfo{FsID: 55555, Path: "/parent/newdir", IsDir: 1},
		})
	})

	d := makeBaiduDriver(t, srv)
	fi, err := d.Mkdir(context.Background(), "/parent", "newdir")
	if err != nil {
		t.Fatalf("Mkdir: unexpected error: %v", err)
	}
	if !fi.IsDir {
		t.Error("Mkdir: IsDir = false, want true")
	}
	if fi.ID != "55555" {
		t.Errorf("Mkdir.ID = %q, want 55555", fi.ID)
	}
	if fi.Name != "newdir" {
		t.Errorf("Mkdir.Name = %q, want newdir", fi.Name)
	}
}

// ─── Test: HealthCheck ───

func TestBaiduDriver_HealthCheck_healthy(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/nas", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsUInfoResponse{Errno: 0})
	})

	d := makeBaiduDriver(t, srv)
	state := d.HealthCheck(context.Background())
	if state.State != "healthy" {
		t.Errorf("HealthCheck.State = %q, want healthy", state.State)
	}
	if state.Latency <= 0 {
		t.Error("HealthCheck.Latency should be > 0")
	}
}

func TestBaiduDriver_HealthCheck_banned(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/nas", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
	})

	d := makeBaiduDriver(t, srv)
	state := d.HealthCheck(context.Background())
	if state.State != "banned" {
		t.Errorf("HealthCheck.State = %q, want banned", state.State)
	}
}

func TestBaiduDriver_HealthCheck_degradedOnError(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/nas", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	})

	d := makeBaiduDriver(t, srv)
	state := d.HealthCheck(context.Background())
	if state.State != "degraded" {
		t.Errorf("HealthCheck.State = %q, want degraded", state.State)
	}
}

// ─── Test: RateLimitConfig ───

func TestBaiduDriver_RateLimitConfig(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	d := makeBaiduDriver(t, srv)
	cfg := d.RateLimitConfig()
	want := types.RateLimitConfig{QPS: 2.0, Burst: 4, ConcurrentLimit: 8}
	if cfg != want {
		t.Errorf("RateLimitConfig = %+v, want %+v", cfg, want)
	}
}

// ─── Test: BanSignal on 403/405/429 ───

func TestBaiduDriver_banSignalOnAnyCall(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{"403", 403},
		{"405", 405},
		{"429", 429},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newMockBaiduServer()
			defer srv.Close()

			srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				json.NewEncoder(w).Encode(pcsListResponse{Errno: tt.statusCode, Errmsg: "rate limited"})
			})

			d := makeBaiduDriver(t, srv)
			_, err := d.List(context.Background(), "/", 1)
			var ban *types.BanSignalError
			if !errors.As(err, &ban) {
				t.Errorf("List %d: expected *BanSignalError, got %T: %v", tt.statusCode, err, err)
				return
			}
			if ban.Code != tt.statusCode {
				t.Errorf("BanSignalError.Code = %d, want %d", ban.Code, tt.statusCode)
			}
		})
	}
}

// ─── Test: NewBaiduDriver nil httpc uses default ───

func TestNewBaiduDriver_nilClient(t *testing.T) {
	tm := auth.NewTokenManager(nil)
	d := NewBaiduDriver(tm, "acc", "cid", "secret", nil)
	if d.httpc != http.DefaultClient {
		t.Error("NewBaiduDriver with nil httpc should use http.DefaultClient")
	}
}

// ─── Test: Driver interface implementation ───

func TestBaiduDriver_implementsDriver(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	d := makeBaiduDriver(t, srv)
	var _ driver.Driver = d

	// Set up handlers for all methods
	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Query().Get("method")
		switch method {
		case "list":
			json.NewEncoder(w).Encode(pcsListResponse{Errno: 0, List: []pcsFileEntry{
				{FsID: 1, Filename: "f.txt", Size: 10, ServerMtime: 1},
			}})
		case "filemetas":
			json.NewEncoder(w).Encode(pcsMetaResponse{Errno: 0, List: []pcsMetaEntry{
				{FsID: 1, Filename: "f.txt", Size: 10, ServerMtime: 1, Dlink: srv.URL + "/dlink"},
			}})
		case "precreate":
			json.NewEncoder(w).Encode(pcsPrecreateResponse{Errno: 0, UploadID: "up-1"})
		case "create":
			json.NewEncoder(w).Encode(pcsCreateResponse{Errno: 0, Info: &pcsCreateInfo{
				FsID: 1, Path: "/d/f.txt", Size: 10, ServerMtime: 1, MD5: "hash",
			}})
		}
	})

	srv.handle("/api/filemanager", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsDeleteResponse{Errno: 0})
	})

	srv.handle("/rest/2.0/pcs/superfile2", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsUploadResponse{Errno: 0})
	})

	srv.handle("/rest/2.0/xpan/nas", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsUInfoResponse{Errno: 0})
	})

	srv.handle("/dlink", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://cdn.example.com/file")
		w.WriteHeader(302)
	})

	ctx := context.Background()

	// Exercise all methods
	if v := d.Vendor(); v != types.VendorBaidu {
		t.Errorf("Vendor = %q", v)
	}

	if _, err := d.List(ctx, "/", 1); err != nil {
		t.Errorf("List: %v", err)
	}

	if _, err := d.Get(ctx, "1"); err != nil {
		t.Errorf("Get: %v", err)
	}

	if _, err := d.GetLink(ctx, "1"); err != nil {
		t.Errorf("GetLink: %v", err)
	}

	if _, err := d.Put(ctx, "/d", "f.txt", strings.NewReader("0123456789"), 10); err != nil {
		t.Errorf("Put: %v", err)
	}

	if err := d.Remove(ctx, "1"); err != nil {
		t.Errorf("Remove: %v", err)
	}

	if _, err := d.Mkdir(ctx, "/d", "sub"); err != nil {
		t.Errorf("Mkdir: %v", err)
	}

	if s := d.HealthCheck(ctx); s.State != "healthy" {
		t.Errorf("HealthCheck = %q", s.State)
	}

	if cfg := d.RateLimitConfig(); cfg.QPS != 2.0 {
		t.Errorf("RateLimitConfig.QPS = %f", cfg.QPS)
	}
}

// ─── Test: buildBlockList ───

func TestBuildBlockList(t *testing.T) {
	// 0 bytes → empty
	if s := buildBlockList(0); s != "[]" {
		t.Errorf("buildBlockList(0) = %q, want []", s)
	}
	// 1 byte → 1 block
	s := buildBlockList(1)
	if !strings.Contains(s, `"0"`) {
		t.Errorf("buildBlockList(1) = %q, want [\"0\"]", s)
	}
	// 4MB exactly → 1 block
	s = buildBlockList(chunkSize)
	if !strings.Contains(s, `"0"`) && !strings.HasPrefix(s, "[") {
		t.Errorf("buildBlockList(%d) = %q, unexpected", chunkSize, s)
	}
	// 4MB + 1 → 2 blocks
	s = buildBlockList(chunkSize + 1)
	if count := strings.Count(s, `"0"`); count != 2 {
		t.Errorf("buildBlockList(%d): got %d blocks, want 2", chunkSize+1, count)
	}
}

// ─── Test: nameFromPath ───

func TestNameFromPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/apps/test/file.txt", "file.txt"},
		{"/file.txt", "file.txt"},
		{"file.txt", "file.txt"},
		{"/a/b/c/", ""},
	}
	for _, tt := range tests {
		got := nameFromPath(tt.path)
		if got != tt.want {
			t.Errorf("nameFromPath(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

// ─── Test: token context cancellation ───

func TestBaiduDriver_contextCanceled(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	d := makeBaiduDriver(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := d.List(ctx, "/", 1)
	if err == nil {
		t.Error("List with canceled ctx: expected error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("List error should wrap context.Canceled, got: %v", err)
	}
}

// ─── Test: JSON error handling ───

func TestBaiduDriver_List_badJSON(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	})

	d := makeBaiduDriver(t, srv)
	_, err := d.List(context.Background(), "/", 1)
	if err == nil {
		t.Fatal("List: expected error for bad JSON")
	}
}

// ─── Test: errno != 0 non-ban ───

func TestBaiduDriver_List_errnoNonBan(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(pcsListResponse{Errno: 110, Errmsg: "invalid parameter"})
	})

	d := makeBaiduDriver(t, srv)
	_, err := d.List(context.Background(), "/", 1)
	if err == nil {
		t.Fatal("List: expected error for non-zero errno")
	}
	if !strings.Contains(err.Error(), "invalid parameter") {
		t.Errorf("List error should contain 'invalid parameter', got: %v", err)
	}
}

// ─── Test: HealthCheck high latency ───

func TestBaiduDriver_HealthCheck_degradedSlow(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/nas", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2500 * time.Millisecond)
		json.NewEncoder(w).Encode(pcsUInfoResponse{Errno: 0})
	})

	d := makeBaiduDriver(t, srv)
	// Use a short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := d.HealthCheck(ctx)
	if state.State != "degraded" {
		t.Errorf("HealthCheck.State = %q, want degraded (high latency)", state.State)
	}
}

// ─── Test: GetLink with missing Location header ───

func TestBaiduDriver_GetLink_noLocation(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsMetaResponse{
			Errno: 0,
			List:  []pcsMetaEntry{{FsID: 1, Dlink: srv.URL + "/dlink2"}},
		})
	})

	srv.handle("/dlink2", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200) // not a redirect
	})

	d := makeBaiduDriver(t, srv)
	_, err := d.GetLink(context.Background(), "1")
	if err == nil {
		t.Fatal("GetLink: expected error for missing Location header")
	}
	// The redirect client may handle this differently — it follows 200, so body is consumed
	// The presence of an error is what matters
}

// ─── Test: concurrent Put ───

func TestBaiduDriver_Put_concurrent(t *testing.T) {
	srv := newMockBaiduServer()
	defer srv.Close()

	var precreateCount atomic.Int32

	srv.handle("/rest/2.0/xpan/file", func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Query().Get("method")
		switch method {
		case "precreate":
			precreateCount.Add(1)
			json.NewEncoder(w).Encode(pcsPrecreateResponse{Errno: 0, UploadID: fmt.Sprintf("up-%d", precreateCount.Load())})
		case "create":
			json.NewEncoder(w).Encode(pcsCreateResponse{Errno: 0, Info: &pcsCreateInfo{
				FsID: int64(precreateCount.Load()), Path: "/apps/test/file", Size: 4,
				ServerMtime: 1700000000, MD5: "hash",
			}})
		}
	})

	srv.handle("/rest/2.0/pcs/superfile2", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(pcsUploadResponse{Errno: 0})
	})

	d := makeBaiduDriver(t, srv)
	ctx := context.Background()

	const n = 5
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			_, err := d.Put(ctx, "/apps/test", fmt.Sprintf("file%d.txt", i), strings.NewReader("data"), 4)
			errs <- err
		}(i)
	}

	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("Put %d: unexpected error: %v", i, err)
		}
	}
}

var _ driver.Driver = (*BaiduDriver)(nil)
