package onedrive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/types"
)

// fakeTokenMgr returns a TokenManager that always gives a static token,
// backed by an httptest.Server that returns a minimal token response.
func fakeTokenMgr(t *testing.T) *auth.TokenManager {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"test-token","expires_in":3600}`))
	}))
	t.Cleanup(srv.Close)

	tm := auth.NewTokenManager(nil)
	tm.Register(types.VendorOneDrive, "test-account", auth.OAuth2Config{
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		RefreshToken: "test-refresh",
		RedirectURI:  "http://localhost",
		TokenURL:     srv.URL,
	})
	return tm
}

// -----------------------------------------------------------------------
// List tests
// -----------------------------------------------------------------------

func Test_List_when_dirID_is_root_expect_children(t *testing.T) {
	// Given: a Graph API mock returning root children
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertAuth(t, r)
		if !strings.Contains(r.URL.String(), "/root/children") {
			t.Errorf("expected /root/children, got %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"value": [
				{"id":"file1","name":"doc.txt","size":1024,"file":{},"fileSystemInfo":{"lastModifiedDateTime":"2025-01-15T10:30:00Z"}},
				{"id":"folder1","name":"docs","size":0,"fileSystemInfo":{"lastModifiedDateTime":"2025-01-14T08:00:00Z"}}
			]
		}`))
	}))
	defer server.Close()

	// When
	d, ctx := newTestDriver(t, server)
	files, err := d.List(ctx, "root", 1)

	// Then
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}
	if files[1].IsDir != true {
		t.Errorf("folder item should have IsDir=true")
	}
	if files[0].IsDir != false {
		t.Errorf("file item should have IsDir=false")
	}
}

func Test_List_when_page_advances_expect_next_link_followed(t *testing.T) {
	// Given: mock with 2 pages, we request page 2
	var callCount atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.String(), "nextLink") {
			_, _ = w.Write([]byte(`{"value":[{"id":"page2","name":"pg2.txt","size":5,"file":{},"fileSystemInfo":{"lastModifiedDateTime":"2025-06-01T00:00:00Z"}}]}`))
		} else {
			_, _ = fmt.Fprintf(w, `{"@odata.nextLink":"%s/nextLink","value":[{"id":"page1","name":"pg1.txt","size":3,"file":{},"fileSystemInfo":{"lastModifiedDateTime":"2025-06-01T00:00:00Z"}}]}`, server.URL)
		}
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	files, err := d.List(ctx, "folder1", 2)

	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file on page 2, got %d", len(files))
	}
	if files[0].ID != "page2" {
		t.Errorf("expected page2, got %s", files[0].ID)
	}
	if callCount.Load() != 2 {
		t.Errorf("expected 2 API calls, got %d", callCount.Load())
	}
}

func Test_List_when_page_exceeds_available_expect_nil(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"value":[{"id":"only","name":"only.txt","size":1,"file":{},"fileSystemInfo":{"lastModifiedDateTime":"2025-01-01T00:00:00Z"}}]}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	files, err := d.List(ctx, "folder1", 3) // page 3, but only 1 page exists

	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if files != nil {
		t.Errorf("expected nil for out-of-range page, got %v", files)
	}
}

// -----------------------------------------------------------------------
// Get tests
// -----------------------------------------------------------------------

func Test_Get_expect_file_info(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"abc","name":"photo.jpg","size":2048,"file":{},"fileSystemInfo":{"lastModifiedDateTime":"2025-03-20T12:00:00Z"}}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	fi, err := d.Get(ctx, "abc")

	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if fi.ID != "abc" {
		t.Errorf("expected ID abc, got %s", fi.ID)
	}
	if fi.Name != "photo.jpg" {
		t.Errorf("expected Name photo.jpg, got %s", fi.Name)
	}
	if fi.Size != 2048 {
		t.Errorf("expected Size 2048, got %d", fi.Size)
	}
	if fi.IsDir {
		t.Error("expected IsDir=false for file")
	}
}

// -----------------------------------------------------------------------
// GetLink tests
// -----------------------------------------------------------------------

func Test_GetLink_expect_anonymous_URL(t *testing.T) {
	const downloadURL = "https://public.example.com/dl/abc"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertAuth(t, r)
		if !strings.Contains(r.URL.RawQuery, "@microsoft.graph.downloadUrl") {
			t.Errorf("expected $select=@microsoft.graph.downloadUrl, got %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"@microsoft.graph.downloadUrl":"%s"}`, downloadURL)
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	link, err := d.GetLink(ctx, "abc")

	if err != nil {
		t.Fatalf("GetLink() error: %v", err)
	}
	if link.URL != downloadURL {
		t.Errorf("expected URL %s, got %s", downloadURL, link.URL)
	}
	if link.IPBound {
		t.Error("expected IPBound=false for anonymous OneDrive link")
	}
	if link.Headers == nil {
		t.Error("expected non-nil Headers map")
	}
	if len(link.Headers) != 0 {
		t.Errorf("expected empty Headers, got %v", link.Headers)
	}
	if time.Until(link.ExpireAt) < 59*time.Minute {
		t.Errorf("ExpireAt should be ~1h ahead, got %v", link.ExpireAt)
	}
}

func Test_GetLink_when_no_downloadURL_expect_error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"abc"}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	_, err := d.GetLink(ctx, "abc")

	if err == nil {
		t.Fatal("expected error for missing downloadUrl")
	}
}

// -----------------------------------------------------------------------
// Put (small file) tests
// -----------------------------------------------------------------------

func Test_PutSmall_expect_direct_upload(t *testing.T) {
	const content = "Hello, OneDrive!"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertAuth(t, r)
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT for small upload, got %s", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != content {
			t.Errorf("expected body %q, got %q", content, string(body))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"new123","name":"test.txt","size":17,"file":{},"fileSystemInfo":{"lastModifiedDateTime":"2025-07-01T00:00:00Z"}}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	fi, err := d.Put(ctx, "root", "test.txt", strings.NewReader(content), int64(len(content)))

	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}
	if fi.ID != "new123" {
		t.Errorf("expected ID new123, got %s", fi.ID)
	}
	if fi.Size != 17 {
		t.Errorf("expected Size 17, got %d", fi.Size)
	}
}

// -----------------------------------------------------------------------
// Put (large file) tests
// -----------------------------------------------------------------------

func Test_PutLarge_expect_chunked_upload(t *testing.T) {
	const chunkSize = uploadChunkSize
	// Create content larger than 4 MB: two chunks.
	const totalSize = chunkSize + 100
	content := bytes.Repeat([]byte("A"), totalSize)

	var chunkRanges []string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "createUploadSession") {
			assertAuth(t, r)
			w.Header().Set("Content-Type", "application/json")
			// Use server URL to construct uploadUrl pointing back to the same server.
			_, _ = fmt.Fprintf(w, `{"uploadUrl":"%s/upload"}`, server.URL)
			return
		}
		if r.URL.Path == "/upload" {
			chunkRanges = append(chunkRanges, r.Header.Get("Content-Range"))
			// Check if this is the last chunk.
			cr := r.Header.Get("Content-Range")
			parts := strings.Split(cr, "/")
			if len(parts) == 2 {
				totalStr := parts[1]
				endStr := strings.Split(strings.Split(strings.Split(cr, " ")[1], "-")[1], "/")[0]
				total, _ := strconv.ParseInt(totalStr, 10, 64)
				end, _ := strconv.ParseInt(endStr, 10, 64)
				if end == total-1 {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"id":"large123","name":"large.bin","size":5242980,"file":{},"fileSystemInfo":{"lastModifiedDateTime":"2025-07-01T00:00:00Z"}}`))
					return
				}
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	reader := bytes.NewReader(content)
	fi, err := d.Put(ctx, "root", "large.bin", reader, totalSize)

	if err != nil {
		t.Fatalf("Put() large error: %v", err)
	}
	if fi.ID != "large123" {
		t.Errorf("expected ID large123, got %s", fi.ID)
	}
	if len(chunkRanges) != 2 {
		t.Errorf("expected 2 chunk uploads, got %d: %v", len(chunkRanges), chunkRanges)
	}
	// First chunk: bytes 0-chunkSize-1/totalSize
	if chunkRanges[0] != fmt.Sprintf("bytes 0-%d/%d", chunkSize-1, totalSize) {
		t.Errorf("chunk 0 wrong range: %s", chunkRanges[0])
	}
	// Second chunk: bytes chunkSize-totalSize-1/totalSize
	if chunkRanges[1] != fmt.Sprintf("bytes %d-%d/%d", chunkSize, totalSize-1, totalSize) {
		t.Errorf("chunk 1 wrong range: %s", chunkRanges[1])
	}
}

// -----------------------------------------------------------------------
// Remove tests
// -----------------------------------------------------------------------

func Test_Remove_expect_delete_call(t *testing.T) {
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertAuth(t, r)
		method = r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	err := d.Remove(ctx, "file123")

	if err != nil {
		t.Fatalf("Remove() error: %v", err)
	}
	if method != http.MethodDelete {
		t.Errorf("expected DELETE, got %s", method)
	}
}

func Test_Remove_when_ban_signal_expect_BanSignalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	err := d.Remove(ctx, "file123")

	if err == nil {
		t.Fatal("expected error for 429")
	}
	var ban *types.BanSignalError
	if !errors.As(err, &ban) {
		t.Fatalf("expected BanSignalError, got %T: %v", err, err)
	}
	if ban.Code != 429 {
		t.Errorf("expected code 429, got %d", ban.Code)
	}
}

// -----------------------------------------------------------------------
// Mkdir tests
// -----------------------------------------------------------------------

func Test_Mkdir_expect_correct_api_call(t *testing.T) {
	var bodyRead string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertAuth(t, r)
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/children") {
			t.Errorf("expected .../children path, got %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		bodyRead = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"dir123","name":"newfolder","size":0,"folder":{},"fileSystemInfo":{"lastModifiedDateTime":"2025-07-01T00:00:00Z"}}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	fi, err := d.Mkdir(ctx, "parent1", "newfolder")

	if err != nil {
		t.Fatalf("Mkdir() error: %v", err)
	}
	if fi.ID != "dir123" {
		t.Errorf("expected ID dir123, got %s", fi.ID)
	}
	if !fi.IsDir {
		t.Error("expected IsDir=true for folder")
	}
	// Check that the conflictBehavior is present in the body.
	if !strings.Contains(bodyRead, `"rename"`) {
		t.Errorf("body missing conflictBehavior rename: %s", bodyRead)
	}
}

// -----------------------------------------------------------------------
// HealthCheck tests
// -----------------------------------------------------------------------

func Test_HealthCheck_when_200_expect_healthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	state := d.HealthCheck(ctx)

	if state.State != "healthy" {
		t.Errorf("expected healthy, got %s — %s", state.State, state.ErrorMsg)
	}
}

func Test_HealthCheck_when_429_expect_BanSignalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	state := d.HealthCheck(ctx)

	if state.State != "banned" {
		t.Errorf("expected banned, got %s", state.State)
	}
}

func Test_HealthCheck_when_slow_expect_healthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	state := d.HealthCheck(ctx)

	if state.State != "healthy" {
		t.Errorf("expected healthy (latency is informational), got %s", state.State)
	}
	if state.Latency < 1200*time.Millisecond {
		t.Errorf("HealthCheck.Latency = %v, want >= 1.2s recorded", state.Latency)
	}
}

// -----------------------------------------------------------------------
// RateLimitConfig test
// -----------------------------------------------------------------------

func Test_RateLimitConfig_expect_onedrive_defaults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	_ = ctx // no-op, just need the driver
	cfg := d.RateLimitConfig()

	if cfg.QPS != 10.0 {
		t.Errorf("expected QPS 10.0, got %f", cfg.QPS)
	}
	if cfg.Burst != 20 {
		t.Errorf("expected Burst 20, got %d", cfg.Burst)
	}
	if cfg.ConcurrentLimit != 16 {
		t.Errorf("expected ConcurrentLimit 16, got %d", cfg.ConcurrentLimit)
	}
}

// -----------------------------------------------------------------------
// Vendor test
// -----------------------------------------------------------------------

func Test_Vendor_returns_OneDrive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	d, _ := newTestDriver(t, server)
	if d.Vendor() != types.VendorOneDrive {
		t.Errorf("expected VendorOneDrive, got %s", d.Vendor())
	}
}

// -----------------------------------------------------------------------
// Token / auth header test
// -----------------------------------------------------------------------

func Test_AuthHeader_is_set_on_requests(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","name":"x","size":0,"file":{},"fileSystemInfo":{}}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	_, _ = d.Get(ctx, "x")

	if authHeader != "Bearer test-token" {
		t.Errorf("expected 'Bearer test-token', got %q", authHeader)
	}
}

// -----------------------------------------------------------------------
// Put small vs large boundary (exactly 4 MiB should use small path)
// -----------------------------------------------------------------------

func Test_Put_exactly_4MB_uses_small_path(t *testing.T) {
	const size = 4 * 1024 * 1024
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","name":"4mb.bin","size":4194304,"file":{},"fileSystemInfo":{}}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	reader := bytes.NewReader(bytes.Repeat([]byte("X"), size))
	_, err := d.Put(ctx, "root", "4mb.bin", reader, size)

	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}
	if method != http.MethodPut {
		t.Errorf("expected direct PUT for 4MB file, got %s", method)
	}
}

// helpers
// -----------------------------------------------------------------------

func newTestDriver(t *testing.T, server *httptest.Server) (*OneDriveDriver, context.Context) {
	t.Helper()
	tm := fakeTokenMgr(t)
	d := newOneDriveDriverWithHost(tm, "test-account", server.URL, nil)
	return d, context.Background()
}

func assertAuth(t *testing.T, r *http.Request) {
	t.Helper()
	if ah := r.Header.Get("Authorization"); ah != "Bearer test-token" {
		t.Errorf("expected Bearer auth, got %q", ah)
	}
}

// -----------------------------------------------------------------------
// Path-style dirID tests (BUG-B)
// -----------------------------------------------------------------------

func Test_PutSmall_when_dirID_is_root_colon_path_expect_path_style_URL(t *testing.T) {
	const content = "Hello, path-style!"
	var urlPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertAuth(t, r)
		urlPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"pc123","name":"test.bin","size":20,"file":{},"fileSystemInfo":{}}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	fi, err := d.Put(ctx, "root:/mediaworker", "test.bin", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}
	if fi.ID != "pc123" {
		t.Errorf("expected ID pc123, got %s", fi.ID)
	}
	expected := "/v1.0/me/drive/root:/mediaworker/test.bin:/content"
	if urlPath != expected {
		t.Errorf("expected URL path %q, got %q", expected, urlPath)
	}
}

func Test_PutSmall_when_400_response_expect_error_with_status(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"invalidRequest","message":"Invalid path"}}`))
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	_, err := d.Put(ctx, "root:/badpath", "test.bin", strings.NewReader("data"), 4)
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "400") {
		t.Errorf("error should contain status 400: %v", errStr)
	}
	if strings.Contains(errStr, "BanSignalError") {
		t.Error("400 should NOT be a BanSignalError")
	}
}

func Test_PutLarge_when_chunk_upload_returns_500_expect_error(t *testing.T) {
	const totalSize = uploadChunkSize + 100
	content := bytes.Repeat([]byte("B"), totalSize)

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "createUploadSession") {
			assertAuth(t, r)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"uploadUrl":"%s/upload"}`, server.URL)
			return
		}
		if r.URL.Path == "/upload" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal server error"}`))
			return
		}
	}))
	defer server.Close()

	d, ctx := newTestDriver(t, server)
	reader := bytes.NewReader(content)
	_, err := d.Put(ctx, "root", "large-fail.bin", reader, totalSize)
	if err == nil {
		t.Fatal("expected error for 500 chunk response")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "500") {
		t.Errorf("error should contain status 500: %v", errStr)
	}
}
