package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testUploadServer returns an httptest server that parses the multipart upload
// and returns the configured response. The handler callback receives the parsed
// form fields so the test can assert correctness.
type uploadServerCase struct {
	handler func(t *testing.T, w http.ResponseWriter, r *http.Request)
}

func newUploadServer(t *testing.T, c uploadServerCase) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.handler(t, w, r)
	}))
}

// writeTestFile creates a temporary file with the given content and returns its
// path and sha256 hex.
func writeTestFile(t *testing.T, content []byte) (path, sha256hex string) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, "test.dat")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	h := sha256.Sum256(content)
	return path, fmt.Sprintf("%x", h)
}

// ─── Happy path ─────────────────────────────────────────────────────

func TestUpload_HappyPath_Image(t *testing.T) {
	testUploadHappyPath(t, "image")
}

func TestUpload_HappyPath_DashVideo(t *testing.T) {
	testUploadHappyPath(t, "dash_video")
}

func testUploadHappyPath(t *testing.T, ct string) {
	t.Helper()

	fileContent := []byte("hello world, this is a test file for mwcli upload")
	filePath, expectedSHA := writeTestFile(t, fileContent)

	srv := newUploadServer(t, uploadServerCase{
		handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			t.Helper()

			// Check method
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}

			// Check URL path
			expectedPath := "/ingest/" + ct
			if r.URL.Path != expectedPath {
				t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				http.Error(w, "not found", http.StatusNotFound)
				return
			}

			// Check Content-Type is multipart
			ctHeader := r.Header.Get("Content-Type")
			if !strings.HasPrefix(ctHeader, "multipart/form-data") {
				t.Errorf("expected multipart Content-Type, got %s", ctHeader)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}

			// Parse the multipart form
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Fatalf("parse multipart: %v", err)
			}
			defer func() { _ = r.MultipartForm.RemoveAll() }()

			// Assert "file" field
			formFile, header, err := r.FormFile("file")
			if err != nil {
				t.Fatalf("FormFile: %v", err)
			}
			defer func() { _ = formFile.Close() }()

			if header.Filename != filepath.Base(filePath) {
				t.Errorf("expected filename %s, got %s", filepath.Base(filePath), header.Filename)
			}

			uploaded, err := io.ReadAll(formFile)
			if err != nil {
				t.Fatalf("read uploaded file: %v", err)
			}
			if string(uploaded) != string(fileContent) {
				t.Errorf("uploaded content mismatch: expected %q, got %q", string(fileContent), string(uploaded))
			}

			// Success response
			resp := map[string]string{"content_id": "abc-123-img"}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	var stdout, stderr strings.Builder
	args := []string{"-addr", srv.URL, "-type", ct, "-file", filePath}
	code := runUpload(args, &stdout, &stderr)

	if code != exitSuccess {
		t.Fatalf("expected exit 0, got %d\nstderr: %s", code, stderr.String())
	}

	// Parse stdout — two lines: content_id: X and blob_hash: sha256:Y
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines of output, got %d:\n%s", len(lines), stdout.String())
	}

	if !strings.HasPrefix(lines[0], "content_id: ") {
		t.Errorf("line 1: expected 'content_id: ...', got %q", lines[0])
	}

	cid := strings.TrimPrefix(lines[0], "content_id: ")
	if cid != "abc-123-img" {
		t.Errorf("content_id: expected 'abc-123-img', got %q", cid)
	}

	expectedBlobLine := "blob_hash: sha256:" + expectedSHA
	if !strings.HasPrefix(lines[1], "blob_hash: sha256:") {
		t.Errorf("line 2: expected 'blob_hash: sha256:...', got %q", lines[1])
	}
	if lines[1] != expectedBlobLine {
		t.Errorf("blob_hash mismatch\nexpected: %s\ngot:      %s", expectedBlobLine, lines[1])
	}

	if stderr.Len() != 0 {
		t.Errorf("expected empty stderr, got: %s", stderr.String())
	}
}

// ─── Optional fields ────────────────────────────────────────────────

func TestUpload_WithOptionalFields(t *testing.T) {
	fileContent := []byte("with optional fields")
	filePath, expectedSHA := writeTestFile(t, fileContent)

	srv := newUploadServer(t, uploadServerCase{
		handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			t.Helper()

			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Fatalf("parse multipart: %v", err)
			}
			defer func() { _ = r.MultipartForm.RemoveAll() }()

			// Check content_id form field
			if cid := r.FormValue("content_id"); cid != "my-custom-id" {
				t.Errorf("expected content_id 'my-custom-id', got %q", cid)
			}

			// Check metadata form field
			if md := r.FormValue("metadata"); md != `{"key":"value"}` {
				t.Errorf("expected metadata '{\"key\":\"value\"}', got %q", md)
			}

			resp := map[string]string{"content_id": "my-custom-id"}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	var stdout, stderr strings.Builder
	args := []string{
		"-addr", srv.URL,
		"-type", "image",
		"-file", filePath,
		"-content-id", "my-custom-id",
		"-metadata", `{"key":"value"}`,
	}
	code := runUpload(args, &stdout, &stderr)
	if code != exitSuccess {
		t.Fatalf("expected exit 0, got %d\nstderr: %s", code, stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d:\n%s", len(lines), stdout.String())
	}
	expectedBlobLine := "blob_hash: sha256:" + expectedSHA
	if lines[1] != expectedBlobLine {
		t.Errorf("blob_hash: expected %s, got %s", expectedBlobLine, lines[1])
	}
}

// ─── Server error ───────────────────────────────────────────────────

func TestUpload_ServerError500(t *testing.T) {
	fileContent := []byte("server error test")
	filePath, _ := writeTestFile(t, fileContent)

	srv := newUploadServer(t, uploadServerCase{
		handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal boom", http.StatusInternalServerError)
		},
	})
	defer srv.Close()

	var stdout, stderr strings.Builder
	code := runUpload([]string{"-addr", srv.URL, "-type", "image", "-file", filePath}, &stdout, &stderr)

	if code != exitRuntime {
		t.Errorf("expected exit 1, got %d", code)
	}

	stderrStr := stderr.String()
	if !strings.Contains(stderrStr, "500") {
		t.Errorf("stderr should contain '500', got: %s", stderrStr)
	}
	if !strings.Contains(stderrStr, "internal boom") {
		t.Errorf("stderr should contain server error body, got: %s", stderrStr)
	}
}

func TestUpload_ServerErrorNon200(t *testing.T) {
	fileContent := []byte("bad request test")
	filePath, _ := writeTestFile(t, fileContent)

	srv := newUploadServer(t, uploadServerCase{
		handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bad content type", http.StatusBadRequest)
		},
	})
	defer srv.Close()

	var stdout, stderr strings.Builder
	code := runUpload([]string{"-addr", srv.URL, "-type", "image", "-file", filePath}, &stdout, &stderr)

	if code != exitRuntime {
		t.Errorf("expected exit 1, got %d", code)
	}

	stderrStr := stderr.String()
	if !strings.Contains(stderrStr, "400") {
		t.Errorf("stderr should contain '400', got: %s", stderrStr)
	}
}

// ─── Unreachable addr ───────────────────────────────────────────────

func TestUpload_Unreachable(t *testing.T) {
	fileContent := []byte("unreachable test")
	filePath, _ := writeTestFile(t, fileContent)

	var stdout, stderr strings.Builder
	// localhost with a port that is definitely not listening
	code := runUpload([]string{"-addr", "http://127.0.0.1:1", "-type", "image", "-file", filePath}, &stdout, &stderr)

	if code != exitRuntime {
		t.Errorf("expected exit 1, got %d", code)
	}

	stderrStr := stderr.String()
	if stderrStr == "" {
		t.Error("expected stderr output, got none")
	}
}

// ─── Missing required flags ─────────────────────────────────────────

func TestUpload_MissingAddr(t *testing.T) {
	var stdout, stderr strings.Builder
	code := runUpload([]string{"-type", "image", "-file", "/dev/null"}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("expected exit 2, got %d", code)
	}
	if !strings.Contains(stderr.String(), "missing required flag") {
		t.Errorf("expected 'missing required flag', got: %s", stderr.String())
	}
}

func TestUpload_MissingType(t *testing.T) {
	var stdout, stderr strings.Builder
	code := runUpload([]string{"-addr", "http://localhost:8080", "-file", "/dev/null"}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("expected exit 2, got %d", code)
	}
	if !strings.Contains(stderr.String(), "missing required flag") {
		t.Errorf("expected 'missing required flag', got: %s", stderr.String())
	}
}

func TestUpload_MissingFile(t *testing.T) {
	var stdout, stderr strings.Builder
	code := runUpload([]string{"-addr", "http://localhost:8080", "-type", "image"}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("expected exit 2, got %d", code)
	}
	if !strings.Contains(stderr.String(), "missing required flag") {
		t.Errorf("expected 'missing required flag', got: %s", stderr.String())
	}
}

func TestUpload_BadType(t *testing.T) {
	var stdout, stderr strings.Builder
	code := runUpload([]string{"-addr", "http://localhost:8080", "-type", "badger", "-file", "/dev/null"}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("expected exit 2, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid -type") {
		t.Errorf("expected 'invalid -type', got: %s", stderr.String())
	}
}

func TestUpload_NoArgs(t *testing.T) {
	var stdout, stderr strings.Builder
	code := runUpload([]string{}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("expected exit 2, got %d", code)
	}
}

func TestUpload_NonexistentFile(t *testing.T) {
	var stdout, stderr strings.Builder
	code := runUpload([]string{"-addr", "http://localhost:8080", "-type", "image", "-file", "/nonexistent/file"}, &stdout, &stderr)
	if code != exitRuntime {
		t.Errorf("expected exit 1, got %d", code)
	}
}

// ─── Subcommand dispatch ────────────────────────────────────────────

func TestRun_NoSubcommand(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run([]string{}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("expected exit 2, got %d", code)
	}
}

func TestRun_UnknownSubcommand(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run([]string{"bogus"}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("expected exit 2, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Errorf("expected 'unknown command' in stderr, got: %s", stderr.String())
	}
}

func TestRun_DownloadMissingFlags(t *testing.T) {
	// Without -config/-blob/-out, runDownload returns exitUsage (2).
	var stdout, stderr strings.Builder
	code := run([]string{"download"}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("expected exit 2 (missing required flags), got %d", code)
	}
	if !strings.Contains(stderr.String(), "missing required flag") {
		t.Errorf("expected missing flag message, got: %s", stderr.String())
	}
}

func TestRun_UploadForwardsToRunUpload(t *testing.T) {
	// Verify dispatch routes "upload" to runUpload correctly
	var stdout, stderr strings.Builder
	code := run([]string{"upload", "-addr", "http://localhost:1", "-type", "image", "-file", "/dev/null"}, &stdout, &stderr)
	// This file won't exist (it's /dev/null and it will try to open it).
	// On macOS/Linux, /dev/null is readable, so it may try to POST to port 1.
	// Either way, it should not be exitUsage (2); it should be exitRuntime (1)
	// because it passed parse/validation. The unreachable server or bad file
	// both produce exitRuntime.
	if code == exitUsage {
		t.Errorf("expected non-usage exit code, got %d (subcommand dispatch broken?)\nstderr: %s", code, stderr.String())
	}
}

// ─── Edge cases ─────────────────────────────────────────────────────

func TestUpload_UnexpectedJSONShape(t *testing.T) {
	fileContent := []byte("bad json response")
	filePath, _ := writeTestFile(t, fileContent)

	srv := newUploadServer(t, uploadServerCase{
		handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"surprise": "not content_id"}`))
		},
	})
	defer srv.Close()

	var stdout, stderr strings.Builder
	code := runUpload([]string{"-addr", srv.URL, "-type", "image", "-file", filePath}, &stdout, &stderr)
	// JSON decodes fine but content_id is empty — no error (it's a valid IngestResponse with empty ContentID)
	if code != exitSuccess {
		t.Errorf("expected exit 0 (valid JSON, even if content_id empty), got %d\nstderr: %s", code, stderr.String())
	}
}
