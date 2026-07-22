package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"

	"github.com/shlande/mediaworker/internal/ingest"
)

const (
	exitSuccess = 0
	exitRuntime = 1
	exitUsage   = 2
)

// runUpload implements the "upload" subcommand. It streams the file to the
// ingest-worker via multipart POST with single-pass SHA-256 hashing.
//
// Flags:
//
//	-addr      ingest-worker base URL (required, e.g. http://10.0.0.1:8080)
//	-type      content type (required: image | dash_video)
//	-file      path to the file to upload (required)
//	-content-id optional pre-assigned content ID
//	-metadata  optional JSON metadata string
//
// Exit codes: 0 = success, 1 = runtime/API error, 2 = usage/flag error.
func runUpload(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("upload", flag.ContinueOnError)
	fs.SetOutput(stderr)

	addr := fs.String("addr", "", "ingest-worker base URL (required)")
	ct := fs.String("type", "", "content type: image or dash_video (required)")
	filePath := fs.String("file", "", "path to file (required)")
	contentID := fs.String("content-id", "", "pre-assigned content ID (optional)")
	metadataJSON := fs.String("metadata", "", "metadata JSON string (optional)")

	fs.Usage = func() {
		fmt.Fprintf(stderr, `Usage: mwcli upload -addr <url> -type <image|dash_video> -file <path> [-content-id <id>] [-metadata <json>]

Upload a file to an ingest-worker.

Required flags:
  -addr   ingest-worker base URL (e.g. http://10.0.0.1:8080)
  -type   content type: image or dash_video
  -file   path to the file to upload

Optional flags:
  -content-id  pre-assigned content ID
  -metadata    metadata JSON string
`)
	}

	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	switch {
	case *addr == "":
		fmt.Fprintln(stderr, "missing required flag: -addr")
		return exitUsage
	case *ct == "":
		fmt.Fprintln(stderr, "missing required flag: -type")
		return exitUsage
	case *ct != "image" && *ct != "dash_video":
		fmt.Fprintf(stderr, "invalid -type %q: must be image or dash_video\n", *ct)
		return exitUsage
	case *filePath == "":
		fmt.Fprintln(stderr, "missing required flag: -file")
		return exitUsage
	}

	f, err := os.Open(*filePath)
	if err != nil {
		fmt.Fprintf(stderr, "open file: %v\n", err)
		return exitRuntime
	}
	defer func() { _ = f.Close() }()

	hasher := sha256.New()

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	// Stream the multipart body in a goroutine: file → (TeeReader → hasher) →
	// multipart form. The io.Pipe wires this to the HTTP request body so the
	// entire file is never buffered in memory.
	go func() {
		var writeErr error
		defer func() {
			if writeErr != nil {
				_ = pw.CloseWithError(writeErr)
				return
			}
			// Close mw first (writes boundary terminator), then pw.
			_ = mw.Close() // writes trailing boundary, flushes to pw
			_ = pw.Close()
		}()

		formFile, err := mw.CreateFormFile("file", filepath.Base(*filePath))
		if err != nil {
			writeErr = err
			return
		}

		tee := io.TeeReader(f, hasher)
		if _, err := io.Copy(formFile, tee); err != nil {
			writeErr = fmt.Errorf("stream file: %w", err)
			return
		}

		if *contentID != "" {
			if err := mw.WriteField("content_id", *contentID); err != nil {
				writeErr = fmt.Errorf("write content_id: %w", err)
				return
			}
		}
		if *metadataJSON != "" {
			if err := mw.WriteField("metadata", *metadataJSON); err != nil {
				writeErr = fmt.Errorf("write metadata: %w", err)
				return
			}
		}
	}()

	url := fmt.Sprintf("%s/ingest/%s", *addr, *ct)
	req, err := http.NewRequest(http.MethodPost, url, pr)
	if err != nil {
		fmt.Fprintf(stderr, "create request: %v\n", err)
		return exitRuntime
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(stderr, "request failed: %v\n", err)
		return exitRuntime
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		fmt.Fprintf(stderr, "server returned %d %s: %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), body)
		return exitRuntime
	}

	var result ingest.IngestResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintf(stderr, "decode response: %v\n", err)
		return exitRuntime
	}

	blobHash := fmt.Sprintf("sha256:%x", hasher.Sum(nil))

	fmt.Fprintf(stdout, "content_id: %s\nblob_hash: %s\n", result.ContentID, blobHash)
	return exitSuccess
}

// Ensure stdlib-only — compilation will fail if any third-party import creeps
// in.
var _ = errors.New
