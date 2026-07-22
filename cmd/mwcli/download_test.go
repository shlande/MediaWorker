package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestDownload_MissingConfig(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDownload([]string{
		"-blob", "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		"-out", "/tmp/out",
	}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("expected exit 2, got %d\nstderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "missing required flag: -config") {
		t.Fatalf("stderr should mention -config, got: %s", stderr.String())
	}
}

func TestDownload_MissingBlob(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDownload([]string{
		"-config", "/nonexistent/config.yaml",
		"-out", "/tmp/out",
	}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("expected exit 2, got %d\nstderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "missing required flag: -blob") {
		t.Fatalf("stderr should mention -blob, got: %s", stderr.String())
	}
}

func TestDownload_InvalidBlobFormat(t *testing.T) {
	cases := []struct {
		name, blob string
	}{
		{"no prefix", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"short", "sha256:abc"},
		{"wrong prefix", "sha512:0000000000000000000000000000000000000000000000000000000000000000"},
		{"uppercase", "sha256:000000000000000000000000000000000000000000000000000000000000000A"},
		{"too short", "sha256:00000000000000000000000000000000000000000000000000000000000000"},
		{"too long", "sha256:00000000000000000000000000000000000000000000000000000000000000000"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runDownload([]string{
				"-config", "/nonexistent/config.yaml",
				"-blob", tc.blob,
				"-out", "/tmp/out",
			}, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("expected exit 2, got %d\nstderr: %s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "sha256:<hex>") {
				t.Fatalf("stderr should mention format hint, got: %s", stderr.String())
			}
		})
	}
}

func TestDownload_MissingOut(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDownload([]string{
		"-config", "/nonexistent/config.yaml",
		"-blob", "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("expected exit 2, got %d\nstderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "missing required flag: -out") {
		t.Fatalf("stderr should mention -out, got: %s", stderr.String())
	}
}

func TestDownload_NoArgs(t *testing.T) {
	// run dispatches to runDownload with no flags.
	var stdout, stderr bytes.Buffer
	code := run([]string{"download"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("expected exit 2 (missing required flags), got %d", code)
	}
}

func TestDownload_PrintUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDownload([]string{"-h"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("expected exit 2 for -h, got %d", code)
	}
	if !strings.Contains(stderr.String(), "-config") {
		t.Fatalf("usage should mention -config, got: %s", stderr.String())
	}
}
