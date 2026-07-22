package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/node/app"
)

var (
	blobHashRx      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	errHashMismatch = errors.New("hash mismatch")
)

func runDownload(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	fs.SetOutput(stderr)

	configPath := fs.String("config", "", "path to node YAML config (required)")
	blob := fs.String("blob", "", "blob hash in sha256:<hex> format (required)")
	outPath := fs.String("out", "", "output file path (required)")
	waitTimeout := fs.Duration("wait-timeout", 60*time.Second, "max time to wait for a usable peer")
	reqTimeout := fs.Duration("req-timeout", 120*time.Second, "max time for the download request")

	fs.Usage = func() {
		fmt.Fprintf(stderr, `Usage: mwcli download -config <yaml> -blob <sha256:hex> -out <path> [-wait-timeout <d>] [-req-timeout <d>]

Download a blob via embedded edge node.

Required flags:
  -config        path to node YAML config
  -blob          blob hash (sha256:<64-char-hex>)
  -out           output file path

Optional flags:
  -wait-timeout  max time to wait for a usable peer (default 60s)
  -req-timeout   max time for the download request (default 120s)
`)
	}

	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	switch {
	case *configPath == "":
		fmt.Fprintln(stderr, "missing required flag: -config")
		return exitUsage
	case *blob == "":
		fmt.Fprintln(stderr, "missing required flag: -blob")
		return exitUsage
	case !blobHashRx.MatchString(*blob):
		fmt.Fprintln(stderr, `-blob must be in sha256:<hex> format (e.g. sha256:1a2b3c... — 64 lowercase hex chars)`)
		return exitUsage
	case *outPath == "":
		fmt.Fprintln(stderr, "missing required flag: -out")
		return exitUsage
	}

	blobPath := *blob
	blobHash := blobPath[7:] // strip "sha256:" prefix

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return exitRuntime
	}

	cpPubKeyHex := os.Getenv("CONTROL_PLANE_PUBKEY")
	if cpPubKeyHex == "" {
		fmt.Fprintln(stderr, "CONTROL_PLANE_PUBKEY environment variable is required — must be hex-encoded Ed25519 public key")
		return exitRuntime
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := app.New(ctx, cfg, app.Options{JWTRequestTimeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintf(stderr, "start embedded node: %v\n", err)
		return exitRuntime
	}
	defer func() { _ = node.Close() }()

	if err := waitForUsablePeer(ctx, node, *waitTimeout); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return exitUsage
	}

	reqCtx, reqCancel := context.WithTimeout(ctx, *reqTimeout)
	defer reqCancel()

	if err := fetchToFile(reqCtx, node, blobHash, blobPath, *outPath, stdout, stderr); err != nil {
		if errors.Is(err, errHashMismatch) {
			return 3
		}
		return exitRuntime
	}
	return exitSuccess
}

func waitForUsablePeer(ctx context.Context, node *app.App, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		for _, entry := range node.PeerStore.ActivePeers() {
			if entry.Capabilities.L4Backhaul || entry.Capabilities.PeerICP {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled while waiting for peer: %w", ctx.Err())
		case <-ticker.C:
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("no usable peer discovered within %v", timeout)
		}
	}
}

func fetchToFile(ctx context.Context, node *app.App, blobHash, blobPath, outPath string,
	stdout, stderr io.Writer,
) error {
	tmpPath := outPath + ".part"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		fmt.Fprintf(stderr, "create temp file: %v\n", err)
		return err
	}
	tmpClosed := false
	defer func() {
		if !tmpClosed {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	hasher := sha256.New()
	w := io.MultiWriter(tmp, hasher)

	if err := node.FetchBlob(ctx, w, blobPath); err != nil {
		fmt.Fprintf(stderr, "download failed: %v\n", err)
		return err
	}

	if err := tmp.Close(); err != nil {
		fmt.Fprintf(stderr, "close temp file: %v\n", err)
		return err
	}
	tmpClosed = true

	if got := fmt.Sprintf("%x", hasher.Sum(nil)); got != blobHash {
		_ = os.Remove(tmpPath)
		fmt.Fprintf(stderr, "hash mismatch: expected %s, got %s\n", blobHash, got)
		return errHashMismatch
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		fmt.Fprintf(stderr, "rename temp to output: %v\n", err)
		return err
	}

	fi, err := os.Stat(outPath)
	if err != nil {
		fmt.Fprintf(stderr, "stat output: %v\n", err)
		return err
	}

	fmt.Fprintf(stdout, "downloaded: %s (%d bytes, sha256 verified)\n", outPath, fi.Size())
	return nil
}
