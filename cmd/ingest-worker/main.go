// Ingest-worker standalone deployment: HTTP service that receives content
// upload requests, runs ContentIngester processing (DashIngester/ImageIngester),
// uploads blobs to cloud drives, writes metadata transactions to PG, and
// publishes ContentIngestedEvent to the control-plane SyncBroadcaster over
// libp2p sync channel (T8: 事件回路接通).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/ingest"
	"github.com/shlande/mediaworker/internal/ingest/syncpub"
	"github.com/shlande/mediaworker/internal/node/monitor"
	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/metadata"
)

func main() {
	configPath := flag.String("config", "configs/ingest-worker.yaml", "path to ingest-worker YAML config")
	flag.Parse()

	if err := run(*configPath); err != nil {
		slog.Error("ingest-worker fatal", "err", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	// 1. Load config.
	cfg, err := config.LoadIngestWorkerConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// 2. Sweep stale temp workdirs from previous runs.
	sweepStaleWorkDir(cfg.Ingest.WorkDir)

	// 3. Check workdir disk space — warn if free < 2x max upload.
	checkWorkDirDiskSpace(cfg.Ingest.WorkDir, cfg.HTTP.MaxUploadBytes)

	// 4. Metadata client (PG).
	mc, err := metadata.NewPGMetadataClient(cfg.Metadata.PGDSN)
	if err != nil {
		return fmt.Errorf("metadata client: %w", err)
	}
	defer mc.Close()

	// 3. Build AccountPool from cloud account configs (upload-only, no libp2p/metadata query).
	pool := accountpool.BuildFromConfig(cfg.Storage.ToAccountPoolConfig(), nil)

	// 4. Build BackendPool adapter.
	selector := &ingestAccountPoolAdapter{pool: pool}
	backendPool := ingest.NewAccountPoolBackend(selector, cfg.Ingest.Redundancy)

	// 5. Event publisher — libp2p sync channel to control plane (T8).
	//    The worker joins the PSK mesh as an infrastructure identity (no
	//    DHT/GossipSub/JWT, plan line 167). PSK admission is enforced at the
	//    transport layer; the worker never dials anyone but the CP.
	syncPub, err := syncpub.NewSyncPublisher(
		cfg.ControlPlane.Multiaddr,
		cfg.ControlPlane.PrivKeyPath,
		"LIBP2P_PSK",
	)
	if err != nil {
		return fmt.Errorf("sync publisher: %w", err)
	}
	defer syncPub.Close()

	// Fail closed: if the CP is unreachable at startup, refuse to serve
	// traffic rather than silently dropping every event (plan line 48 —
	// PinOrchestrator would starve).
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := syncPub.CheckConnectivity(dialCtx); err != nil {
		dialCancel()
		return fmt.Errorf("control plane unreachable at startup: %w", err)
	}
	dialCancel()
	slog.Info("control plane reachable",
		"cp_peer", syncPub.CPPeer().ShortString(),
	)

	// 6. Build pipeline with registered ingesters.
	pipeline := ingest.NewIngestPipeline(backendPool, mc, syncPub, cfg.Ingest.Redundancy)
	pipeline.RegisterIngester(ingest.NewDashIngester(cfg.Ingest.FFmpegPath, cfg.Ingest.WorkDir))
	pipeline.RegisterIngester(ingest.NewImageIngester(cfg.Ingest.WorkDir))

	// 6b. Metrics (T20). Mounted on the same mux as /ingest and /healthz.
	// No auth — intranet assumption (plan line 275). The /metrics handler
	// also refreshes the ingest_publish_failures gauge from
	// syncpub.PublishFailures() on each scrape, so the gauge stays in sync
	// with the package-level atomic counter without needing a polling
	// goroutine.
	metrics := monitor.NewMetrics()
	metricsHandler := metrics.HTTPHandler()

	// 7. HTTP handler.
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest/", func(w http.ResponseWriter, r *http.Request) {
		handleIngest(w, r, pipeline, cfg.HTTP.MaxUploadBytes, metrics)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Refresh the publish-failures gauge before each scrape so the
		// value reflects the latest syncpub counter. The gauge is a
		// mirror, not a counter — the source of truth lives in syncpub.
		metrics.SetIngestPublishFailures(syncpub.PublishFailures())
		metricsHandler.ServeHTTP(w, r)
	}))

	srv := &http.Server{
		Addr:         cfg.HTTP.Listen,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// 8. Context + signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 9. Start HTTP server.
	go func() {
		slog.Info("ingest-worker listening", "addr", cfg.HTTP.Listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP serve", "err", err)
		}
	}()

	// 10. Wait for shutdown.
	select {
	case sig := <-sigCh:
		slog.Info("shutting down", "signal", sig)
	case <-ctx.Done():
	}

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP shutdown", "err", err)
	}
	slog.Info("ingest-worker shutdown complete")
	return nil
}

// ─── Startup sweep ──────────────────────────────────────────────────────

// sweepStaleWorkDir removes first-level subdirectories and src.mp4 files in
// workDir whose mtime is before the current time. These are orphaned from
// previous runs. Failures are logged as warnings only — they never block startup.
func sweepStaleWorkDir(workDir string) {
	startupTime := time.Now()
	entries, err := os.ReadDir(workDir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		slog.Warn("sweep workdir read", "dir", workDir, "err", err)
		return
	}
	for _, entry := range entries {
		path := filepath.Join(workDir, entry.Name())
		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				slog.Warn("sweep stat dir", "path", path, "err", err)
				continue
			}
			if info.ModTime().Before(startupTime) {
				if err := os.RemoveAll(path); err != nil {
					slog.Warn("sweep remove dir", "path", path, "err", err)
				}
			}
			continue
		}
		// Match *_src.mp4 files.
		if strings.HasSuffix(entry.Name(), "_src.mp4") {
			info, err := entry.Info()
			if err != nil {
				slog.Warn("sweep stat file", "path", path, "err", err)
				continue
			}
			if info.ModTime().Before(startupTime) {
				if err := os.RemoveAll(path); err != nil {
					slog.Warn("sweep remove file", "path", path, "err", err)
				}
			}
		}
	}
}

// checkWorkDirDiskSpace warns if the workdir filesystem has fewer free bytes
// than 2*maxUploadBytes. It never blocks startup — failures are Warn-only.
func checkWorkDirDiskSpace(workDir string, maxUploadBytes int64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(workDir, &stat); err != nil {
		slog.Warn("disk space check failed", "workdir", workDir, "err", err)
		return
	}
	free := int64(stat.Bavail) * int64(stat.Bsize)
	threshold := 2 * maxUploadBytes
	if free < threshold {
		slog.Warn("low disk space on workdir",
			"workdir", workDir,
			"free_bytes", free,
			"threshold_bytes", threshold,
		)
	}
}

// ─── HTTP handler ──────────────────────────────────────────────────────

func handleIngest(w http.ResponseWriter, r *http.Request, pipeline *ingest.IngestPipeline, maxUploadBytes int64, metrics *monitor.Metrics) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract content_type from URL path: /ingest/{content_type}
	contentType := strings.TrimPrefix(r.URL.Path, "/ingest/")
	if contentType == "" {
		http.Error(w, "missing content_type in path", http.StatusBadRequest)
		return
	}

	// Max upload size from config.
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)

	// Parse multipart form (max 64 MB in memory, rest spills to disk).
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, fmt.Sprintf("parse multipart: %v", err), http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, fmt.Sprintf("missing 'file' field: %v", err), http.StatusBadRequest)
		return
	}
	defer file.Close()

	var opts ingest.ProcessOptions
	if metadataJSON := r.FormValue("metadata"); metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &opts.Metadata); err != nil {
			http.Error(w, fmt.Sprintf("invalid metadata JSON: %v", err), http.StatusBadRequest)
			return
		}
	}
	if cid := r.FormValue("content_id"); cid != "" {
		opts.ContentID = cid
	}

	ctx := r.Context()
	contentID, err := pipeline.Ingest(ctx, contentType, file, opts)
	if err != nil {
		slog.Error("ingest failed", "content_type", contentType, "err", err)
		if metrics != nil {
			metrics.RecordIngest(contentType, monitor.IngestOutcomeFailure)
		}
		http.Error(w, fmt.Sprintf("ingest failed: %v", err), http.StatusInternalServerError)
		return
	}

	if metrics != nil {
		metrics.RecordIngest(contentType, monitor.IngestOutcomeSuccess)
	}

	resp := map[string]string{"content_id": contentID}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// ─── Account pool construction ─────────────────────────────────────────

// ingestAccountPoolAdapter wraps accountpool.AccountPool to satisfy
// ingest.AccountSelector. It calls pool.SelectK for uploads and adapts
// each *accountpool.Account to an ingest.UploadableAccount.
type ingestAccountPoolAdapter struct {
	pool *accountpool.AccountPool
}

func (a *ingestAccountPoolAdapter) SelectKForUpload(ctx context.Context, k int) ([]*ingest.UploadableAccount, error) {
	accounts, err := a.pool.SelectK(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("select accounts: %w", err)
	}
	out := make([]*ingest.UploadableAccount, len(accounts))
	for i, acct := range accounts {
		backendID := string(acct.Vendor) + ":" + acct.AccountID
		drv := acct.Driver
		out[i] = &ingest.UploadableAccount{
			BackendID: backendID,
			PutFunc: func(ctx context.Context, blobHash string, reader io.Reader, size int64) (string, error) {
				fi, err := drv.Put(ctx, blobHash, blobHash+".bin", reader, size)
				if err != nil {
					return "", fmt.Errorf("driver put: %w", err)
				}
				return fi.ID, nil
			},
		}
	}
	return out, nil
}
