// Ingest-worker standalone deployment: HTTP service that receives content
// upload requests, runs ContentIngester processing (DashIngester/ImageIngester),
// uploads blobs to cloud drives, writes metadata transactions to PG, and
// publishes ContentIngestedEvent to the control-plane SyncBroadcaster over
// libp2p sync channel (T8: 事件回路接通).
//
// @title MediaWorker Ingest-Worker API
// @version 1.0
// @description 入库 Worker HTTP API：multipart 内容上传入库、存活探针与 Prometheus 指标。
// @host localhost:8080
// @BasePath /
//
// @tag.name ingest
// @tag.description 内容上传入库
//
// @tag.name ops
// @tag.description 运维与指标
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
	"github.com/shlande/mediaworker/internal/storage/healthcheck"
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
	defer func() { _ = mc.Close() }()

	// 3. Build AccountPool from the CP account snapshot (PG cloud_account is
	//    the source of truth; YAML storage.cloud_accounts is deprecated).
	if len(cfg.Storage.CloudAccounts) > 0 {
		slog.Warn("storage.cloud_accounts in YAML is deprecated and ignored — accounts are sourced from the control plane (PG)")
	}
	pool := accountpool.NewAccountPool(nil)
	snap, err := mc.LoadAccountSnapshot(context.Background())
	if err != nil {
		return fmt.Errorf("load account snapshot: %w", err)
	}
	pool.ReplaceFromSnapshot(snap)
	slog.Info("account pool built from CP snapshot", "accounts", len(pool.SnapshotAccounts()))

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
	defer func() { _ = syncPub.Close() }()

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
	mux.HandleFunc("/healthz", handleHealthz)
	mux.Handle("/metrics", handleMetrics(metricsHandler, metrics))

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

	// 9. Account health checker — probes only idle accounts (>1h without
	// successful use) every 5min; recent traffic itself is the health signal.
	checker := healthcheck.NewHealthChecker(pool, 5*time.Minute, time.Hour, mc)
	go checker.Start(ctx)

	// 9b. Refresh the account pool from the CP snapshot every 60s, mirroring
	//     the ACCOUNT_SNAPSHOT cadence on edge nodes (new/rotated/banned
	//     accounts converge without a sync client).
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snap, err := mc.LoadAccountSnapshot(ctx)
				if err != nil {
					slog.Warn("account snapshot refresh failed", "err", err)
					continue
				}
				pool.ReplaceFromSnapshot(snap)
			}
		}
	}()

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

// handleIngest receives multipart file uploads and runs the ingest pipeline to
// produce derivative content, upload blobs to cloud drives, write metadata
// transactions, and publish ContentIngestedEvent.
//
//	@Summary		multipart 上传内容并入库
//	@Description	接收 multipart/form-data 文件上传，经 ingest pipeline 处理后入库到云盘并写入元数据。
//	@Tags			ingest
//	@Accept			multipart/form-data
//	@Produce		json
//	@Param			content_type	path		string	true	"内容类型（dash_video|image）"
//	@Param			file			formData	file	true	"上传文件"
//	@Param			metadata		formData	string	false	"JSON 元数据（ingest.ProcessOptions.Metadata）"
//	@Param			content_id		formData	string	false	"指定内容 ID"
//	@Success		200				{object}	ingest.IngestResponse
//	@Failure		400				{object}	types.ErrorResponse
//	@Failure		405				{object}	types.ErrorResponse
//	@Failure		500				{object}	types.ErrorResponse
//	@Router			/ingest/{content_type} [post]
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
	defer func() { _ = r.MultipartForm.RemoveAll() }() // best-effort: temp file cleanup

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, fmt.Sprintf("missing 'file' field: %v", err), http.StatusBadRequest)
		return
	}
	defer func() { _ = file.Close() }()

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

// handleHealthz is a liveness probe returning 200 "ok".
//
//	@Summary		存活探针
//	@Description	返回 200 "ok"，供 Kubernetes liveness/readiness 探针使用。
//	@Tags			ops
//	@Produce		plain
//	@Success		200	{string}	string	"ok"
//	@Router			/healthz [get]
func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleMetrics returns an http.Handler that refreshes the
// ingest_publish_failures gauge from syncpub.PublishFailures() before each
// scrape, preserving all existing Prometheus metrics served by the
// underlying metricsHandler.
//
//	@Summary		Prometheus 指标
//	@Description	返回 Prometheus 文本格式指标，抓取前刷新 ingest_publish_failures 计数。
//	@Tags			ops
//	@Produce		plain
//	@Success		200	{string}	string
//	@Router			/metrics [get]
func handleMetrics(metricsHandler http.Handler, metrics *monitor.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		metrics.SetIngestPublishFailures(syncpub.PublishFailures())
		metricsHandler.ServeHTTP(w, r)
	}
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
				fileName := strings.TrimPrefix(blobHash, "sha256:") + ".bin"
				fi, err := drv.Put(ctx, "root:/mediaworker", fileName, reader, size)
				if err != nil {
					return "", fmt.Errorf("driver put: %w", err)
				}
				return fi.ID, nil
			},
		}
	}
	return out, nil
}
