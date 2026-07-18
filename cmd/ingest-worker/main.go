// Ingest-worker standalone deployment: HTTP service that receives content
// upload requests, runs ContentIngester processing (DashIngester/ImageIngester),
// uploads blobs to cloud drives, writes metadata transactions to PG, and
// publishes ContentIngestedEvent via log (no SyncBroadcaster).
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
	"strings"
	"syscall"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/controlplane/metadata"
	"github.com/shlande/mediaworker/internal/ingest"
	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/storage/driver/baidu"
	"github.com/shlande/mediaworker/internal/storage/driver/onedrive"
	"github.com/shlande/mediaworker/internal/types"
	"golang.org/x/time/rate"
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

	// 2. Metadata client (PG).
	mc, err := metadata.NewPGMetadataClient(cfg.Metadata.PGDSN)
	if err != nil {
		return fmt.Errorf("metadata client: %w", err)
	}
	defer mc.Close()

	// 3. Build AccountPool from cloud account configs (upload-only, no libp2p/metadata query).
	pool := buildAccountPool(cfg)

	// 4. Build BackendPool adapter.
	selector := &ingestAccountPoolAdapter{pool: pool}
	backendPool := ingest.NewAccountPoolBackend(selector, cfg.Ingest.Redundancy)

	// 5. Event publisher (log-only for standalone).
	eventBus := ingest.NewLogPublisher()

	// 6. Build pipeline with registered ingesters.
	pipeline := ingest.NewIngestPipeline(backendPool, mc, eventBus)
	pipeline.RegisterIngester(ingest.NewDashIngester(cfg.Ingest.FFmpegPath, cfg.Ingest.WorkDir))
	pipeline.RegisterIngester(ingest.NewImageIngester(cfg.Ingest.WorkDir))

	// 7. HTTP handler.
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest/", func(w http.ResponseWriter, r *http.Request) {
		handleIngest(w, r, pipeline)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

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

// ─── HTTP handler ──────────────────────────────────────────────────────

func handleIngest(w http.ResponseWriter, r *http.Request, pipeline *ingest.IngestPipeline) {
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

	// Max upload 10 GB.
	r.Body = http.MaxBytesReader(w, r.Body, 10<<30)

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
		http.Error(w, fmt.Sprintf("ingest failed: %v", err), http.StatusInternalServerError)
		return
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

// buildAccountPool creates an AccountPool from cloud-account configuration,
// creates per-vendor drivers, and adds them to the pool with rate limiters
// and circuit breakers (same pattern as the edge-node integration tests).
func buildAccountPool(cfg *config.IngestWorkerConfig) *accountpool.AccountPool {
	// Ingest worker is upload-only — it does not call GetBlobLocations, so
	// the BlobLocationClient can be nil.
	pool := accountpool.NewAccountPool(nil)
	tokenMgr := auth.NewTokenManager(nil)

	for _, acctCfg := range cfg.Storage.CloudAccounts {
		if !acctCfg.Enabled {
			continue
		}
		vendor := types.Vendor(acctCfg.Vendor)

		// Create the appropriate driver.
		var drv driver.Driver
		switch vendor {
		case types.VendorBaidu:
			drv = baidu.NewBaiduDriver(tokenMgr, acctCfg.AccountID, acctCfg.ClientID, acctCfg.ClientSecret, nil)
		case types.VendorOneDrive:
			drv = onedrive.NewOneDriveDriver(tokenMgr, acctCfg.AccountID, acctCfg.Region, nil)
		default:
			slog.Warn("unknown vendor, skipping", "vendor", acctCfg.Vendor, "account_id", acctCfg.AccountID)
			continue
		}

		rateCfg := drv.RateLimitConfig()
		if override, ok := cfg.Storage.RateLimits[acctCfg.Vendor]; ok {
			if override.QPS > 0 {
				rateCfg.QPS = override.QPS
			}
			if override.Burst > 0 {
				rateCfg.Burst = override.Burst
			}
			if override.Concurrent > 0 {
				rateCfg.ConcurrentLimit = override.Concurrent
			}
		}

		vendorWeight := 2.0
		if vp, ok := cfg.Storage.VendorProfiles[acctCfg.Vendor]; ok {
			vendorWeight = vp.Weight
		}

		key := string(vendor) + ":" + acctCfg.AccountID
		acct := &accountpool.Account{
			Vendor:       vendor,
			AccountID:    acctCfg.AccountID,
			Driver:       drv,
			Limiter:      rate.NewLimiter(rate.Limit(rateCfg.QPS), rateCfg.Burst),
			CB:           circuitbreaker.New(key, 5, 100*time.Millisecond),
			VendorWeight: vendorWeight,
		}
		acct.Health.Store(types.HealthState{State: "healthy"})
		pool.AddAccount(acct)
		slog.Info("account added", "key", key, "vendor", acctCfg.Vendor)
	}
	return pool
}


