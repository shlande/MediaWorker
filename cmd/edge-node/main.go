// Command edge-node is the MediaWorker edge distribution node binary.
// It assembles the complete node stack via internal/node/app, then starts
// the HTTP and node-local admin servers on top of the App.
//
// The binary does NOT import any internal/controlplane/ package.
//
// @title MediaWorker Edge-Node API
// @version 1.0
// @description 边缘节点 HTTP API：终端 blob 分发、节点本地管理 API 与 Prometheus 指标。
// @host localhost:8080
// @BasePath /
//
// @securityDefinitions.apikey AdminToken
// @in header
// @name X-Admin-Token
// @description 节点本地管理 API 令牌
//
// @tag.name blob
// @tag.description 终端 blob 分发
//
// @tag.name node-admin
// @tag.description 节点本地管理
//
// @tag.name ops
// @tag.description 运维与指标
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	nodeadmin "github.com/shlande/mediaworker/internal/node/adminapi"
	"github.com/shlande/mediaworker/internal/node/app"
	"github.com/shlande/mediaworker/internal/node/routing"

	"github.com/shlande/mediaworker/internal/config"
)

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

var (
	configPath  = flag.String("config", "configs/node-edge.yaml", "Path to YAML configuration file")
	showVersion = flag.Bool("version", false, "Print version and exit")
)

// build-time injection
var (
	BuildVersion = "dev"
	BuildCommit  = "unknown"
)

const defaultHTTPListen = ":8080"

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("MediaWorker edge-node %s (%s)\n", BuildVersion, BuildCommit)
		os.Exit(0)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	logger := slog.Default()

	// -------------------------------------------------------------------
	// 1. Load node configuration
	// -------------------------------------------------------------------
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	logger.Info("config loaded", "path", *configPath)

	// -------------------------------------------------------------------
	// 10. Context with signal handling
	// -------------------------------------------------------------------
	rootCtx, cancel := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM,
	)

	// -------------------------------------------------------------------
	// Steps 2-21: assemble the full node stack via internal/node/app
	// -------------------------------------------------------------------
	a, err := app.New(rootCtx, cfg, app.Options{
		BuildVersion: BuildVersion,
		BuildCommit:  BuildCommit,
		Logger:       logger,
	})
	if err != nil {
		log.Fatalf("app.New: %v", err)
	}

	// -------------------------------------------------------------------
	// 22. HTTP server for client blob requests
	// -------------------------------------------------------------------
	httpListen := defaultHTTPListen
	mux := http.NewServeMux()
	mux.HandleFunc("GET /blob/{hash}", handleGetBlob(a.Router, logger))

	// T20 — /metrics endpoint on the same port as /blob. No auth — see
	// step 10a for the intranet deployment assumption.
	mux.Handle("GET /metrics", handleMetrics(a.Metrics.HTTPHandler()))

	httpSrv := &http.Server{
		Addr:    httpListen,
		Handler: mux,
	}
	go func() {
		logger.Info("HTTP server listening", "addr", httpListen)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "err", err)
		}
	}()

	// -------------------------------------------------------------------
	// 22b. Node-local admin server (gated by admin_api.listen, empty =
	// disabled). Independent listen address — does NOT share the :8080
	// client mux above; /blob and /metrics stay unauthenticated per the
	// intranet assumption in section 10a. Every admin route requires the
	// X-Admin-Token header. The token is never logged.
	// -------------------------------------------------------------------
	if cfg.AdminAPI.Listen != "" {
		adminSrv := nodeadmin.NewServer(cfg.AdminAPI.Token)
		adminSrv.HandleUnauthenticated("GET /v1/healthz", handleAdminHealthz())

		// NODE ADMIN ROUTES: consolidated mounts (todo 49). Typed-nil guard:
		// components that are nil on this node (pinStore/warmCache when their
		// cache tier is disabled, pool/linkP on non-L4) must reach the
		// handlers as a NIL INTERFACE, not a nil pointer inside a non-nil
		// interface — the handlers branch on dep != nil.
		var pinQuerier nodeadmin.PinSpaceQuerier
		var pinsStore any
		if a.PinStore != nil {
			pinQuerier = a.PinStore
			pinsStore = a.PinStore
		}
		var warmReader nodeadmin.WarmCacheReader
		var warmFlusher nodeadmin.WarmCacheFlusher
		if a.WarmCache != nil {
			warmReader = a.WarmCache
			warmFlusher = a.WarmCache
		}
		var poolSnap nodeadmin.AccountSnapshotter
		var linkReader nodeadmin.LinkpoolReader
		if a.AccountPool != nil {
			poolSnap = a.AccountPool
		}
		if a.LinkPool != nil {
			linkReader = a.LinkPool
		}

		nodeadmin.RegisterStatusRoutes(adminSrv, nodeadmin.StatusDeps{
			PeerID:             a.PeerID,
			Capabilities:       a.Capabilities,
			L4Mode:             a.L4Mode,
			Region:             a.Region,
			Version:            BuildVersion,
			StartedAt:          a.StartedAt,
			RefreshBefore:      cfg.Node.JWTService.ParsedRefreshBeforeExpiry,
			ControlPlanePubKey: a.CPKey,
			JWTClient:          a.JWTClient,
			Scorer:             a.Scorer,
			Network:            nodeadmin.Libp2pNetworkReporter(a.Host.Network()),
			Backhaul:           a.BackhaulMgr,
		})
		nodeadmin.RegisterCacheRoutes(adminSrv, pinQuerier, warmReader)
		nodeadmin.RegisterPinsRoutes(adminSrv, pinsStore, a.PlanLog)
		nodeadmin.RegisterNetworkRoutes(adminSrv, nodeadmin.NetworkDeps{
			Host:    a.Host,
			Conns:   a.Host.Network(),
			DHT:     a.DHTDiscovery,
			DHTMode: cfg.Node.Libp2p.DHT.Mode,
			Ring:    a.HashRing,
			Peers:   a.PeerStore,
			Stats:   a.NetTracker,
		})
		nodeadmin.RegisterBackhaulRoutes(adminSrv, nodeadmin.BackhaulDeps{
			L4Enabled:            a.L4Mode,
			BackhaulCapacityMbps: cfg.Access.DataPlane.BackhaulCapacityMbps,
			Stats:                a.BackhaulMgr,
			Linkpool:             linkReader,
			Pool:                 poolSnap,
		})
		nodeadmin.NewReloader(*configPath, cfg, a.RefreshDurations).RegisterReloadRoutes(adminSrv)
		nodeadmin.RegisterFlushRoutes(adminSrv, warmFlusher)

		go func() {
			if err := adminSrv.Serve(rootCtx, cfg.AdminAPI.Listen); err != nil {
				logger.Error("node admin server error", "err", err)
			}
		}()
		logger.Info("node admin server listening", "addr", cfg.AdminAPI.Listen)
	}

	// -------------------------------------------------------------------
	// 22. Wait for shutdown signal
	// -------------------------------------------------------------------
	<-rootCtx.Done()
	cancel()
	logger.Info("shutting down...")

	// -------------------------------------------------------------------
	// 23. Graceful shutdown (5s deadline)
	// -------------------------------------------------------------------
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := a.CloseStores(); err != nil {
		logger.Error("closing stores", "err", err)
	}

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "err", err)
	}

	if err := a.CloseHost(); err != nil {
		logger.Error("closing host", "err", err)
	}

	logger.Info("shutdown complete")
}

// ---------------------------------------------------------------------------
// HTTP handler functions (extracted from anonymous closures for swag
// annotations — swag only parses ast.FuncDecl doc comments).
// ---------------------------------------------------------------------------

// handleGetBlob 按 SHA-256 哈希取 blob 字节流。
//
//	@Summary		按哈希取 blob 字节流
//	@Description	根据 SHA-256 哈希值从缓存或对等节点获取 blob 内容并返回二进制流。
//	@Tags			blob
//	@Param			hash	path	string	true	"blob SHA-256 哈希"
//	@Produce		application/octet-stream
//	@Success		200	{file}		binary
//	@Failure		404	{string}	string
//	@Router			/blob/{hash} [get]
func handleGetBlob(router *routing.EdgeRouter, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		blobHash := r.PathValue("hash")
		ctx, reqCancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer reqCancel()

		logger.Info("blob request", "hash", blobHash)
		if err := router.HandleBlobRequest(ctx, w, blobHash); err != nil {
			logger.Error("blob request failed", "hash", blobHash, "err", err)
			http.Error(w, "blob not found", http.StatusNotFound)
		}
	}
}

// handleMetrics 代理 Prometheus 指标端点。
//
//	@Summary		节点指标
//	@Description	Prometheus 文本格式的运行指标，包括缓存命中、对等点请求等度量。
//	@Tags			ops
//	@Produce		text/plain
//	@Success		200	{string}	string
//	@Router			/metrics [get]
func handleMetrics(h http.Handler) http.HandlerFunc {
	return h.ServeHTTP
}

// handleAdminHealthz 管理接口健康探测。
//
//	@Summary		管理接口健康探测
//	@Description	返回 {"status":"ok"} 表示节点管理 API 可正常响应。
//	@Tags			ops
//	@Success		200	{object}	map[string]string
//	@Router			/v1/healthz [get]
func handleAdminHealthz() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeadmin.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
