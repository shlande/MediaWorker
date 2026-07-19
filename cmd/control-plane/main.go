// Control-plane binary: wires JWT HTTP server + DHT bootstrap host +
// SyncBroadcaster + MetadataClient + PinOrchestrator into a single process.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/controlplane/accountregistry"
	"github.com/shlande/mediaworker/internal/controlplane/adminapi"
	cpdht "github.com/shlande/mediaworker/internal/controlplane/dhtbootstrap"
	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/controlplane/locationsvc"
	cpmetrics "github.com/shlande/mediaworker/internal/controlplane/metrics"
	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
	"github.com/shlande/mediaworker/internal/controlplane/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/storage/quota"
	"github.com/shlande/mediaworker/internal/types"
)

func main() {
	configPath := flag.String("config", "configs/control-plane.yaml", "path to control-plane YAML config file")
	flag.Parse()

	if err := run(*configPath); err != nil {
		slog.Error("control-plane fatal", "err", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	// 1. Load config.
	cfg, err := config.LoadControlPlaneConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// 2. Load JWT signing key (PEM PKCS#8 Ed25519).
	privKey, err := config.LoadOrGenerateControlPlaneKey(cfg.Identity.PrivKeyPath)
	if err != nil {
		return fmt.Errorf("load control-plane key: %w", err)
	}

	// 3. Prepare L4 whitelist.
	wlStore, err := cpjwt.NewWhitelistStore(cfg.L4Whitelist.DBPath)
	if err != nil {
		return fmt.Errorf("whitelist store: %w", err)
	}

	ps := cpjwt.NewPeerIdSet()
	if err := wlStore.Restore(ps); err != nil {
		return fmt.Errorf("restore whitelist: %w", err)
	}

	// 4. Rate limiter + audit log.
	rateLimiter := cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval)
	auditLog := cpjwt.NewAuditLog(os.Stdout)

	// 4b. Metrics (T20). Constructed once per process and shared across the
	// JWT HTTP server, PinOrchestrator, and SyncBroadcaster subscribe loop.
	// Mounted on the JWT HTTP server's mux (no separate port — plan line 275).
	metrics := cpmetrics.NewMetrics()

	// 5. JWT service + HTTP server.
	jwtSvc := cpjwt.NewJWTService(privKey, ps, rateLimiter, auditLog, cfg.JWTPolicy)

	// 5b. Node registry (todo 12): runtime view of node status reports +
	//     JWT issuance records. Kept as a named variable — later admin
	//     wiring (todos 18/24/31/32/52) consumes nodeReg here.
	nodeReg := noderegistry.NewRegistry()
	jwtSvc.SetIssuanceRecorder(nodeReg.RecordIssuance)

	httpServer := cpjwt.NewJWTHTTPServer(jwtSvc)

	// 6. Load libp2p identity (separate key path from JWT — protobuf format).
	nodeID, err := identity.LoadOrGenerateIdentity(cfg.Identity.Libp2pPrivKeyPath)
	if err != nil {
		return fmt.Errorf("load libp2p identity: %w", err)
	}

	// 7. PSK.
	psk := types.PSK(os.Getenv("LIBP2P_PSK"))
	if len(psk) == 0 {
		slog.Warn("LIBP2P_PSK not set, running with no private network (open to any peer)")
	}

	// 8. DHT bootstrap host.
	bootHost, err := cpdht.NewBootstrapHost(nodeID, cfg.DHTBootstrap, psk)
	if err != nil {
		return fmt.Errorf("dht bootstrap: %w", err)
	}

	// 9. SyncBroadcaster on bootstrap host.
	sbOpts := []syncbroadcaster.Option{}
	if cfg.SyncBroadcaster.ProtocolID != "" {
		sbOpts = append(sbOpts, syncbroadcaster.WithProtocolID(cfg.SyncBroadcaster.ProtocolID))
	}
	if cfg.SyncBroadcaster.SendTimeout != "" {
		if d, err := time.ParseDuration(cfg.SyncBroadcaster.SendTimeout); err == nil && d > 0 {
			sbOpts = append(sbOpts, syncbroadcaster.WithSendTimeout(d))
		} else {
			return fmt.Errorf("parse sync_broadcaster.send_timeout %q: %w", cfg.SyncBroadcaster.SendTimeout, err)
		}
	}
	sb := syncbroadcaster.New(bootHost.Host(), sbOpts...)

	// 10. MetadataClient (gracefully degrade if PG unavailable — PinOrchestrator
	//     uses cached state via NodeStatusReport channels).
	var mc *metadata.PGMetadataClient
	mc, err = metadata.NewPGMetadataClient(cfg.Metadata.PGDSN)
	if err != nil {
		slog.Warn("PG metadata client unavailable, PinOrchestrator will use cached state", "err", err)
		// mc remains nil; NewPinOrchestrator will receive nil MetadataClient.
	}

	// 10b. Blob-location query API (T9). Mounted on the JWT HTTP server's mux
	//      so no new listening port is introduced (plan line 176). When PG is
	//      unavailable (mc == nil) the handler is still registered; it returns
	//      503 on every authenticated request — edges see a deterministic
	//      contract instead of a missing route.
	var mcBlob metadata.BlobStoreClient
	if mc != nil {
		mcBlob = mc
	}
	httpServer.RegisterLocationHandler(locationsvc.NewHandler(jwtSvc.PubKey(), mcBlob))

	// 10c. /metrics endpoint (T20). Mounted on the same mux as /v1/node/jwt
	//      and /v1/blob-locations/{hash}. No auth — intranet assumption
	//      (plan line 275).
	httpServer.RegisterMetricsHandler(metrics)

	// 11. PinOrchestrator.
	po := pinstrategy.NewPinOrchestrator(mc, mc, sb)
	po.RegisterStrategy("dash_video", &pinstrategy.DashPinStrategy{})
	po.SetMetrics(metrics)

	rebalanceIntv, err := time.ParseDuration(cfg.PinOrchestrator.RebalanceInterval)
	if err != nil {
		return fmt.Errorf("parse rebalance_interval %q: %w", cfg.PinOrchestrator.RebalanceInterval, err)
	}

	// Parse JWT HTTP timeouts (empty string → default 10s).
	readTimeout, err := parseJWTHTTPDuration(cfg.JWT.ReadTimeout, "jwt_http.read_timeout")
	if err != nil {
		return err
	}
	writeTimeout, err := parseJWTHTTPDuration(cfg.JWT.WriteTimeout, "jwt_http.write_timeout")
	if err != nil {
		return err
	}

	// 12. Context + signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 13. Start all components.
	slog.Info("control-plane starting")

	go func() {
		slog.Info("JWT HTTP listening", "addr", cfg.JWT.Listen, "read_timeout", readTimeout, "write_timeout", writeTimeout)
		if err := httpServer.Serve(ctx, cfg.JWT.Listen, readTimeout, writeTimeout); err != nil {
			slog.Error("JWT HTTP serve", "err", err)
		}
	}()

	if err := bootHost.Start(ctx); err != nil {
		return fmt.Errorf("dht bootstrap start: %w", err)
	}
	slog.Info("DHT bootstrap listening",
		"addrs", cfg.DHTBootstrap.ListenAddrs,
		"namespace", cfg.DHTBootstrap.Namespace,
	)

	go po.Run(ctx, rebalanceIntv, cfg.PinOrchestrator.TopContentsLimit)
	slog.Info("PinOrchestrator started", "interval", rebalanceIntv, "top_contents_limit", cfg.PinOrchestrator.TopContentsLimit)

	// 13b. AccountRegistry snapshot sync (todo 18, Metis gap #4): without this
	//      instantiation ACCOUNT_SNAPSHOT is never broadcast and todo 17's
	//      node-side account pool never fills. StartSync spawns its own
	//      goroutine and emits one snapshot immediately. Skipped (Warn) on the
	//      PG-unavailable degraded path.
	if mc != nil {
		registry := accountregistry.NewAccountRegistry(mc.DB(), sb)
		registry.StartSync(ctx, 60*time.Second)
		slog.Info("account snapshot sync started", "interval", "60s")
	} else {
		slog.Warn("account snapshot sync skipped: PG metadata client unavailable")
	}

	// 13c. Admin API server (todo 18): auth endpoints + bootstrap admin seed,
	//      mounted only when admin_api.listen is configured. Auth routes need
	//      the user table, so mc == nil leaves them unmounted (Warn).
	//      ADMIN ROUTES: consolidated mounts below (todo 54).
	if cfg.AdminAPI.Listen != "" {
		adminSrv := adminapi.NewServer([]byte(cfg.AdminAPI.TokenSecret))
		if mc != nil {
			if err := adminapi.SeedAdminIfEmpty(ctx, mc); err != nil {
				slog.Warn("admin bootstrap seed failed", "err", err)
			}
			adminapi.RegisterAuthRoutes(adminSrv, mc)
		} else {
			slog.Warn("admin API enabled without PG: auth endpoints not mounted")
		}
		go func() {
			slog.Info("admin API listening", "addr", cfg.AdminAPI.Listen)
			if err := adminSrv.Serve(ctx, cfg.AdminAPI.Listen); err != nil {
				slog.Error("admin API serve", "err", err)
			}
		}()
	}

	// 13d. QuotaAllocator (todo 53): distributes per-account QPS across nodes
	//      and rebroadcasts QUOTA_UPDATE on every rebalance. Seeded from the
	//      account table (zero-value rate limits skipped); nodes register
	//      per account on each NODE_STATUS_REPORT (idempotent — allocator
	//      dedupes). Node load input is deliberately NOT wired (Metis gap
	//      #15: nodes do not report per-account load in v1, so every node
	//      gets the full base share).
	//      QUOTA: accounts handlers call qa.SetGlobalLimit on rate_limit
	//      changes (wired in todo 54 consolidation).
	qa := quota.NewQuotaAllocator(sb)
	if mc != nil {
		accounts, err := mc.ListAccounts(ctx, "", "")
		if err != nil {
			slog.Warn("quota seed: list accounts failed", "err", err)
		} else {
			for _, a := range accounts {
				if a.RateLimitCfg == (types.RateLimitConfig{}) {
					continue
				}
				qa.SetGlobalLimit(a.Vendor+":"+a.AccountID, a.RateLimitCfg)
			}
			slog.Info("quota allocator seeded", "accounts", len(qa.AccountKeys()))
		}
	} else {
		slog.Warn("quota allocator seed skipped: PG metadata client unavailable")
	}
	quotaInterval := parseQuotaRebalanceInterval(cfg.AdminAPI.QuotaRebalanceInterval)
	go qa.Run(ctx, quotaInterval)

	// 14. Subscribe to reverse channels.
	// histWriter is nil on the PG-unavailable startup path (section 10);
	// handleNodeStatusReport then skips history writes entirely.
	var histWriter nodeStatusHistoryWriter
	if mc != nil {
		histWriter = mc
	}
	statusCh := sb.Subscribe("NODE_STATUS_REPORT")
	go func() {
		reportCounts := map[types.PeerId]int{}
		for evt := range statusCh {
			var report types.NodeStatusReport
			if err := json.Unmarshal(evt.Payload, &report); err != nil {
				slog.Warn("failed to decode NODE_STATUS_REPORT", "err", err)
				continue
			}
			po.OnNodeStatusReport(report)
			handleNodeStatusReport(ctx, report, nodeReg, histWriter, reportCounts)
			for _, key := range qa.AccountKeys() {
				qa.RegisterNode(key, report.NodeID)
			}
		}
	}()

	ingestCh := sb.Subscribe("CONTENT_INGESTED")
	go func() {
		for evt := range ingestCh {
			var evtData types.ContentIngestedEvent
			if err := json.Unmarshal(evt.Payload, &evtData); err != nil {
				slog.Warn("failed to decode CONTENT_INGESTED", "err", err)
				continue
			}
			metrics.RecordCPContentIngestedReceived()
			po.OnContentIngested(evtData)
		}
	}()

	// 15. Wait for shutdown signal.
	select {
	case sig := <-sigCh:
		slog.Info("shutting down", "signal", sig)
	case <-ctx.Done():
	}

	cancel()

	// 16. Graceful shutdown (5s deadline).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if cerr := wlStore.Close(); cerr != nil {
			slog.Error("whitelist store close", "err", cerr)
		}
		if cerr := bootHost.Close(); cerr != nil {
			slog.Error("dht bootstrap close", "err", cerr)
		}
		if mc != nil {
			if cerr := mc.Close(); cerr != nil {
				slog.Error("metadata client close", "err", cerr)
			}
		}
	}()

	select {
	case <-done:
		slog.Info("control-plane shutdown complete")
	case <-shutdownCtx.Done():
		slog.Error("shutdown timed out", "err", shutdownCtx.Err())
	}

	return nil
}

// parseJWTHTTPDuration parses a JWTHTTPConfig duration string. An empty string
// yields cpjwt.DefaultJWTHTTPTimeout (10s). A non-empty but unparseable string
// returns an error naming the field so operators can locate the bad value
// (plan line 243: failure = invalid duration → startup error names field).
func parseJWTHTTPDuration(s string, fieldName string) (time.Duration, error) {
	if s == "" {
		return cpjwt.DefaultJWTHTTPTimeout, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("parse %s %q: %w", fieldName, s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %q", fieldName, s)
	}
	return d, nil
}

// nodeStatusHistoryWriter is the narrow seam used by handleNodeStatusReport
// to double-write reports into PG (todo 5 accessors).
// *metadata.PGMetadataClient satisfies it; nil disables history writes.
type nodeStatusHistoryWriter interface {
	InsertNodeStatusHistory(ctx context.Context, row metadata.NodeStatusHistoryRow) error
	PruneNodeStatusHistory(ctx context.Context, peerID string, keep int) error
}

// nodeStatusHistoryKeep bounds node_status_history rows per peer.
const nodeStatusHistoryKeep = 50

// nodeStatusPruneEvery prunes history once per this many reports per peer,
// keeping DELETEs off the per-report hot path.
const nodeStatusPruneEvery = 10

// handleNodeStatusReport double-writes a decoded report: always into the
// in-memory node registry, and (when hw != nil) into node_status_history.
// History-write failures are Warn-only: the registry update has already
// happened, and PG unavailability must not affect the runtime node view.
// counts is a caller-owned per-peer report counter driving the prune
// cadence; the subscribe loop is single-goroutine so no locking is needed.
func handleNodeStatusReport(ctx context.Context, report types.NodeStatusReport, reg *noderegistry.Registry, hw nodeStatusHistoryWriter, counts map[types.PeerId]int) {
	reg.UpsertReport(report)
	if hw == nil {
		return
	}
	if err := hw.InsertNodeStatusHistory(ctx, nodeStatusHistoryRowFromReport(report)); err != nil {
		slog.Warn("node status history insert failed", "peer_id", report.PeerID, "err", err)
	}
	counts[report.PeerID]++
	if counts[report.PeerID]%nodeStatusPruneEvery != 0 {
		return
	}
	if err := hw.PruneNodeStatusHistory(ctx, string(report.PeerID), nodeStatusHistoryKeep); err != nil {
		slog.Warn("node status history prune failed", "peer_id", report.PeerID, "err", err)
	}
}

// nodeStatusHistoryRowFromReport converts a wire report into a history row.
// Pointer columns are nil when the report does not carry the value
// (Region/Version/NodeID empty = old node build, stored as NULL).
func nodeStatusHistoryRowFromReport(report types.NodeStatusReport) metadata.NodeStatusHistoryRow {
	connCount := int32(report.ConnCount)
	row := metadata.NodeStatusHistoryRow{
		PeerID:      string(report.PeerID),
		Healthy:     report.Healthy,
		ReportedAt:  time.Unix(report.LastUpdate, 0),
		PrefixUsed:  ptr(report.PrefixSpace.UsedBytes),
		PrefixTotal: ptr(report.PrefixSpace.TotalBytes),
		WarmUsed:    ptr(report.WarmSpace.UsedBytes),
		WarmTotal:   ptr(report.WarmSpace.TotalBytes),
		ConnCount:   &connCount,
	}
	if report.NodeID != "" {
		row.NodeID = ptr(report.NodeID)
	}
	if report.Region != "" {
		row.Region = ptr(report.Region)
	}
	if report.Version != "" {
		row.Version = ptr(report.Version)
	}
	return row
}

// defaultQuotaRebalanceInterval backs the quota allocator when the
// configured interval is missing or unparseable.
const defaultQuotaRebalanceInterval = 60 * time.Second

// parseQuotaRebalanceInterval resolves admin_api.quota_rebalance_interval.
// LOCKED behaviour: an empty or invalid value does NOT abort startup — the
// allocator is a background optimization independent of the admin API, so a
// bad interval degrades to 60s with a Warn naming the field (unlike
// parseJWTHTTPDuration, which gates the request-serving JWT server).
func parseQuotaRebalanceInterval(s string) time.Duration {
	if s == "" {
		return defaultQuotaRebalanceInterval
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		slog.Warn("invalid admin_api.quota_rebalance_interval, using default",
			"value", s, "default", defaultQuotaRebalanceInterval, "err", err)
		return defaultQuotaRebalanceInterval
	}
	return d
}

func ptr[T any](v T) *T { return &v }
