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
	cpdht "github.com/shlande/mediaworker/internal/controlplane/dhtbootstrap"
	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/controlplane/metadata"
	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
	"github.com/shlande/mediaworker/internal/controlplane/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/shared/identity"
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

	// 5. JWT service + HTTP server.
	jwtSvc := cpjwt.NewJWTService(privKey, ps, rateLimiter, auditLog)
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
	sb := syncbroadcaster.New(bootHost.Host())

	// 10. MetadataClient (gracefully degrade if PG unavailable — PinOrchestrator
	//     uses cached state via NodeStatusReport channels).
	var mc *metadata.PGMetadataClient
	mc, err = metadata.NewPGMetadataClient(cfg.Metadata.PGDSN)
	if err != nil {
		slog.Warn("PG metadata client unavailable, PinOrchestrator will use cached state", "err", err)
		// mc remains nil; NewPinOrchestrator will receive nil MetadataClient.
	}

	// 11. PinOrchestrator.
	po := pinstrategy.NewPinOrchestrator(mc, mc, sb)
	po.RegisterStrategy("dash_video", &pinstrategy.DashPinStrategy{})

	rebalanceIntv, err := time.ParseDuration(cfg.PinOrchestrator.RebalanceInterval)
	if err != nil {
		return fmt.Errorf("parse rebalance_interval %q: %w", cfg.PinOrchestrator.RebalanceInterval, err)
	}

	// 12. Context + signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 13. Start all components.
	slog.Info("control-plane starting")

	go func() {
		slog.Info("JWT HTTP listening", "addr", cfg.JWT.Listen)
		if err := httpServer.Serve(ctx, cfg.JWT.Listen); err != nil {
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

	go po.Run(ctx, rebalanceIntv)
	slog.Info("PinOrchestrator started", "interval", rebalanceIntv)

	// 14. Subscribe to reverse channels.
	statusCh := sb.Subscribe("NODE_STATUS_REPORT")
	go func() {
		for evt := range statusCh {
			var report types.NodeStatusReport
			if err := json.Unmarshal(evt.Payload, &report); err != nil {
				slog.Warn("failed to decode NODE_STATUS_REPORT", "err", err)
				continue
			}
			po.OnNodeStatusReport(report)
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
