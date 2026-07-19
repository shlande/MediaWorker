// Command edge-node is the MediaWorker edge distribution node binary.
// It assembles the complete node stack: libp2p host, DHT discovery, JWT
// client, connection gater, cache tiers, ICP handler, hash ring, pin store,
// GossipSub popularity sync, SyncBroadcaster client, backhaul manager, and
// an HTTP server for client blob requests.
//
// The binary does NOT import any internal/controlplane/ package.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"syscall"
	"time"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/node/backhaul"
	"github.com/shlande/mediaworker/internal/node/cache"
	"github.com/shlande/mediaworker/internal/node/dht"
	"github.com/shlande/mediaworker/internal/node/gossippop"
	"github.com/shlande/mediaworker/internal/node/hashring"
	"github.com/shlande/mediaworker/internal/node/icp"
	nodejwt "github.com/shlande/mediaworker/internal/node/jwt"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	"github.com/shlande/mediaworker/internal/node/pinstore"
	nodepinstrategy "github.com/shlande/mediaworker/internal/node/pinstrategy"
	"github.com/shlande/mediaworker/internal/node/reporter"
	"github.com/shlande/mediaworker/internal/node/routing"
	nodesync "github.com/shlande/mediaworker/internal/node/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/shared/identity"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"

	// T20 — metrics package (added at end of import block to avoid merge
	// conflicts with T15/T18's parallel edits in this file).
	"github.com/shlande/mediaworker/internal/node/monitor"
)

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

var (
	configPath = flag.String("config", "configs/node-edge.yaml",
		"Path to YAML configuration file")
	showVersion = flag.Bool("version", false,
		"Print version and exit")
)

// build-time injection
var (
	BuildVersion = "dev"
	BuildCommit  = "unknown"
)

const defaultHTTPListen = ":8080"

func main() {
	startedAt := time.Now()
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
	// 2. Load node identity (Ed25519 key + PeerId)
	// -------------------------------------------------------------------
	nodeIdentity, err := identity.LoadOrGenerateIdentity(cfg.Node.Identity.PrivKeyPath)
	if err != nil {
		log.Fatalf("load identity: %v", err)
	}
	logger.Info("node identity loaded",
		"peer_id", string(nodeIdentity.PeerID),
		"key_path", cfg.Node.Identity.PrivKeyPath,
	)

	// -------------------------------------------------------------------
	// 3. PeerEntryStore (persistent peer metadata)
	// -------------------------------------------------------------------
	ps, err := peerstore.NewPeerEntryStore(cfg.Node.Libp2p.PeerStore.Path)
	if err != nil {
		log.Fatalf("open peerstore: %v", err)
	}
	if err := ps.Restore(); err != nil {
		log.Fatalf("restore peerstore: %v", err)
	}
	gcCtx, gcCancel := context.WithCancel(context.Background())
	defer gcCancel()
	ps.StartValueLogGC(gcCtx, cfg.Node.Libp2p.PeerStore.ParsedGCInterval)
	logger.Info("peerstore restored",
		"path", cfg.Node.Libp2p.PeerStore.Path,
		"gc_interval", cfg.Node.Libp2p.PeerStore.ParsedGCInterval.String(),
	)

	// -------------------------------------------------------------------
	// 4. Extract Ed25519 private key from libp2p key
	// -------------------------------------------------------------------
	rawPriv, err := nodeIdentity.PrivKey.Raw()
	if err != nil {
		log.Fatalf("extract raw ed25519 key: %v", err)
	}
	edPriv := ed25519.PrivateKey(rawPriv)

	// -------------------------------------------------------------------
	// 5. Control-plane public key for JWT verification
	// -------------------------------------------------------------------
	cpPubKeyHex := os.Getenv("CONTROL_PLANE_PUBKEY")
	if cpPubKeyHex == "" {
		log.Fatalf("CONTROL_PLANE_PUBKEY env var not set; must be hex-encoded Ed25519 public key")
	}
	cpPubKey, err := hex.DecodeString(cpPubKeyHex)
	if err != nil {
		log.Fatalf("decode CONTROL_PLANE_PUBKEY: %v", err)
	}
	if len(cpPubKey) != ed25519.PublicKeySize {
		log.Fatalf("CONTROL_PLANE_PUBKEY: expected %d bytes, got %d",
			ed25519.PublicKeySize, len(cpPubKey))
	}
	jwtVerifier := nodejwt.NewJWTVerifier(ed25519.PublicKey(cpPubKey))

	// -------------------------------------------------------------------
	// 6. Connection gater (IP rate limiting + CIDR allowlist)
	// -------------------------------------------------------------------
	connGaterIPRate := rate.Limit(cfg.Node.Libp2p.ConnGater.IPRateLimit)
	if connGaterIPRate == 0 {
		connGaterIPRate = 50
	}
	cidrRanges := parseCIDRRanges(cfg.Node.Libp2p.ConnGater.CIDRAllowlist)
	gater := libp2phost.NewEdgeConnectionGater(
		ps, jwtVerifier, connGaterIPRate, 5 /* burst */, cidrRanges,
	)

	// -------------------------------------------------------------------
	// 7. PSK (pre-shared key for private network)
	// -------------------------------------------------------------------
	var pskBytes types.PSK
	if cfg.Node.Libp2p.PrivateNetwork.ForcePnetEnv {
		pskEnv := os.Getenv("LIBP2P_PSK")
		if pskEnv == "" {
			log.Fatalf("LIBP2P_FORCE_PNET=1 but LIBP2P_PSK env var is not set")
		}
		pskBytes, err = hex.DecodeString(pskEnv)
		if err != nil {
			log.Fatalf("decode LIBP2P_PSK: %v", err)
		}
	} else if cfg.Node.Libp2p.PrivateNetwork.Enabled {
		pskEnv := os.Getenv("LIBP2P_PSK")
		if pskEnv != "" {
			pskBytes, err = hex.DecodeString(pskEnv)
			if err != nil {
				log.Fatalf("decode LIBP2P_PSK: %v", err)
			}
		}
	}

	// -------------------------------------------------------------------
	// 8. libp2p host (T15: NAT traversal gated by node.libp2p.nat_traversal)
	// -------------------------------------------------------------------
	natOpts := libp2phost.ResolveNATOptions(
		cfg.Node.Libp2p.NATTraversal.AutoNAT,
		cfg.Node.Libp2p.NATTraversal.AutoRelay,
		cfg.Node.Libp2p.NATTraversal.DCUtR,
	)
	h, err := libp2phost.NewEdgeHostWithNAT(nodeIdentity, cfg.Node.Libp2p.Listen, pskBytes, gater, natOpts)
	if err != nil {
		log.Fatalf("create libp2p host: %v", err)
	}
	logger.Info("libp2p host started",
		"peer_id", h.ID().ShortString(),
		"addrs", logAddrs(h),
		"nat_explicit", natOpts.Explicit,
	)

	// -------------------------------------------------------------------
	// 9. Auth stream handler (/edge/auth/1.0.0)
	// -------------------------------------------------------------------
	h.SetStreamHandler(libp2phost.AuthProtocol, func(s network.Stream) {
		if err := libp2phost.HandleAuth(s, gater); err != nil {
			logger.Warn("auth handler error", "err", err)
		}
	})

	// -------------------------------------------------------------------
	// 10. Context with signal handling
	// -------------------------------------------------------------------
	rootCtx, cancel := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM,
	)

	// -------------------------------------------------------------------
	// 10a. Metrics (T20). Constructed once per process; mounted on the HTTP
	// mux alongside /blob (plan line 275 — no separate port). Auth: NONE
	// — /metrics is reachable from the same network as /blob. This is the
	// documented intranet assumption: edge-node is deployed behind a
	// network ACL that restricts port 8080 to the operator network and
	// the Prometheus scraper. Do NOT expose /metrics to the public
	// internet without an auth proxy.
	// -------------------------------------------------------------------
	metrics := monitor.NewMetrics()

	// -------------------------------------------------------------------
	// 11. JWT client — request initial capability JWT from control plane
	// -------------------------------------------------------------------
	jwtClient := nodejwt.NewJWTClient(edPriv, nodeIdentity.PeerID, cfg.Node.JWTService.Endpoint, types.NodeCapabilities{
		Edge:          cfg.Node.DeclaredCapabilities.Edge,
		L4Backhaul:    cfg.Node.DeclaredCapabilities.L4Backhaul,
		RelayProvider: cfg.Node.DeclaredCapabilities.RelayProvider,
		PeerICP:       cfg.Node.DeclaredCapabilities.PeerICP,
	})

	jwtResp, err := jwtClient.RequestJWTWithRetry(rootCtx)
	if err != nil {
		logger.Error("initial JWT request failed (entering degraded mode)", "err", err)
		metrics.RecordJWTInitialFailure()
	} else {
		logger.Info("JWT obtained", "refresh_before", jwtResp.RefreshBefore)
		metrics.RecordJWTInitialSuccess()
	}

	// -------------------------------------------------------------------
	// 11b. JWT refresh goroutine — periodically renew the capability JWT
	// before it expires. On failure logs an Error and continues (does NOT
	// panic/Fatal — consistent with §11 degraded-mode behaviour). The
	// goroutine exits when rootCtx is cancelled (process shutdown).
	// -------------------------------------------------------------------
	go runJWTRefreshLoop(rootCtx, jwtClient, ed25519.PublicKey(cpPubKey),
		cfg.Node.JWTService.ParsedRefreshInterval,
		cfg.Node.JWTService.ParsedRefreshBeforeExpiry,
		logger,
		metrics)

	// -------------------------------------------------------------------
	// 12. JWT refresh push handler (/edge/jwt-refresh/1.0.0)
	// -------------------------------------------------------------------
	jwtPeerStore := newPeerStoreWriterAdapter(ps)
	h.SetStreamHandler(nodejwt.JWTRefreshProtocolID, func(s network.Stream) {
		entry, err := nodejwt.HandleJWTPush(s, jwtVerifier, jwtPeerStore)
		if err != nil {
			logger.Warn("jwt push handler error", "err", err)
			return
		}
		logger.Debug("jwt refresh accepted", "peer", entry.PeerID)
	})

	// -------------------------------------------------------------------
	// 13. DHT edge discovery
	// -------------------------------------------------------------------
	ttl, err := time.ParseDuration(cfg.Node.Libp2p.DHT.AdvertiseTTL)
	if err != nil {
		log.Fatalf("parse dht.advertise_ttl: %v", err)
	}
	var dhtMode kaddht.ModeOpt
	switch cfg.Node.Libp2p.DHT.Mode {
	default:
		dhtMode = kaddht.ModeClient
	case "server":
		dhtMode = kaddht.ModeServer
	}
	bootstrapAddrs := parseBootstrapAddrs(cfg.Node.Libp2p.DHT.BootstrapPeers)

	advertiseInterval := cfg.Node.Libp2p.DHT.ParsedAdvertiseInterval
	disc := dht.NewEdgeDiscovery(h, ps, bootstrapAddrs,
		cfg.Node.Libp2p.DHT.Namespace, ttl, advertiseInterval, dhtMode)
	if err := disc.Start(rootCtx); err != nil {
		log.Fatalf("start edge discovery: %v", err)
	}
	logger.Info("DHT edge discovery started",
		"namespace", cfg.Node.Libp2p.DHT.Namespace,
		"mode", cfg.Node.Libp2p.DHT.Mode,
		"advertise_ttl", ttl.String(),
		"advertise_interval", advertiseInterval.String(),
	)

	// -------------------------------------------------------------------
	// 14. Cache — MemoryIndex + WarmCache (T15: gated by edge.warm_cache.enabled)
	// -------------------------------------------------------------------
	memIndex := cache.NewMemoryIndex()
	var warmCache *cache.WarmCache
	if cfg.Edge.WarmCache.Enabled {
		warmCache = cache.NewWarmCache(
			cfg.Edge.WarmCache.Path,
			int64(cfg.Edge.WarmCache.SizeGB)*1_000_000_000,
			memIndex,
			nil, // PinChecker — wired below via pin store
			nil, // PopSource — wired below via GossipSub
		)
		logger.Info("warm cache initialized",
			"path", cfg.Edge.WarmCache.Path,
			"max_size_gb", cfg.Edge.WarmCache.SizeGB,
		)
	} else {
		logger.Info("warm cache disabled (edge.warm_cache.enabled=false)")
	}

	// Cold cache (edge.cold_cache) was removed in T17 — the on-disk cold-store
	// was never wired. Operators with stale YAML still load fine: LoadConfig
	// emits a deprecated-key Warn. No construction step here.

	// -------------------------------------------------------------------
	// 15. ICP handlers — register stream protocols for sibling cache
	// -------------------------------------------------------------------
	icp.RegisterHandlers(h, warmCacheBlobStore{warmCache})
	logger.Info("ICP handlers registered",
		"head_proto", string(icp.BlobHeadProtocol),
		"get_proto", string(icp.BlobGetProtocol),
	)

	// -------------------------------------------------------------------
	// 16. Hash ring
	// -------------------------------------------------------------------
	ring := hashring.NewHashRing(nodeIdentity.PeerID, ps, cfg.HashRing.Replicas)
	ring.StartRebuildLoop(rootCtx)
	ring.OnPeerStoreChange() // trigger initial build
	logger.Info("hash ring created", "replicas", cfg.HashRing.Replicas)

	// -------------------------------------------------------------------
	// 17. Pin store (T15: gated by edge.prefix_cache.enabled)
	// -------------------------------------------------------------------
	var pinStore *pinstore.PinStore
	if cfg.Edge.PrefixCache.Enabled {
		ps2, err := pinstore.NewPinStore(
			cfg.Edge.PrefixCache.Path+".pin.db",
			cfg.Edge.PrefixCache.Path,
			int64(cfg.Edge.PrefixCache.SizeGB)*1_000_000_000,
			func(blobHash string) ([]byte, error) {
				if warmCache == nil {
					return nil, fmt.Errorf("blob %s not in warm cache (warm cache disabled)", blobHash)
				}
				data, ok := warmCache.Get(blobHash)
				if !ok {
					return nil, fmt.Errorf("blob %s not in warm cache", blobHash)
				}
				return data, nil
			},
		)
		if err != nil {
			log.Fatalf("create pin store: %v", err)
		}
		if err := ps2.Restore(); err != nil {
			logger.Error("pin store restore failed (continuing with empty index)", "err", err)
		}
		pinStore = ps2
		logger.Info("pin store initialized",
			"prefix_path", cfg.Edge.PrefixCache.Path,
			"max_size_gb", cfg.Edge.PrefixCache.SizeGB,
		)
	} else {
		logger.Info("prefix cache disabled (edge.prefix_cache.enabled=false) — pin store not built, syncbroadcaster PinPlan handler is no-op")
	}

	// -------------------------------------------------------------------
	// 18. SyncBroadcaster client — receive PinPlan from control plane
	// -------------------------------------------------------------------
	syncClient := nodesync.NewClient(h, func(plan types.PinPlan) {
		if pinStore == nil {
			logger.Debug("syncbroadcaster PinPlan received but prefix cache disabled — skipping",
				"seq", plan.Seq, "target_node", plan.TargetNode)
			return
		}
		nodepinstrategy.HandlePinPlan(plan, pinStore, nil, nil)
	}, nil)
	logger.Info("syncbroadcaster client registered",
		"protocol", string(nodesync.ControlProtocol),
	)

	// -------------------------------------------------------------------
	// 18b. Node status reporter — periodic NODE_STATUS_REPORT push to the
	// control plane (reverse direction of the /edge/control/1.0.0 channel).
	// Runs in its own goroutine; send failures are Warn-logged inside the
	// reporter and the next cycle covers (no retry, never blocks main).
	// -------------------------------------------------------------------
	if len(bootstrapAddrs) == 0 {
		logger.Warn("node status reporter disabled: no bootstrap peers configured (no CP target)")
	} else {
		collectReport := func() types.NodeStatusReport {
			report := types.NodeStatusReport{
				NodeID: h.ID().String(),
				PeerID: nodeIdentity.PeerID,
				Capabilities: types.NodeCapabilities{
					Edge:          cfg.Node.DeclaredCapabilities.Edge,
					L4Backhaul:    cfg.Node.DeclaredCapabilities.L4Backhaul,
					RelayProvider: cfg.Node.DeclaredCapabilities.RelayProvider,
					PeerICP:       cfg.Node.DeclaredCapabilities.PeerICP,
				},
				LastUpdate: time.Now().Unix(),
				Region:     cfg.Node.Region,
				Version:    BuildVersion,
				StartedAt:  startedAt.Unix(),
				ConnCount:  len(h.Network().Conns()),
				ColdSpace:  nil, // cold cache unwired (see section 14 note)
			}
			if jwtClient != nil {
				report.Healthy = !jwtClient.IsDegraded()
				// Capabilities authorized by the current JWT win over the
				// declared config; expired/absent JWT falls back to declared.
				if jwt := jwtClient.CurrentJWT(); jwt != "" {
					if payload, err := sjwt.VerifyJWTAnyPeerID(jwt, ed25519.PublicKey(cpPubKey)); err == nil {
						report.Capabilities = payload.Capabilities
					}
				}
				_, _, report.JWTRefreshFail24h = jwtClient.RefreshStats()
			}
			if pinStore != nil {
				space := pinStore.QuerySpace()
				report.PrefixSpace = types.PartitionStatus{
					TotalBytes: space.TotalPinnedSize + space.AvailableBytes,
					UsedBytes:  space.TotalPinnedSize,
					BlobCount:  space.PinnedCount,
				}
			}
			report.WarmSpace = types.PartitionStatus{
				TotalBytes: int64(cfg.Edge.WarmCache.SizeGB) * 1_000_000_000,
			}
			if warmCache != nil {
				used, total := warmCache.Usage()
				report.WarmSpace.UsedBytes = used
				report.WarmSpace.TotalBytes = total
				report.WarmSpace.BlobCount = int32(warmCache.Count())
			}
			return report
		}
		statusReporter := reporter.NewReporter(reporter.Config{
			Client:  syncClient,
			CP:      bootstrapAddrs[0].ID,
			Collect: collectReport,
			Logger:  logger,
		})
		go statusReporter.Run(rootCtx)
		logger.Info("node status reporter started",
			"cp_peer", bootstrapAddrs[0].ID.ShortString(),
			"interval", reporter.DefaultInterval.String(),
		)
	}

	// -------------------------------------------------------------------
	// 19. GossipSub + PeerScorer + MergedPopularity
	// -------------------------------------------------------------------
	scorer := gossippop.NewPeerScorer()
	gs, err := gossippop.NewGossipSub(rootCtx, h, scorer)
	if err != nil {
		log.Fatalf("create gossipsub: %v", err)
	}
	mergedPop := gossippop.NewMergedPopularity()

	// Subscribe to the popularity topic.
	popTopic, err := gossippop.JoinTopic(rootCtx, gs, gossippop.PopularityTopic)
	if err != nil {
		log.Fatalf("join popularity topic: %v", err)
	}
	go gossippop.PublishPopularity(
		rootCtx, popTopic,
		gossippop.NewLocalPopularity(), nodeIdentity.PeerID, edPriv,
	)

	// Wire gossip popularity into cache eviction:
	//   - PinChecker: PinStore.IsPinned protects pinned blobs from eviction.
	//   - PopSource: adapter closure converts mergedPop.Snapshot() into the
	//     []*cache.VideoMeta shape that Evict expects (one VideoMeta per blob
	//     hash with a single SegmentMeta carrying the same hash so Evict's
	//     index.Get(seg.BlobHash) lookup in evict.go:80 succeeds).
	if warmCache != nil {
		if pinStore != nil {
			warmCache.SetPinChecker(pinStore.IsPinned)
		}
		warmCache.SetPopSource(func() []*cache.VideoMeta {
			heat := mergedPop.Snapshot()
			out := make([]*cache.VideoMeta, 0, len(heat))
			for h, p := range heat {
				out = append(out, &cache.VideoMeta{
					BlobHash:   h,
					Popularity: p,
					Segments:   []*cache.SegmentMeta{{BlobHash: h}},
				})
			}
			return out
		})
	}

	// Receive loop: process incoming gossip popularity updates so the local
	// merged view stays fresh. Exits when rootCtx is cancelled.
	go func() {
		sub, err := popTopic.Subscribe()
		if err != nil {
			logger.Error("popularity topic subscribe failed", "err", err)
			return
		}
		defer sub.Cancel()
		hostAdapter := ed25519PeerstoreAdapter{h: h}
		for {
			msg, err := sub.Next(rootCtx)
			if err != nil {
				if rootCtx.Err() != nil {
					return
				}
				logger.Debug("popularity sub next error", "err", err)
				continue
			}
			gossippop.HandlePopularityMessage(mergedPop, scorer, msg, hostAdapter, peerEntryLookupAdapter{store: ps})
		}
	}()

	logger.Info("gossipsub popularity sync started", "topic", gossippop.PopularityTopic)

	// -------------------------------------------------------------------
	// 20. Backhaul manager
	// -------------------------------------------------------------------
	var backhaulMgr *backhaul.BackhaulManager
	switch {
	case cfg.Access.DataPlane.Enabled:
		// L4 node: data plane available. The DataPlane interface is not yet
		// implemented — passed as nil. HandleBlobL4 will fall through to L4
		// after cache miss + ICP miss (until data plane is wired).
		backhaulMgr = backhaul.NewBackhaulManager(
			backhaulWarmCache{warmCache},
			nil, // dataPlane — TBD
			backhaulICPFetcher{h: h, ring: ring, self: nodeIdentity.PeerID},
			nil, // l4Fetcher — this IS an L4 node
		)
		logger.Info("backhaul manager created (L4 mode, data plane TBD)")

	default:
		// Non-L4 edge node: no data plane. HandleBlobNoL4 uses cache → ICP →
		// L4 stream fallback.
		// l4Fetcher is nil for now — L4 stream protocol not yet defined.
		backhaulMgr = backhaul.NewBackhaulManager(
			backhaulWarmCache{warmCache},
			nil, // dataPlane — disabled
			backhaulICPFetcher{h: h, ring: ring, self: nodeIdentity.PeerID},
			nil, // l4Fetcher — not yet implemented
		)
		logger.Info("backhaul manager created (edge mode, no L4)")
	}

	// T20 — wire the metrics instance into the backhaul manager so
	// HandleBlobL4/NoL4 can increment edge_cache_request_total +
	// edge_cache_hit_total + edge_peer_request_total + edge_peer_hit_total.
	backhaulMgr.SetMetrics(metrics)

	// -------------------------------------------------------------------
	// 21. Edge router — hash-ring routing with proxy fallback
	// -------------------------------------------------------------------
	router := routing.NewEdgeRouter(ring, backhaulMgr, nodeIdentity.PeerID, cfg.Access.DataPlane.Enabled, h)
	logger.Info("edge router created",
		"self_peer", nodeIdentity.PeerID,
		"is_l4", cfg.Access.DataPlane.Enabled,
	)

	// -------------------------------------------------------------------
	// 22. HTTP server for client blob requests
	// -------------------------------------------------------------------
	httpListen := defaultHTTPListen
	mux := http.NewServeMux()
	mux.HandleFunc("GET /blob/{hash}", func(w http.ResponseWriter, r *http.Request) {
		blobHash := r.PathValue("hash")
		ctx, reqCancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer reqCancel()

		logger.Info("blob request", "hash", blobHash)
		if err := router.HandleBlobRequest(ctx, w, blobHash); err != nil {
			logger.Error("blob request failed", "hash", blobHash, "err", err)
			http.Error(w, "blob not found", http.StatusNotFound)
		}
	})

	// T20 — /metrics endpoint on the same port as /blob. No auth — see
	// step 10a for the intranet deployment assumption.
	mux.Handle("GET /metrics", metrics.HTTPHandler())

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

	if pinStore != nil {
		if err := pinStore.Close(); err != nil {
			logger.Error("pin store close error", "err", err)
		}
	}
	if err := ps.Close(); err != nil {
		logger.Error("peerstore close error", "err", err)
	}
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "err", err)
	}
	if err := h.Close(); err != nil {
		logger.Error("libp2p host close error", "err", err)
	}

	logger.Info("shutdown complete")
}

// ---------------------------------------------------------------------------
// Adapters — map package interfaces that differ
// ---------------------------------------------------------------------------

// peerStoreWriterAdapter adapts *peerstore.PeerEntryStore to
// nodejwt.PeerStoreWriter for HandleJWTPush.
//
// PeerEntryStore.Get returns (types.PeerStoreEntry, bool);
// PeerStoreWriter.Get expects (types.PeerStoreEntry, error).
// The adapter converts "not found" (false) into a non-nil error.
//
// PeerEntryStore.Put expects (peerID, entry);
// PeerStoreWriter.Put expects only the entry (PeerID is inside).
// The adapter extracts PeerID before delegating.
type peerStoreWriterAdapter struct {
	store *peerstore.PeerEntryStore
}

func newPeerStoreWriterAdapter(s *peerstore.PeerEntryStore) *peerStoreWriterAdapter {
	return &peerStoreWriterAdapter{store: s}
}

func (a *peerStoreWriterAdapter) Get(peerID types.PeerId) (types.PeerStoreEntry, error) {
	entry, ok := a.store.Get(peerID)
	if !ok {
		return types.PeerStoreEntry{}, fmt.Errorf("peer not found: %s", peerID)
	}
	return entry, nil
}

func (a *peerStoreWriterAdapter) Put(entry types.PeerStoreEntry) error {
	return a.store.Put(entry.PeerID, entry)
}

// warmCacheBlobStore adapts *cache.WarmCache to icp.BlobStore.
// WarmCache.Get returns ([]byte, bool); BlobStore.Get returns (io.ReadCloser, error).
type warmCacheBlobStore struct {
	wc *cache.WarmCache
}

func (s warmCacheBlobStore) Has(blobHash string) bool {
	if s.wc == nil {
		return false
	}
	return s.wc.Has(blobHash)
}

func (s warmCacheBlobStore) Get(blobHash string) (io.ReadCloser, error) {
	if s.wc == nil {
		return nil, fmt.Errorf("warm cache disabled")
	}
	data, ok := s.wc.Get(blobHash)
	if !ok {
		return nil, fmt.Errorf("blob %s not in warm cache", blobHash)
	}
	return &byteReadCloser{data: data}, nil
}

// byteReadCloser is an io.ReadCloser wrapping a byte slice.
type byteReadCloser struct {
	data   []byte
	offset int
}

func (r *byteReadCloser) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *byteReadCloser) Close() error {
	r.offset = 0
	return nil
}

// ed25519PeerstoreAdapter adapts a libp2p host.Host to the narrow interface
// expected by gossippop.HandlePopularityMessage. libp2p's Peerstore.PubKey
// returns the generic ic.PubKey interface; HandlePopularityMessage wants an
// ed25519.PublicKey directly so it can call ed25519.Verify without a type
// assertion. The adapter bridges this by extracting the raw Ed25519 bytes via
// gossippop.Ed25519PubKey (which itself uses peer.ID.ExtractPublicKey).
//
// If a peer.ID does not embed an Ed25519 key (e.g. RSA or Secp256k1 host),
// PubKey returns nil and HandlePopularityMessage drops the message.
type ed25519PeerstoreAdapter struct {
	h host.Host
}

func (a ed25519PeerstoreAdapter) Peerstore() interface {
	PubKey(peer.ID) ed25519.PublicKey
} {
	return ed25519PopKeyView(a)
}

type ed25519PopKeyView struct {
	h host.Host
}

func (v ed25519PopKeyView) PubKey(id peer.ID) ed25519.PublicKey {
	raw := gossippop.Ed25519PubKey(id)
	if raw == nil {
		return nil
	}
	return ed25519.PublicKey(raw)
}

// peerEntryLookupAdapter adapts *peerstore.PeerEntryStore to the narrow
// gossippop.PeerEntryLookup interface. HandlePopularityMessage consults it to
// discard heat from peers that are either absent from the local store
// (unknown — never JWT-verified) or marked Stale (JWT expired / evicted).
type peerEntryLookupAdapter struct {
	store *peerstore.PeerEntryStore
}

func (a peerEntryLookupAdapter) StaleOrUnknown(peerID types.PeerId) bool {
	entry, ok := a.store.Get(peerID)
	if !ok {
		return true
	}
	return entry.Stale
}

// backhaulWarmCache adapts *cache.WarmCache to backhaul.CacheReader and
// backhaul.CacheWriter (via its Put method). The adapter is necessary because
// backhaul.BackhaulManager accepts the narrow CacheReader/CacheWriter
// interfaces rather than *cache.WarmCache directly.
type backhaulWarmCache struct {
	wc *cache.WarmCache
}

func (c backhaulWarmCache) Get(blobHash string) ([]byte, bool) {
	if c.wc == nil {
		return nil, false
	}
	return c.wc.Get(blobHash)
}

func (c backhaulWarmCache) Put(blobHash string, data []byte, bitrate int) error {
	if c.wc == nil {
		return fmt.Errorf("warm cache disabled")
	}
	return c.wc.Put(blobHash, data, bitrate)
}

// backhaulICPFetcher adapts ICP FetchFromPeer to backhaul.ICPFetcher.
//
// Routing: ring.Get(blobHash) returns the primary peer for this blob.
//   - empty ring (target == "")        → return (nil, false, nil): fall back
//     to local backhaul (data plane / L4 stream). Zero network calls.
//   - target == self (we are primary)  → return (nil, false, nil): same
//     fall-back; avoids looping a fetch to ourselves.
//   - target is a sibling              → call icp.FetchFromPeer once
//     (single ICP request, no retry storm per plan line 203) and return
//     its (io.ReadCloser, bool, error) as-is.
type backhaulICPFetcher struct {
	h    host.Host
	ring *hashring.HashRing
	self types.PeerId
}

func (f backhaulICPFetcher) FetchFromPeer(ctx context.Context, blobHash string) (interface{}, bool, error) {
	if f.ring == nil {
		// Ring not wired — preserve legacy stub behaviour: fall back to local.
		return nil, false, nil
	}
	target := f.ring.Get(blobHash)
	if target == "" || target == f.self {
		return nil, false, nil
	}
	// types.PeerId is the base58-encoded string form; peer.Decode reverses
	// the encoding to recover the raw-multihash peer.ID that libp2p APIs
	// expect. A plain peer.ID(target) cast would treat the ASCII bytes of
	// the base58 string as the peer ID and produce a different identity.
	targetID, err := peer.Decode(string(target))
	if err != nil {
		return nil, false, fmt.Errorf("backhaul icp: decode target peer %q: %w", target, err)
	}
	return icp.FetchFromPeer(ctx, f.h, targetID, blobHash)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// logAddrs formats host addresses for structured logging.
func logAddrs(h host.Host) []string {
	addrs := h.Addrs()
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.String()
	}
	return out
}

// parseCIDRRanges parses CIDR strings into netip.Prefix values.
// Invalid entries are logged and skipped.
func parseCIDRRanges(cidrs []string) []netip.Prefix {
	if len(cidrs) == 0 {
		return nil
	}
	var result []netip.Prefix
	for _, cidr := range cidrs {
		pfx, err := netip.ParsePrefix(cidr)
		if err != nil {
			log.Printf("parse cidr %q: %v (skipping)", cidr, err)
			continue
		}
		result = append(result, pfx)
	}
	return result
}

// parseBootstrapAddrs parses multiaddr strings (with /p2p/ suffix) into
// peer.AddrInfo values.
func parseBootstrapAddrs(addrs []string) []peer.AddrInfo {
	var result []peer.AddrInfo
	for _, addr := range addrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			log.Printf("parse bootstrap addr %q: %v (skipping)", addr, err)
			continue
		}
		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log.Printf("extract peer from addr %q: %v (skipping)", addr, err)
			continue
		}
		result = append(result, *ai)
	}
	return result
}

// runJWTRefreshLoop periodically re-requests a capability JWT from the control
// plane. The next refresh fires at min(refreshInterval, exp-now-refreshBeforeExpiry):
//   - refreshInterval: caller-configured steady cadence (e.g. 5m)
//   - exp-now-refreshBeforeExpiry: time-to-expiry minus a safety margin so we
//     renew before the CP TTL lapses; if the CP-issued JWT has a shorter TTL
//     than the interval, we refresh sooner to avoid drift into an unauthenticated
//     window.
//
// The CP-side RefreshBefore hint (jwtResp.RefreshBefore) is honoured indirectly
// through refreshBeforeExpiry from the node config: the CP informs the node of
// its desired lead time via the `refresh_before_expiry` YAML field, which the
// operator is expected to align with the CP's `RefreshBeforeSeconds` policy.
//
// On request failure: logs at Error, increments edge_jwt_refresh_failure_total,
// and continues the loop (NO panic/Fatal — consistent with the initial-failure
// degraded mode). On success, increments edge_jwt_refresh_success_total and
// updates edge_jwt_refresh_last_success_timestamp. The goroutine exits when
// ctx is cancelled (process shutdown via rootCtx).
func runJWTRefreshLoop(
	ctx context.Context,
	jwtClient *nodejwt.JWTClient,
	cpPubKey ed25519.PublicKey,
	refreshInterval, refreshBeforeExpiry time.Duration,
	logger *slog.Logger,
	metrics *monitor.Metrics,
) {
	// Fallbacks: if config produced zero (e.g. loader path bypassed), use 5m.
	if refreshInterval <= 0 {
		refreshInterval = 5 * time.Minute
	}
	if refreshBeforeExpiry <= 0 {
		refreshBeforeExpiry = 5 * time.Minute
	}

	for {
		wait := refreshInterval

		// If we have a cached JWT, refine the wait so we renew at
		// exp-now-refreshBeforeExpiry (but never later than refreshInterval).
		if jwt := jwtClient.CurrentJWT(); jwt != "" {
			if payload, err := sjwt.VerifyJWTAnyPeerID(jwt, cpPubKey); err == nil {
				exp := time.Unix(payload.Exp, 0)
				remaining := time.Until(exp)
				if remaining > refreshBeforeExpiry {
					wait = remaining - refreshBeforeExpiry
					if wait > refreshInterval {
						wait = refreshInterval
					}
				} else {
					// Already within the safety margin — refresh now.
					wait = 0
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		if _, err := jwtClient.RequestJWT(ctx); err != nil {
			logger.Error("jwt refresh request failed", "err", err)
			if metrics != nil {
				metrics.RecordJWTRefreshFailure()
			}
			continue
		}
		logger.Info("jwt refreshed")
		if metrics != nil {
			metrics.RecordJWTRefreshSuccess()
			metrics.SetJWTRefreshLastTS(time.Now().Unix())
		}
	}
}
