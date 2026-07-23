// Package app provides the reusable edge-node assembly. The App struct
// exposes every component that the calling binary (cmd/edge-node or
// cmd/mwcli) needs — HTTP handlers, admin server, or custom supervision.
// New() performs the full main.go steps 2-21 in order.
//
// Isolation: this package does NOT import internal/controlplane/* or
// internal/storage/metadata.
package app

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"sync"
	"time"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/config"
	"github.com/shlande/mediaworker/internal/node/backhaul"
	"github.com/shlande/mediaworker/internal/node/cache"
	"github.com/shlande/mediaworker/internal/node/dht"
	"github.com/shlande/mediaworker/internal/node/events"
	"github.com/shlande/mediaworker/internal/node/gossippop"
	"github.com/shlande/mediaworker/internal/node/hashring"
	"github.com/shlande/mediaworker/internal/node/icp"
	nodejwt "github.com/shlande/mediaworker/internal/node/jwt"
	"github.com/shlande/mediaworker/internal/node/l4fetch"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/node/netstats"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	"github.com/shlande/mediaworker/internal/node/pinstore"
	nodepinstrategy "github.com/shlande/mediaworker/internal/node/pinstrategy"
	"github.com/shlande/mediaworker/internal/node/planlog"
	"github.com/shlande/mediaworker/internal/node/reporter"
	"github.com/shlande/mediaworker/internal/node/routing"
	nodesync "github.com/shlande/mediaworker/internal/node/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/shared/identity"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/dataplane"
	"github.com/shlande/mediaworker/internal/storage/linkpool"
	"github.com/shlande/mediaworker/internal/types"

	"github.com/shlande/mediaworker/internal/node/monitor"
)

// ---------------------------------------------------------------------------
// Options & App
// ---------------------------------------------------------------------------

// Options carries build-time and tuning parameters for the node assembly.
type Options struct {
	// BuildVersion and BuildCommit are embedded into the NODE_STATUS_REPORT
	// and the admin status endpoint. Use "dev" / "unknown" for development.
	BuildVersion string
	BuildCommit  string

	// JWTRequestTimeout bounds the initial RequestJWTWithRetry call inside New().
	// The retry loop (up to 10 retries with exponential backoff, max 30s each)
	// checks ctx.Done() between attempts, so WithTimeout governs the total wall
	// time. 0 = current behaviour unchanged (the caller's ctx controls).
	JWTRequestTimeout time.Duration

	// Logger is used for all structured logging. nil = slog.Default().
	Logger *slog.Logger
}

// App holds every component assembled by New(). Callers read its exported
// fields to wire HTTP / admin handlers; call Close() after the caller's
// context is done to tear down persistent resources.
type App struct {
	Host        host.Host
	Router      *routing.EdgeRouter
	PeerStore   *peerstore.PeerEntryStore
	JWTClient   *nodejwt.JWTClient
	WarmCache   *cache.WarmCache
	PinStore    *pinstore.PinStore
	BackhaulMgr *backhaul.BackhaulManager

	// Admin-server dependencies.
	AccountPool      *accountpool.AccountPool
	LinkPool         *linkpool.LinkPool
	DataPlane        *dataplane.LocalDataPlane
	Scorer           *gossippop.PeerScorer
	DHTDiscovery     *dht.EdgeDiscovery
	HashRing         *hashring.HashRing
	PlanLog          *planlog.Log
	Capabilities     types.NodeCapabilities
	Region           string
	L4Mode           bool
	PeerID           string
	CPKey            ed25519.PublicKey
	StartedAt        time.Time
	Metrics          *monitor.Metrics
	RefreshDurations *config.RefreshDurations
	NetTracker       *netstats.Tracker
}

// ---------------------------------------------------------------------------
// New — steps 2-21 from cmd/edge-node/main.go
// ---------------------------------------------------------------------------

// New assembles the full edge-node stack. ctx governs the lifetime of
// background goroutines (JWT refresh, DHT advertise, gossipsub, reporter,
// popularity loops) — the caller should cancel ctx at shutdown.
func New(ctx context.Context, cfg *config.Config, opts Options) (app *App, err error) {
	startedAt := time.Now()
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// -------------------------------------------------------------------
	// 2. Load node identity (Ed25519 key + PeerId)
	// -------------------------------------------------------------------
	nodeIdentity, err := identity.LoadOrGenerateIdentity(cfg.Node.Identity.PrivKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
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
		return nil, fmt.Errorf("open peerstore: %w", err)
	}
	if err := ps.Restore(); err != nil {
		return nil, fmt.Errorf("restore peerstore: %w", err)
	}
	ps.StartValueLogGC(ctx, cfg.Node.Libp2p.PeerStore.ParsedGCInterval)
	logger.Info("peerstore restored",
		"path", cfg.Node.Libp2p.PeerStore.Path,
		"gc_interval", cfg.Node.Libp2p.PeerStore.ParsedGCInterval.String(),
	)

	// -------------------------------------------------------------------
	// 4. Extract Ed25519 private key from libp2p key
	// -------------------------------------------------------------------
	rawPriv, err := nodeIdentity.PrivKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("extract raw ed25519 key: %w", err)
	}
	edPriv := ed25519.PrivateKey(rawPriv)

	// -------------------------------------------------------------------
	// 5. Control-plane public key for JWT verification
	// -------------------------------------------------------------------
	cpPubKeyHex := os.Getenv("CONTROL_PLANE_PUBKEY")
	if cpPubKeyHex == "" {
		return nil, fmt.Errorf("CONTROL_PLANE_PUBKEY env var not set; must be hex-encoded Ed25519 public key")
	}
	cpPubKey, err := hex.DecodeString(cpPubKeyHex)
	if err != nil {
		return nil, fmt.Errorf("decode CONTROL_PLANE_PUBKEY: %w", err)
	}
	if len(cpPubKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("CONTROL_PLANE_PUBKEY: expected %d bytes, got %d",
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
			return nil, fmt.Errorf("LIBP2P_FORCE_PNET=1 but LIBP2P_PSK env var is not set")
		}
		pskBytes, err = hex.DecodeString(pskEnv)
		if err != nil {
			return nil, fmt.Errorf("decode LIBP2P_PSK: %w", err)
		}
	} else if cfg.Node.Libp2p.PrivateNetwork.Enabled {
		pskEnv := os.Getenv("LIBP2P_PSK")
		if pskEnv != "" {
			pskBytes, err = hex.DecodeString(pskEnv)
			if err != nil {
				return nil, fmt.Errorf("decode LIBP2P_PSK: %w", err)
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
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}
	logger.Info("libp2p host started",
		"peer_id", h.ID().ShortString(),
		"addrs", logAddrs(h),
		"nat_explicit", natOpts.Explicit,
	)

	// Deferred cleanup: on any error return after the host exists, close
	// the host, peerstore, and pinstore (if constructed) in reverse order.
	var pinStore *pinstore.PinStore
	defer func() {
		if err != nil {
			if pinStore != nil {
				if cerr := pinStore.Close(); cerr != nil {
					logger.Error("pin store close during error cleanup", "err", cerr)
				}
			}
			if ps != nil {
				if cerr := ps.Close(); cerr != nil {
					logger.Error("peerstore close during error cleanup", "err", cerr)
				}
			}
			if h != nil {
				if cerr := h.Close(); cerr != nil {
					logger.Error("libp2p host close during error cleanup", "err", cerr)
				}
			}
		}
	}()

	// -------------------------------------------------------------------
	// 9. Auth stream handler (/edge/auth/1.0.0)
	// -------------------------------------------------------------------
	h.SetStreamHandler(libp2phost.AuthProtocol, func(s network.Stream) {
		if err := libp2phost.HandleAuth(s, gater); err != nil {
			logger.Warn("auth handler error", "err", err)
		}
	})

	// -------------------------------------------------------------------
	// 10. Netstats tracker
	// -------------------------------------------------------------------
	netTracker := netstats.New()
	if err := netstats.Subscribe(ctx, netTracker, h.EventBus()); err != nil {
		logger.Warn("netstats event-bus subscription failed (reachability stays unknown)", "err", err)
	}

	// -------------------------------------------------------------------
	// 10b. Persist peer addresses from identify (warm-restart fix).
	// DHT rendezvous FindPeers returns empty Addrs in this cluster, so
	// PutDiscovery never populates BadgerDB addresses. libp2p's in-memory
	// peerstore has them (populated by identify) but dies with the process.
	// This subscriber persists addresses from EvtPeerIdentificationCompleted
	// so warm restarts have addresses to dial.
	// -------------------------------------------------------------------
	subscribeIdentifyPersistence(ctx, h, ps, logger)

	// -------------------------------------------------------------------
	// 10a. Metrics (T20). Constructed once per process.
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

	jwtCtx := ctx
	if opts.JWTRequestTimeout > 0 {
		var jwtCancel context.CancelFunc
		jwtCtx, jwtCancel = context.WithTimeout(ctx, opts.JWTRequestTimeout)
		defer jwtCancel()
	}
	jwtResp, err := jwtClient.RequestJWTWithRetry(jwtCtx)
	if err != nil {
		logger.Error("initial JWT request failed (entering degraded mode)", "err", err)
		metrics.RecordJWTInitialFailure()
	} else {
		logger.Info("JWT obtained", "refresh_before", jwtResp.RefreshBefore)
		metrics.RecordJWTInitialSuccess()
	}

	// -------------------------------------------------------------------
	// 11b. JWT refresh goroutine
	// -------------------------------------------------------------------
	refreshDurations := &config.RefreshDurations{}
	refreshDurations.Store(
		cfg.Node.JWTService.ParsedRefreshInterval,
		cfg.Node.JWTService.ParsedRefreshBeforeExpiry,
	)
	go runJWTRefreshLoop(ctx, jwtClient, ed25519.PublicKey(cpPubKey),
		refreshDurations,
		logger,
		metrics,
		h, ps)

	// -------------------------------------------------------------------
	// 11c. On-peer-connected auth exchange.
	// -------------------------------------------------------------------
	var authDebounce sync.Map
	libp2phost.SetOnPeerConnectedCallback(h, func(_ peer.ID, remote peer.ID) {
		jwt := jwtClient.CurrentJWT()
		if jwt == "" {
			return
		}
		now := time.Now()
		if last, ok := authDebounce.Load(remote); ok {
			if now.Sub(last.(time.Time)) < 60*time.Second {
				return
			}
		}
		authDebounce.Store(remote, now)
		go func() {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := libp2phost.PresentAuth(ctx, h, remote, jwt); err != nil {
				logger.Debug("on-peer-connected PresentAuth failed", "peer", remote.ShortString(), "err", err)
				authDebounce.Delete(remote)
			}
		}()
	})

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
		return nil, fmt.Errorf("parse dht.advertise_ttl: %w", err)
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
	if err := disc.Start(ctx); err != nil {
		return nil, fmt.Errorf("start edge discovery: %w", err)
	}
	logger.Info("DHT edge discovery started",
		"namespace", cfg.Node.Libp2p.DHT.Namespace,
		"mode", cfg.Node.Libp2p.DHT.Mode,
		"advertise_ttl", ttl.String(),
		"advertise_interval", advertiseInterval.String(),
	)

	// -------------------------------------------------------------------
	// 14. Cache — MemoryIndex + WarmCache
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

	// -------------------------------------------------------------------
	// 15. ICP handlers
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
	ring.StartRebuildLoop(ctx)
	ring.OnPeerStoreChange() // trigger initial build
	logger.Info("hash ring created", "replicas", cfg.HashRing.Replicas)

	// -------------------------------------------------------------------
	// 17b. L4 data plane stack
	// -------------------------------------------------------------------
	var (
		dataPlane *dataplane.LocalDataPlane
		pool      *accountpool.AccountPool
		linkP     *linkpool.LinkPool
	)
	if cfg.Access.DataPlane.Enabled {
		locClient := dataplane.NewHTTPLocationClient(
			cfg.Access.DataPlane.LocationEndpoint,
			func() string { return string(jwtClient.CurrentJWT()) },
		)
		pool = accountpool.BuildFromSnapshot(nil, locClient)
		maxEntries := cfg.Access.DataPlane.LinkPool.MaxEntries
		if maxEntries <= 0 {
			maxEntries = 100
		}
		linkP = linkpool.NewLinkPool(maxEntries)
		dataPlane = dataplane.NewLocalDataPlane(pool, linkP, locClient, http.DefaultClient)
		logger.Info("local data plane stack built",
			"location_endpoint", cfg.Access.DataPlane.LocationEndpoint,
			"link_pool_max_entries", maxEntries,
		)
	}
	dispatcher := events.NewDispatcher(pool, nil, logger)

	// -------------------------------------------------------------------
	// 17c. Backhaul manager
	// -------------------------------------------------------------------
	var backhaulMgr *backhaul.BackhaulManager
	switch {
	case cfg.Access.DataPlane.Enabled:
		// L4 node: local data plane wired.
		backhaulMgr = backhaul.NewBackhaulManager(
			backhaulWarmCache{warmCache},
			dataPlane,
			backhaulICPFetcher{h: h, ring: ring, self: nodeIdentity.PeerID, addrSrc: ps},
			nil, // l4Fetcher — this IS an L4 node
		)
		logger.Info("backhaul manager created (L4 mode, data plane wired)")

	default:
		// Non-L4 edge node: no data plane.
		backhaulMgr = backhaul.NewBackhaulManager(
			backhaulWarmCache{warmCache},
			nil, // dataPlane — disabled
			backhaulICPFetcher{h: h, ring: ring, self: nodeIdentity.PeerID, addrSrc: ps},
			l4fetch.NewFetcher(h, ps, ps),
		)
		logger.Info("backhaul manager created (edge mode, L4 stream fallback wired)")
	}

	// T21 — L4 stream protocol handler.
	if cfg.Access.DataPlane.Enabled {
		l4fetch.RegisterHandler(h, func(ctx context.Context, w io.Writer, hash string) error {
			return backhaulMgr.HandleBlobL4(ctx, w, hash)
		})
		logger.Info("L4 stream protocol handler registered", "proto", l4fetch.L4GetProtocol)
	}

	// T20 — wire metrics into backhaul manager.
	backhaulMgr.SetMetrics(metrics)

	// -------------------------------------------------------------------
	// 17d. Pin store
	// -------------------------------------------------------------------
	if cfg.Edge.PrefixCache.Enabled {
		var warmGet func(string) ([]byte, bool)
		if warmCache != nil {
			warmGet = warmCache.Get
		}
		backhaulFetch := backhaulMgr.HandleBlobNoL4
		if cfg.Access.DataPlane.Enabled {
			backhaulFetch = backhaulMgr.HandleBlobL4
		}
		ps2, err := pinstore.NewPinStore(
			cfg.Edge.PrefixCache.Path+".pin.db",
			cfg.Edge.PrefixCache.Path,
			int64(cfg.Edge.PrefixCache.SizeGB)*1_000_000_000,
			makePinFetchFunc(warmGet, backhaulFetch),
		)
		if err != nil {
			return nil, fmt.Errorf("create pin store: %w", err)
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
	// 18. SyncBroadcaster client
	// -------------------------------------------------------------------
	planLog := planlog.New()
	syncClient := nodesync.NewClient(h, func(plan types.PinPlan) {
		pins, unpins := planlog.Counts(plan)
		planLog.Add(planlog.Record{
			Seq:        plan.Seq,
			ReceivedAt: time.Now(),
			Pins:       pins,
			Unpins:     unpins,
			Applied:    pinStore != nil,
		})
		if pinStore == nil {
			logger.Debug("syncbroadcaster PinPlan received but prefix cache disabled — skipping",
				"seq", plan.Seq, "target_node", plan.TargetNode)
			return
		}
		nodepinstrategy.HandlePinPlan(plan, pinStore, nil, nil)
	}, dispatcher.HandleEvent)
	logger.Info("syncbroadcaster client registered",
		"protocol", string(nodesync.ControlProtocol),
	)

	// -------------------------------------------------------------------
	// 18b. Node status reporter
	// -------------------------------------------------------------------
	capabilities := types.NodeCapabilities{
		Edge:          cfg.Node.DeclaredCapabilities.Edge,
		L4Backhaul:    cfg.Node.DeclaredCapabilities.L4Backhaul,
		RelayProvider: cfg.Node.DeclaredCapabilities.RelayProvider,
		PeerICP:       cfg.Node.DeclaredCapabilities.PeerICP,
	}
	if len(bootstrapAddrs) == 0 {
		logger.Warn("node status reporter disabled: no bootstrap peers configured (no CP target)")
	} else {
		collectReport := func() types.NodeStatusReport {
			report := types.NodeStatusReport{
				NodeID:       h.ID().String(),
				PeerID:       nodeIdentity.PeerID,
				Capabilities: capabilities,
				LastUpdate:   time.Now().Unix(),
				Region:       cfg.Node.Region,
				Version:      opts.BuildVersion,
				StartedAt:    startedAt.Unix(),
				ConnCount:    len(h.Network().Conns()),
				ColdSpace:    nil,
			}
			if jwtClient != nil {
				report.Healthy = !jwtClient.IsDegraded()
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
		go statusReporter.Run(ctx)
		logger.Info("node status reporter started",
			"cp_peer", bootstrapAddrs[0].ID.ShortString(),
			"interval", reporter.DefaultInterval.String(),
		)
	}

	// -------------------------------------------------------------------
	// 19. GossipSub + PeerScorer + MergedPopularity
	// -------------------------------------------------------------------
	scorer := gossippop.NewPeerScorer()
	gs, err := gossippop.NewGossipSub(ctx, h, scorer)
	if err != nil {
		return nil, fmt.Errorf("create gossipsub: %w", err)
	}
	mergedPop := gossippop.NewMergedPopularity()

	// Subscribe to the popularity topic.
	popTopic, err := gossippop.JoinTopic(ctx, gs, gossippop.PopularityTopic)
	if err != nil {
		return nil, fmt.Errorf("join popularity topic: %w", err)
	}
	go gossippop.PublishPopularity(
		ctx, popTopic,
		gossippop.NewLocalPopularity(), nodeIdentity.PeerID, edPriv,
	)

	// Wire gossip popularity into cache eviction.
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

	// Receive loop: process incoming gossip popularity updates.
	go func() {
		sub, err := popTopic.Subscribe()
		if err != nil {
			logger.Error("popularity topic subscribe failed", "err", err)
			return
		}
		defer sub.Cancel()
		hostAdapter := ed25519PeerstoreAdapter{h: h}
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				if ctx.Err() != nil {
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
	// 21. Edge router — hash-ring routing with proxy fallback
	// -------------------------------------------------------------------
	router := routing.NewEdgeRouter(ring, backhaulMgr, nodeIdentity.PeerID, cfg.Access.DataPlane.Enabled, h, ps)
	logger.Info("edge router created",
		"self_peer", nodeIdentity.PeerID,
		"is_l4", cfg.Access.DataPlane.Enabled,
	)

	return &App{
		Host:        h,
		Router:      router,
		PeerStore:   ps,
		JWTClient:   jwtClient,
		WarmCache:   warmCache,
		PinStore:    pinStore,
		BackhaulMgr: backhaulMgr,

		AccountPool:      pool,
		LinkPool:         linkP,
		DataPlane:        dataPlane,
		Scorer:           scorer,
		DHTDiscovery:     disc,
		HashRing:         ring,
		PlanLog:          planLog,
		Capabilities:     capabilities,
		Region:           cfg.Node.Region,
		L4Mode:           cfg.Access.DataPlane.Enabled,
		PeerID:           string(nodeIdentity.PeerID),
		CPKey:            ed25519.PublicKey(cpPubKey),
		StartedAt:        startedAt,
		Metrics:          metrics,
		RefreshDurations: refreshDurations,
		NetTracker:       netTracker,
	}, nil
}

// ---------------------------------------------------------------------------
// FetchBlob — production router path for blob downloads
// ---------------------------------------------------------------------------

// FetchBlob routes a blob download request through the EdgeRouter, which
// handles hash-ring routing, proxy fallback, and the backhaul pipeline (cache →
// ICP → L4 stream). This is the canonical path used by mwcli download and is
// exported so integration tests can exercise the full stack without shelling out.
func (a *App) FetchBlob(ctx context.Context, w io.Writer, blobHash string) error {
	return a.Router.HandleBlobRequest(ctx, w, blobHash)
}

// ---------------------------------------------------------------------------
// Close* — teardown in original main.go shutdown order
// ---------------------------------------------------------------------------

// Close tears down all persistent resources (store → stores → host). This is
// the convenience method for callers that do NOT have an HTTP server to drain
// (e.g. mwcli). Callers that DO run an HTTP server should use the split
// methods so the host stays alive during http.Server.Shutdown:
//
//	shutdownCtx (5s) → a.CloseStores() → httpSrv.Shutdown(shutdownCtx) → a.CloseHost()
func (a *App) Close() error {
	if err := a.CloseStores(); err != nil {
		return err
	}
	return a.CloseHost()
}

// CloseStores tears down the pin store and peer store (in that order).
// The libp2p host is NOT closed — it must stay alive while the HTTP server
// drains in-flight /blob requests.
func (a *App) CloseStores() error {
	if a == nil {
		return nil
	}
	if a.PinStore != nil {
		if err := a.PinStore.Close(); err != nil {
			slog.Error("pin store close error", "err", err)
		}
	}
	if a.PeerStore != nil {
		if err := a.PeerStore.Close(); err != nil {
			slog.Error("peerstore close error", "err", err)
		}
	}
	return nil
}

// CloseHost tears down the libp2p host. Must be called AFTER the HTTP server
// has drained (see CloseStores doc).
func (a *App) CloseHost() error {
	if a == nil {
		return nil
	}
	if a.Host != nil {
		libp2phost.DeregisterConnNotifee(a.Host)
		if err := a.Host.Close(); err != nil {
			slog.Error("libp2p host close error", "err", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// identify-persistence subscription (warm-restart fix)
// ---------------------------------------------------------------------------

// subscribeIdentifyPersistence wires the host event bus so that every completed
// identify round persists the discovered multiaddrs into the BadgerDB
// PeerEntryStore. DHT FindPeers returns empty Addrs in this cluster, so
// PutDiscovery never populates addresses; the only authoritative source is the
// libp2p identify protocol, whose addresses live in the in-memory peerstore
// (pstoremem) and die at process exit. This subscriber makes them durable so
// warm restarts (l4fetch reseed, hash ring rebuild) have addresses to dial.
func subscribeIdentifyPersistence(ctx context.Context, h host.Host, ps *peerstore.PeerEntryStore, logger *slog.Logger) {
	sub, err := h.EventBus().Subscribe(new(event.EvtPeerIdentificationCompleted))
	if err != nil {
		logger.Warn("identify persistence subscription failed (warm restarts may lack addresses)", "err", err)
		return
	}
	go func() {
		defer func() { _ = sub.Close() }()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-sub.Out():
				if !ok {
					return
				}
				e, ok := ev.(event.EvtPeerIdentificationCompleted)
				if !ok {
					continue
				}
				addrs := h.Peerstore().Addrs(e.Peer)
				if len(addrs) == 0 {
					continue
				}
				strs := make([]string, len(addrs))
				for i, a := range addrs {
					strs[i] = a.String()
				}
				if err := ps.PutDiscovery(peerstore.PeerIdFromPeerID(e.Peer), strs); err != nil {
					logger.Debug("identify persistence put failed", "peer", e.Peer.ShortString(), "err", err)
					continue
				}
				logger.Debug("identify persistence stored addrs", "peer", e.Peer.ShortString(), "n", len(strs))
			}
		}
	}()
}

// ---------------------------------------------------------------------------
// Helpers (moved verbatim from cmd/edge-node/main.go)
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
