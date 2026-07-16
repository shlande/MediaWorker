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
	nodepinstrategy "github.com/shlande/mediaworker/internal/node/pinstrategy"
	"github.com/shlande/mediaworker/internal/node/pinstore"
	nodesync "github.com/shlande/mediaworker/internal/node/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/types"
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
	logger.Info("peerstore restored", "path", cfg.Node.Libp2p.PeerStore.Path)

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
	// 8. libp2p host
	// -------------------------------------------------------------------
	h, err := libp2phost.NewEdgeHost(nodeIdentity, cfg.Node.Libp2p.Listen, pskBytes, gater)
	if err != nil {
		log.Fatalf("create libp2p host: %v", err)
	}
	logger.Info("libp2p host started",
		"peer_id", h.ID().ShortString(),
		"addrs", logAddrs(h),
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
	// 11. JWT client — request initial capability JWT from control plane
	// -------------------------------------------------------------------
	jwtClient := nodejwt.NewJWTClient(edPriv, nodeIdentity.PeerID, cfg.Node.JWTService.Endpoint)

	jwtResp, err := jwtClient.RequestJWTWithRetry(rootCtx)
	if err != nil {
		logger.Error("initial JWT request failed (entering degraded mode)", "err", err)
	} else {
		logger.Info("JWT obtained", "refresh_before", jwtResp.RefreshBefore)
	}

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

	disc := dht.NewEdgeDiscovery(h, ps, bootstrapAddrs,
		cfg.Node.Libp2p.DHT.Namespace, ttl, dhtMode)
	if err := disc.Start(rootCtx); err != nil {
		log.Fatalf("start edge discovery: %v", err)
	}
	logger.Info("DHT edge discovery started",
		"namespace", cfg.Node.Libp2p.DHT.Namespace,
		"mode", cfg.Node.Libp2p.DHT.Mode,
	)

	// -------------------------------------------------------------------
	// 14. Cache — MemoryIndex + WarmCache
	// -------------------------------------------------------------------
	memIndex := cache.NewMemoryIndex()
	warmCache := cache.NewWarmCache(
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
	// 17. Pin store
	// -------------------------------------------------------------------
	pinStore, err := pinstore.NewPinStore(
		cfg.Edge.PrefixCache.Path+".pin.db",
		cfg.Edge.PrefixCache.Path,
		int64(cfg.Edge.PrefixCache.SizeGB)*1_000_000_000,
		func(blobHash string) ([]byte, error) {
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
	if err := pinStore.Restore(); err != nil {
		logger.Error("pin store restore failed (continuing with empty index)", "err", err)
	}
	logger.Info("pin store initialized",
		"prefix_path", cfg.Edge.PrefixCache.Path,
		"max_size_gb", cfg.Edge.PrefixCache.SizeGB,
	)

	// -------------------------------------------------------------------
	// 18. SyncBroadcaster client — receive PinPlan from control plane
	// -------------------------------------------------------------------
	_ = nodesync.NewClient(h, func(plan types.PinPlan) {
		nodepinstrategy.HandlePinPlan(plan, pinStore)
	}, nil)
	logger.Info("syncbroadcaster client registered",
		"protocol", string(nodesync.ControlProtocol),
	)

	// -------------------------------------------------------------------
	// 19. GossipSub + PeerScorer + MergedPopularity
	// -------------------------------------------------------------------
	scorer := gossippop.NewPeerScorer()
	gs, err := gossippop.NewGossipSub(rootCtx, h, scorer)
	if err != nil {
		log.Fatalf("create gossipsub: %v", err)
	}
	mergedPop := gossippop.NewMergedPopularity()
	_ = mergedPop // used via HandlePopularityMessage subscriber

	// Subscribe to the popularity topic.
	popTopic, err := gossippop.JoinTopic(rootCtx, gs, gossippop.PopularityTopic)
	if err != nil {
		log.Fatalf("join popularity topic: %v", err)
	}
	go gossippop.PublishPopularity(
		rootCtx, popTopic,
		gossippop.NewLocalPopularity(), nodeIdentity.PeerID, edPriv,
	)
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
			nil,                         // dataPlane — TBD
			backhaulICPFetcher{h: h},    // ICP: fetch from sibling caches
			nil,                         // l4Fetcher — this IS an L4 node
		)
		logger.Info("backhaul manager created (L4 mode, data plane TBD)")

	default:
		// Non-L4 edge node: no data plane. HandleBlobNoL4 uses cache → ICP →
		// L4 stream fallback.
		// l4Fetcher is nil for now — L4 stream protocol not yet defined.
		backhaulMgr = backhaul.NewBackhaulManager(
			backhaulWarmCache{warmCache},
			nil,                         // dataPlane — disabled
			backhaulICPFetcher{h: h},    // ICP: fetch from sibling caches
			nil,                         // l4Fetcher — not yet implemented
		)
		logger.Info("backhaul manager created (edge mode, no L4)")
	}

	// -------------------------------------------------------------------
	// 21. HTTP server for client blob requests
	// -------------------------------------------------------------------
	httpListen := defaultHTTPListen
	mux := http.NewServeMux()
	mux.HandleFunc("GET /blob/{hash}", func(w http.ResponseWriter, r *http.Request) {
		blobHash := r.PathValue("hash")
		ctx, reqCancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer reqCancel()

		logger.Info("blob request", "hash", blobHash)

		var writeErr error
		if cfg.Access.DataPlane.Enabled {
			writeErr = backhaulMgr.HandleBlobL4(ctx, w, blobHash)
		} else {
			writeErr = backhaulMgr.HandleBlobNoL4(ctx, w, blobHash)
		}
		if writeErr != nil {
			logger.Error("blob request failed", "hash", blobHash, "err", writeErr)
			http.Error(w, "blob not found", http.StatusNotFound)
		}
	})

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

	if err := pinStore.Close(); err != nil {
		logger.Error("pin store close error", "err", err)
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
	return s.wc.Has(blobHash)
}

func (s warmCacheBlobStore) Get(blobHash string) (io.ReadCloser, error) {
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

// backhaulWarmCache adapts *cache.WarmCache to backhaul.CacheReader.
type backhaulWarmCache struct {
	wc *cache.WarmCache
}

func (c backhaulWarmCache) Get(blobHash string) ([]byte, bool) {
	return c.wc.Get(blobHash)
}

// backhaulICPFetcher adapts ICP FetchFromPeer to backhaul.ICPFetcher.
// TODO: Determine target peer from hash ring when hashring integration is complete.
type backhaulICPFetcher struct {
	h host.Host
}

func (f backhaulICPFetcher) FetchFromPeer(ctx context.Context, blobHash string) (interface{}, bool, error) {
	// Needs a target peer. Once hashring.Get() is wired for ICP routing,
	// this will determine sibling nodes and call icp.FetchFromPeer.
	return nil, false, nil
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
