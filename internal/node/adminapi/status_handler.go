package adminapi

import (
	"crypto/ed25519"
	"net/http"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Narrow dependency interfaces (todo 49 wires the real components) ──────

// StatusJWTClient is the narrow subset of *jwt.JWTClient the status handler
// reads. Production wires the live client; tests use a fake.
type StatusJWTClient interface {
	CurrentJWT() types.CapabilityJWT
	IsDegraded() bool
	RefreshStats() (lastAt time.Time, lastOK bool, failCount24h int)
}

// GraylistCounter is the narrow subset of *gossippop.PeerScorer the status
// handler reads — a count only, never the scorer's internal maps.
type GraylistCounter interface {
	GraylistedCount() int
}

// NetworkReporter is the narrow connection-count view of the libp2p host.
// It returns pre-aggregated counts (not []network.Conn) because Go has no
// slice covariance and tests should not have to mock libp2p's ~15-method
// network.Conn. Production wires Libp2pNetworkReporter(h.Network()).
type NetworkReporter interface {
	ConnCounts() (total, inbound, outbound int)
}

// BackhaulStatsReporter is the narrow subset of *backhaul.BackhaulManager
// the status handler reads for instantaneous cache/ttfb values.
type BackhaulStatsReporter interface {
	// WarmCacheHitRate is cumulative since process start (v1 simplification).
	WarmCacheHitRate() float64
	// TTFBP95Ms is the nearest-rank P95 of the ttfb sample ring (0 = no samples).
	TTFBP95Ms() int64
}

// networkConnSource is satisfied by libp2p's network.Network (h.Network()).
type networkConnSource interface {
	Conns() []network.Conn
}

type networkReporterFunc func() (int, int, int)

func (f networkReporterFunc) ConnCounts() (int, int, int) { return f() }

// Libp2pNetworkReporter adapts a libp2p network source (h.Network()) to
// NetworkReporter. The direction-classification loop lives here in adminapi
// (not in main.go) so todo 49's wiring stays a one-liner and the
// inbound/outbound mapping is unit-testable. DirUnknown conns count toward
// total but neither side.
func Libp2pNetworkReporter(n networkConnSource) NetworkReporter {
	return networkReporterFunc(func() (int, int, int) {
		conns := n.Conns()
		inbound, outbound := 0, 0
		for _, c := range conns {
			switch c.Stat().Direction {
			case network.DirInbound:
				inbound++
			case network.DirOutbound:
				outbound++
			}
		}
		return len(conns), inbound, outbound
	})
}

// ─── Deps + response types ─────────────────────────────────────────────────

// StatusDeps carries everything GET /v1/status reads, pre-digested into
// plain values (identity/config) and narrow interfaces (live components).
// Todo 49 constructs it in main.go; nil interface fields degrade the
// corresponding response section to zero values rather than panicking.
type StatusDeps struct {
	PeerID       string                 // nodeIdentity.PeerID
	Capabilities types.NodeCapabilities // cfg.Node.DeclaredCapabilities
	L4Mode       bool                   // cfg.Access.DataPlane.Enabled
	Region       string                 // cfg.Node.Region
	Version      string                 // main.BuildVersion
	StartedAt    time.Time              // process start (uptime_sec baseline)
	// RefreshBefore is cfg.Node.JWTService.ParsedRefreshBeforeExpiry.
	RefreshBefore time.Duration
	// ControlPlanePubKey verifies the current JWT's signature before its
	// exp is reported. An unverifiable or expired JWT yields jwt.exp=null
	// (sjwt.VerifyJWTAnyPeerID rejects expired tokens).
	ControlPlanePubKey ed25519.PublicKey

	JWTClient StatusJWTClient       // nil → healthy=false, jwt zero-valued
	Scorer    GraylistCounter       // nil → graylisted_peers=0
	Network   NetworkReporter       // nil → conn all zero
	Backhaul  BackhaulStatsReporter // nil → hit rates and ttfb zero

	// Now overrides the clock (tests); nil → time.Now.
	Now func() time.Time
}

type jwtStatus struct {
	Exp                 *int64     `json:"exp"` // null when no verifiable JWT
	RefreshBefore       int64      `json:"refresh_before"`
	LastRefreshAt       *time.Time `json:"last_refresh_at"` // null when never attempted
	LastRefreshOK       bool       `json:"last_refresh_ok"`
	RefreshFailCount24h int        `json:"refresh_fail_count_24h"`
}

type scoreView struct {
	GraylistedPeers int `json:"graylisted_peers"`
}

type connStats struct {
	Total    int `json:"total"`
	Inbound  int `json:"inbound"`
	Outbound int `json:"outbound"`
}

// cacheHitRates reports cumulative-since-process-start rates (v1
// simplification, see backhaul.WarmCacheHitRate). prefix is always 0: the
// pin store has no read-serving path in the /blob flow, so there is no
// prefix-tier counting point to read — trends come from Prometheus directly.
type cacheHitRates struct {
	Prefix float64 `json:"prefix"`
	Warm   float64 `json:"warm"`
}

type statusResponse struct {
	PeerID        string        `json:"peer_id"`
	Capabilities  []string      `json:"capabilities"`
	Mode          string        `json:"mode"` // "l4" | "edge"
	Region        string        `json:"region"`
	Version       string        `json:"version"`
	UptimeSec     int64         `json:"uptime_sec"`
	Healthy       bool          `json:"healthy"`
	JWT           jwtStatus     `json:"jwt"`
	ScoreView     scoreView     `json:"score_view"`
	Conn          connStats     `json:"conn"`
	CacheHitRate  cacheHitRates `json:"cache_hit_rate"`
	TTFBP95Ms     int64         `json:"ttfb_p95_ms"`
	RelayBytes24h int64         `json:"relay_bytes_24h"`
}

// capabilityNames mirrors the CP admin naming order
// (edge, l4_backhaul, relay_provider, peer_icp).
func capabilityNames(c types.NodeCapabilities) []string {
	out := []string{}
	if c.Edge {
		out = append(out, "edge")
	}
	if c.L4Backhaul {
		out = append(out, "l4_backhaul")
	}
	if c.RelayProvider {
		out = append(out, "relay_provider")
	}
	if c.PeerICP {
		out = append(out, "peer_icp")
	}
	return out
}

// RegisterStatusRoutes mounts GET /v1/status on srv. Per orchestrator
// decision D1 it does NOT edit main.go — todo 49 consolidates all node-admin
// route mounts and constructs StatusDeps from the live components.
func RegisterStatusRoutes(srv *Server, deps StatusDeps) {
	now := deps.Now
	if now == nil {
		now = time.Now
	}

	srv.Handle("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		resp := statusResponse{
			PeerID:       deps.PeerID,
			Capabilities: capabilityNames(deps.Capabilities),
			Mode:         "edge",
			Region:       deps.Region,
			Version:      deps.Version,
			UptimeSec:    int64(now().Sub(deps.StartedAt).Seconds()),
			CacheHitRate: cacheHitRates{Prefix: 0}, // prefix 计数点未接入，v1 恒 0
			// relay_bytes_24h: 计数点未接入，v1 恒 0（edge_relay_bytes_total
			// 无生产侧调用点，prometheus Counter 亦无法回读）。
			RelayBytes24h: 0,
		}
		if deps.L4Mode {
			resp.Mode = "l4"
		}

		if deps.JWTClient != nil {
			resp.Healthy = !deps.JWTClient.IsDegraded()
			resp.JWT.RefreshBefore = int64(deps.RefreshBefore.Seconds())
			lastAt, lastOK, fails := deps.JWTClient.RefreshStats()
			if !lastAt.IsZero() {
				resp.JWT.LastRefreshAt = &lastAt
			}
			resp.JWT.LastRefreshOK = lastOK
			resp.JWT.RefreshFailCount24h = fails
			if current := deps.JWTClient.CurrentJWT(); current != "" {
				if payload, err := sjwt.VerifyJWTAnyPeerID(current, deps.ControlPlanePubKey); err == nil {
					exp := payload.Exp
					resp.JWT.Exp = &exp
				}
			}
		}

		if deps.Scorer != nil {
			resp.ScoreView.GraylistedPeers = deps.Scorer.GraylistedCount()
		}

		if deps.Network != nil {
			resp.Conn.Total, resp.Conn.Inbound, resp.Conn.Outbound = deps.Network.ConnCounts()
		}

		if deps.Backhaul != nil {
			resp.CacheHitRate.Warm = deps.Backhaul.WarmCacheHitRate()
			resp.TTFBP95Ms = deps.Backhaul.TTFBP95Ms()
		}

		WriteJSON(w, http.StatusOK, resp)
	})
}
