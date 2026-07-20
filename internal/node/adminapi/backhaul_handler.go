package adminapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/types"
)

// BackhaulStatsReader is the narrow subset of *backhaul.BackhaulManager used
// by the backhaul handler (todo 25 stats). Tests satisfy it with a mock.
type BackhaulStatsReader interface {
	Stats24h(now time.Time) (successRate float64, p95Ms int64, totalBytes int64)
	BackhaulUtilization() float64 // bytes/sec averaged over the trailing 60s
}

// LinkpoolReader is the narrow subset of *linkpool.LinkPool used by the
// backhaul handler.
type LinkpoolReader interface {
	Len() int
	HitRate() float64
}

// AccountSnapshotter is the narrow subset of *accountpool.AccountPool used by
// the backhaul handler.
type AccountSnapshotter interface {
	SnapshotAccounts() []*accountpool.Account
}

// BackhaulDeps carries every dependency of GET /v1/backhaul so the route
// registration stays a two-argument call. L4Enabled mirrors
// cfg.Access.DataPlane.Enabled; BackhaulCapacityMbps mirrors
// cfg.Access.DataPlane.BackhaulCapacityMbps (0 = capacity unknown → null).
// Stats, Linkpool and Pool may be nil on a half-wired L4 node — the handler
// degrades to zero values / an empty account list instead of panicking.
type BackhaulDeps struct {
	L4Enabled            bool
	BackhaulCapacityMbps int
	Stats                BackhaulStatsReader
	Linkpool             LinkpoolReader
	Pool                 AccountSnapshotter
}

type backhaulBandwidth struct {
	UsedBps     float64 `json:"used_bps"`
	CapacityBps *int64  `json:"capacity_bps"`
}

type backhaulLinkpool struct {
	Entries int     `json:"entries"`
	HitRate float64 `json:"hit_rate"`
}

type backhaulQPS struct {
	Used  *float64 `json:"used"`
	Limit float64  `json:"limit"`
}

type backhaulAccount struct {
	BackendID string      `json:"backend_id"`
	Health    string      `json:"health"`
	Circuit   string      `json:"circuit"`
	QPS       backhaulQPS `json:"qps"`
	Inflight  int32       `json:"inflight"`
}

// backhaulResponse is the GET /v1/backhaul response body (L4 nodes only).
type backhaulResponse struct {
	Bandwidth      backhaulBandwidth `json:"bandwidth"`
	SuccessRate24h float64           `json:"success_rate_24h"`
	LatencyP95Ms   int64             `json:"latency_p95_ms"`
	Linkpool       *backhaulLinkpool `json:"linkpool"`
	Accounts       []backhaulAccount `json:"accounts"`
}

// RegisterBackhaulRoutes mounts GET /v1/backhaul on srv. Per orchestrator
// decision D1, this function does not edit main.go; todo 49 consolidates all
// node-admin route mounts.
func RegisterBackhaulRoutes(srv *Server, deps BackhaulDeps) {
	srv.Handle("GET /v1/backhaul", handleBackhaul(deps))
}

// handleBackhaul 返回 L4 回传链路状态（仅 L4 节点可用）。
//
//	@Summary		回传链路状态
//	@Description	返回带宽使用率、24h 成功率、P95 延迟、linkpool 命中率与网盘账号列表。非 L4 节点返回 409。
//	@Tags			node-admin
//	@Produce		json
//	@Success		200	{object}	backhaulResponse
//	@Failure		409	{object}	types.ErrorResponse
//	@Security		AdminToken
//	@Router			/v1/backhaul [get]
func handleBackhaul(deps BackhaulDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !deps.L4Enabled {
			WriteError(w, http.StatusConflict, "node is not L4; backhaul unavailable")
			return
		}

		var usedBps, successRate float64
		var p95Ms int64
		if deps.Stats != nil {
			// BackhaulUtilization returns bytes/sec; the contract is bits/sec.
			usedBps = deps.Stats.BackhaulUtilization() * 8
			successRate, p95Ms, _ = deps.Stats.Stats24h(time.Now())
		}

		var capacityBps *int64
		if deps.BackhaulCapacityMbps > 0 {
			c := int64(deps.BackhaulCapacityMbps) * 1_000_000
			capacityBps = &c
		}

		var lp *backhaulLinkpool
		if deps.Linkpool != nil {
			lp = &backhaulLinkpool{Entries: deps.Linkpool.Len(), HitRate: deps.Linkpool.HitRate()}
		}

		accounts := []backhaulAccount{}
		if deps.Pool != nil {
			for _, a := range deps.Pool.SnapshotAccounts() {
				accounts = append(accounts, mapAccount(a))
			}
		}

		WriteJSON(w, http.StatusOK, backhaulResponse{
			Bandwidth:      backhaulBandwidth{UsedBps: usedBps, CapacityBps: capacityBps},
			SuccessRate24h: successRate,
			LatencyP95Ms:   p95Ms,
			Linkpool:       lp,
			Accounts:       accounts,
		})
	}
}

func mapAccount(a *accountpool.Account) backhaulAccount {
	var qpsLimit float64
	if a.Driver != nil {
		qpsLimit = a.Driver.RateLimitConfig().QPS
	}
	return backhaulAccount{
		BackendID: fmt.Sprintf("%s:%s", a.Vendor, a.AccountID),
		Health:    healthStateString(a),
		Circuit:   circuitStateString(a.CB),
		QPS: backhaulQPS{
			// 令牌桶无回读，v1 不报 used — the Limiter interface exposes no
			// read-back of consumed tokens, so used stays null in v1.
			Used:  nil,
			Limit: qpsLimit,
		},
		Inflight: a.Concurrent.Load(),
	}
}

func healthStateString(a *accountpool.Account) string {
	if h, ok := a.Health.Load().(types.HealthState); ok {
		return h.State
	}
	return "unknown"
}

func circuitStateString(cb accountpool.CircuitBreaker) string {
	if cb == nil {
		return "closed" // no breaker wired = effectively closed
	}
	switch cb.State() {
	case accountpool.StateClosed:
		return "closed"
	case accountpool.StateHalfOpen:
		return "half_open"
	case accountpool.StateOpen:
		return "open"
	}
	return "unknown"
}
