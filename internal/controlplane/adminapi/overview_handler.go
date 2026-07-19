package adminapi

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/storage/metadata"
)

// ─── Dashboard overview aggregation (ui-admin-apis todo 52) ─────────────────
//
// GET /v1/admin/overview (auth=true) is the dashboard's single aggregation
// endpoint (docs/ui-api-requirements.md §3.1). It fans out to five sources
// concurrently (errgroup): Prometheus (ttfb_p95, cache_hit_rate,
// backhaul_success_rate, backhaul used/capacity), PG account_health (health
// rate), the node registry (in-memory), PG hot contents + dispatch-log
// pin counts, and PG alert_events + dispatch-log Stats1h.
//
// PARTIAL-FAILURE CONTRACT (locked in overview_handler_test.go): no single
// source failure may 500 the page. A failing source degrades its own fields
// to null (hot_contents / alerts degrade to null arrays) and flips the
// top-level "partial" marker to true. A source that is merely absent
// (Prometheus not configured, no account_health rows yet) yields null fields
// WITHOUT setting partial — absence is not a failure.
//
// Scope exclusions (docs/ui-adjustments.md §2): no graylist, no score, no
// community-node fields; the "community" card is served by nodes.non_l4 and
// "stale reporting" is covered by the nodes.online freshness window.

// ─── Narrow dependency interfaces (testable; todo 54 fake-assembles) ───────

// OverviewPromReader is the Prometheus surface the overview needs.
// *PromClient satisfies it directly. QueryScalar backs the two queries that
// have no pre-canned PromClient method (backhaul success rate, capacity).
type OverviewPromReader interface {
	TTFBP95(ctx context.Context) (float64, bool, error)
	CacheHitRate(ctx context.Context) (float64, bool, error)
	BackhaulBandwidthBps(ctx context.Context) (float64, bool, error)
	QueryScalar(ctx context.Context, promQL string) (float64, bool, error)
}

// OverviewMetadataReader is the PG surface for the overview.
// *metadata.PGMetadataClient satisfies it directly.
type OverviewMetadataReader interface {
	AccountHealthRate(ctx context.Context) (float64, bool, error)
	ListContents(ctx context.Context, q metadata.ListContentsQuery) ([]metadata.AdminContentRow, int, error)
	ListAlertEvents(ctx context.Context, status string, limit int) ([]metadata.AlertEventRow, error)
}

// OverviewNodeReader is the registry surface (in-memory, cannot fail).
// *noderegistry.Registry satisfies it directly.
type OverviewNodeReader interface {
	Snapshot() []noderegistry.NodeView
}

// OverviewDispatchReader is the dispatch-log bookkeeping surface (in-memory,
// cannot fail). *pinstrategy.DispatchLog satisfies it directly.
type OverviewDispatchReader interface {
	CountByContent() map[string]int
	Stats1h(now time.Time) (batches, pins, unpins, manual int)
}

// OverviewDeps bundles the five aggregation sources. Now is the clock seam
// for tests; nil means time.Now.
type OverviewDeps struct {
	Prom     OverviewPromReader
	Metadata OverviewMetadataReader
	Registry OverviewNodeReader
	Dispatch OverviewDispatchReader
	Now      func() time.Time
}

// ─── PromQL fragments ───────────────────────────────────────────────────────

// overviewBackhaulSuccessRateQuery computes the backhaul success share over
// 5m. NOTE: the storage_access_backhaul_{success,request}_total counters are
// NOT yet emitted by nodes (verified against internal/node/monitor/metrics.go
// at todo-52 time — only edge_backhaul_bytes_total/bandwidth/capacity exist).
// Until nodes export them the query yields an empty vector, which degrades
// backhaul_success_rate to null — the designed partial behavior, not a bug.
const overviewBackhaulSuccessRateQuery = `sum(rate(storage_access_backhaul_success_total[5m]))/sum(rate(storage_access_backhaul_request_total[5m]))`

// overviewBackhaulCapacityQuery mirrors PromClient.BackhaulBandwidthBps for
// the capacity gauge (edge_backhaul_capacity_bytes exists, bytes/sec -> bits).
const overviewBackhaulCapacityQuery = `sum(edge_backhaul_capacity_bytes)*8`

// ─── Tunables ───────────────────────────────────────────────────────────────

const (
	// overviewHotContentsLimit is the dashboard Top-N (ui-api-requirements
	// §3.1: 24h 热门 Top8).
	overviewHotContentsLimit = 8
	// overviewAlertsLimit caps the firing-alerts panel.
	overviewAlertsLimit = 10
	// Space-bucket thresholds on prefix_space free bytes (Total-Used).
	spaceBucketSufficientBytes = 20 << 30 // sufficient: free > 20 GiB
	spaceBucketTightBytes      = 5 << 30  // tight: 5–20 GiB; exhausted: < 5 GiB
)

// ─── Wire shapes ────────────────────────────────────────────────────────────

type overviewSLO struct {
	TTFBP95             *float64 `json:"ttfb_p95"`
	CacheHitRate        *float64 `json:"cache_hit_rate"`
	BackhaulSuccessRate *float64 `json:"backhaul_success_rate"`
	AccountHealthRate   *float64 `json:"account_health_rate"`
}

type overviewCapabilityCounts struct {
	Edge          int `json:"edge"`
	L4Backhaul    int `json:"l4_backhaul"`
	RelayProvider int `json:"relay_provider"`
	PeerICP       int `json:"peer_icp"`
}

type overviewSpaceBuckets struct {
	Sufficient int `json:"sufficient"`
	Tight      int `json:"tight"`
	Exhausted  int `json:"exhausted"`
}

type overviewNodes struct {
	Total        int                      `json:"total"`
	Online       int                      `json:"online"`
	ByCapability overviewCapabilityCounts `json:"by_capability"`
	NonL4        int                      `json:"non_l4"`
	SpaceBuckets overviewSpaceBuckets     `json:"space_buckets"`
}

type overviewHotContent struct {
	ContentID    string           `json:"content_id"`
	Title        string           `json:"title"`
	ContentType  string           `json:"content_type"`
	Window24h    int64            `json:"window_24h"`
	PinNodeCount int              `json:"pin_node_count"`
	Replicas     replicasResponse `json:"replicas"`
}

type overviewPinStats struct {
	Batches int `json:"batches"`
	Pins    int `json:"pins"`
	Unpins  int `json:"unpins"`
	Manual  int `json:"manual"`
}

type overviewBackhaul struct {
	UsedBPS     *float64 `json:"used_bps"`
	CapacityBPS *float64 `json:"capacity_bps"`
}

// overviewResponse is the top-level dashboard payload. Partial is the
// field-level degradation marker: true iff at least one source errored.
type overviewResponse struct {
	SLO         overviewSLO          `json:"slo"`
	Nodes       overviewNodes        `json:"nodes"`
	HotContents []overviewHotContent `json:"hot_contents"`
	Alerts      []alertItem          `json:"alerts"`
	PinStats1h  overviewPinStats     `json:"pin_stats_1h"`
	Backhaul    overviewBackhaul     `json:"backhaul"`
	Partial     bool                 `json:"partial"`
}

// ─── Route registration (D1-compliant: no main.go edit; todo 54 mounts) ────

// RegisterOverviewRoutes mounts GET /v1/admin/overview behind the bearer
// middleware. All deps must be non-nil; Prometheus degradation is expressed
// through PromClient semantics (disabled or erroring), not nil.
func RegisterOverviewRoutes(srv *Server, deps OverviewDeps) {
	srv.Handle("GET /v1/admin/overview", overviewHandler(deps), true)
}

// ─── Handler ────────────────────────────────────────────────────────────────

func overviewHandler(deps OverviewDeps) http.Handler {
	nowFn := deps.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var resp overviewResponse
		// Per-source failure flags: the PG-sourced flags are each written by
		// exactly one goroutine; promFailed is shared by five Prom goroutines
		// and therefore atomic. g.Wait() is the happens-before barrier before
		// the final OR.
		var promFailed atomic.Bool
		var healthFailed, hotFailed, alertsFailed bool

		var g errgroup.Group

		// Source 1: Prometheus SLO trio + backhaul bandwidth. Each query is
		// independent; one erring query must not null its siblings, so Prom
		// fetches are per-field goroutines sharing one failure flag. Prom
		// disabled (not configured) returns ok=false without error: null,
		// not partial.
		promFloat := func(dst **float64, fetch func(context.Context) (float64, bool, error)) func() error {
			return func() error {
				v, ok, err := fetch(ctx)
				if err != nil {
					promFailed.Store(true)
					return nil
				}
				if ok {
					*dst = &v
				}
				return nil
			}
		}
		g.Go(promFloat(&resp.SLO.TTFBP95, deps.Prom.TTFBP95))
		g.Go(promFloat(&resp.SLO.CacheHitRate, deps.Prom.CacheHitRate))
		g.Go(promFloat(&resp.SLO.BackhaulSuccessRate, func(ctx context.Context) (float64, bool, error) {
			return deps.Prom.QueryScalar(ctx, overviewBackhaulSuccessRateQuery)
		}))
		g.Go(promFloat(&resp.Backhaul.UsedBPS, deps.Prom.BackhaulBandwidthBps))
		g.Go(promFloat(&resp.Backhaul.CapacityBPS, func(ctx context.Context) (float64, bool, error) {
			return deps.Prom.QueryScalar(ctx, overviewBackhaulCapacityQuery)
		}))

		// Source 2: PG account_health aggregate. ok=false (empty table) is
		// absence, not failure: null without partial.
		g.Go(func() error {
			rate, ok, err := deps.Metadata.AccountHealthRate(ctx)
			if err != nil {
				healthFailed = true
				return nil
			}
			if ok {
				resp.SLO.AccountHealthRate = &rate
			}
			return nil
		})

		// Source 3: hot contents — PG popularity list (todo 14 query reused
		// with limit 8) merged with dispatch-log pin counts (todo 16) and
		// the ReplicasWant constant (todo 28 convention).
		g.Go(func() error {
			rows, _, err := deps.Metadata.ListContents(ctx, metadata.ListContentsQuery{
				Sort:     "popularity",
				Page:     1,
				PageSize: overviewHotContentsLimit,
			})
			if err != nil {
				hotFailed = true
				return nil
			}
			pinCounts := deps.Dispatch.CountByContent()
			out := make([]overviewHotContent, 0, len(rows))
			for _, row := range rows {
				out = append(out, overviewHotContent{
					ContentID:    row.ContentID,
					Title:        row.Title,
					ContentType:  row.ContentType,
					Window24h:    row.Window24h,
					PinNodeCount: pinCounts[row.ContentID],
					Replicas:     replicasResponse{Have: row.ReplicasHave, Want: ReplicasWant},
				})
			}
			resp.HotContents = out
			return nil
		})

		// Source 4: firing alerts (todo 51 store, capped at 10).
		g.Go(func() error {
			rows, err := deps.Metadata.ListAlertEvents(ctx, "firing", overviewAlertsLimit)
			if err != nil {
				alertsFailed = true
				return nil
			}
			items := make([]alertItem, 0, len(rows))
			for _, row := range rows {
				items = append(items, alertItem{
					Name:     row.Name,
					Severity: deref(row.Severity),
					Target:   deref(row.Target),
					Since:    row.Since,
					Detail:   row.Detail,
				})
			}
			resp.Alerts = items
			return nil
		})

		// Every goroutine returns nil by contract (degradation is flag-based,
		// never error-based), so Wait cannot fail.
		_ = g.Wait()

		// Source 5: in-memory views (registry + dispatch log) — cannot fail,
		// computed after the fan-out so now is sampled once per request.
		now := nowFn()
		resp.Nodes = aggregateNodes(deps.Registry.Snapshot(), now)
		batches, pins, unpins, manual := deps.Dispatch.Stats1h(now)
		resp.PinStats1h = overviewPinStats{Batches: batches, Pins: pins, Unpins: unpins, Manual: manual}

		resp.Partial = promFailed.Load() || healthFailed || hotFailed || alertsFailed
		WriteJSON(w, http.StatusOK, resp)
	})
}

// aggregateNodes folds the registry snapshot into the nodes block: totals,
// the 2×30s online freshness window (nodeOnlineMaxAge, shared with the
// quota handler), capability counts, the non-L4 count (ui-adjustments §2
// replacement for the removed community card), and prefix-space buckets.
func aggregateNodes(views []noderegistry.NodeView, now time.Time) overviewNodes {
	out := overviewNodes{Total: len(views)}
	cutoff := now.Add(-nodeOnlineMaxAge)
	for _, v := range views {
		if !v.ReceivedAt.Before(cutoff) {
			out.Online++
		}
		caps := v.Capabilities
		if caps.Edge {
			out.ByCapability.Edge++
		}
		if caps.L4Backhaul {
			out.ByCapability.L4Backhaul++
		}
		if caps.RelayProvider {
			out.ByCapability.RelayProvider++
		}
		if caps.PeerICP {
			out.ByCapability.PeerICP++
		}
		if !caps.L4Backhaul {
			out.NonL4++
		}
		free := v.PrefixSpace.TotalBytes - v.PrefixSpace.UsedBytes
		switch {
		case free > spaceBucketSufficientBytes:
			out.SpaceBuckets.Sufficient++
		case free >= spaceBucketTightBytes:
			out.SpaceBuckets.Tight++
		default:
			out.SpaceBuckets.Exhausted++
		}
	}
	return out
}
