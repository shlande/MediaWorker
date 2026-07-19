package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── GET /v1/admin/overview (ui-admin-apis todo 52) ─────────────────────────
//
// These tests LOCK the partial-failure contract:
//   - a failing source degrades its own fields to null and sets partial=true;
//   - an absent source (Prom disabled, empty account_health) yields null
//     fields WITHOUT partial;
//   - no single-source failure ever produces a 500;
//   - no graylist/score/community fields exist in the payload.

var overviewTestSecret = []byte("overview-test-secret")

// ─── Fakes ──────────────────────────────────────────────────────────────────

type fakeOverviewProm struct {
	ttfb    float64
	ttfbOK  bool
	hit     float64
	hitOK   bool
	used    float64
	usedOK  bool
	scalars map[string]struct {
		v  float64
		ok bool
	}
	err error // when non-nil, every query fails (Prom down)
}

func (f *fakeOverviewProm) TTFBP95(context.Context) (float64, bool, error) {
	if f.err != nil {
		return 0, false, f.err
	}
	return f.ttfb, f.ttfbOK, nil
}

func (f *fakeOverviewProm) CacheHitRate(context.Context) (float64, bool, error) {
	if f.err != nil {
		return 0, false, f.err
	}
	return f.hit, f.hitOK, nil
}

func (f *fakeOverviewProm) BackhaulBandwidthBps(context.Context) (float64, bool, error) {
	if f.err != nil {
		return 0, false, f.err
	}
	return f.used, f.usedOK, nil
}

func (f *fakeOverviewProm) QueryScalar(_ context.Context, promQL string) (float64, bool, error) {
	if f.err != nil {
		return 0, false, f.err
	}
	if s, ok := f.scalars[promQL]; ok {
		return s.v, s.ok, nil
	}
	return 0, false, nil
}

type fakeOverviewMetadata struct {
	healthRate float64
	healthOK   bool
	healthErr  error

	contents    []metadata.AdminContentRow
	contentsErr error

	alerts    []metadata.AlertEventRow
	alertsErr error
}

func (f *fakeOverviewMetadata) AccountHealthRate(context.Context) (float64, bool, error) {
	return f.healthRate, f.healthOK, f.healthErr
}

func (f *fakeOverviewMetadata) ListContents(context.Context, metadata.ListContentsQuery) ([]metadata.AdminContentRow, int, error) {
	if f.contentsErr != nil {
		return nil, 0, f.contentsErr
	}
	return f.contents, len(f.contents), nil
}

func (f *fakeOverviewMetadata) ListAlertEvents(_ context.Context, status string, limit int) ([]metadata.AlertEventRow, error) {
	if f.alertsErr != nil {
		return nil, f.alertsErr
	}
	if status != "firing" {
		return nil, errors.New("unexpected status filter")
	}
	out := f.alerts
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type fakeOverviewNodes struct {
	views []noderegistry.NodeView
}

func (f *fakeOverviewNodes) Snapshot() []noderegistry.NodeView { return f.views }

type fakeOverviewDispatch struct {
	pinCounts map[string]int
	batches   int
	pins      int
	unpins    int
	manual    int
}

func (f *fakeOverviewDispatch) CountByContent() map[string]int { return f.pinCounts }

func (f *fakeOverviewDispatch) Stats1h(time.Time) (int, int, int, int) {
	return f.batches, f.pins, f.unpins, f.manual
}

// ─── Helpers ────────────────────────────────────────────────────────────────

var overviewNow = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

func scalarPair(v float64, ok bool) struct {
	v  float64
	ok bool
} {
	return struct {
		v  float64
		ok bool
	}{v: v, ok: ok}
}

// happyDeps returns a fully-populated dependency set: every source healthy.
func happyDeps() OverviewDeps {
	return OverviewDeps{
		Prom: &fakeOverviewProm{
			ttfb:   0.42,
			ttfbOK: true,
			hit:    0.87,
			hitOK:  true,
			used:   12_000_000,
			usedOK: true,
			scalars: map[string]struct {
				v  float64
				ok bool
			}{
				overviewBackhaulSuccessRateQuery: scalarPair(0.995, true),
				overviewBackhaulCapacityQuery:    scalarPair(80_000_000, true),
			},
		},
		Metadata: &fakeOverviewMetadata{
			healthRate: 0.75,
			healthOK:   true,
			contents: []metadata.AdminContentRow{
				{ContentID: "cid-hot-1", Title: "Hot One", ContentType: "dash", ReplicasHave: 2, Window24h: 900},
				{ContentID: "cid-hot-2", Title: "Hot Two", ContentType: "image", ReplicasHave: 1, Window24h: 300},
			},
			alerts: []metadata.AlertEventRow{
				{Name: "HighTTFB", Severity: strPtr("warning"), Target: strPtr("edge-1"), Status: "firing"},
				{Name: "BackhaulStall", Severity: strPtr("critical"), Target: strPtr("l4-2"), Status: "firing"},
			},
		},
		Registry: &fakeOverviewNodes{views: []noderegistry.NodeView{
			{ // online, sufficient (>20 GiB free), edge+icp, non-L4
				PeerID:       "peer-a",
				Capabilities: types.NodeCapabilities{Edge: true, PeerICP: true},
				PrefixSpace:  types.PartitionStatus{TotalBytes: 100 << 30, UsedBytes: 10 << 30},
				ReceivedAt:   overviewNow.Add(-10 * time.Second),
			},
			{ // online, tight (5-20 GiB free), L4
				PeerID:       "peer-b",
				Capabilities: types.NodeCapabilities{Edge: true, L4Backhaul: true, RelayProvider: true},
				PrefixSpace:  types.PartitionStatus{TotalBytes: 20 << 30, UsedBytes: 10 << 30},
				ReceivedAt:   overviewNow.Add(-30 * time.Second),
			},
			{ // offline (stale >60s), exhausted (<5 GiB free), non-L4
				PeerID:       "peer-c",
				Capabilities: types.NodeCapabilities{Edge: true, PeerICP: true},
				PrefixSpace:  types.PartitionStatus{TotalBytes: 5 << 30, UsedBytes: 4 << 30},
				ReceivedAt:   overviewNow.Add(-120 * time.Second),
			},
		}},
		Dispatch: &fakeOverviewDispatch{
			pinCounts: map[string]int{"cid-hot-1": 3, "cid-hot-2": 1},
			batches:   7,
			pins:      42,
			unpins:    5,
			manual:    2,
		},
		Now: func() time.Time { return overviewNow },
	}
}

func serveOverview(t *testing.T, deps OverviewDeps, auth bool) *httptest.ResponseRecorder {
	t.Helper()
	srv := NewServer(overviewTestSecret)
	RegisterOverviewRoutes(srv, deps)
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/overview", nil)
	if auth {
		req.Header.Set("Authorization", "Bearer "+signedToken(t, overviewTestSecret, []string{"admin"}))
	}
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	return rec
}

func decodeOverview(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v\nbody: %s", err, rec.Body.String())
	}
	return body
}

func floatField(t *testing.T, m map[string]any, path ...string) float64 {
	t.Helper()
	cur := m
	for i, p := range path {
		v, ok := cur[p]
		if !ok {
			t.Fatalf("missing field %q at %v", p, path[:i+1])
		}
		if i == len(path)-1 {
			f, ok := v.(float64)
			if !ok {
				t.Fatalf("field %v is %T, want number", path, v)
			}
			return f
		}
		cur, ok = v.(map[string]any)
		if !ok {
			t.Fatalf("field %q at %v is %T, want object", p, path[:i+1], v)
		}
	}
	return 0
}

// ─── Tests ──────────────────────────────────────────────────────────────────

// TestOverview_AllSourcesAggregated is the happy path: five healthy sources
// produce a full payload, partial=false, and no forbidden fields appear.
func TestOverview_AllSourcesAggregated(t *testing.T) {
	rec := serveOverview(t, happyDeps(), true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
	body := decodeOverview(t, rec)

	// SLO four cards.
	if got := floatField(t, body, "slo", "ttfb_p95"); got != 0.42 {
		t.Fatalf("ttfb_p95 = %v", got)
	}
	if got := floatField(t, body, "slo", "cache_hit_rate"); got != 0.87 {
		t.Fatalf("cache_hit_rate = %v", got)
	}
	if got := floatField(t, body, "slo", "backhaul_success_rate"); got != 0.995 {
		t.Fatalf("backhaul_success_rate = %v", got)
	}
	if got := floatField(t, body, "slo", "account_health_rate"); got != 0.75 {
		t.Fatalf("account_health_rate = %v", got)
	}

	// Nodes block.
	if got := floatField(t, body, "nodes", "total"); got != 3 {
		t.Fatalf("nodes.total = %v", got)
	}
	if got := floatField(t, body, "nodes", "online"); got != 2 {
		t.Fatalf("nodes.online = %v", got)
	}
	if got := floatField(t, body, "nodes", "by_capability", "edge"); got != 3 {
		t.Fatalf("by_capability.edge = %v", got)
	}
	if got := floatField(t, body, "nodes", "by_capability", "l4_backhaul"); got != 1 {
		t.Fatalf("by_capability.l4_backhaul = %v", got)
	}
	if got := floatField(t, body, "nodes", "by_capability", "relay_provider"); got != 1 {
		t.Fatalf("by_capability.relay_provider = %v", got)
	}
	if got := floatField(t, body, "nodes", "by_capability", "peer_icp"); got != 2 {
		t.Fatalf("by_capability.peer_icp = %v", got)
	}
	if got := floatField(t, body, "nodes", "non_l4"); got != 2 {
		t.Fatalf("nodes.non_l4 = %v", got)
	}
	if got := floatField(t, body, "nodes", "space_buckets", "sufficient"); got != 1 {
		t.Fatalf("space_buckets.sufficient = %v", got)
	}
	if got := floatField(t, body, "nodes", "space_buckets", "tight"); got != 1 {
		t.Fatalf("space_buckets.tight = %v", got)
	}
	if got := floatField(t, body, "nodes", "space_buckets", "exhausted"); got != 1 {
		t.Fatalf("space_buckets.exhausted = %v", got)
	}

	// Hot contents merged with pin counts and replicas.
	hot, ok := body["hot_contents"].([]any)
	if !ok || len(hot) != 2 {
		t.Fatalf("hot_contents = %v", body["hot_contents"])
	}
	first := hot[0].(map[string]any)
	if first["content_id"] != "cid-hot-1" || first["title"] != "Hot One" || first["content_type"] != "dash" {
		t.Fatalf("hot_contents[0] = %v", first)
	}
	if got := floatField(t, first, "window_24h"); got != 900 {
		t.Fatalf("hot_contents[0].window_24h = %v", got)
	}
	if got := floatField(t, first, "pin_node_count"); got != 3 {
		t.Fatalf("hot_contents[0].pin_node_count = %v", got)
	}
	if got := floatField(t, first, "replicas", "have"); got != 2 {
		t.Fatalf("hot_contents[0].replicas.have = %v", got)
	}
	if got := floatField(t, first, "replicas", "want"); got != ReplicasWant {
		t.Fatalf("hot_contents[0].replicas.want = %v", got)
	}

	// Alerts.
	alerts, ok := body["alerts"].([]any)
	if !ok || len(alerts) != 2 {
		t.Fatalf("alerts = %v", body["alerts"])
	}
	if alerts[0].(map[string]any)["name"] != "HighTTFB" {
		t.Fatalf("alerts[0] = %v", alerts[0])
	}

	// Pin stats.
	if got := floatField(t, body, "pin_stats_1h", "batches"); got != 7 {
		t.Fatalf("pin_stats_1h.batches = %v", got)
	}
	if got := floatField(t, body, "pin_stats_1h", "pins"); got != 42 {
		t.Fatalf("pin_stats_1h.pins = %v", got)
	}
	if got := floatField(t, body, "pin_stats_1h", "unpins"); got != 5 {
		t.Fatalf("pin_stats_1h.unpins = %v", got)
	}
	if got := floatField(t, body, "pin_stats_1h", "manual"); got != 2 {
		t.Fatalf("pin_stats_1h.manual = %v", got)
	}

	// Backhaul.
	if got := floatField(t, body, "backhaul", "used_bps"); got != 12_000_000 {
		t.Fatalf("backhaul.used_bps = %v", got)
	}
	if got := floatField(t, body, "backhaul", "capacity_bps"); got != 80_000_000 {
		t.Fatalf("backhaul.capacity_bps = %v", got)
	}

	if body["partial"] != false {
		t.Fatalf("partial = %v, want false", body["partial"])
	}

	// Forbidden fields (ui-adjustments §2): no graylist/score/community.
	raw := rec.Body.String()
	for _, banned := range []string{"graylist", "score", "community"} {
		if strings.Contains(raw, banned) {
			t.Fatalf("body contains forbidden field %q: %s", banned, raw)
		}
	}
}

// TestOverview_PromDownDegrades locks the key acceptance case: Prometheus
// unreachable → the three Prom SLO cards AND both backhaul fields go null,
// every other source still renders, status stays 200, partial=true.
func TestOverview_PromDownDegrades(t *testing.T) {
	deps := happyDeps()
	deps.Prom = &fakeOverviewProm{err: errors.New("connection refused")}

	rec := serveOverview(t, deps, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
	body := decodeOverview(t, rec)

	slo := body["slo"].(map[string]any)
	for _, f := range []string{"ttfb_p95", "cache_hit_rate", "backhaul_success_rate"} {
		if slo[f] != nil {
			t.Fatalf("slo.%s = %v, want null", f, slo[f])
		}
	}
	// PG-sourced card survives Prom failure.
	if got := floatField(t, body, "slo", "account_health_rate"); got != 0.75 {
		t.Fatalf("account_health_rate = %v", got)
	}
	backhaul := body["backhaul"].(map[string]any)
	if backhaul["used_bps"] != nil || backhaul["capacity_bps"] != nil {
		t.Fatalf("backhaul = %v, want both null", backhaul)
	}
	// Everything else intact.
	if got := floatField(t, body, "nodes", "total"); got != 3 {
		t.Fatalf("nodes.total = %v", got)
	}
	if hot, ok := body["hot_contents"].([]any); !ok || len(hot) != 2 {
		t.Fatalf("hot_contents = %v", body["hot_contents"])
	}
	if body["partial"] != true {
		t.Fatalf("partial = %v, want true", body["partial"])
	}
}

// TestOverview_PromDisabledIsAbsenceNotFailure locks the distinction: a
// disabled Prometheus (all queries ok=false, no error) nulls the Prom fields
// WITHOUT setting partial.
func TestOverview_PromDisabledIsAbsenceNotFailure(t *testing.T) {
	deps := happyDeps()
	deps.Prom = &fakeOverviewProm{} // everything ok=false, err=nil

	rec := serveOverview(t, deps, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := decodeOverview(t, rec)
	slo := body["slo"].(map[string]any)
	if slo["ttfb_p95"] != nil {
		t.Fatalf("ttfb_p95 = %v, want null", slo["ttfb_p95"])
	}
	if body["partial"] != false {
		t.Fatalf("partial = %v, want false (absence is not failure)", body["partial"])
	}
}

// TestOverview_HealthRateDivisionByZero locks the empty-table case:
// AccountHealthRate ok=false → account_health_rate null, NOT an error,
// partial stays false.
func TestOverview_HealthRateDivisionByZero(t *testing.T) {
	deps := happyDeps()
	deps.Metadata = &fakeOverviewMetadata{
		healthOK: false, // NULL aggregate
		contents: deps.Metadata.(*fakeOverviewMetadata).contents,
		alerts:   deps.Metadata.(*fakeOverviewMetadata).alerts,
	}

	rec := serveOverview(t, deps, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := decodeOverview(t, rec)
	if body["slo"].(map[string]any)["account_health_rate"] != nil {
		t.Fatalf("account_health_rate = %v, want null", body["slo"])
	}
	if body["partial"] != false {
		t.Fatalf("partial = %v, want false", body["partial"])
	}
}

// TestOverview_PGErrorDegradesHealthRate: PG failure on the health aggregate
// → field null + partial=true, the rest of the page intact.
func TestOverview_PGErrorDegradesHealthRate(t *testing.T) {
	deps := happyDeps()
	md := deps.Metadata.(*fakeOverviewMetadata)
	md.healthErr = errors.New("pg: connection reset")

	rec := serveOverview(t, deps, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := decodeOverview(t, rec)
	if body["slo"].(map[string]any)["account_health_rate"] != nil {
		t.Fatalf("account_health_rate = %v, want null", body["slo"])
	}
	if got := floatField(t, body, "slo", "ttfb_p95"); got != 0.42 {
		t.Fatalf("ttfb_p95 = %v", got)
	}
	if body["partial"] != true {
		t.Fatalf("partial = %v, want true", body["partial"])
	}
}

// TestOverview_PGErrorDegradesHotContents: the contents query failing
// degrades hot_contents to null (not []) without touching alerts or SLO.
func TestOverview_PGErrorDegradesHotContents(t *testing.T) {
	deps := happyDeps()
	md := deps.Metadata.(*fakeOverviewMetadata)
	md.contentsErr = errors.New("pg: deadlock detected")

	rec := serveOverview(t, deps, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := decodeOverview(t, rec)
	if body["hot_contents"] != nil {
		t.Fatalf("hot_contents = %v, want null", body["hot_contents"])
	}
	if alerts, ok := body["alerts"].([]any); !ok || len(alerts) != 2 {
		t.Fatalf("alerts = %v", body["alerts"])
	}
	if body["partial"] != true {
		t.Fatalf("partial = %v, want true", body["partial"])
	}
}

// TestOverview_PGErrorDegradesAlerts: the alerts query failing degrades
// alerts to null while hot_contents stays populated.
func TestOverview_PGErrorDegradesAlerts(t *testing.T) {
	deps := happyDeps()
	md := deps.Metadata.(*fakeOverviewMetadata)
	md.alertsErr = errors.New("pg: relation does not exist")

	rec := serveOverview(t, deps, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := decodeOverview(t, rec)
	if body["alerts"] != nil {
		t.Fatalf("alerts = %v, want null", body["alerts"])
	}
	if hot, ok := body["hot_contents"].([]any); !ok || len(hot) != 2 {
		t.Fatalf("hot_contents = %v", body["hot_contents"])
	}
	if body["partial"] != true {
		t.Fatalf("partial = %v, want true", body["partial"])
	}
}

// TestOverview_NoTokenUnauthorized: auth=true route — no bearer token → 401.
func TestOverview_NoTokenUnauthorized(t *testing.T) {
	rec := serveOverview(t, happyDeps(), false)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}
