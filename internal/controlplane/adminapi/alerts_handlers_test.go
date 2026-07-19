package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/storage/metadata"
)

// ---------------------------------------------------------------------------
// Fake store: faithfully simulates the (fingerprint, since) upsert semantics
// of PGMetadataClient.InsertAlertEvent so resend dedup is observable.
// ---------------------------------------------------------------------------

type fakeAlertStore struct {
	mu         sync.Mutex
	rows       map[string]metadata.AlertEventRow
	order      []string
	refreshes  int
	insertErr  error
	lastStatus string
	lastLimit  int
}

func newFakeAlertStore() *fakeAlertStore {
	return &fakeAlertStore{rows: make(map[string]metadata.AlertEventRow)}
}

func alertKeyOf(r metadata.AlertEventRow) string {
	k := r.Fingerprint + "|"
	if r.Since != nil {
		k += r.Since.UTC().Format(time.RFC3339Nano)
	}
	return k
}

func (f *fakeAlertStore) InsertAlertEvent(_ context.Context, row metadata.AlertEventRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertErr != nil {
		return f.insertErr
	}
	k := alertKeyOf(row)
	if existing, ok := f.rows[k]; ok {
		// ON CONFLICT (fingerprint, since) DO UPDATE SET status, received_at.
		existing.Status = row.Status
		existing.ReceivedAt = time.Now()
		f.rows[k] = existing
		f.refreshes++
		return nil
	}
	row.ReceivedAt = time.Now()
	f.rows[k] = row
	f.order = append(f.order, k)
	return nil
}

func (f *fakeAlertStore) ListAlertEvents(_ context.Context, status string, limit int) ([]metadata.AlertEventRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastStatus, f.lastLimit = status, limit
	out := make([]metadata.AlertEventRow, 0, len(f.rows))
	for _, k := range f.order {
		r := f.rows[k]
		if status == "" || r.Status == status {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReceivedAt.After(out[j].ReceivedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeAlertStore) rowCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var alertsTestSecret = []byte("alerts-test-secret")

func newAlertsServer(t *testing.T, store AlertEventStore, webhookToken string) *Server {
	t.Helper()
	srv := NewServer(alertsTestSecret)
	RegisterAlertsRoutes(srv, store, webhookToken)
	return srv
}

func postAlertsWebhook(t *testing.T, srv *Server, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/alerts/webhook", strings.NewReader(body))
	if token != "" {
		req.Header.Set("X-Alert-Token", token)
	}
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	return rec
}

func getAlerts(t *testing.T, srv *Server, query, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/alerts"+query, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	return rec
}

const threeAlertPayload = `{
  "status": "firing",
  "alerts": [
    {"status": "firing", "fingerprint": "fp-1", "startsAt": "2026-07-20T08:00:00Z",
     "labels": {"alertname": "HighTTFB", "severity": "warning", "instance": "edge-1", "peer_id": "peer-ignored"},
     "annotations": {"summary": "ttfb p95 above slo"}},
    {"status": "firing", "fingerprint": "fp-2", "startsAt": "2026-07-20T08:01:00Z",
     "labels": {"alertname": "CacheFillStall", "severity": "critical", "peer_id": "peer-9"},
     "annotations": {"summary": "fill stalled"}},
    {"status": "firing", "fingerprint": "fp-3", "startsAt": "2026-07-20T08:02:00Z",
     "labels": {"alertname": "NodeOffline"},
     "annotations": {}}
  ]
}`

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestAlertsWebhookThenListThreeAlerts posts a 3-alert webhook and verifies
// the GET endpoint returns all 3 with the §3.1 wire shape (name, severity,
// target, since, detail), including target preferring instance over peer_id.
func TestAlertsWebhookThenListThreeAlerts(t *testing.T) {
	store := newFakeAlertStore()
	srv := newAlertsServer(t, store, "hook-token")

	rec := postAlertsWebhook(t, srv, "hook-token", threeAlertPayload)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("webhook status = %d, want 202 (body %s)", rec.Code, rec.Body)
	}
	var ack struct {
		Received int `json:"received"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Received != 3 {
		t.Errorf("received = %d, want 3", ack.Received)
	}

	token := signedToken(t, alertsTestSecret, []string{"admin"})
	rec = getAlerts(t, srv, "?status=firing", token)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200 (body %s)", rec.Code, rec.Body)
	}
	var items []alertItem
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
		t.Fatalf("decode alerts: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("len(items) = %d, want 3", len(items))
	}

	byName := make(map[string]alertItem, len(items))
	for _, it := range items {
		byName[it.Name] = it
	}

	high := byName["HighTTFB"]
	if high.Severity != "warning" {
		t.Errorf("HighTTFB severity = %q, want warning", high.Severity)
	}
	if high.Target != "edge-1" {
		t.Errorf("HighTTFB target = %q, want edge-1 (instance preferred over peer_id)", high.Target)
	}
	if high.Since == nil || !high.Since.Equal(time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)) {
		t.Errorf("HighTTFB since = %v, want 2026-07-20T08:00:00Z", high.Since)
	}
	var detail struct {
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	}
	if err := json.Unmarshal(high.Detail, &detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail.Labels["alertname"] != "HighTTFB" || detail.Annotations["summary"] != "ttfb p95 above slo" {
		t.Errorf("detail = %+v, want labels+annotations carried through", detail)
	}

	stall := byName["CacheFillStall"]
	if stall.Target != "peer-9" {
		t.Errorf("CacheFillStall target = %q, want peer-9 (peer_id fallback)", stall.Target)
	}

	offline := byName["NodeOffline"]
	if offline.Severity != "" || offline.Target != "" {
		t.Errorf("NodeOffline severity/target = %q/%q, want empty for absent labels", offline.Severity, offline.Target)
	}
}

// TestAlertsWebhookRepeatResendDeduped delivers the same fingerprint+startsAt
// twice (Alertmanager repeat_interval) and verifies the store holds exactly 1
// row whose status/received_at were refreshed by the second delivery.
func TestAlertsWebhookRepeatResendDeduped(t *testing.T) {
	store := newFakeAlertStore()
	srv := newAlertsServer(t, store, "hook-token")

	payload := `{"status":"firing","alerts":[{"status":"firing","fingerprint":"fp-dup","startsAt":"2026-07-20T08:00:00Z","labels":{"alertname":"HighTTFB","severity":"warning"},"annotations":{}}]}`
	for i := 0; i < 2; i++ {
		rec := postAlertsWebhook(t, srv, "hook-token", payload)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("delivery %d: status = %d, want 202", i+1, rec.Code)
		}
	}

	if got := store.rowCount(); got != 1 {
		t.Errorf("stored rows = %d, want exactly 1 after resend", got)
	}
	if store.refreshes != 1 {
		t.Errorf("refreshes = %d, want 1 (second delivery hit the upsert path)", store.refreshes)
	}

	token := signedToken(t, alertsTestSecret, []string{"admin"})
	rec := getAlerts(t, srv, "?status=firing", token)
	var items []alertItem
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
		t.Fatalf("decode alerts: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("GET returned %d alerts, want 1", len(items))
	}
}

// TestAlertsWebhookFingerprintFallback verifies an alert without Alertmanager's
// native fingerprint degrades to a deterministic alertname+startsAt hash, so
// resends of the same payload still dedup.
func TestAlertsWebhookFingerprintFallback(t *testing.T) {
	store := newFakeAlertStore()
	srv := newAlertsServer(t, store, "hook-token")

	payload := `{"status":"firing","alerts":[{"status":"firing","startsAt":"2026-07-20T08:00:00Z","labels":{"alertname":"NoFp"},"annotations":{}}]}`
	for i := 0; i < 2; i++ {
		rec := postAlertsWebhook(t, srv, "hook-token", payload)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("delivery %d: status = %d, want 202", i+1, rec.Code)
		}
	}
	if got := store.rowCount(); got != 1 {
		t.Fatalf("stored rows = %d, want 1 (fallback fingerprint must be deterministic)", got)
	}
	for _, row := range store.rows {
		if len(row.Fingerprint) != 32 {
			t.Errorf("fallback fingerprint = %q, want 32 hex chars", row.Fingerprint)
		}
	}
}

// TestAlertsWebhookBadToken verifies wrong and missing X-Alert-Token headers
// both get 401 and nothing is persisted.
func TestAlertsWebhookBadToken(t *testing.T) {
	store := newFakeAlertStore()
	srv := newAlertsServer(t, store, "hook-token")

	rec := postAlertsWebhook(t, srv, "wrong-token", threeAlertPayload)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: status = %d, want 401", rec.Code)
	}
	rec = postAlertsWebhook(t, srv, "", threeAlertPayload)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("missing token: status = %d, want 401", rec.Code)
	}
	if got := store.rowCount(); got != 0 {
		t.Errorf("stored rows = %d, want 0 after rejected deliveries", got)
	}
}

// TestAlertsWebhookUnconfiguredNotMounted verifies that an empty configured
// token means the webhook route is never registered (plain mux 404), while
// the bearer-authed GET endpoint stays mounted.
func TestAlertsWebhookUnconfiguredNotMounted(t *testing.T) {
	store := newFakeAlertStore()
	srv := newAlertsServer(t, store, "")

	rec := postAlertsWebhook(t, srv, "anything", threeAlertPayload)
	if rec.Code != http.StatusNotFound {
		t.Errorf("webhook with unconfigured token: status = %d, want 404 (not mounted)", rec.Code)
	}

	token := signedToken(t, alertsTestSecret, []string{"admin"})
	rec = getAlerts(t, srv, "", token)
	if rec.Code != http.StatusOK {
		t.Errorf("GET with unconfigured webhook token: status = %d, want 200 (GET always mounted)", rec.Code)
	}
}

// TestAlertsWebhookMalformedPayload covers the 400 paths: unparseable JSON
// and a body missing the "alerts" field. An explicit empty list is accepted.
func TestAlertsWebhookMalformedPayload(t *testing.T) {
	store := newFakeAlertStore()
	srv := newAlertsServer(t, store, "hook-token")

	rec := postAlertsWebhook(t, srv, "hook-token", `{`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("invalid JSON: status = %d, want 400", rec.Code)
	}

	rec = postAlertsWebhook(t, srv, "hook-token", `{"status":"firing"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("missing alerts field: status = %d, want 400", rec.Code)
	}

	rec = postAlertsWebhook(t, srv, "hook-token", `{"status":"firing","alerts":[]}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("empty alerts list: status = %d, want 202", rec.Code)
	}
	var ack struct {
		Received int `json:"received"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Received != 0 {
		t.Errorf("received = %d, want 0", ack.Received)
	}
}

// TestAlertsWebhookStoreErrorReturns500 verifies a persistence failure surfaces
// as 500 so Alertmanager retries (upsert makes the retry safe).
func TestAlertsWebhookStoreErrorReturns500(t *testing.T) {
	store := newFakeAlertStore()
	store.insertErr = errors.New("pg down")
	srv := newAlertsServer(t, store, "hook-token")

	rec := postAlertsWebhook(t, srv, "hook-token", threeAlertPayload)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

// TestAlertsListDefaultsAndAuth verifies GET requires the bearer middleware,
// defaults to status=firing with the v1 limit of 100, and passes explicit
// status/limit through to the store.
func TestAlertsListDefaultsAndAuth(t *testing.T) {
	store := newFakeAlertStore()
	srv := newAlertsServer(t, store, "hook-token")

	rec := getAlerts(t, srv, "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no bearer: status = %d, want 401", rec.Code)
	}

	token := signedToken(t, alertsTestSecret, []string{"admin"})

	rec = getAlerts(t, srv, "", token)
	if rec.Code != http.StatusOK {
		t.Fatalf("default GET: status = %d, want 200", rec.Code)
	}
	if store.lastStatus != "firing" || store.lastLimit != alertsListLimit {
		t.Errorf("default query = status %q limit %d, want firing/%d", store.lastStatus, store.lastLimit, alertsListLimit)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "[]" {
		t.Errorf("empty store body = %s, want [] (never null)", got)
	}

	rec = getAlerts(t, srv, "?status=resolved&limit=5", token)
	if rec.Code != http.StatusOK {
		t.Fatalf("explicit GET: status = %d, want 200", rec.Code)
	}
	if store.lastStatus != "resolved" || store.lastLimit != 5 {
		t.Errorf("explicit query = status %q limit %d, want resolved/5", store.lastStatus, store.lastLimit)
	}
}
