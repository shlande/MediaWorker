package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/node/pinstore"
	"github.com/shlande/mediaworker/internal/node/planlog"
)

// ─── Fakes ───────────────────────────────────────────────────────────────────

// fakePinStoreDB implements both PinListReader and PinRetrier for tests.
type fakePinStoreDB struct {
	entries    []pinstore.PinEntry
	retryOK    map[string]bool // hash → retry returns true
	retryCalls []string
}

func (f *fakePinStoreDB) List(filter pinstore.PinFilter) []pinstore.PinEntry {
	out := make([]pinstore.PinEntry, 0, len(f.entries))
	for i := range f.entries {
		e := &f.entries[i]
		if filter.Ready != nil && (e.State == pinstore.PinStateReady) != *filter.Ready {
			continue
		}
		if filter.Role != "" && e.Role != filter.Role {
			continue
		}
		if filter.ContentID != "" && e.ContentID != filter.ContentID {
			continue
		}
		out = append(out, snapshotEntry(e))
	}
	return out
}

// snapshotEntry builds a copylocks-clean copy of a PinEntry by constructing
// scalar fields and re-Storing the atomic.Bool. Named return + naked return
// avoids the go vet copylocks warning on value append.
func snapshotEntry(e *pinstore.PinEntry) (ne pinstore.PinEntry) {
	ne = pinstore.PinEntry{
		BlobHash:  e.BlobHash,
		BlobType:  e.BlobType,
		Role:      e.Role,
		Size:      e.Size,
		PinnedAt:  e.PinnedAt,
		ContentID: e.ContentID,
		State:     e.State,
		LastError: e.LastError,
	}
	ne.Ready.Store(e.Ready.Load())
	return
}

func (f *fakePinStoreDB) RetryPin(hash string) bool {
	f.retryCalls = append(f.retryCalls, hash)
	return f.retryOK[hash]
}

func makeEntry(blobHash, role, contentID, state string, lastError string) pinstore.PinEntry {
	return pinstore.PinEntry{
		BlobHash:  blobHash,
		Role:      role,
		ContentID: contentID,
		Size:      1024,
		PinnedAt:  time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC),
		State:     state,
		LastError: lastError,
	}
}

// ─── Test helpers ────────────────────────────────────────────────────────────

func doPinsGet(t *testing.T, s *Server, path string, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)
	return rr
}

func doPinsPost(t *testing.T, s *Server, path string, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, nil)
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)
	return rr
}

func assertStatus(t *testing.T, rr *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rr.Code != want {
		t.Fatalf("status = %d, want %d; body = %s", rr.Code, want, rr.Body.String())
	}
}

func decodePinsResponse(t *testing.T, rr *httptest.ResponseRecorder) pinsResponse {
	t.Helper()
	var resp pinsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode pins response: %v", err)
	}
	return resp
}

func decodeRecentResponse(t *testing.T, rr *httptest.ResponseRecorder) []planlog.Record {
	t.Helper()
	var records []planlog.Record
	if err := json.Unmarshal(rr.Body.Bytes(), &records); err != nil {
		t.Fatalf("decode recent response: %v", err)
	}
	return records
}

// ─── Token tests ─────────────────────────────────────────────────────────────

func TestPinsList_NoToken_401(t *testing.T) {
	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, planlog.New())
	rr := doPinsGet(t, s, "/v1/pins", "")
	assertStatus(t, rr, http.StatusUnauthorized)
}

func TestPinsRetry_NoToken_401(t *testing.T) {
	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, planlog.New())
	rr := doPinsPost(t, s, "/v1/pins/abc/retry", "")
	assertStatus(t, rr, http.StatusUnauthorized)
}

func TestPinPlansRecent_NoToken_401(t *testing.T) {
	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, planlog.New())
	rr := doPinsGet(t, s, "/v1/pin-plans/recent", "")
	assertStatus(t, rr, http.StatusUnauthorized)
}

// ─── GET /v1/pins — filters ──────────────────────────────────────────────────

func TestPinsList_NoFilter_ReturnsAll(t *testing.T) {
	db := &fakePinStoreDB{entries: []pinstore.PinEntry{
		makeEntry("h1", "init", "c1", pinstore.PinStateReady, ""),
		makeEntry("h2", "media", "c1", pinstore.PinStatePulling, ""),
		makeEntry("h3", "init", "c2", pinstore.PinStateFailed, "origin 404"),
	}}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 3 {
		t.Fatalf("pins count = %d, want 3", len(resp.Pins))
	}
}

func TestPinsList_ReadyFilter_True(t *testing.T) {
	db := &fakePinStoreDB{entries: []pinstore.PinEntry{
		makeEntry("h1", "init", "c1", pinstore.PinStateReady, ""),
		makeEntry("h2", "media", "c1", pinstore.PinStatePulling, ""),
		makeEntry("h3", "init", "c2", pinstore.PinStateFailed, "err"),
	}}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins?ready=true", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 1 {
		t.Fatalf("pins count = %d, want 1", len(resp.Pins))
	}
	if resp.Pins[0].BlobHash != "h1" {
		t.Fatalf("blob = %s, want h1", resp.Pins[0].BlobHash)
	}
}

func TestPinsList_ReadyFilter_False_ReturnsNonReady(t *testing.T) {
	db := &fakePinStoreDB{entries: []pinstore.PinEntry{
		makeEntry("h1", "init", "c1", pinstore.PinStateReady, ""),
		makeEntry("h2", "media", "c1", pinstore.PinStatePulling, ""),
		makeEntry("h3", "init", "c2", pinstore.PinStateFailed, "err"),
	}}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins?ready=false", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 2 {
		t.Fatalf("pins count = %d, want 2 (pulling + failed)", len(resp.Pins))
	}
	for _, p := range resp.Pins {
		if p.State == pinstore.PinStateReady {
			t.Fatalf("got ready pin when ready=false: %s", p.BlobHash)
		}
	}
}

func TestPinsList_ReadyFilter_Invalid(t *testing.T) {
	db := &fakePinStoreDB{}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins?ready=notabool", testToken)
	assertStatus(t, rr, http.StatusBadRequest)
	assertErrorBody(t, rr, "ready must be true or false")
}

func TestPinsList_RoleFilter(t *testing.T) {
	db := &fakePinStoreDB{entries: []pinstore.PinEntry{
		makeEntry("h1", "init", "c1", pinstore.PinStateReady, ""),
		makeEntry("h2", "media", "c1", pinstore.PinStateReady, ""),
		makeEntry("h3", "media", "c2", pinstore.PinStateReady, ""),
	}}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins?role=media", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 2 {
		t.Fatalf("pins count = %d, want 2", len(resp.Pins))
	}
	for _, p := range resp.Pins {
		if p.Role != "media" {
			t.Fatalf("got non-media pin: %s role=%s", p.BlobHash, p.Role)
		}
	}
}

func TestPinsList_ContentIDFilter(t *testing.T) {
	db := &fakePinStoreDB{entries: []pinstore.PinEntry{
		makeEntry("h1", "init", "c1", pinstore.PinStateReady, ""),
		makeEntry("h2", "media", "c2", pinstore.PinStateReady, ""),
	}}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins?content_id=c2", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 1 {
		t.Fatalf("pins count = %d, want 1", len(resp.Pins))
	}
	if resp.Pins[0].BlobHash != "h2" {
		t.Fatalf("blob = %s, want h2", resp.Pins[0].BlobHash)
	}
}

func TestPinsList_CombinedFilters(t *testing.T) {
	db := &fakePinStoreDB{entries: []pinstore.PinEntry{
		makeEntry("h1", "init", "c1", pinstore.PinStateReady, ""),
		makeEntry("h2", "init", "c1", pinstore.PinStateFailed, "err"),
		makeEntry("h3", "media", "c1", pinstore.PinStateReady, ""),
	}}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	// ready=true + role=init + content_id=c1 → only h1
	rr := doPinsGet(t, s, "/v1/pins?ready=true&role=init&content_id=c1", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 1 {
		t.Fatalf("pins count = %d, want 1", len(resp.Pins))
	}
	if resp.Pins[0].BlobHash != "h1" {
		t.Fatalf("blob = %s, want h1", resp.Pins[0].BlobHash)
	}
}

// ─── GET /v1/pins — summary ──────────────────────────────────────────────────

func TestPinsList_SummaryCounts(t *testing.T) {
	db := &fakePinStoreDB{entries: []pinstore.PinEntry{
		makeEntry("h1", "init", "c1", pinstore.PinStateReady, ""),
		makeEntry("h2", "init", "c1", pinstore.PinStateReady, ""),
		makeEntry("h3", "media", "c1", pinstore.PinStatePulling, ""),
		makeEntry("h4", "media", "c1", pinstore.PinStateFailed, "err1"),
		makeEntry("h5", "media", "c1", pinstore.PinStateFailed, "err2"),
	}}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if resp.Summary.Total != 5 {
		t.Fatalf("summary.total = %d, want 5", resp.Summary.Total)
	}
	if resp.Summary.Ready != 2 {
		t.Fatalf("summary.ready = %d, want 2", resp.Summary.Ready)
	}
	if resp.Summary.Pulling != 1 {
		t.Fatalf("summary.pulling = %d, want 1", resp.Summary.Pulling)
	}
	if resp.Summary.Failed != 2 {
		t.Fatalf("summary.failed = %d, want 2", resp.Summary.Failed)
	}
}

func TestPinsList_EmptyStore(t *testing.T) {
	db := &fakePinStoreDB{}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 0 {
		t.Fatalf("pins count = %d, want 0", len(resp.Pins))
	}
	if resp.Summary.Total != 0 {
		t.Fatalf("summary.total = %d, want 0", resp.Summary.Total)
	}
}

// ─── GET /v1/pins — response shape ───────────────────────────────────────────

func TestPinsList_ResponseShape(t *testing.T) {
	db := &fakePinStoreDB{entries: []pinstore.PinEntry{
		{
			BlobHash:  "abc123",
			ContentID: "content-1",
			Role:      "init",
			Size:      1048576,
			PinnedAt:  time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC),
			State:     pinstore.PinStateReady,
			LastError: "",
		},
	}}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 1 {
		t.Fatalf("pins count = %d, want 1", len(resp.Pins))
	}

	p := resp.Pins[0]
	if p.BlobHash != "abc123" {
		t.Fatalf("blob_hash = %s, want abc123", p.BlobHash)
	}
	if p.ContentID != "content-1" {
		t.Fatalf("content_id = %s, want content-1", p.ContentID)
	}
	if p.Role != "init" {
		t.Fatalf("role = %s, want init", p.Role)
	}
	if p.Size != 1048576 {
		t.Fatalf("size = %d, want 1048576", p.Size)
	}
	if p.PinnedAt != "2026-07-20T12:00:00Z" {
		t.Fatalf("pinned_at = %s, want 2026-07-20T12:00:00Z", p.PinnedAt)
	}
	if p.State != pinstore.PinStateReady {
		t.Fatalf("state = %s, want ready", p.State)
	}
	if p.LastError != "" {
		t.Fatalf("last_error = %s, want empty", p.LastError)
	}

	if resp.Summary.Total != 1 {
		t.Fatalf("summary.total = %d, want 1", resp.Summary.Total)
	}
	if resp.Summary.Ready != 1 {
		t.Fatalf("summary.ready = %d, want 1", resp.Summary.Ready)
	}
}

// ─── POST /v1/pins/{hash}/retry ───────────────────────────────────────────────

func TestPinsRetry_Success_202(t *testing.T) {
	db := &fakePinStoreDB{
		retryOK: map[string]bool{"abc": true},
	}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsPost(t, s, "/v1/pins/abc/retry", testToken)
	assertStatus(t, rr, http.StatusAccepted)

	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "retrying" {
		t.Fatalf("status = %q, want retrying", body["status"])
	}

	if len(db.retryCalls) != 1 || db.retryCalls[0] != "abc" {
		t.Fatalf("retryCalls = %v, want [abc]", db.retryCalls)
	}
}

func TestPinsRetry_NotFound_404(t *testing.T) {
	db := &fakePinStoreDB{
		retryOK: map[string]bool{}, // all hashes → false
	}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsPost(t, s, "/v1/pins/nonexistent/retry", testToken)
	assertStatus(t, rr, http.StatusNotFound)
	assertErrorBody(t, rr, "pin not found or not in failed state")
}

func TestPinsRetry_NotFailed_404(t *testing.T) {
	db := &fakePinStoreDB{
		retryOK: map[string]bool{
			"ready-hash":   false, // RetryPin returns false for non-failed
			"pulling-hash": false,
		},
	}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsPost(t, s, "/v1/pins/ready-hash/retry", testToken)
	assertStatus(t, rr, http.StatusNotFound)
	assertErrorBody(t, rr, "pin not found or not in failed state")
}

func TestPinsRetry_NilStore_404(t *testing.T) {
	s := NewServer(testToken)
	RegisterPinsRoutes(s, nil, planlog.New())

	rr := doPinsPost(t, s, "/v1/pins/abc/retry", testToken)
	assertStatus(t, rr, http.StatusNotFound)
	assertErrorBody(t, rr, "pin not found or not in failed state")
}

// ─── GET /v1/pin-plans/recent ────────────────────────────────────────────────

func TestPinPlansRecent_Empty(t *testing.T) {
	pl := planlog.New()
	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, pl)

	rr := doPinsGet(t, s, "/v1/pin-plans/recent", testToken)
	assertStatus(t, rr, http.StatusOK)

	records := decodeRecentResponse(t, rr)
	if len(records) != 0 {
		t.Fatalf("records count = %d, want 0", len(records))
	}
}

func TestPinPlansRecent_ReturnsAll(t *testing.T) {
	pl := planlog.New()
	pl.Add(planlog.Record{Seq: 1, Pins: 10, Unpins: 2, Applied: true})
	pl.Add(planlog.Record{Seq: 2, Pins: 5, Unpins: 0, Applied: true})
	pl.Add(planlog.Record{Seq: 3, Pins: 8, Unpins: 1, Applied: false})

	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, pl)

	rr := doPinsGet(t, s, "/v1/pin-plans/recent", testToken)
	assertStatus(t, rr, http.StatusOK)

	records := decodeRecentResponse(t, rr)
	if len(records) != 3 {
		t.Fatalf("records count = %d, want 3", len(records))
	}
}

func TestPinPlansRecent_NewestFirst(t *testing.T) {
	pl := planlog.New()
	pl.Add(planlog.Record{Seq: 1, Pins: 1, ReceivedAt: time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)})
	pl.Add(planlog.Record{Seq: 2, Pins: 2, ReceivedAt: time.Date(2026, 7, 20, 11, 0, 0, 0, time.UTC)})
	pl.Add(planlog.Record{Seq: 3, Pins: 3, ReceivedAt: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)})

	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, pl)

	rr := doPinsGet(t, s, "/v1/pin-plans/recent", testToken)
	assertStatus(t, rr, http.StatusOK)

	records := decodeRecentResponse(t, rr)
	if len(records) != 3 {
		t.Fatalf("records count = %d, want 3", len(records))
	}
	// Newest first: Seq 3, 2, 1
	if records[0].Seq != 3 {
		t.Fatalf("records[0].seq = %d, want 3", records[0].Seq)
	}
	if records[1].Seq != 2 {
		t.Fatalf("records[1].seq = %d, want 2", records[1].Seq)
	}
	if records[2].Seq != 1 {
		t.Fatalf("records[2].seq = %d, want 1", records[2].Seq)
	}
}

func TestPinPlansRecent_WithLimit(t *testing.T) {
	pl := planlog.New()
	pl.Add(planlog.Record{Seq: 1, Pins: 1})
	pl.Add(planlog.Record{Seq: 2, Pins: 2})
	pl.Add(planlog.Record{Seq: 3, Pins: 3})
	pl.Add(planlog.Record{Seq: 4, Pins: 4})
	pl.Add(planlog.Record{Seq: 5, Pins: 5})

	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, pl)

	rr := doPinsGet(t, s, "/v1/pin-plans/recent?limit=2", testToken)
	assertStatus(t, rr, http.StatusOK)

	records := decodeRecentResponse(t, rr)
	if len(records) != 2 {
		t.Fatalf("records count = %d, want 2", len(records))
	}
	if records[0].Seq != 5 {
		t.Fatalf("records[0].seq = %d, want 5", records[0].Seq)
	}
	if records[1].Seq != 4 {
		t.Fatalf("records[1].seq = %d, want 4", records[1].Seq)
	}
}

func TestPinPlansRecent_InvalidLimit(t *testing.T) {
	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, planlog.New())

	rr := doPinsGet(t, s, "/v1/pin-plans/recent?limit=0", testToken)
	assertStatus(t, rr, http.StatusBadRequest)
	assertErrorBody(t, rr, "limit must be a positive integer")
}

func TestPinPlansRecent_NonNumericLimit(t *testing.T) {
	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, planlog.New())

	rr := doPinsGet(t, s, "/v1/pin-plans/recent?limit=abc", testToken)
	assertStatus(t, rr, http.StatusBadRequest)
	assertErrorBody(t, rr, "limit must be a positive integer")
}

func TestPinPlansRecent_NilLog(t *testing.T) {
	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, nil)

	rr := doPinsGet(t, s, "/v1/pin-plans/recent", testToken)
	assertStatus(t, rr, http.StatusOK)

	records := decodeRecentResponse(t, rr)
	if len(records) != 0 {
		t.Fatalf("records count = %d, want 0", len(records))
	}
}

func TestPinPlansRecent_RecordShape(t *testing.T) {
	pl := planlog.New()
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	pl.Add(planlog.Record{
		Seq:        42,
		ReceivedAt: now,
		Pins:       15,
		Unpins:     3,
		Applied:    true,
	})

	s := NewServer(testToken)
	RegisterPinsRoutes(s, &fakePinStoreDB{}, pl)

	rr := doPinsGet(t, s, "/v1/pin-plans/recent", testToken)
	assertStatus(t, rr, http.StatusOK)

	records := decodeRecentResponse(t, rr)
	if len(records) != 1 {
		t.Fatalf("records count = %d, want 1", len(records))
	}

	r := records[0]
	if r.Seq != 42 {
		t.Fatalf("seq = %d, want 42", r.Seq)
	}
	if r.Pins != 15 {
		t.Fatalf("pins = %d, want 15", r.Pins)
	}
	if r.Unpins != 3 {
		t.Fatalf("unpins = %d, want 3", r.Unpins)
	}
	if !r.Applied {
		t.Fatal("applied = false, want true")
	}
	if !r.ReceivedAt.Equal(now) {
		t.Fatalf("received_at = %v, want %v", r.ReceivedAt, now)
	}
}

// ─── Prefix clash guard ──────────────────────────────────────────────────────

func TestPinsEndpoints_PrefixClash_NoConflict(t *testing.T) {
	s := NewServer(testToken)
	db := &fakePinStoreDB{
		entries: []pinstore.PinEntry{
			makeEntry("h1", "init", "c1", pinstore.PinStateReady, ""),
		},
	}
	RegisterPinsRoutes(s, db, planlog.New())

	// GET /v1/pins should not match /v1/pin-plans/recent
	rr := doPinsGet(t, s, "/v1/pins?ready=true", testToken)
	assertStatus(t, rr, http.StatusOK)
	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 1 {
		t.Fatalf("pins count = %d, want 1", len(resp.Pins))
	}

	// POST /v1/pins/abc/retry should work
	db.retryOK = map[string]bool{"abc": true}
	rr2 := doGet(t, s, "/v1/pin-plans/recent", testToken)
	assertStatus(t, rr2, http.StatusOK)
}

// ─── RegisterPinsRoutes with nil pinStore ─────────────────────────────────────

func TestPinsList_NilStore_EmptyResponse(t *testing.T) {
	s := NewServer(testToken)
	RegisterPinsRoutes(s, nil, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 0 {
		t.Fatalf("pins count = %d, want 0", len(resp.Pins))
	}
}

func TestPinsList_NotPinListReader_EmptyResponse(t *testing.T) {
	// pinStore that implements PinRetrier but NOT PinListReader
	s := NewServer(testToken)
	RegisterPinsRoutes(s, struct{ *fakePinStoreDB }{&fakePinStoreDB{
		retryOK: map[string]bool{"abc": true},
	}}, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins", testToken)
	assertStatus(t, rr, http.StatusOK)

	resp := decodePinsResponse(t, rr)
	if len(resp.Pins) != 0 {
		t.Fatalf("pins count = %d, want 0", len(resp.Pins))
	}
}

// TestPinsRetry_WithContentID ensures URL encoding of content_id path param
// works through the stdlib mux.
func TestPinsRetry_HashWithSlashes(t *testing.T) {
	db := &fakePinStoreDB{
		retryOK: map[string]bool{"sha256/aBc+123=": true},
	}
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	// "+" unencoded by Go's mux parser, so we test that it survives
	rr := doPinsPost(t, s, "/v1/pins/sha256%2FaBc%2B123=/retry", testToken)
	assertStatus(t, rr, http.StatusAccepted)
}

// TestPinPlansRecent_NoTokenAcrossEndpoints verifies token enforcement is
// consistent across all three fresh endpoints.
func TestPinPlansRecent_NoTokenAcrossEndpoints(t *testing.T) {
	endpoints := []struct {
		method string
		path   string
	}{
		{"GET", "/v1/pins"},
		{"POST", "/v1/pins/abc/retry"},
		{"GET", "/v1/pin-plans/recent"},
	}

	for _, ep := range endpoints {
		db := &fakePinStoreDB{
			entries: []pinstore.PinEntry{
				makeEntry("abc", "init", "c1", pinstore.PinStateFailed, "err"),
			},
			retryOK: map[string]bool{"abc": true},
		}
		s := NewServer(testToken)
		RegisterPinsRoutes(s, db, planlog.New())

		var rr *httptest.ResponseRecorder
		if ep.method == "POST" {
			rr = doPinsPost(t, s, ep.path, "")
		} else {
			rr = doPinsGet(t, s, ep.path, "")
		}
		assertStatus(t, rr, http.StatusUnauthorized)

		// Also verify with wrong token
		s2 := NewServer(testToken)
		RegisterPinsRoutes(s2, db, planlog.New())
		var rr2 *httptest.ResponseRecorder
		if ep.method == "POST" {
			rr2 = doPinsPost(t, s2, ep.path, "wrong")
		} else {
			rr2 = doPinsGet(t, s2, ep.path, "wrong")
		}
		assertStatus(t, rr2, http.StatusUnauthorized)
	}
}

// TestPinsList_NilEntries_Safe ensures no nil panic on List results.
func TestPinsList_NilEntries_Safe(t *testing.T) {
	db := &fakePinStoreDB{} // entries is nil slice
	s := NewServer(testToken)
	RegisterPinsRoutes(s, db, planlog.New())

	rr := doPinsGet(t, s, "/v1/pins", testToken)
	assertStatus(t, rr, http.StatusOK)
	resp := decodePinsResponse(t, rr)
	if resp.Pins == nil {
		t.Fatal("pins should be an empty slice, not nil")
	}
}

// ─── POST /v1/pins/{hash}/retry path extraction ──────────────────────────────

func TestPinsRetry_PathValueExtraction(t *testing.T) {
	hashes := []string{"abc", "sha256:def", "hash-with-dashes", "hash_with_underscores"}
	for _, hash := range hashes {
		db := &fakePinStoreDB{
			retryOK: map[string]bool{hash: true},
		}
		s := NewServer(testToken)
		RegisterPinsRoutes(s, db, planlog.New())

		rr := doPinsPost(t, s, "/v1/pins/"+strings.ReplaceAll(hash, ":", "%3A")+"/retry", testToken)
		assertStatus(t, rr, http.StatusAccepted)

		if len(db.retryCalls) != 1 || db.retryCalls[0] != hash {
			t.Fatalf("retryCalls = %v, want [%s]", db.retryCalls, hash)
		}
	}
}
