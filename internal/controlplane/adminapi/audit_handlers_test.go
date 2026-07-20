package adminapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Fakes ──────────────────────────────────────────────────────────────────

type fakeAdminAuditLister struct {
	lastQuery metadata.AdminAuditQuery
	rows      []metadata.AdminAuditRow
	total     int
	err       error
}

func (f *fakeAdminAuditLister) ListAdminAudit(_ context.Context, q metadata.AdminAuditQuery) ([]metadata.AdminAuditRow, int, error) {
	f.lastQuery = q
	return f.rows, f.total, f.err
}

// ─── Helpers ────────────────────────────────────────────────────────────────

func newAuditServer(t *testing.T, auditLog AuditLogQuerier, mc AdminAuditLister) *httptest.Server {
	t.Helper()
	s := NewServer(testSecret())
	RegisterAuditRoutes(s, auditLog, mc)
	ts := httptest.NewServer(s.mux)
	t.Cleanup(ts.Close)
	return ts
}

func auditGet(t *testing.T, ts *httptest.Server, params url.Values, withToken bool) (int, auditQueryResponse) {
	t.Helper()
	u := ts.URL + "/v1/admin/audit"
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if withToken {
		req.Header.Set("Authorization", "Bearer "+signedToken(t, testSecret(), []string{"admin"}))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body auditQueryResponse
	_ = json.NewDecoder(resp.Body).Decode(&body) // error bodies don't match the shape; status is asserted first
	return resp.StatusCode, body
}

func seededJWTLog() *cpjwt.AuditLog {
	l := cpjwt.NewAuditLog(io.Discard)
	l.Log(types.PeerId("peer-alpha"), "10.0.0.1", true, 1, 1, "ok", "")
	l.Log(types.PeerId("peer-beta"), "10.0.0.2", false, 1, 1, "fail", "rate_limited")
	l.Log(types.PeerId("peer-gamma"), "10.0.0.3", true, 1, 1, "ok", "")
	return l
}

// ─── jwt source ─────────────────────────────────────────────────────────────

// Given a seeded jwt ring, when kind=jwt, then entries map to the wire shape
// with actor=target=peer_id, action=jwt_issue.
func TestAuditQuery_JWTSourceMapping(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	status, body := auditGet(t, ts, url.Values{"kind": {"jwt"}}, true)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.Total != 3 || len(body.Entries) != 3 {
		t.Fatalf("total=%d len=%d, want 3/3", body.Total, len(body.Entries))
	}
	// Newest first: peer-gamma is the last write.
	first := body.Entries[0]
	if first.Kind != "jwt" || first.Action != "jwt_issue" {
		t.Errorf("kind/action = %q/%q, want jwt/jwt_issue", first.Kind, first.Action)
	}
	if first.Actor != "peer-gamma" || first.Target != "peer-gamma" {
		t.Errorf("actor/target = %q/%q, want peer-gamma/peer-gamma", first.Actor, first.Target)
	}
	if first.IP != "10.0.0.3" || first.Result != "ok" {
		t.Errorf("ip/result = %q/%q", first.IP, first.Result)
	}
	if _, err := time.Parse(time.RFC3339, first.TS); err != nil {
		t.Errorf("ts %q not RFC3339: %v", first.TS, err)
	}
	if body.Entries[1].Result != "fail" {
		t.Errorf("second entry result = %q, want fail", body.Entries[1].Result)
	}
}

// Given a seeded jwt ring, when kind is EMPTY, then the jwt source answers
// (LOCKED default — comment in auditQueryHandler pins this contract).
func TestAuditQuery_DefaultKindIsJWTSource(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	status, body := auditGet(t, ts, url.Values{}, true)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.Total != 3 || body.Entries[0].Kind != "jwt" {
		t.Fatalf("default kind did not select jwt source: %+v", body)
	}
}

// Given a seeded jwt ring, when q filters, then only peer_id substring
// matches return.
func TestAuditQuery_JWTSourceQFiltersPeerID(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	status, body := auditGet(t, ts, url.Values{"kind": {"jwt"}, "q": {"beta"}}, true)
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if body.Total != 1 || body.Entries[0].Target != "peer-beta" {
		t.Fatalf("q filter gave %+v, want only peer-beta", body)
	}
}

// Given a seeded jwt ring, when from/to bound the window, then entries
// outside the window are excluded.
func TestAuditQuery_JWTSourceFromTo(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	future := time.Now().Add(time.Hour).Format(time.RFC3339)
	past := time.Now().Add(-time.Hour).Format(time.RFC3339)

	_, body := auditGet(t, ts, url.Values{"from": {future}}, true)
	if body.Total != 0 {
		t.Fatalf("from=future gave total %d, want 0", body.Total)
	}
	_, body = auditGet(t, ts, url.Values{"to": {past}}, true)
	if body.Total != 0 {
		t.Fatalf("to=past gave total %d, want 0", body.Total)
	}
	_, body = auditGet(t, ts, url.Values{"from": {past}, "to": {future}}, true)
	if body.Total != 3 {
		t.Fatalf("wide window gave total %d, want 3", body.Total)
	}
}

// Given 3 jwt entries, when paging with page_size=2, then page 2 carries the
// remaining entry and total stays 3.
func TestAuditQuery_JWTSourcePagination(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	_, p1 := auditGet(t, ts, url.Values{"page": {"1"}, "page_size": {"2"}}, true)
	if p1.Total != 3 || len(p1.Entries) != 2 || p1.Entries[0].Target != "peer-gamma" {
		t.Fatalf("page1 = %+v", p1)
	}
	_, p2 := auditGet(t, ts, url.Values{"page": {"2"}, "page_size": {"2"}}, true)
	if p2.Total != 3 || len(p2.Entries) != 1 || p2.Entries[0].Target != "peer-alpha" {
		t.Fatalf("page2 = %+v", p2)
	}
	_, p3 := auditGet(t, ts, url.Values{"page": {"3"}, "page_size": {"2"}}, true)
	if p3.Total != 3 || len(p3.Entries) != 0 {
		t.Fatalf("page3 = %+v, want empty entries", p3)
	}
}

// ─── admin source ───────────────────────────────────────────────────────────

// Given admin_audit rows, when kind=auth, then ListAdminAudit receives the
// exact kind filter and rows map onto the wire shape.
func TestAuditQuery_AdminSourceKindFilterAndMapping(t *testing.T) {
	target, ip := "vendor:acct-1", "192.168.1.5"
	mc := &fakeAdminAuditLister{
		rows: []metadata.AdminAuditRow{{
			ID: 7, TS: time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC),
			Kind: "auth", Actor: "root", Action: "login",
			Target: &target, IP: &ip, Result: "ok",
		}},
		total: 1,
	}
	ts := newAuditServer(t, nil, mc)
	status, body := auditGet(t, ts, url.Values{"kind": {"auth"}, "page": {"2"}, "page_size": {"5"}, "q": {"acct"}}, true)
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if mc.lastQuery.Kind != "auth" {
		t.Errorf("ListAdminAudit.Kind = %q, want auth", mc.lastQuery.Kind)
	}
	if mc.lastQuery.Page != 2 || mc.lastQuery.PageSize != 5 || mc.lastQuery.Q != "acct" {
		t.Errorf("ListAdminAudit page/pageSize/q = %d/%d/%q", mc.lastQuery.Page, mc.lastQuery.PageSize, mc.lastQuery.Q)
	}
	e := body.Entries[0]
	if e.Kind != "auth" || e.Actor != "root" || e.Action != "login" || e.Target != target || e.IP != ip || e.Result != "ok" {
		t.Errorf("mapped entry = %+v", e)
	}
	if body.Total != 1 {
		t.Errorf("total = %d, want 1 (SQL total, not page len)", body.Total)
	}
}

// Given kind=admin, then the admin source is queried with an EMPTY kind
// filter (all kinds).
func TestAuditQuery_AdminKindMeansAllAdminKinds(t *testing.T) {
	mc := &fakeAdminAuditLister{total: 0}
	ts := newAuditServer(t, nil, mc)
	status, _ := auditGet(t, ts, url.Values{"kind": {"admin"}}, true)
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if mc.lastQuery.Kind != "" {
		t.Errorf("kind=admin must map to empty Kind filter, got %q", mc.lastQuery.Kind)
	}
}

// Given from/to on the admin source, then the timestamps propagate as
// pointers into AdminAuditQuery.
func TestAuditQuery_AdminSourceFromToPropagate(t *testing.T) {
	mc := &fakeAdminAuditLister{}
	ts := newAuditServer(t, nil, mc)
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	status, _ := auditGet(t, ts, url.Values{
		"kind": {"account"},
		"from": {from.Format(time.RFC3339)},
		"to":   {to.Format(time.RFC3339)},
	}, true)
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if mc.lastQuery.From == nil || !mc.lastQuery.From.Equal(from) {
		t.Errorf("From = %v, want %v", mc.lastQuery.From, from)
	}
	if mc.lastQuery.To == nil || !mc.lastQuery.To.Equal(to) {
		t.Errorf("To = %v, want %v", mc.lastQuery.To, to)
	}
}

// Given a failing admin store, when queried, then 500.
func TestAuditQuery_AdminSourceError(t *testing.T) {
	mc := &fakeAdminAuditLister{err: errors.New("db down")}
	ts := newAuditServer(t, nil, mc)
	status, _ := auditGet(t, ts, url.Values{"kind": {"pin"}}, true)
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", status)
	}
}

// ─── failure matrix ─────────────────────────────────────────────────────────

// Given a malformed from/to, when queried, then 400 (both sources share the
// parse step).
func TestAuditQuery_BadTimeFormat_400(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), &fakeAdminAuditLister{})
	for _, p := range []url.Values{
		{"from": {"not-a-time"}},
		{"to": {"2026-13-45"}},
		{"kind": {"auth"}, "from": {"yesterday"}},
	} {
		if status, _ := auditGet(t, ts, p, true); status != http.StatusBadRequest {
			t.Errorf("params %v: status = %d, want 400", p, status)
		}
	}
}

// Given an unknown kind, when queried, then 400.
func TestAuditQuery_UnknownKind_400(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), &fakeAdminAuditLister{})
	if status, _ := auditGet(t, ts, url.Values{"kind": {"bogus"}}, true); status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
}

// Given no bearer token, when queried, then 401.
func TestAuditQuery_NoToken_401(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), &fakeAdminAuditLister{})
	if status, _ := auditGet(t, ts, url.Values{}, false); status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
}

// Given a nil jwt audit log, when kind=jwt is queried, then 500 (wiring bug
// surfaces loudly, never as a silent empty page).
func TestAuditQuery_NilJWTLog_500(t *testing.T) {
	ts := newAuditServer(t, nil, &fakeAdminAuditLister{})
	if status, _ := auditGet(t, ts, url.Values{}, true); status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", status)
	}
}

// ─── export helpers ─────────────────────────────────────────────────────────

type exportHarness struct {
	status  int
	headers http.Header
	lines   []auditEntryResponse
}

func auditExportGet(t *testing.T, ts *httptest.Server, params url.Values, withToken bool) exportHarness {
	t.Helper()
	u := ts.URL + "/v1/admin/audit/export"
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if withToken {
		req.Header.Set("Authorization", "Bearer "+signedToken(t, testSecret(), []string{"admin"}))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	sc := bufio.NewScanner(resp.Body)
	var lines []auditEntryResponse
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e auditEntryResponse
		if err := json.Unmarshal(line, &e); err != nil {
			t.Errorf("line not valid JSON: %s: %v", string(line), err)
			continue
		}
		lines = append(lines, e)
	}
	if err := sc.Err(); err != nil {
		t.Errorf("scan body: %v", err)
	}
	return exportHarness{status: resp.StatusCode, headers: resp.Header, lines: lines}
}

// ─── export tests ───────────────────────────────────────────────────────────

// Given a seeded jwt ring, when exporting, then Content-Type is
// application/x-ndjson and Content-Disposition has attachment filename.
func TestAuditExport_Headers(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	h := auditExportGet(t, ts, url.Values{}, true)
	if h.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", h.status)
	}
	if ct := h.headers.Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want application/x-ndjson", ct)
	}
	cd := h.headers.Get("Content-Disposition")
	if !strings.HasPrefix(cd, `attachment; filename="audit-`) || !strings.HasSuffix(cd, `.jsonl"`) {
		t.Errorf("Content-Disposition = %q, want attachment; filename=\"audit-<ts>.jsonl\"", cd)
	}
}

// Given a seeded jwt ring, when exporting, then every line parses as a valid
// auditEntryResponse.
func TestAuditExport_LinesParseable(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	h := auditExportGet(t, ts, url.Values{}, true)
	if h.status != http.StatusOK {
		t.Fatalf("status = %d", h.status)
	}
	if len(h.lines) != 3 {
		t.Fatalf("line count = %d, want 3", len(h.lines))
	}
}

// Given a seeded jwt ring, when exporting, then line count matches total.
func TestAuditExport_LineCountMatchesTotal(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	h := auditExportGet(t, ts, url.Values{}, true)
	if h.status != http.StatusOK {
		t.Fatalf("status = %d", h.status)
	}

	// Query the same source to get the total independently.
	_, body := auditGet(t, ts, url.Values{}, true)
	if len(h.lines) != body.Total {
		t.Errorf("export lines = %d, query total = %d", len(h.lines), body.Total)
	}
}

// Given an empty admin_audit table, when exporting, then 200 with empty body
// (not 500).
func TestAuditExport_EmptyTable_200(t *testing.T) {
	mc := &fakeAdminAuditLister{rows: nil, total: 0}
	ts := newAuditServer(t, nil, mc)
	h := auditExportGet(t, ts, url.Values{"kind": {"admin"}}, true)
	if h.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", h.status)
	}
	if len(h.lines) != 0 {
		t.Errorf("lines = %d, want 0", len(h.lines))
	}
}

// Given a seeded jwt ring, when exporting, then entries are in ts ASC order
// (forced by the export contract).
func TestAuditExport_TSAscOrder(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), nil)
	h := auditExportGet(t, ts, url.Values{}, true)
	if h.status != http.StatusOK {
		t.Fatalf("status = %d", h.status)
	}
	if len(h.lines) < 2 {
		t.Skip("need at least 2 entries for order check")
	}
	for i := 1; i < len(h.lines); i++ {
		if h.lines[i-1].TS > h.lines[i].TS {
			t.Errorf("ts not ASC at index %d: %s > %s", i, h.lines[i-1].TS, h.lines[i].TS)
		}
	}
}

// Given no bearer token, when exporting, then 401.
func TestAuditExport_NoToken_401(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), &fakeAdminAuditLister{})
	h := auditExportGet(t, ts, url.Values{}, false)
	if h.status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", h.status)
	}
}

// Given admin_audit rows with q filter, when exporting, then filter is applied.
func TestAuditExport_AdminSourceQFilter(t *testing.T) {
	target, ip := "baidu:acct-zz", "10.1.1.1"
	// Real ListAdminAudit orders ts DESC, id DESC. The fake returns
	// rows in the order listed — so list the newer row first.
	mc := &fakeAdminAuditLister{
		rows: []metadata.AdminAuditRow{{
			ID: 2, TS: time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC),
			Kind: "account", Actor: "admin1", Action: "unban",
			Target: &target, IP: &ip, Result: "ok",
		}, {
			ID: 1, TS: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			Kind: "account", Actor: "admin1", Action: "ban",
			Target: &target, IP: &ip, Result: "ok",
		}},
		total: 2,
	}
	ts := newAuditServer(t, nil, mc)
	h := auditExportGet(t, ts, url.Values{"kind": {"account"}, "q": {"acct"}}, true)
	if h.status != http.StatusOK {
		t.Fatalf("status = %d", h.status)
	}
	if mc.lastQuery.Q != "acct" {
		t.Errorf("Q = %q, want acct", mc.lastQuery.Q)
	}
	if len(h.lines) != 2 {
		t.Errorf("line count = %d, want 2", len(h.lines))
	}
	// Export forces ts ASC by reversing the DESC output: earlier
	// first (ban before unban).
	if h.lines[0].Action != "ban" || h.lines[1].Action != "unban" {
		t.Errorf("order = %s/%s, want ban/unban", h.lines[0].Action, h.lines[1].Action)
	}
}

// Given a source with nil dependencies, when exporting, then 500 (wiring bug
// surfaces loudly).
func TestAuditExport_NilJWTLog_500(t *testing.T) {
	ts := newAuditServer(t, nil, &fakeAdminAuditLister{})
	h := auditExportGet(t, ts, url.Values{}, true)
	if h.status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", h.status)
	}
}

// Given a bad from/to on export, when requested, then 400.
func TestAuditExport_BadTimeFormat_400(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), &fakeAdminAuditLister{})
	h := auditExportGet(t, ts, url.Values{"from": {"yesterday"}}, true)
	if h.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", h.status)
	}
}

// Given an unknown kind on export, when requested, then 400.
func TestAuditExport_UnknownKind_400(t *testing.T) {
	ts := newAuditServer(t, seededJWTLog(), &fakeAdminAuditLister{})
	h := auditExportGet(t, ts, url.Values{"kind": {"bogus"}}, true)
	if h.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", h.status)
	}
}
