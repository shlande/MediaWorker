package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Test doubles ───────────────────────────────────────────────────────────

// captureRecorder records every AuditEntry it receives (synchronous tests).
type captureRecorder struct {
	entries []AuditEntry
}

func (c *captureRecorder) Record(_ context.Context, e AuditEntry) {
	c.entries = append(c.entries, e)
}

// captureInserter records AdminAuditRows; err simulates a failing DB.
type captureInserter struct {
	rows []metadata.AdminAuditRow
	err  error
}

func (c *captureInserter) InsertAdminAudit(_ context.Context, row metadata.AdminAuditRow) error {
	if c.err != nil {
		return c.err
	}
	c.rows = append(c.rows, row)
	return nil
}

// ─── Helpers ────────────────────────────────────────────────────────────────

func signAuditToken(t *testing.T, secret []byte, username string) string {
	t.Helper()
	token, err := SignUserToken(UserTokenPayload{
		UserID:   "user-" + username,
		Username: username,
		Roles:    []string{"admin"},
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(time.Hour).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}
	return token
}

func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func serveMux(t *testing.T, srv *Server) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	return ts
}

// ─── PGAuditRecorder unit tests ─────────────────────────────────────────────

// Given a full audit entry, when Record runs, then the inserter receives the
// mapped row (target/ip pointerized, detail as raw JSON).
func TestAudit_PGRecorderInsertMapping(t *testing.T) {
	ins := &captureInserter{}
	rec := NewPGAuditRecorder(ins)
	ts := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	rec.Record(context.Background(), AuditEntry{
		TS:     ts,
		Kind:   "account",
		Actor:  "admin",
		Action: "ban",
		Target: "baidu:mw_bak_01",
		IP:     "127.0.0.1:9000",
		Result: "ok",
		Detail: map[string]any{"reason": "abuse"},
	})

	if len(ins.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(ins.rows))
	}
	row := ins.rows[0]
	if !row.TS.Equal(ts) || row.Kind != "account" || row.Actor != "admin" || row.Action != "ban" || row.Result != "ok" {
		t.Errorf("unexpected row: %+v", row)
	}
	if row.Target == nil || *row.Target != "baidu:mw_bak_01" {
		t.Errorf("target = %v, want baidu:mw_bak_01", row.Target)
	}
	if row.IP == nil || *row.IP != "127.0.0.1:9000" {
		t.Errorf("ip = %v, want 127.0.0.1:9000", row.IP)
	}
	if string(row.Detail) != `{"reason":"abuse"}` {
		t.Errorf("detail = %s, want reason JSON", row.Detail)
	}
}

// Given an empty target/ip and nil detail, when Record runs, then those
// columns stay nil (-> SQL NULL).
func TestAudit_PGRecorderEmptyFieldsBecomeNull(t *testing.T) {
	ins := &captureInserter{}
	NewPGAuditRecorder(ins).Record(context.Background(), AuditEntry{
		Kind: "auth", Actor: "admin", Action: "login", Result: "fail",
	})
	if len(ins.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(ins.rows))
	}
	row := ins.rows[0]
	if row.Target != nil || row.IP != nil || row.Detail != nil || !row.TS.IsZero() {
		t.Errorf("empty fields must map to nil: %+v", row)
	}
}

// Given the DB insert fails, when Record runs, then it returns normally
// (never panics, never blocks) and Warn-logs the failure.
func TestAudit_PGRecorderInsertFailureWarnOnly(t *testing.T) {
	buf := captureSlog(t)
	ins := &captureInserter{err: errors.New("db down")}
	rec := NewPGAuditRecorder(ins)

	rec.Record(context.Background(), AuditEntry{Kind: "pin", Actor: "admin", Action: "pin", Result: "ok"})

	if !strings.Contains(buf.String(), "admin audit: insert failed") {
		t.Errorf("expected Warn log for insert failure, got: %s", buf.String())
	}
}

// Given a nil recorder or nil inserter, when Record runs, then nothing
// happens (no panic) — todo 54 assembles wiring late, handlers must tolerate.
func TestAudit_PGRecorderNilSafe(t *testing.T) {
	var nilRec *PGAuditRecorder
	nilRec.Record(context.Background(), AuditEntry{Kind: "account", Actor: "a", Action: "create", Result: "ok"})
	NewPGAuditRecorder(nil).Record(context.Background(), AuditEntry{Kind: "account", Actor: "a", Action: "create", Result: "ok"})
}

// Given an unmarshalable detail value, when Record runs, then the row is
// still inserted (without detail) and the marshal failure is Warn-logged.
func TestAudit_PGRecorderDetailMarshalFailure(t *testing.T) {
	buf := captureSlog(t)
	ins := &captureInserter{}
	NewPGAuditRecorder(ins).Record(context.Background(), AuditEntry{
		Kind: "account", Actor: "admin", Action: "update", Result: "ok",
		Detail: map[string]any{"bad": func() {}},
	})
	if len(ins.rows) != 1 {
		t.Fatalf("rows = %d, want 1 (insert must proceed without detail)", len(ins.rows))
	}
	if ins.rows[0].Detail != nil {
		t.Errorf("detail = %s, want nil after marshal failure", ins.rows[0].Detail)
	}
	if !strings.Contains(buf.String(), "detail marshal failed") {
		t.Errorf("expected Warn log for marshal failure, got: %s", buf.String())
	}
}

// ─── Handler instrumentation: accounts ──────────────────────────────────────

var auditTestSecret = []byte("test-secret-key-for-admin-tokens")

func makeAuditAccountsServer(rec AuditRecorder) (*fakeAccountRegistry, *Server) {
	reg := newFakeAccountRegistry()
	srv := NewServer(auditTestSecret)
	RegisterAccountsRoutes(srv, reg, nil, reg, &fakeBroadcaster{}, rec)
	return reg, srv
}

func postAccountBody(t *testing.T) *string {
	t.Helper()
	return jsonBody(t, map[string]any{
		"vendor":     "baidu",
		"account_id": "mw_bak_01",
		"auth": map[string]any{
			"client_id":     "appkey-1",
			"client_secret": sentinelClientSecret,
			"refresh_token": sentinelRefreshToken,
			"redirect_uri":  "https://example.com/callback",
		},
	})
}

// Given a working accounts stack with a capturing recorder, when POST
// /v1/admin/accounts succeeds, then exactly one kind=account/action=create/
// actor=admin/result=ok entry is recorded with the vendor:account_id target
// and the request IP.
func TestAudit_PostAccountRecordsEntry(t *testing.T) {
	cap := &captureRecorder{}
	_, srv := makeAuditAccountsServer(cap)
	ts := serveMux(t, srv)

	status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", signAuditToken(t, auditTestSecret, "admin"), postAccountBody(t))
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want 201", status)
	}

	if len(cap.entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(cap.entries))
	}
	e := cap.entries[0]
	if e.Kind != "account" || e.Action != "create" || e.Actor != "admin" || e.Result != "ok" {
		t.Errorf("unexpected entry: %+v", e)
	}
	if e.Target != "baidu:mw_bak_01" {
		t.Errorf("target = %q, want baidu:mw_bak_01", e.Target)
	}
	if e.IP == "" {
		t.Error("ip must carry the request RemoteAddr")
	}
	if e.TS.IsZero() {
		t.Error("ts must be set")
	}
}

// Given a failing write (missing account), when PUT runs, then the entry
// records result=fail (and the 404 business response is unchanged).
func TestAudit_UpdateAccountFailureRecorded(t *testing.T) {
	cap := &captureRecorder{}
	_, srv := makeAuditAccountsServer(cap)
	ts := serveMux(t, srv)

	status, _ := doRaw(t, ts, http.MethodPut, "/v1/admin/accounts/baidu/nope",
		signAuditToken(t, auditTestSecret, "admin"), jsonBody(t, map[string]any{"enabled": true}))
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
	if len(cap.entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(cap.entries))
	}
	e := cap.entries[0]
	if e.Kind != "account" || e.Action != "update" || e.Result != "fail" || e.Target != "baidu:nope" {
		t.Errorf("unexpected entry: %+v", e)
	}
}

// Given write requests carrying credential material (create + rotate) and a
// ban with a reason, when entries are recorded, then the serialized detail
// NEVER contains secret values or a "credential" key, while the ban detail
// does carry the spec-sanctioned reason.
func TestAudit_DetailNeverContainsCredential(t *testing.T) {
	cap := &captureRecorder{}
	_, srv := makeAuditAccountsServer(cap)
	ts := serveMux(t, srv)
	token := signAuditToken(t, auditTestSecret, "admin")

	// create (auth carries sentinel secrets)
	if status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", token, postAccountBody(t)); status != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", status)
	}
	// rotate the created account (auth patch carries sentinel secrets)
	if status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/rotate", token,
		jsonBody(t, map[string]any{
			"client_id":     "appkey-2",
			"client_secret": sentinelClientSecret,
			"refresh_token": sentinelRefreshToken,
		})); status != http.StatusAccepted {
		t.Fatalf("rotate status = %d, want 202", status)
	}
	// ban (reason is spec-sanctioned detail)
	if status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts/baidu/mw_bak_01/ban", token,
		jsonBody(t, map[string]any{"reason": "abuse detected"})); status != http.StatusAccepted {
		t.Fatalf("ban status = %d, want 202", status)
	}

	if len(cap.entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(cap.entries))
	}
	for _, e := range cap.entries {
		raw, err := json.Marshal(e.Detail)
		if err != nil {
			t.Fatalf("marshal detail: %v", err)
		}
		s := string(raw)
		for _, sentinel := range []string{sentinelRefreshToken, sentinelClientSecret, sentinelCookieValue} {
			if strings.Contains(s, sentinel) {
				t.Errorf("audit detail leaks sentinel secret (kind=%s action=%s): %s", e.Kind, e.Action, s)
			}
		}
		if strings.Contains(s, `"credential"`) || strings.Contains(s, `"client_secret"`) || strings.Contains(s, `"refresh_token"`) || strings.Contains(s, `"cookies"`) {
			t.Errorf("audit detail carries secret-shaped key (kind=%s action=%s): %s", e.Kind, e.Action, s)
		}
	}

	// The ban entry (last) must carry the reason — detail is not suppressed,
	// only secrets are.
	ban := cap.entries[2]
	if ban.Action != "ban" || ban.Detail["reason"] != "abuse detected" {
		t.Errorf("ban detail = %v, want reason carried", ban.Detail)
	}
}

// Given the recorder's DB is down, when POST /v1/admin/accounts runs, then
// the business response is still 201 (audit failure never blocks) and the
// failure is Warn-logged.
func TestAudit_RecorderErrorKeepsBusinessResponse(t *testing.T) {
	buf := captureSlog(t)
	failing := NewPGAuditRecorder(&captureInserter{err: errors.New("db down")})
	_, srv := makeAuditAccountsServer(failing)
	ts := serveMux(t, srv)

	status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", signAuditToken(t, auditTestSecret, "admin"), postAccountBody(t))
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want 201 even when the recorder fails", status)
	}
	if !strings.Contains(buf.String(), "admin audit: insert failed") {
		t.Errorf("expected Warn log for recorder failure, got: %s", buf.String())
	}
}

// ─── Handler instrumentation: auth ──────────────────────────────────────────

// Given the auth routes with a recorder installed BEFORE registration, when
// login fails (bad password) and succeeds, then kind=auth entries record
// result=fail and result=ok respectively.
func TestAudit_LoginFailureAndSuccess(t *testing.T) {
	cap := &captureRecorder{}
	store := newFakeUserStore()
	store.addUser(t, "admin", "correct-password", []string{"admin"}, false)

	srv := NewServer(testAuthSecret)
	srv.SetAuditRecorder(cap)
	RegisterAuthRoutes(srv, store)
	ts := serveMux(t, srv)

	resp := postJSON(t, ts.Client(), ts.URL+"/v1/auth/login", `{"username":"admin","password":"wrong"}`, "")
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if len(cap.entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(cap.entries))
	}
	e := cap.entries[0]
	if e.Kind != "auth" || e.Action != "login" || e.Result != "fail" || e.Actor != "admin" || e.Target != "admin" {
		t.Errorf("unexpected failure entry: %+v", e)
	}
	if e.IP == "" {
		t.Error("ip must carry the request RemoteAddr")
	}

	resp = postJSON(t, ts.Client(), ts.URL+"/v1/auth/login", `{"username":"admin","password":"correct-password"}`, "")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(cap.entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(cap.entries))
	}
	if cap.entries[1].Kind != "auth" || cap.entries[1].Result != "ok" {
		t.Errorf("unexpected success entry: %+v", cap.entries[1])
	}
}

// ─── Handler instrumentation: whitelist / pin / content ─────────────────────

// Given the whitelist stack, when POST then DELETE run, then kind=whitelist
// entries record action=add and action=remove with the peer_id target.
func TestAudit_WhitelistAddRemove(t *testing.T) {
	cap := &captureRecorder{}
	_, peerID := newPeerID(t)
	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()

	srv := NewServer(auditTestSecret)
	RegisterWhitelistRoutes(srv, store, ps, reg, cap)
	ts := serveMux(t, srv)
	token := signAuditToken(t, auditTestSecret, "admin")

	status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/whitelist", token,
		jsonBody(t, map[string]any{"peer_id": string(peerID)}))
	if status != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201", status)
	}
	status, _ = doRaw(t, ts, http.MethodDelete, "/v1/admin/whitelist/"+string(peerID), token, nil)
	if status != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", status)
	}

	if len(cap.entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(cap.entries))
	}
	if e := cap.entries[0]; e.Kind != "whitelist" || e.Action != "add" || e.Result != "ok" || e.Target != string(peerID) || e.Actor != "admin" {
		t.Errorf("unexpected add entry: %+v", e)
	}
	if e := cap.entries[1]; e.Kind != "whitelist" || e.Action != "remove" || e.Result != "ok" || e.Target != string(peerID) {
		t.Errorf("unexpected remove entry: %+v", e)
	}
}

// Given the pin stack, when POST /v1/admin/pin and /v1/admin/unpin succeed,
// then kind=pin entries record action=pin/unpin with the content_id target
// and non-secret detail.
func TestAudit_PinUnpin(t *testing.T) {
	cap := &captureRecorder{}
	mc := &mockPinContentMeta{
		meta:  &types.ContentMeta{ContentID: "content-1", ContentType: "dash_video"},
		blobs: []types.BlobDescriptor{{BlobHash: "abc-def-1", BlobType: "mp4_init_segment", Size: 500}},
	}
	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID:      "peer-A",
		NodeID:      "node-A",
		PrefixSpace: types.PartitionStatus{TotalBytes: 10000, UsedBytes: 100},
	})
	po := &mockPinOrchestrator{seqs: []uint64{1}}

	srv := NewServer(auditTestSecret)
	RegisterPinRoutes(srv, mc, reg, po, cap)
	ts := serveMux(t, srv)
	token := signAuditToken(t, auditTestSecret, "admin")

	pinBody := jsonBody(t, map[string]any{"content_id": "content-1", "target_nodes": []string{"node-A"}})
	if status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/pin", token, pinBody); status != http.StatusAccepted {
		t.Fatalf("pin status = %d, want 202", status)
	}
	if status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/unpin", token, pinBody); status != http.StatusAccepted {
		t.Fatalf("unpin status = %d, want 202", status)
	}

	if len(cap.entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(cap.entries))
	}
	if e := cap.entries[0]; e.Kind != "pin" || e.Action != "pin" || e.Result != "ok" || e.Target != "content-1" || e.Actor != "admin" {
		t.Errorf("unexpected pin entry: %+v", e)
	}
	if e := cap.entries[1]; e.Kind != "pin" || e.Action != "unpin" || e.Result != "ok" || e.Target != "content-1" {
		t.Errorf("unexpected unpin entry: %+v", e)
	}
	raw, _ := json.Marshal(cap.entries[0].Detail)
	if !strings.Contains(string(raw), "node-A") {
		t.Errorf("pin detail should carry target_nodes, got %s", raw)
	}
}

// Given the contents stack, when DELETE succeeds and fails, then kind=content
// entries record result=ok and result=fail with the content_id target.
func TestAudit_ContentDelete(t *testing.T) {
	cap := &captureRecorder{}
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader:   &mockContentsListReader{},
		ContentsDetailReader: &mockContentsDetailReader{},
		ContentMetaReader:    &mockContentMetaReader{meta: &types.ContentMeta{ContentID: "content-to-delete"}},
	}
	dlog := &mockPinCountReader{counts: map[string]int{}}
	deleter := &mockContentDeleter{}

	srv := NewServer([]byte(contentsTestSecret))
	RegisterContentsRoutes(srv, mc, dlog, deleter, cap)
	ts := serveMux(t, srv)
	token := signAuditToken(t, []byte(contentsTestSecret), "admin")

	if status, _ := doRaw(t, ts, http.MethodDelete, "/v1/admin/contents/content-to-delete", token, nil); status != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", status)
	}
	deleter.err = errors.New("db down")
	if status, _ := doRaw(t, ts, http.MethodDelete, "/v1/admin/contents/content-to-delete", token, nil); status != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, want 500", status)
	}

	if len(cap.entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(cap.entries))
	}
	if e := cap.entries[0]; e.Kind != "content" || e.Action != "delete" || e.Result != "ok" || e.Target != "content-to-delete" || e.Actor != "admin" {
		t.Errorf("unexpected ok entry: %+v", e)
	}
	if e := cap.entries[1]; e.Kind != "content" || e.Action != "delete" || e.Result != "fail" || e.Target != "content-to-delete" {
		t.Errorf("unexpected fail entry: %+v", e)
	}
}

// ─── Nil-recorder tolerance ─────────────────────────────────────────────────

// Given a nil recorder (todo 54 has not wired auditing yet), when a write
// endpoint runs, then the business flow is completely unaffected.
func TestAudit_NilRecorderEndpointsWork(t *testing.T) {
	_, srv := makeAuditAccountsServer(nil)
	ts := serveMux(t, srv)
	if status, _ := doRaw(t, ts, http.MethodPost, "/v1/admin/accounts", signAuditToken(t, auditTestSecret, "admin"), postAccountBody(t)); status != http.StatusCreated {
		t.Fatalf("status = %d, want 201 with nil recorder", status)
	}
}
