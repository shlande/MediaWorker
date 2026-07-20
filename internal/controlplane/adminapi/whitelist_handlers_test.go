package adminapi

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mock WhitelistStoreReader ──────────────────────────────────────────

type mockWhitelistStore struct {
	entries   []jwt.WhitelistEntry
	addErr    error
	listErr   error
	removeErr error

	// Spy: last arguments.
	lastAddPeer    types.PeerId
	lastAddBy      string
	lastRemovePeer types.PeerId
}

func (m *mockWhitelistStore) Add(peerID types.PeerId, addedBy string) error {
	m.lastAddPeer = peerID
	m.lastAddBy = addedBy
	if m.addErr != nil {
		return m.addErr
	}
	// Upsert: overwrite existing.
	for i, e := range m.entries {
		if e.PeerID == string(peerID) {
			m.entries[i] = jwt.WhitelistEntry{
				PeerID:  string(peerID),
				AddedAt: time.Now(),
				AddedBy: addedBy,
			}
			return nil
		}
	}
	m.entries = append(m.entries, jwt.WhitelistEntry{
		PeerID:  string(peerID),
		AddedAt: time.Now(),
		AddedBy: addedBy,
	})
	return nil
}

func (m *mockWhitelistStore) Remove(peerID types.PeerId) error {
	m.lastRemovePeer = peerID
	if m.removeErr != nil {
		return m.removeErr
	}
	for i, e := range m.entries {
		if e.PeerID == string(peerID) {
			m.entries = append(m.entries[:i], m.entries[i+1:]...)
			return nil
		}
	}
	return nil
}

func (m *mockWhitelistStore) ListAll() ([]jwt.WhitelistEntry, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	cp := make([]jwt.WhitelistEntry, len(m.entries))
	copy(cp, m.entries)
	return cp, nil
}

func (m *mockWhitelistStore) Contains(peerID types.PeerId) bool {
	for _, e := range m.entries {
		if e.PeerID == string(peerID) {
			return true
		}
	}
	return false
}

// ─── Mock WhitelistSet ──────────────────────────────────────────────────

type mockWhitelistSet struct {
	m map[types.PeerId]bool
}

func newMockWhitelistSet() *mockWhitelistSet {
	return &mockWhitelistSet{m: make(map[types.PeerId]bool)}
}

func (s *mockWhitelistSet) Add(p types.PeerId)    { s.m[p] = true }
func (s *mockWhitelistSet) Remove(p types.PeerId) { delete(s.m, p) }
func (s *mockWhitelistSet) Contains(p types.PeerId) bool {
	return s.m[p]
}

// ─── Mock WhitelistIssuanceReader ───────────────────────────────────────

type mockIssuanceReader struct {
	records map[types.PeerId]issuanceRecord
}

func newMockIssuanceReader() *mockIssuanceReader {
	return &mockIssuanceReader{records: make(map[types.PeerId]issuanceRecord)}
}

func (m *mockIssuanceReader) Issuance(peerID types.PeerId) (exp int64, l4 bool, ok bool) {
	r, exists := m.records[peerID]
	if !exists {
		return 0, false, false
	}
	return r.exp, r.l4, r.ok
}

// ─── Test helpers ───────────────────────────────────────────────────────

// newPeerID generates a valid libp2p Ed25519 PeerID for testing.
// Returns its base58 string as a types.PeerId for use in API calls.
func newPeerID(t *testing.T) (peer.ID, types.PeerId) {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateEd25519Key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("IDFromPrivateKey: %v", err)
	}
	return id, types.PeerId(id.String())
}

func newWhitelistServer(wlStore WhitelistStoreReader, ps WhitelistSet, reg WhitelistIssuanceReader) (*Server, []byte) {
	secret := []byte("test-secret-key-for-whitelist-tests")
	srv := NewServer(secret)
	RegisterWhitelistRoutes(srv, wlStore, ps, reg, nil)
	return srv, secret
}

func signWhitelistToken(t *testing.T, secret []byte) string {
	t.Helper()
	token, err := SignUserToken(UserTokenPayload{
		UserID:   "user-1",
		Username: "root",
		Roles:    []string{"admin"},
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(time.Hour).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}
	return token
}

func serveWhitelist(srv *Server) *httptest.Server {
	return httptest.NewServer(srv.mux)
}

// doWhitelistGet performs a GET /v1/admin/whitelist with the given bearer token.
func doWhitelistGet(t *testing.T, ts *httptest.Server, token string) (*http.Response, [][]byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/whitelist", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	// Try to decode as JSON array; if not an array (error), return raw body.
	var arr []json.RawMessage
	if err := json.Unmarshal(body, &arr); err != nil {
		return resp, [][]byte{body}
	}
	out := make([][]byte, len(arr))
	for i, r := range arr {
		out[i] = r
	}
	return resp, out
}

// doWhitelistPost performs a POST /v1/admin/whitelist with the given JSON body.
func doWhitelistPost(t *testing.T, ts *httptest.Server, token string, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/admin/whitelist", strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	rbody, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return resp, rbody
}

// doWhitelistDelete performs a DELETE /v1/admin/whitelist/{peer_id}.
func doWhitelistDelete(t *testing.T, ts *httptest.Server, token string, peerIDStr string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/admin/whitelist/"+peerIDStr, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return resp, body
}

// ─── Tests ──────────────────────────────────────────────────────────────

// TestWhitelist_GET_empty verifies that an empty whitelist returns [].
func TestWhitelist_GET_empty(t *testing.T) {
	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, entries := doWhitelistGet(t, ts, token)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty array, got %d entries", len(entries))
	}
}

// TestWhitelist_GET_withEntries verifies listing with effective computation.
func TestWhitelist_GET_withEntries(t *testing.T) {
	_, peerIDa := newPeerID(t)
	_, peerIDb := newPeerID(t)
	_, peerIDc := newPeerID(t)

	store := &mockWhitelistStore{
		entries: []jwt.WhitelistEntry{
			{PeerID: string(peerIDa), AddedAt: time.Now(), AddedBy: "alice"},
			{PeerID: string(peerIDb), AddedAt: time.Now(), AddedBy: "bob"},
			{PeerID: string(peerIDc), AddedAt: time.Now(), AddedBy: "carol"},
		},
	}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	// peerA: effective (l4, not expired)
	reg.records[peerIDa] = issuanceRecord{exp: time.Now().Add(time.Hour).Unix(), l4: true, ok: true}
	// peerB: has issuance but l4=false → not effective
	reg.records[peerIDb] = issuanceRecord{exp: time.Now().Add(time.Hour).Unix(), l4: false, ok: true}
	// peerC: has issuance but expired → not effective
	reg.records[peerIDc] = issuanceRecord{exp: time.Now().Add(-time.Hour).Unix(), l4: true, ok: true}

	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, entries := doWhitelistGet(t, ts, token)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, entries[0])
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	type entry struct {
		PeerID    string `json:"peer_id"`
		AddedAt   string `json:"added_at"`
		AddedBy   string `json:"added_by"`
		Effective bool   `json:"effective"`
	}
	for i, raw := range entries {
		var e entry
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatalf("entry %d: unmarshal: %v", i, err)
		}
		switch e.PeerID {
		case string(peerIDa):
			if !e.Effective {
				t.Errorf("peerA should be effective")
			}
			if e.AddedBy != "alice" {
				t.Errorf("peerA added_by: expected alice, got %q", e.AddedBy)
			}
		case string(peerIDb):
			if e.Effective {
				t.Errorf("peerB should not be effective (l4=false)")
			}
			if e.AddedBy != "bob" {
				t.Errorf("peerB added_by: expected bob, got %q", e.AddedBy)
			}
		case string(peerIDc):
			if e.Effective {
				t.Errorf("peerC should not be effective (expired)")
			}
			if e.AddedBy != "carol" {
				t.Errorf("peerC added_by: expected carol, got %q", e.AddedBy)
			}
		default:
			t.Errorf("unexpected peer_id: %s", e.PeerID)
		}
	}
}

// TestWhitelist_GET_noToken verifies 401 without auth.
func TestWhitelist_GET_noToken(t *testing.T) {
	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, _ := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	resp, _ := doWhitelistGet(t, ts, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

// TestWhitelist_POST_happy verifies successful add with double-write.
func TestWhitelist_POST_happy(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	body := `{"peer_id":"` + string(peerIDa) + `"}`
	resp, rbody := doWhitelistPost(t, ts, token, body)

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rbody)
	}

	// Verify double-write: both store.Add and ps.Add were called.
	if store.lastAddPeer != peerIDa {
		t.Errorf("store.Add peer: expected %s, got %s", peerIDa, store.lastAddPeer)
	}
	if store.lastAddBy != "root" {
		t.Errorf("store.Add added_by: expected root, got %q", store.lastAddBy)
	}
	if !ps.Contains(peerIDa) {
		t.Fatal("ps.Contains should be true after double-write")
	}

	var respObj whitelistPostResponse
	if err := json.Unmarshal(rbody, &respObj); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if respObj.PeerID != string(peerIDa) {
		t.Errorf("response peer_id: expected %s, got %s", peerIDa, respObj.PeerID)
	}
	if respObj.EffectiveAfter != effectiveAfterNote {
		t.Errorf("effective_after: expected %q, got %q", effectiveAfterNote, respObj.EffectiveAfter)
	}
}

// TestWhitelist_POST_idempotent verifies duplicate POST returns 200 (locked choice).
func TestWhitelist_POST_idempotent(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	ps.Add(peerIDa) // pre-existing in the set
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	body := `{"peer_id":"` + string(peerIDa) + `"}`
	resp, _ := doWhitelistPost(t, ts, token, body)

	// Locked choice: duplicate → 200, not 201.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for duplicate POST (idempotent), got %d", resp.StatusCode)
	}

	if !ps.Contains(peerIDa) {
		t.Fatal("peer should still be in set after idempotent add")
	}
	if store.lastAddPeer != peerIDa {
		t.Errorf("store.Add should be called even for duplicate")
	}
}

// TestWhitelist_POST_badPeerID verifies 400 on invalid peer.ID.
func TestWhitelist_POST_badPeerID(t *testing.T) {
	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, rbody := doWhitelistPost(t, ts, token, `{"peer_id":"not-a-valid-peer-id"}`)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, rbody)
	}

	var errResp map[string]string
	if err := json.Unmarshal(rbody, &errResp); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if !strings.Contains(errResp["error"], "invalid peer_id") {
		t.Errorf("expected error to mention invalid peer_id, got %q", errResp["error"])
	}
}

// TestWhitelist_POST_missingPeerID verifies 400 when peer_id is empty.
func TestWhitelist_POST_missingPeerID(t *testing.T) {
	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, rbody := doWhitelistPost(t, ts, token, `{}`)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, rbody)
	}
}

// TestWhitelist_POST_invalidJSON verifies 400 on bad JSON body.
func TestWhitelist_POST_invalidJSON(t *testing.T) {
	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, rbody := doWhitelistPost(t, ts, token, `{{{bad json`)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, rbody)
	}
}

// TestWhitelist_POST_noToken verifies 401 without auth.
func TestWhitelist_POST_noToken(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, _ := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	body := `{"peer_id":"` + string(peerIDa) + `"}`
	resp, _ := doWhitelistPost(t, ts, "", body)

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

// TestWhitelist_DELETE_happy verifies successful removal with double-write.
func TestWhitelist_DELETE_happy(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	ps.Add(peerIDa) // pre-existing
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, rbody := doWhitelistDelete(t, ts, token, string(peerIDa))

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", resp.StatusCode, rbody)
	}

	// Verify double-remove: ps.Remove was called synchronously.
	if ps.Contains(peerIDa) {
		t.Fatal("ps.Contains should be false after double-remove")
	}
	if store.lastRemovePeer != peerIDa {
		t.Errorf("store.Remove peer: expected %s, got %s", peerIDa, store.lastRemovePeer)
	}

	// 204 No Content has empty body.
	if len(rbody) != 0 {
		t.Errorf("expected empty body for 204, got %d bytes", len(rbody))
	}
}

// TestWhitelist_DELETE_missing verifies 404 when peer is not in the set.
func TestWhitelist_DELETE_missing(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet() // empty
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, rbody := doWhitelistDelete(t, ts, token, string(peerIDa))

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.StatusCode, rbody)
	}
}

// TestWhitelist_DELETE_noToken verifies 401 without auth.
func TestWhitelist_DELETE_noToken(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	ps.Add(peerIDa)
	reg := newMockIssuanceReader()
	srv, _ := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	resp, _ := doWhitelistDelete(t, ts, "", string(peerIDa))

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

// TestWhitelist_storeAddError verifies store.Add failure returns 500 and
// ps is NOT updated (we write store first).
func TestWhitelist_storeAddError(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{addErr: errors.New("badger disk full")}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	body := `{"peer_id":"` + string(peerIDa) + `"}`
	resp, rbody := doWhitelistPost(t, ts, token, body)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, rbody)
	}
	if ps.Contains(peerIDa) {
		t.Fatal("ps should not contain peer on store failure")
	}
}

// TestWhitelist_GET_listError verifies store.ListAll failure returns 500.
func TestWhitelist_GET_listError(t *testing.T) {
	store := &mockWhitelistStore{listErr: errors.New("badger io error")}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, body := doWhitelistGet(t, ts, token)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, body[0])
	}
}

// TestWhitelist_DELETE_storeRemoveError verifies store.Remove failure returns 500
// and does NOT remove from the PeerIdSet.
func TestWhitelist_DELETE_storeRemoveError(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{removeErr: errors.New("badger io error")}
	ps := newMockWhitelistSet()
	ps.Add(peerIDa)
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, rbody := doWhitelistDelete(t, ts, token, string(peerIDa))

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, rbody)
	}
	if !ps.Contains(peerIDa) {
		t.Fatal("ps should still contain peer on store remove failure")
	}
}

// TestWhitelist_effective_noIssuanceRecord verifies effective=false when no
// JWT issuance record exists.
func TestWhitelist_effective_noIssuanceRecord(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{
		entries: []jwt.WhitelistEntry{
			{PeerID: string(peerIDa), AddedAt: time.Now(), AddedBy: "alice"},
		},
	}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader() // no records at all
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	resp, entries := doWhitelistGet(t, ts, token)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	type entry struct {
		Effective bool `json:"effective"`
	}
	var e entry
	if err := json.Unmarshal(entries[0], &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if e.Effective {
		t.Error("effective should be false when no issuance record exists")
	}
}

// TestWhitelistPOST_usesUsernameFromCtx verifies addedBy comes from UserFromCtx.
func TestWhitelistPOST_usesUsernameFromCtx(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	// Generate a token with a specific username.
	payload := UserTokenPayload{
		UserID:   "user-2",
		Username: "ops-user",
		Roles:    []string{"admin"},
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(time.Hour).Unix(),
	}
	token, err := SignUserToken(payload, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	body := `{"peer_id":"` + string(peerIDa) + `"}`
	resp, _ := doWhitelistPost(t, ts, token, body)

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	if store.lastAddBy != "ops-user" {
		t.Errorf("added_by should be ops-user (from token), got %q", store.lastAddBy)
	}
	if strings.Contains(body, "ops-user") {
		t.Error("response body should not leak the authenticated username")
	}
}

// TestWhitelist_roundTrip verifies end-to-end: POST → GET → DELETE cycle.
func TestWhitelist_roundTrip(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)

	// 1. GET empty.
	resp, entries := doWhitelistGet(t, ts, token)
	if resp.StatusCode != http.StatusOK || len(entries) != 0 {
		t.Fatalf("initial GET: expected 200 + empty, got %d + %d entries", resp.StatusCode, len(entries))
	}

	// 2. POST the peer.
	body := `{"peer_id":"` + string(peerIDa) + `"}`
	resp, _ = doWhitelistPost(t, ts, token, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST: expected 201, got %d", resp.StatusCode)
	}

	// 3. GET should now contain the peer.
	resp, entries = doWhitelistGet(t, ts, token)
	if resp.StatusCode != http.StatusOK || len(entries) != 1 {
		t.Fatalf("GET after POST: expected 200 + 1 entry, got %d + %d", resp.StatusCode, len(entries))
	}
	if !ps.Contains(peerIDa) {
		t.Fatal("ps.Contains should be true after POST")
	}

	type entry struct {
		PeerID  string `json:"peer_id"`
		AddedBy string `json:"added_by"`
	}
	var e entry
	if err := json.Unmarshal(entries[0], &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if e.PeerID != string(peerIDa) {
		t.Errorf("peer_id: expected %s, got %s", peerIDa, e.PeerID)
	}
	if e.AddedBy != "root" {
		t.Errorf("added_by: expected root, got %q", e.AddedBy)
	}

	// 4. DELETE the peer.
	resp, _ = doWhitelistDelete(t, ts, token, string(peerIDa))
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: expected 204, got %d", resp.StatusCode)
	}
	if ps.Contains(peerIDa) {
		t.Fatal("ps.Contains should be false after DELETE")
	}

	// 5. GET empty again.
	resp, entries = doWhitelistGet(t, ts, token)
	if resp.StatusCode != http.StatusOK || len(entries) != 0 {
		t.Fatalf("GET after DELETE: expected 200 + empty, got %d + %d", resp.StatusCode, len(entries))
	}

	// 6. DELETE again → 404.
	resp, _ = doWhitelistDelete(t, ts, token, string(peerIDa))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("DELETE after DELETE: expected 404, got %d", resp.StatusCode)
	}
}

// TestWhitelist_effective_expired verifies effective=false for expired issuance.
func TestWhitelist_effective_expired(t *testing.T) {
	_, peerIDa := newPeerID(t)
	_, peerIDb := newPeerID(t)

	store := &mockWhitelistStore{
		entries: []jwt.WhitelistEntry{
			{PeerID: string(peerIDa), AddedAt: time.Now(), AddedBy: "alice"},
			{PeerID: string(peerIDb), AddedAt: time.Now(), AddedBy: "bob"},
		},
	}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	// peerA: l4, not expired → effective
	reg.records[peerIDa] = issuanceRecord{exp: time.Now().Add(time.Hour).Unix(), l4: true, ok: true}
	// peerB: l4, expired → not effective
	reg.records[peerIDb] = issuanceRecord{exp: time.Now().Add(-time.Hour).Unix(), l4: true, ok: true}

	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	_, entries := doWhitelistGet(t, ts, token)

	type entry struct {
		PeerID    string `json:"peer_id"`
		Effective bool   `json:"effective"`
	}
	for _, raw := range entries {
		var e entry
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		switch e.PeerID {
		case string(peerIDa):
			if !e.Effective {
				t.Errorf("peerA should be effective (l4 + not expired)")
			}
		case string(peerIDb):
			if e.Effective {
				t.Errorf("peerB should not be effective (expired)")
			}
		}
	}
}

// TestWhitelist_effective_l4False verifies effective=false when l4=false
// even when the JWT is not expired.
func TestWhitelist_effective_l4False(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{
		entries: []jwt.WhitelistEntry{
			{PeerID: string(peerIDa), AddedAt: time.Now(), AddedBy: "alice"},
		},
	}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	reg.records[peerIDa] = issuanceRecord{exp: time.Now().Add(time.Hour).Unix(), l4: false, ok: true}

	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)
	_, entries := doWhitelistGet(t, ts, token)

	type entry struct {
		Effective bool `json:"effective"`
	}
	var e entry
	if err := json.Unmarshal(entries[0], &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if e.Effective {
		t.Error("effective should be false when l4=false")
	}
}

// TestWhitelist_registerRoutes verifies that RegisterWhitelistRoutes mounts all 3 endpoints.
func TestWhitelist_registerRoutes(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	ps.Add(peerIDa)
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)

	// GET works.
	resp, _ := doWhitelistGet(t, ts, token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: expected 200, got %d", resp.StatusCode)
	}

	// POST works (idempotent, already in ps → 200).
	body := `{"peer_id":"` + string(peerIDa) + `"}`
	resp, _ = doWhitelistPost(t, ts, token, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST (idempotent): expected 200, got %d", resp.StatusCode)
	}

	// DELETE works.
	resp, _ = doWhitelistDelete(t, ts, token, string(peerIDa))
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: expected 204, got %d", resp.StatusCode)
	}
}

// TestWhitelist_noTokenAcrossEndpoints verifies 401 on all three endpoints.
func TestWhitelist_noTokenAcrossEndpoints(t *testing.T) {
	_, peerIDa := newPeerID(t)

	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	ps.Add(peerIDa)
	reg := newMockIssuanceReader()
	srv, _ := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	resp, _ := doWhitelistGet(t, ts, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET without token: expected 401, got %d", resp.StatusCode)
	}

	resp, _ = doWhitelistPost(t, ts, "", `{"peer_id":"`+string(peerIDa)+`"}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST without token: expected 401, got %d", resp.StatusCode)
	}

	resp, _ = doWhitelistDelete(t, ts, "", string(peerIDa))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("DELETE without token: expected 401, got %d", resp.StatusCode)
	}
}

// TestWhitelist_usertoken_expired verifies expired admin tokens get 401.
func TestWhitelist_usertoken_expired(t *testing.T) {
	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token, err := SignUserToken(UserTokenPayload{
		UserID:   "user-1",
		Username: "root",
		Roles:    []string{"admin"},
		Iat:      time.Now().Add(-2 * time.Hour).Unix(),
		Exp:      time.Now().Add(-1 * time.Hour).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}

	resp, _ := doWhitelistGet(t, ts, token)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for expired token, got %d", resp.StatusCode)
	}
}

// TestWhitelist_prefix_clash validates that GET /v1/admin/whitelist/extra
// does not match the exact GET /v1/admin/whitelist pattern.
func TestWhitelist_prefix_clash(t *testing.T) {
	store := &mockWhitelistStore{}
	ps := newMockWhitelistSet()
	reg := newMockIssuanceReader()
	srv, secret := newWhitelistServer(store, ps, reg)
	ts := serveWhitelist(srv)
	defer ts.Close()

	token := signWhitelistToken(t, secret)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/whitelist/extra", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	_ = resp.Body.Close()
	// Go 1.22+ ServeMux: the DELETE /v1/admin/whitelist/{peer_id} pattern has a
	// wildcard segment, so GET /v1/admin/whitelist/extra matches the path but not
	// the method → 405 Method Not Allowed.
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /v1/admin/whitelist/extra: expected 405, got %d", resp.StatusCode)
	}
}
