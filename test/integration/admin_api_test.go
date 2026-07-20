// Control-plane admin API end-to-end integration test (plan todo 54).
//
// Assembles the CP admin Server EXACTLY like cmd/control-plane/main.go's
// consolidated block (13c): every Register*Routes call with the same dep
// wiring, driven over a REAL socket (srv.Serve on a free loopback port).
// Real components where cheap: noderegistry.Registry, pinstrategy.DispatchLog,
// jwt.WhitelistStore + PeerIdSet (temp BadgerDB), quota.QuotaAllocator, and
// real usertoken sign/verify (login issues a genuine HS256 token). PG-backed
// surfaces are one stateful in-memory fake (cpStore) so handler WRITES are
// visible through the reader interfaces; Prometheus is a mock
// OverviewPromReader. Per-handler field semantics are covered by each
// handler's own unit tests and are intentionally NOT re-asserted here — this
// matrix covers routing + auth + assembly + the seven-step write chain.
package integration_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/controlplane/accountregistry"
	"github.com/shlande/mediaworker/internal/controlplane/accounttester"
	"github.com/shlande/mediaworker/internal/controlplane/adminapi"
	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/storage/quota"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Stateful in-memory fake for every PG-backed narrow interface ──────────

type cpUser struct {
	userID   string
	hash     string
	roles    []string
	disabled bool
}

type cpAccount struct {
	info   accountregistry.AccountInfo
	health *metadata.HealthView
}

type cpContent struct {
	meta      types.ContentMeta
	blobs     []types.BlobDescriptor
	roles     []types.BlobRole
	row       metadata.AdminContentRow
	locations []metadata.AdminContentLocation
}

// cpStore implements: AdminUserStore, AdminAccountsReader, VendorProfilesReader,
// AdminAccountsWriter (+AccountAuthWriter), ContentsListReader,
// ContentsDetailReader, ContentMetaReader, PinContentMetaReader,
// ContentDeleter, AlertEventStore, OverviewMetadataReader, AdminAuditInserter,
// NodeHistoryReader, accounttester.SecretReader. One shared store so the
// chain's write→read linkage (ban → health view) is real.
type cpStore struct {
	mu             sync.Mutex
	users          map[string]cpUser
	accounts       map[string]*cpAccount // key "vendor:account_id"
	contents       map[string]*cpContent
	vendorProfiles []metadata.VendorProfileRow
	alerts         []metadata.AlertEventRow
	auditRows      []metadata.AdminAuditRow
}

func newCPStore() *cpStore {
	return &cpStore{
		users:    map[string]cpUser{},
		accounts: map[string]*cpAccount{},
		contents: map[string]*cpContent{},
	}
}

func cpAccountKey(vendor types.Vendor, accountID string) string {
	return string(vendor) + ":" + accountID
}

// ── AdminUserStore ──

func (s *cpStore) GetUserByUsername(_ context.Context, username string) (string, string, []string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[username]
	if !ok {
		return "", "", nil, false, fmt.Errorf("%w: %s", metadata.ErrUserNotFound, username)
	}
	return u.userID, u.hash, append([]string(nil), u.roles...), u.disabled, nil
}

func (s *cpStore) CountUsers(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.users), nil
}

func (s *cpStore) CreateUser(_ context.Context, username, passwordHash string, roles []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[username] = cpUser{userID: username, hash: passwordHash, roles: roles}
	return nil
}

// ── AdminAccountsReader / VendorProfilesReader ──

func cpCredentialMeta(info accountregistry.AccountInfo) metadata.CredentialMeta {
	meta := metadata.CredentialMeta{
		AuthType:        "oauth2",
		HasClientSecret: info.ClientConfig.ClientSecret != "",
		HasRefreshToken: info.Credential.RefreshToken != "",
		Region:          info.ClientConfig.Region,
	}
	if len(info.Credential.Cookies) > 0 {
		meta.AuthType = "cookie"
		for k := range info.Credential.Cookies {
			meta.CookieKeys = append(meta.CookieKeys, k)
		}
		sort.Strings(meta.CookieKeys)
	}
	return meta
}

func (s *cpStore) ListAccounts(_ context.Context, vendorFilter, stateFilter string) ([]metadata.AdminAccountView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]metadata.AdminAccountView, 0, len(s.accounts))
	for _, a := range s.accounts {
		if vendorFilter != "" && string(a.info.Vendor) != vendorFilter {
			continue
		}
		var health *metadata.HealthView
		state := "healthy"
		if a.health != nil {
			h := *a.health
			health = &h
			state = h.State
		}
		if stateFilter != "" && state != stateFilter {
			continue
		}
		out = append(out, metadata.AdminAccountView{
			Vendor:         string(a.info.Vendor),
			AccountID:      a.info.AccountID,
			Enabled:        a.info.Enabled,
			RateLimitCfg:   a.info.RateLimitCfg,
			VendorProfile:  a.info.VendorProfile,
			Health:         health,
			CredentialMeta: cpCredentialMeta(a.info),
		})
	}
	return out, nil
}

func (s *cpStore) ListVendorProfiles(_ context.Context) ([]metadata.VendorProfileRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]metadata.VendorProfileRow(nil), s.vendorProfiles...), nil
}

// ── AdminAccountsWriter (+ AccountAuthWriter) ──

func (s *cpStore) getAccount(vendor types.Vendor, accountID string) (*cpAccount, error) {
	a, ok := s.accounts[cpAccountKey(vendor, accountID)]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", accountregistry.ErrAccountNotFound, vendor, accountID)
	}
	return a, nil
}

func (s *cpStore) CreateAccount(_ context.Context, info accountregistry.AccountInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[cpAccountKey(info.Vendor, info.AccountID)] = &cpAccount{info: info}
	return nil
}

func (s *cpStore) GetAccountSecret(_ context.Context, vendor types.Vendor, accountID string) (types.Credential, types.ClientConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.getAccount(vendor, accountID)
	if err != nil {
		return types.Credential{}, types.ClientConfig{}, err
	}
	return a.info.Credential, a.info.ClientConfig, nil
}

func (s *cpStore) UpdateCredential(_ context.Context, vendor types.Vendor, accountID string, cred types.Credential) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.getAccount(vendor, accountID)
	if err != nil {
		return err
	}
	a.info.Credential = cred
	return nil
}

func (s *cpStore) UpdateClientConfig(_ context.Context, vendor types.Vendor, accountID string, cc types.ClientConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.getAccount(vendor, accountID)
	if err != nil {
		return err
	}
	a.info.ClientConfig = cc
	return nil
}

func (s *cpStore) OnCredentialChange(_ context.Context, _ types.Vendor, _ string) {}

func (s *cpStore) SetEnabled(_ context.Context, vendor types.Vendor, accountID string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.getAccount(vendor, accountID)
	if err != nil {
		return err
	}
	a.info.Enabled = enabled
	return nil
}

func (s *cpStore) SetRateLimit(_ context.Context, vendor types.Vendor, accountID string, cfg types.RateLimitConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.getAccount(vendor, accountID)
	if err != nil {
		return err
	}
	a.info.RateLimitCfg = cfg
	return nil
}

func (s *cpStore) SetVendorProfile(_ context.Context, vendor types.Vendor, accountID string, vp types.VendorProfile) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.getAccount(vendor, accountID)
	if err != nil {
		return err
	}
	a.info.VendorProfile = vp
	return nil
}

func (s *cpStore) Ban(_ context.Context, vendor types.Vendor, accountID, reason string, banUntil time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.getAccount(vendor, accountID)
	if err != nil {
		return err
	}
	a.health = &metadata.HealthView{
		State:     "banned",
		ErrorMsg:  reason,
		BanUntil:  &banUntil,
		LastCheck: time.Now(),
	}
	return nil
}

func (s *cpStore) Unban(_ context.Context, vendor types.Vendor, accountID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.getAccount(vendor, accountID)
	if err != nil {
		return err
	}
	a.health = &metadata.HealthView{State: "healthy", LastCheck: time.Now()}
	return nil
}

// ── Contents readers / deleter ──

func (s *cpStore) ListContents(_ context.Context, q metadata.ListContentsQuery) ([]metadata.AdminContentRow, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]metadata.AdminContentRow, 0, len(s.contents))
	for _, c := range s.contents {
		if c.meta.DeletedAt != nil {
			continue
		}
		if q.Type != "" && c.meta.ContentType != q.Type {
			continue
		}
		out = append(out, c.row)
	}
	return out, len(out), nil
}

func (s *cpStore) GetContentDetail(_ context.Context, contentID string) (*metadata.AdminContentDetail, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contents[contentID]
	if !ok {
		return nil, fmt.Errorf("metadata: content %q: %w", contentID, metadata.ErrContentNotFound)
	}
	meta := c.meta
	detail := &metadata.AdminContentDetail{Meta: &meta}
	for i, b := range c.blobs {
		blob := metadata.AdminContentBlob{Hash: b.BlobHash, Size: b.Size, BlobType: b.BlobType}
		if i < len(c.roles) {
			blob.Role = c.roles[i].Role
			blob.SortOrder = c.roles[i].SortOrder
			blob.BusinessMeta = c.roles[i].BusinessMeta
		}
		detail.Blobs = append(detail.Blobs, blob)
	}
	detail.Locations = append(detail.Locations, c.locations...)
	return detail, nil
}

func (s *cpStore) GetContentMeta(_ context.Context, contentID string) (*types.ContentMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contents[contentID]
	if !ok {
		return nil, fmt.Errorf("metadata: content %q: %w", contentID, sql.ErrNoRows)
	}
	meta := c.meta
	return &meta, nil
}

func (s *cpStore) GetContentBlobs(_ context.Context, contentID string) ([]types.BlobDescriptor, []types.BlobRole, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contents[contentID]
	if !ok {
		return nil, nil, fmt.Errorf("metadata: content %q: %w", contentID, sql.ErrNoRows)
	}
	return c.blobs, c.roles, nil
}

func (s *cpStore) SoftDeleteContent(_ context.Context, contentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contents[contentID]
	if !ok {
		return fmt.Errorf("metadata: content %q: %w", contentID, sql.ErrNoRows)
	}
	now := time.Now()
	c.meta.DeletedAt = &now
	return nil
}

// ── AlertEventStore ──

func (s *cpStore) InsertAlertEvent(_ context.Context, row metadata.AlertEventRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts = append(s.alerts, row)
	return nil
}

func (s *cpStore) ListAlertEvents(_ context.Context, status string, limit int) ([]metadata.AlertEventRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]metadata.AlertEventRow, 0, len(s.alerts))
	for _, a := range s.alerts {
		if status != "" && a.Status != status {
			continue
		}
		out = append(out, a)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// ── OverviewMetadataReader (AccountHealthRate; ListContents/ListAlertEvents above) ──

func (s *cpStore) AccountHealthRate(_ context.Context) (float64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	total, healthy := 0, 0
	for _, a := range s.accounts {
		if a.health == nil {
			continue
		}
		total++
		if a.health.State == "healthy" {
			healthy++
		}
	}
	if total == 0 {
		return 0, false, nil
	}
	return float64(healthy) / float64(total), true, nil
}

// ── AdminAuditInserter (PGAuditRecorder sink; the rows ListAdminAudit reads) ──

func (s *cpStore) InsertAdminAudit(_ context.Context, row metadata.AdminAuditRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auditRows = append(s.auditRows, row)
	return nil
}

func (s *cpStore) auditSnapshot() []metadata.AdminAuditRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]metadata.AdminAuditRow(nil), s.auditRows...)
}

// ── AdminAuditLister (todo 34 query source; same rows the recorder wrote) ──

func (s *cpStore) ListAdminAudit(_ context.Context, q metadata.AdminAuditQuery) ([]metadata.AdminAuditRow, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	matched := make([]metadata.AdminAuditRow, 0, len(s.auditRows))
	// ts DESC mirrors the PG ordering contract.
	for i := len(s.auditRows) - 1; i >= 0; i-- {
		r := s.auditRows[i]
		if q.Kind != "" && r.Kind != q.Kind {
			continue
		}
		if q.From != nil && r.TS.Before(*q.From) {
			continue
		}
		if q.To != nil && r.TS.After(*q.To) {
			continue
		}
		if q.Q != "" && (r.Target == nil || !strings.Contains(*r.Target, q.Q)) {
			continue
		}
		matched = append(matched, r)
	}
	page := q.Page
	if page < 1 {
		page = 1
	}
	pageSize := q.PageSize
	if pageSize < 1 {
		pageSize = 20
	}
	start := (page - 1) * pageSize
	if start > len(matched) {
		start = len(matched)
	}
	end := start + pageSize
	if end > len(matched) {
		end = len(matched)
	}
	return matched[start:end], len(matched), nil
}

// ── NodeHistoryReader ──

func (s *cpStore) GetNodeStatusHistory(_ context.Context, _ string, _ int) ([]metadata.NodeStatusHistoryRow, error) {
	return []metadata.NodeStatusHistoryRow{}, nil
}

// ─── Remaining narrow fakes ───

// cpPinOrchestrator satisfies the adminapi.PinOrchestrator seam by recording
// every manual plan into the REAL DispatchLog, so the pin → pin-plans chain
// exercises the actual bookkeeping surface.
type cpPinOrchestrator struct {
	mu      sync.Mutex
	nextSeq uint64
	dl      *pinstrategy.DispatchLog
}

func (f *cpPinOrchestrator) SendManualPlan(contentID string, targets []string, pinBlobs, unpinBlobs []string) ([]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seqs := make([]uint64, 0, len(targets))
	for _, target := range targets {
		f.nextSeq++
		f.dl.Add(pinstrategy.DispatchRecord{
			Seq:        f.nextSeq,
			TargetNode: target,
			ContentID:  contentID,
			Pins:       len(pinBlobs),
			Unpins:     len(unpinBlobs),
			Trigger:    pinstrategy.TriggerManual,
			SentAt:     time.Now(),
		})
		seqs = append(seqs, f.nextSeq)
	}
	return seqs, nil
}

// cpPromReader is the mock OverviewPromReader (no real Prometheus).
type cpPromReader struct{}

func (cpPromReader) TTFBP95(context.Context) (float64, bool, error)             { return 123.0, true, nil }
func (cpPromReader) CacheHitRate(context.Context) (float64, bool, error)        { return 0.75, true, nil }
func (cpPromReader) BackhaulBandwidthBps(context.Context) (float64, bool, error) { return 1e6, true, nil }
func (cpPromReader) QueryScalar(context.Context, string) (float64, bool, error) { return 0.99, true, nil }

// cpBroadcaster records broadcast event types (SyncBroadcaster seam for the
// circuit handler + QuotaAllocator).
type cpBroadcaster struct {
	mu     sync.Mutex
	events []string
}

func (b *cpBroadcaster) Broadcast(eventType string, _ any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, eventType)
	return nil
}

// cpQuotaWriter mirrors main.go's quotaAwareAccountsWriter (todo 53 linkage):
// successful rate_limit writes propagate into the allocator. The mirror lets
// the chain assert the hook through the real handler → wrapper → qa →
// GET /v1/admin/quota path.
type cpQuotaWriter struct {
	adminapi.AdminAccountsWriter
	qa *quota.QuotaAllocator
}

func (w cpQuotaWriter) CreateAccount(ctx context.Context, info accountregistry.AccountInfo) error {
	if err := w.AdminAccountsWriter.CreateAccount(ctx, info); err != nil {
		return err
	}
	if info.RateLimitCfg != (types.RateLimitConfig{}) {
		w.qa.SetGlobalLimit(string(info.Vendor)+":"+info.AccountID, info.RateLimitCfg)
	}
	return nil
}

func (w cpQuotaWriter) SetRateLimit(ctx context.Context, vendor types.Vendor, accountID string, cfg types.RateLimitConfig) error {
	if err := w.AdminAccountsWriter.SetRateLimit(ctx, vendor, accountID, cfg); err != nil {
		return err
	}
	w.qa.SetGlobalLimit(string(vendor)+":"+accountID, cfg)
	return nil
}

// ─── Fixture ───

var cpAdminSecret = []byte("integration-cp-admin-secret")

const (
	cpSeedUser     = "admin"
	cpSeedPassword = "pw-admin"
	cpAlertToken   = "integration-alert-token"
)

type cpFixture struct {
	addr        string
	store       *cpStore
	dispatchLog *pinstrategy.DispatchLog
}

func newCPPeerID(t *testing.T) string {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateEd25519Key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("IDFromPrivateKey: %v", err)
	}
	return id.String()
}

// assembleCPAdmin builds the admin server with the same Register*Routes
// wiring as main.go's consolidated 13c block (SetAuditRecorder BEFORE
// RegisterAuthRoutes included).
func assembleCPAdmin(t *testing.T) *cpFixture {
	t.Helper()
	dir := t.TempDir()

	store := newCPStore()
	hash, err := bcrypt.GenerateFromPassword([]byte(cpSeedPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	store.users[cpSeedUser] = cpUser{userID: cpSeedUser, hash: string(hash), roles: []string{"admin"}}
	store.accounts["baidu:mw_01"] = &cpAccount{
		info: accountregistry.AccountInfo{
			Vendor:       types.VendorBaidu,
			AccountID:    "mw_01",
			Credential:   types.Credential{RefreshToken: "rt-seed"},
			ClientConfig: types.ClientConfig{ClientID: "cid-seed", ClientSecret: "cs-seed"},
			RateLimitCfg: types.RateLimitConfig{QPS: 2, Burst: 4, ConcurrentLimit: 8},
			Enabled:      true,
		},
		health: &metadata.HealthView{State: "healthy", LatencyMs: 42, LastCheck: time.Now()},
	}
	store.contents["content-aaaa"] = &cpContent{
		meta: types.ContentMeta{ContentID: "content-aaaa", ContentType: "dash_video", Title: "chain video"},
		blobs: []types.BlobDescriptor{
			{BlobHash: "blob-init-1", BlobType: "mp4_init_segment", Size: 100},
			{BlobHash: "blob-media-1", BlobType: "m4s_media_segment", Size: 200},
		},
		roles: []types.BlobRole{
			{BlobHash: "blob-init-1", Role: "init"},
			{BlobHash: "blob-media-1", Role: "media", SortOrder: 1},
		},
		row: metadata.AdminContentRow{
			ContentID: "content-aaaa", Title: "chain video", ContentType: "dash_video",
			TotalBytes: 300, BlobCount: 2, ReplicasHave: 2, Window24h: 7,
		},
	}
	store.vendorProfiles = []metadata.VendorProfileRow{
		{Vendor: "baidu", VendorProfile: types.VendorProfile{Vendor: types.VendorBaidu, Weight: 1, BaseLatencyMs: 120, BandwidthMbps: 100}},
	}

	nodeReg := noderegistry.NewRegistry()
	nodeReg.UpsertReport(types.NodeStatusReport{
		NodeID:       "node-1",
		PeerID:       types.PeerId("peer-1"),
		Capabilities: types.NodeCapabilities{Edge: true},
		PrefixSpace:  types.PartitionStatus{TotalBytes: 1 << 40, UsedBytes: 1 << 20},
		Healthy:      true,
		LastUpdate:   time.Now().Unix(),
	})
	dispatchLog := pinstrategy.NewDispatchLog()

	wlStore, err := cpjwt.NewWhitelistStore(filepath.Join(dir, "wl.db"))
	if err != nil {
		t.Fatalf("NewWhitelistStore: %v", err)
	}
	t.Cleanup(func() { _ = wlStore.Close() })
	ps := cpjwt.NewPeerIdSet()
	if err := wlStore.Restore(ps); err != nil {
		t.Fatalf("whitelist restore: %v", err)
	}

	bc := &cpBroadcaster{}
	qa := quota.NewQuotaAllocator(bc)
	fakePO := &cpPinOrchestrator{dl: dispatchLog}

	srv := adminapi.NewServer(cpAdminSecret)
	// Registration-time capture: the audit recorder must precede auth routes.
	auditRec := adminapi.NewPGAuditRecorder(store)
	srv.SetAuditRecorder(auditRec)
	jwtAudit := cpjwt.NewAuditLog(io.Discard)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	adminapi.RegisterNodesRoutes(srv, nodeReg, store, dispatchLog, logger, time.Now)
	adminapi.RegisterPinPlansRoutes(srv, dispatchLog, nodeReg)
	adminapi.RegisterWhitelistRoutes(srv, wlStore, ps, nodeReg, auditRec)
	adminapi.RegisterQuotaRoutes(srv, qa, nodeReg)
	adminapi.RegisterFormSchemaRoutes(srv)
	adminapi.RegisterAuthRoutes(srv, store)
	adminapi.RegisterAccountsRoutes(srv, store, store, cpQuotaWriter{AdminAccountsWriter: store, qa: qa}, bc, auditRec)
	adminapi.RegisterPinRoutes(srv, store, nodeReg, fakePO, auditRec)
	adminapi.RegisterContentsRoutes(srv, struct {
		adminapi.ContentsListReader
		adminapi.ContentsDetailReader
		adminapi.ContentMetaReader
	}{store, store, store}, dispatchLog, store, auditRec)
	adminapi.RegisterAlertsRoutes(srv, store, cpAlertToken)
	adminapi.RegisterAccountTestRoutes(srv, accounttester.NewTester(store, adminapi.ValidateAuth, nil))
	adminapi.RegisterOverviewRoutes(srv, adminapi.OverviewDeps{
		Prom:     cpPromReader{},
		Metadata: store,
		Registry: nodeReg,
		Dispatch: dispatchLog,
	})
	adminapi.RegisterAuditRoutes(srv, jwtAudit, store)

	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := srv.Serve(ctx, addr); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("admin serve: %v", err)
		}
	}()

	// Deterministic readiness: poll an auth-gated route for its 401.
	base := "http://" + addr
	deadline := time.Now().Add(2 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodGet, base+"/v1/admin/nodes", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("admin server did not become ready on %s", addr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return &cpFixture{addr: addr, store: store, dispatchLog: dispatchLog}
}

func cpDo(t *testing.T, method, url, token string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = strings.NewReader(string(raw))
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, data
}

func cpDecode[T any](t *testing.T, data []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("decode %s: %v", string(data), err)
	}
	return v
}

// ─── Wire decode shapes (chain assertions only — not handler field contracts) ───

type cpLoginWire struct {
	Token string   `json:"token"`
	Roles []string `json:"roles"`
}

type cpAccountRowWire struct {
	Vendor    string `json:"vendor"`
	AccountID string `json:"account_id"`
	Enabled   bool   `json:"enabled"`
	Health    *struct {
		State string `json:"state"`
	} `json:"health"`
	RateLimit struct {
		QPS float64 `json:"qps"`
	} `json:"rate_limit"`
}

type cpAccountsWire struct {
	Accounts []cpAccountRowWire `json:"accounts"`
}

type cpWhitelistWire struct {
	PeerID string `json:"peer_id"`
}

type cpContentsWire struct {
	Contents []struct {
		ContentID string `json:"content_id"`
	} `json:"contents"`
}

type cpPinWire struct {
	Seq []uint64 `json:"seq"`
}

type cpPinPlanWire struct {
	TargetNode string `json:"target_node"`
	ContentID  string `json:"content_id"`
}

type cpOverviewWire struct {
	SLO struct {
		CacheHitRate *float64 `json:"cache_hit_rate"`
	} `json:"slo"`
	Nodes struct {
		Total int `json:"total"`
	} `json:"nodes"`
}

type cpQuotaWire struct {
	GlobalQPS float64 `json:"global_qps"`
}

// TestAdminAPI_EndToEnd walks the seven-step CP admin chain (plan todo 54 /
// docs F3 acceptance): login → accounts CRUD → ban → whitelist add/remove →
// contents list → manual pin → audit contains all writes → overview 200.
func TestAdminAPI_EndToEnd(t *testing.T) {
	fx := assembleCPAdmin(t)
	base := "http://" + fx.addr

	// Given no bearer token, when any /v1/admin/* route is called, then 401
	// (docs/next-iteration-requirements.md:86 acceptance).
	st, _ := cpDo(t, http.MethodGet, base+"/v1/admin/accounts", "", nil)
	if st != http.StatusUnauthorized {
		t.Fatalf("no-token GET /v1/admin/accounts = %d, want 401", st)
	}

	// Step 1: login issues a REAL usertoken (bcrypt-verified user store,
	// HS256 signing). Given seeded admin credentials, when login, then 200 +
	// verifiable token carrying the admin role.
	st, body := cpDo(t, http.MethodPost, base+"/v1/auth/login", "", map[string]any{
		"username": cpSeedUser, "password": cpSeedPassword,
	})
	if st != http.StatusOK {
		t.Fatalf("login = %d, want 200: %s", st, string(body))
	}
	login := cpDecode[cpLoginWire](t, body)
	claims, err := adminapi.VerifyUserToken(login.Token, cpAdminSecret)
	if err != nil {
		t.Fatalf("issued token does not verify: %v", err)
	}
	if claims.Username != cpSeedUser || len(claims.Roles) != 1 || claims.Roles[0] != "admin" {
		t.Fatalf("claims = %+v, want admin user with admin role", claims)
	}
	token := login.Token

	// Step 2: accounts CRUD. Given a valid create body, when POST, then 201;
	// when PUT a new rate_limit, then 202 and the change is visible on read
	// AND reaches the quota allocator (todo 53 hook); when GET, then the row
	// reflects the update.
	st, body = cpDo(t, http.MethodPost, base+"/v1/admin/accounts", token, map[string]any{
		"vendor":     "baidu",
		"account_id": "mw_02",
		"enabled":    true,
		"rate_limit": map[string]any{"qps": 5, "burst": 10, "concurrent_limit": 2},
		"auth":       map[string]any{"client_id": "cid2", "client_secret": "cs2", "refresh_token": "rt2"},
	})
	if st != http.StatusCreated {
		t.Fatalf("create account = %d, want 201: %s", st, string(body))
	}
	st, body = cpDo(t, http.MethodPut, base+"/v1/admin/accounts/baidu/mw_02", token, map[string]any{
		"rate_limit": map[string]any{"qps": 9, "burst": 9, "concurrent_limit": 3},
	})
	if st != http.StatusAccepted {
		t.Fatalf("update account = %d, want 202: %s", st, string(body))
	}
	st, body = cpDo(t, http.MethodGet, base+"/v1/admin/accounts", token, nil)
	if st != http.StatusOK {
		t.Fatalf("list accounts = %d, want 200", st)
	}
	accounts := cpDecode[cpAccountsWire](t, body)
	mw02 := -1
	for i, a := range accounts.Accounts {
		if a.Vendor == "baidu" && a.AccountID == "mw_02" {
			mw02 = i
		}
	}
	if mw02 < 0 {
		t.Fatalf("created account mw_02 absent from list: %s", string(body))
	}
	if accounts.Accounts[mw02].RateLimit.QPS != 9 {
		t.Fatalf("mw_02 rate_limit.qps = %v, want 9 after PUT", accounts.Accounts[mw02].RateLimit.QPS)
	}
	// Todo 53 linkage: PUT rate_limit reached the allocator (create's 5 was
	// replaced by the update's 9).
	st, body = cpDo(t, http.MethodGet, base+"/v1/admin/quota", token, nil)
	if st != http.StatusOK {
		t.Fatalf("quota = %d, want 200", st)
	}
	if q := cpDecode[cpQuotaWire](t, body); q.GlobalQPS != 9 {
		t.Fatalf("quota global_qps = %v, want 9 (rate_limit hook)", q.GlobalQPS)
	}

	// Step 3: ban. Given an existing healthy account, when POST ban, then
	// 202; and the M3 acceptance — the CP-side account health view reads
	// banned on the immediate next read (node-side 10s propagation: todo 36).
	st, body = cpDo(t, http.MethodPost, base+"/v1/admin/accounts/baidu/mw_01/ban", token, map[string]any{
		"reason": "abuse report",
	})
	if st != http.StatusAccepted {
		t.Fatalf("ban = %d, want 202: %s", st, string(body))
	}
	st, body = cpDo(t, http.MethodGet, base+"/v1/admin/accounts?vendor=baidu&state=banned", token, nil)
	if st != http.StatusOK {
		t.Fatalf("list banned accounts = %d, want 200", st)
	}
	banned := cpDecode[cpAccountsWire](t, body)
	m3 := false
	for _, a := range banned.Accounts {
		if a.AccountID == "mw_01" && a.Health != nil && a.Health.State == "banned" {
			m3 = true
		}
	}
	if !m3 {
		t.Fatalf("M3: mw_01 health view not banned after ban: %s", string(body))
	}

	// Step 4: whitelist add/remove. Given a valid peer_id, when POST, then
	// 201 and the entry lists; when DELETE, then 204 and the entry is gone.
	pid := newCPPeerID(t)
	st, body = cpDo(t, http.MethodPost, base+"/v1/admin/whitelist", token, map[string]any{"peer_id": pid})
	if st != http.StatusCreated {
		t.Fatalf("whitelist add = %d, want 201: %s", st, string(body))
	}
	st, body = cpDo(t, http.MethodGet, base+"/v1/admin/whitelist", token, nil)
	if st != http.StatusOK {
		t.Fatalf("whitelist list = %d, want 200", st)
	}
	entries := cpDecode[[]cpWhitelistWire](t, body)
	found := false
	for _, e := range entries {
		if e.PeerID == pid {
			found = true
		}
	}
	if !found {
		t.Fatalf("added peer %s absent from whitelist: %s", pid, string(body))
	}
	st, _ = cpDo(t, http.MethodDelete, base+"/v1/admin/whitelist/"+pid, token, nil)
	if st != http.StatusNoContent {
		t.Fatalf("whitelist delete = %d, want 204", st)
	}
	_, body = cpDo(t, http.MethodGet, base+"/v1/admin/whitelist", token, nil)
	entries = cpDecode[[]cpWhitelistWire](t, body)
	for _, e := range entries {
		if e.PeerID == pid {
			t.Fatalf("peer %s still present after DELETE", pid)
		}
	}

	// Step 5: contents list. Given a seeded content, when GET, then 200 and
	// the content is present.
	st, body = cpDo(t, http.MethodGet, base+"/v1/admin/contents", token, nil)
	if st != http.StatusOK {
		t.Fatalf("contents list = %d, want 200", st)
	}
	contents := cpDecode[cpContentsWire](t, body)
	found = false
	for _, c := range contents.Contents {
		if c.ContentID == "content-aaaa" {
			found = true
		}
	}
	if !found {
		t.Fatalf("seeded content absent from list: %s", string(body))
	}

	// Step 6: manual pin. Given a registered node with space, when POST pin,
	// then 202 with seqs; and the REAL DispatchLog backs GET pin-plans with
	// the dispatched record.
	st, body = cpDo(t, http.MethodPost, base+"/v1/admin/pin", token, map[string]any{
		"content_id":   "content-aaaa",
		"target_nodes": []string{"node-1"},
	})
	if st != http.StatusAccepted {
		t.Fatalf("manual pin = %d, want 202: %s", st, string(body))
	}
	if pin := cpDecode[cpPinWire](t, body); len(pin.Seq) == 0 {
		t.Fatalf("manual pin returned no seqs: %s", string(body))
	}
	st, body = cpDo(t, http.MethodGet, base+"/v1/admin/pin-plans", token, nil)
	if st != http.StatusOK {
		t.Fatalf("pin-plans = %d, want 200", st)
	}
	plans := cpDecode[[]cpPinPlanWire](t, body)
	found = false
	for _, p := range plans {
		if p.ContentID == "content-aaaa" && p.TargetNode == "node-1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("manual pin record absent from pin-plans (DispatchLog linkage): %s", string(body))
	}

	// Step 7: audit chain. Every write above funnels through PGAuditRecorder
	// into admin_audit rows; the todo-34 query endpoint must surface them all
	// (kind=admin reads the same fake rows the recorder wrote).
	st, body = cpDo(t, http.MethodGet, base+"/v1/admin/audit?kind=admin&page_size=100", token, nil)
	if st != http.StatusOK {
		t.Fatalf("audit query = %d, want 200: %s", st, string(body))
	}
	type auditEntryWire struct {
		Kind   string `json:"kind"`
		Action string `json:"action"`
		Result string `json:"result"`
	}
	auditResp := cpDecode[struct {
		Entries []auditEntryWire `json:"entries"`
		Total   int              `json:"total"`
	}](t, body)
	for _, want := range [][2]string{
		{"auth", "login"},
		{"account", "create"},
		{"account", "update"},
		{"account", "ban"},
		{"whitelist", "add"},
		{"whitelist", "remove"},
		{"pin", "pin"},
	} {
		found := false
		for _, e := range auditResp.Entries {
			if e.Kind == want[0] && e.Action == want[1] && e.Result == "ok" {
				found = true
			}
		}
		if !found {
			t.Fatalf("audit query missing kind=%s action=%s ok entry: %s", want[0], want[1], string(body))
		}
	}
	// The recorder contract itself (captured rows == what the endpoint read).
	if rows := fx.store.auditSnapshot(); len(rows) != auditResp.Total {
		t.Fatalf("recorder rows (%d) != audit query total (%d)", len(rows), auditResp.Total)
	}

	// Step 8: overview 200 (five-source fan-out with mock Prom + fake PG).
	st, body = cpDo(t, http.MethodGet, base+"/v1/admin/overview", token, nil)
	if st != http.StatusOK {
		t.Fatalf("overview = %d, want 200: %s", st, string(body))
	}
	overview := cpDecode[cpOverviewWire](t, body)
	if overview.SLO.CacheHitRate == nil || *overview.SLO.CacheHitRate != 0.75 {
		t.Fatalf("overview cache_hit_rate = %v, want 0.75 (mock prom)", overview.SLO.CacheHitRate)
	}
	if overview.Nodes.Total != 1 {
		t.Fatalf("overview nodes.total = %d, want 1", overview.Nodes.Total)
	}

	// Assembly smoke: the remaining mounted GETs answer 200 — routing + auth
	// proof only (field contracts are unit-tested per handler).
	for _, path := range []string{
		"/v1/admin/nodes",
		"/v1/admin/nodes/peer-1",
		"/v1/admin/vendors/form-schema",
		"/v1/admin/vendor-profiles",
		"/v1/admin/alerts",
	} {
		if st, body := cpDo(t, http.MethodGet, base+path, token, nil); st != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200: %s", path, st, string(body))
		}
	}
}
