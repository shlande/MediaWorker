package accountpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/storage/driver/mock"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── test helpers ───

// mockCB implements CircuitBreaker for testing.
type mockCB struct {
	state int
}

func (m *mockCB) State() int  { return m.state }
func (m *mockCB) ForceOpen()  { m.state = StateOpen }
func (m *mockCB) ForceClose() { m.state = StateClosed }

func newMockCB(state int) *mockCB { return &mockCB{state: state} }

// mockBlobLocationClient implements BlobLocationClient for testing.
type mockBlobLocationClient struct {
	locations map[string][]types.BlobLocation
	err       error
}

func (m *mockBlobLocationClient) GetBlobLocations(ctx context.Context, blobHash string) ([]types.BlobLocation, error) {
	if m.err != nil {
		return nil, m.err
	}
	locs, ok := m.locations[blobHash]
	if !ok {
		return nil, nil
	}
	return locs, nil
}

// callTrackingDriver wraps a mock.MockDriver to track GetLink call count.
type callTrackingDriver struct {
	*mock.MockDriver
	getLinkCalls atomic.Int32
}

func (d *callTrackingDriver) GetLink(ctx context.Context, fileID string) (*types.DownloadLink, error) {
	d.getLinkCalls.Add(1)
	return d.MockDriver.GetLink(ctx, fileID)
}

func newAccount(vendor string, accID string, driver *mock.MockDriver, weight float64, lim Limiter, cb CircuitBreaker) *Account {
	h := types.HealthState{State: "healthy"}
	a := &Account{
		Vendor:       types.Vendor(vendor),
		AccountID:    accID,
		Driver:       driver,
		Limiter:      lim,
		CB:           cb,
		VendorWeight: weight,
	}
	a.Health.Store(h)
	return a
}

// alwaysDenyLimiter rejects every Allow call. Used for CB open tests.
var alwaysDenyLimiter Limiter = &denyLimiter{}

type denyLimiter struct{}

func (d *denyLimiter) Allow() bool         { return false }
func (d *denyLimiter) SetLimit(rate.Limit) {}

// ─── SelectForRead tests ───

func TestSelectForRead_selectsHealthyLowLoad(t *testing.T) {
	ctx := context.Background()

	// 2 locations for the same blob: 115:acct1 and baidu:acct2
	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash1": {
				{BackendID: "115:acct1", FileID: "fid1"},
				{BackendID: "baidu:acct2", FileID: "fid2"},
			},
		},
	}
	pool := NewAccountPool(mc)

	baiduDrv := mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{
		RateLimit: types.RateLimitConfig{QPS: 10, Burst: 20, ConcurrentLimit: 16},
	})

	// acct1: 115, weight=3, concurrent=10 → score = 10/3 = 3.33
	acct1 := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct1.Concurrent.Store(10)

	// acct2: baidu, weight=2, concurrent=4 → score = 4/2 = 2.0 (better)
	acct2 := newAccount(string(types.VendorBaidu), "acct2", baiduDrv, 2.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct2.Concurrent.Store(4)

	pool.AddAccount(acct1)
	pool.AddAccount(acct2)

	got, err := pool.SelectForRead(ctx, "hash1")
	if err != nil {
		t.Fatalf("SelectForRead: unexpected error: %v", err)
	}
	if got.AccountID != "acct2" {
		t.Errorf("SelectForRead = %q (baidu), want %q (baidu, lower load/weight ratio)", got.AccountID, "acct2")
	}
}

func TestSelectForRead_CBOpen_skipsAccount(t *testing.T) {
	ctx := context.Background()

	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash2": {
				{BackendID: "115:acct1", FileID: "fid1"},
				{BackendID: "baidu:acct2", FileID: "fid2"},
			},
		},
	}
	pool := NewAccountPool(mc)

	// acct1: CB open → should be skipped
	acct1 := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateOpen))
	acct1.Concurrent.Store(0)

	acct2 := newAccount(string(types.VendorBaidu), "acct2", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct2.Concurrent.Store(0)

	pool.AddAccount(acct1)
	pool.AddAccount(acct2)

	got, err := pool.SelectForRead(ctx, "hash2")
	if err != nil {
		t.Fatalf("SelectForRead: unexpected error: %v", err)
	}
	if got.AccountID != "acct2" {
		t.Errorf("SelectForRead with CB open = %q, want %q", got.AccountID, "acct2")
	}
}

func TestSelectForRead_limiterDeny_skipsAccount(t *testing.T) {
	ctx := context.Background()

	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash3": {
				{BackendID: "115:acct1", FileID: "fid1"},
				{BackendID: "baidu:acct2", FileID: "fid2"},
			},
		},
	}
	pool := NewAccountPool(mc)

	// acct1: limiter denies → skipped
	acct1 := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, alwaysDenyLimiter, newMockCB(StateClosed))

	acct2 := newAccount(string(types.VendorBaidu), "acct2", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))

	pool.AddAccount(acct1)
	pool.AddAccount(acct2)

	got, err := pool.SelectForRead(ctx, "hash3")
	if err != nil {
		t.Fatalf("SelectForRead: unexpected error: %v", err)
	}
	if got.AccountID != "acct2" {
		t.Errorf("SelectForRead with limiter deny = %q, want %q", got.AccountID, "acct2")
	}
}

func TestSelectForRead_concurrentLimit_skipsAccount(t *testing.T) {
	ctx := context.Background()

	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash4": {
				{BackendID: "115:acct1", FileID: "fid1"},
				{BackendID: "baidu:acct2", FileID: "fid2"},
			},
		},
	}
	pool := NewAccountPool(mc)

	// acct1: concurrent >= concurrent limit (default for 115 is 5)
	acct1 := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct1.Concurrent.Store(5)

	acct2 := newAccount(string(types.VendorBaidu), "acct2", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct2.Concurrent.Store(0)

	pool.AddAccount(acct1)
	pool.AddAccount(acct2)

	got, err := pool.SelectForRead(ctx, "hash4")
	if err != nil {
		t.Fatalf("SelectForRead: unexpected error: %v", err)
	}
	if got.AccountID != "acct2" {
		t.Errorf("SelectForRead with concurrent limit = %q, want %q", got.AccountID, "acct2")
	}
}

func TestSelectForRead_allBanned_returnsError(t *testing.T) {
	ctx := context.Background()

	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash5": {
				{BackendID: "115:acct1", FileID: "fid1"},
				{BackendID: "baidu:acct2", FileID: "fid2"},
			},
		},
	}
	pool := NewAccountPool(mc)

	acct1 := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct1.Health.Store(types.HealthState{State: "banned"})

	acct2 := newAccount(string(types.VendorBaidu), "acct2", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), newMockCB(StateOpen))
	acct2.Health.Store(types.HealthState{State: "degraded"})

	pool.AddAccount(acct1)
	pool.AddAccount(acct2)

	_, err := pool.SelectForRead(ctx, "hash5")
	if err == nil {
		t.Fatal("SelectForRead: expected error when all accounts are banned, got nil")
	}
}

func TestSelectForRead_taintSemantics(t *testing.T) {
	ctx := context.Background()

	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash6": {
				{BackendID: "115:acct1", FileID: "fid1"},
				{BackendID: "baidu:acct2", FileID: "fid2"},
			},
		},
	}
	pool := NewAccountPool(mc)

	acct1 := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct1.Health.Store(types.HealthState{State: "degraded"})

	acct2 := newAccount(string(types.VendorBaidu), "acct2", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct2.Concurrent.Store(1)

	pool.AddAccount(acct1)
	pool.AddAccount(acct2)

	// degraded is informational (no taint): acct1 stays eligible and wins the
	// score tie-break (0/3.0 < 1/2.0).
	got, err := pool.SelectForRead(ctx, "hash6")
	if err != nil {
		t.Fatalf("SelectForRead: unexpected error: %v", err)
	}
	if got.AccountID != "acct1" {
		t.Errorf("SelectForRead with degraded = %q, want %q (degraded must stay eligible)", got.AccountID, "acct1")
	}

	// banned is a taint: acct1 is excluded, acct2 wins.
	acct1.Health.Store(types.HealthState{State: "banned"})
	got, err = pool.SelectForRead(ctx, "hash6")
	if err != nil {
		t.Fatalf("SelectForRead: unexpected error: %v", err)
	}
	if got.AccountID != "acct2" {
		t.Errorf("SelectForRead with banned = %q, want %q", got.AccountID, "acct2")
	}
}

func TestSelectForRead_noLocations_returnsError(t *testing.T) {
	ctx := context.Background()

	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash1": {},
		},
	}
	pool := NewAccountPool(mc)
	pool.AddAccount(newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed)))

	_, err := pool.SelectForRead(ctx, "hash1")
	if err == nil {
		t.Fatal("SelectForRead: expected error for empty locations, got nil")
	}
}

func TestSelectForRead_noAccountInPool_returnsError(t *testing.T) {
	ctx := context.Background()

	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash1": {
				{BackendID: "115:nonexistent", FileID: "fid1"},
			},
		},
	}
	pool := NewAccountPool(mc)

	_, err := pool.SelectForRead(ctx, "hash1")
	if err == nil {
		t.Fatal("SelectForRead: expected error when location account not in pool, got nil")
	}
}

func TestSelectForRead_metadataError_returnsError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("metadata unavailable")
	mc := &mockBlobLocationClient{err: expectedErr}
	pool := NewAccountPool(mc)

	_, err := pool.SelectForRead(ctx, "hash1")
	if err == nil {
		t.Fatal("SelectForRead: expected error from metadata client, got nil")
	}
}

// ─── SelectForRead does NOT call Driver.GetLink ───

func TestSelectForRead_doesNotCallGetLink(t *testing.T) {
	ctx := context.Background()

	mc := &mockBlobLocationClient{
		locations: map[string][]types.BlobLocation{
			"hash_nolink": {
				{BackendID: "115:acct1", FileID: "fid1"},
			},
		},
	}
	pool := NewAccountPool(mc)

	trkDrv := &callTrackingDriver{
		MockDriver: mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}),
	}
	acct := newAccount(string(types.Vendor115), "acct1", trkDrv.MockDriver, 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))

	// Put a file so GetLink would succeed if called
	_, _ = trkDrv.Put(ctx, "dir", "fid1", nil, 0)

	pool.AddAccount(acct)

	got, err := pool.SelectForRead(ctx, "hash_nolink")
	if err != nil {
		t.Fatalf("SelectForRead: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("SelectForRead: got nil Account")
	}
	if calls := trkDrv.getLinkCalls.Load(); calls != 0 {
		t.Errorf("SelectForRead called Driver.GetLink %d times, want 0", calls)
	}
}

// ─── SelectK tests ───

func TestSelectK_crossVendorPriority(t *testing.T) {
	ctx := context.Background()
	pool := NewAccountPool(&mockBlobLocationClient{})

	// 3 accounts from vendor115, 1 from baidu — should prefer cross-vendor
	for i := 0; i < 3; i++ {
		acct := newAccount(string(types.Vendor115), "acct115_"+string(rune('A'+i)), mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
		pool.AddAccount(acct)
	}
	acctBaidu := newAccount(string(types.VendorBaidu), "acct_baidu", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	pool.AddAccount(acctBaidu)

	selected, err := pool.SelectK(ctx, 2)
	if err != nil {
		t.Fatalf("SelectK: unexpected error: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("SelectK: got %d accounts, want 2", len(selected))
	}

	// Should have one 115 and one baidu (cross-vendor)
	vendors := make(map[types.Vendor]int)
	for _, a := range selected {
		vendors[a.Vendor]++
	}
	if vendors[types.Vendor115] != 1 || vendors[types.VendorBaidu] != 1 {
		t.Errorf("SelectK vendor distribution: got %v, want {115:1, baidu:1}", vendors)
	}
}

func TestSelectK_noHealthy_returnsError(t *testing.T) {
	ctx := context.Background()
	pool := NewAccountPool(&mockBlobLocationClient{})

	acct := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct.Health.Store(types.HealthState{State: "banned"})
	pool.AddAccount(acct)

	_, err := pool.SelectK(ctx, 2)
	if err == nil {
		t.Fatal("SelectK: expected error when no healthy accounts, got nil")
	}
}

func TestSelectK_emptyPool_returnsError(t *testing.T) {
	ctx := context.Background()
	pool := NewAccountPool(&mockBlobLocationClient{})

	_, err := pool.SelectK(ctx, 2)
	if err == nil {
		t.Fatal("SelectK: expected error for empty pool, got nil")
	}
}

func TestSelectK_returnsAllIfLessThanK(t *testing.T) {
	ctx := context.Background()
	pool := NewAccountPool(&mockBlobLocationClient{})

	acct := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	pool.AddAccount(acct)

	selected, err := pool.SelectK(ctx, 5)
	if err != nil {
		t.Fatalf("SelectK: unexpected error: %v", err)
	}
	if len(selected) != 1 {
		t.Errorf("SelectK: got %d accounts, want 1 (pool only has 1)", len(selected))
	}
}

// ─── UploadBlob tests ───

func TestUploadSegment_concurrentUpload(t *testing.T) {
	ctx := context.Background()
	pool := NewAccountPool(&mockBlobLocationClient{})

	d115 := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{})
	dBaidu := mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{})

	acct115 := newAccount(string(types.Vendor115), "acct1", d115, 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acctBaidu := newAccount(string(types.VendorBaidu), "acct2", dBaidu, 2.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))

	pool.AddAccount(acct115)
	pool.AddAccount(acctBaidu)

	data := []byte("test segment data")
	err := pool.UploadBlob(ctx, "seg123", data)
	if err != nil {
		t.Fatalf("UploadBlob: unexpected error: %v", err)
	}

	// Both accounts should have the file
	for _, d := range []*mock.MockDriver{d115, dBaidu} {
		fi, err := d.Get(ctx, "seg123/seg123.bin")
		if err != nil {
			t.Errorf("UploadSegment: Get on %s: %v", d.Vendor(), err)
		}
		if fi.Size != int64(len(data)) {
			t.Errorf("UploadSegment: size on %s = %d, want %d", d.Vendor(), fi.Size, len(data))
		}
	}
}

func TestUploadSegment_oneAccountHealthy_usesSingleAccount(t *testing.T) {
	ctx := context.Background()
	pool := NewAccountPool(&mockBlobLocationClient{})

	d115 := mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{})

	acct115 := newAccount(string(types.Vendor115), "acct1", d115, 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	pool.AddAccount(acct115)

	err := pool.UploadBlob(ctx, "seg456", []byte("data"))
	if err != nil {
		t.Fatalf("UploadBlob: unexpected error: %v", err)
	}

	fi, err := d115.Get(ctx, "seg456/seg456.bin")
	if err != nil {
		t.Errorf("UploadSegment: Get on 115: %v", err)
	}
	if fi.Size != 4 {
		t.Errorf("UploadSegment: size = %d, want 4", fi.Size)
	}
}

func TestUploadSegment_noHealthyAccounts_returnsError(t *testing.T) {
	ctx := context.Background()
	pool := NewAccountPool(&mockBlobLocationClient{})

	acct := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	acct.Health.Store(types.HealthState{State: "banned"})
	pool.AddAccount(acct)

	err := pool.UploadBlob(ctx, "seg789", []byte("data"))
	if err == nil {
		t.Fatal("UploadBlob: expected error when no healthy accounts, got nil")
	}
}

// ─── ReplaceAll / UpdateCredential / UpdateHealth / MarkBanned tests ───

func TestReplaceAll(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	pool.AddAccount(newAccount(string(types.Vendor115), "old", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed)))

	// Replace with new set
	newAccounts := []Account{
		{
			Vendor:    types.Vendor115,
			AccountID: "new1",
			Driver:    mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}),
		},
		{
			Vendor:    types.VendorBaidu,
			AccountID: "new2",
			Driver:    mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}),
		},
	}
	for i := range newAccounts {
		newAccounts[i].Limiter = rate.NewLimiter(10, 20)
		newAccounts[i].CB = newMockCB(StateClosed)
		newAccounts[i].VendorWeight = 2.0
		newAccounts[i].Health.Store(types.HealthState{State: "healthy"})
	}

	pool.ReplaceAll(newAccounts)

	if len(pool.accounts) != 2 {
		t.Fatalf("ReplaceAll: got %d accounts, want 2", len(pool.accounts))
	}
	if _, ok := pool.accounts["115:new1"]; !ok {
		t.Error("ReplaceAll: missing 115:new1")
	}
	if _, ok := pool.accounts["baidu:new2"]; !ok {
		t.Error("ReplaceAll: missing baidu:new2")
	}
	if _, ok := pool.accounts["115:old"]; ok {
		t.Error("ReplaceAll: old account should be removed")
	}
}

func TestUpdateCredential(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	acct := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	pool.AddAccount(acct)

	newCred := types.Credential{AccessToken: "new_token"}
	pool.UpdateCredential("115:acct1", newCred)

	if acct.Credential.AccessToken != "new_token" {
		t.Errorf("UpdateCredential: AccessToken = %q, want %q", acct.Credential.AccessToken, "new_token")
	}
}

func TestUpdateCredential_nonexistentAccount_noop(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	// Should not panic
	pool.UpdateCredential("115:nonexistent", types.Credential{AccessToken: "x"})
}

func TestUpdateHealth(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	acct := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	pool.AddAccount(acct)

	newHealth := types.HealthState{State: "degraded", ErrorMsg: "high latency"}
	pool.UpdateHealth("115:acct1", newHealth)

	h := acct.Health.Load().(types.HealthState)
	if h.State != "degraded" {
		t.Errorf("UpdateHealth: State = %q, want %q", h.State, "degraded")
	}
	if h.ErrorMsg != "high latency" {
		t.Errorf("UpdateHealth: ErrorMsg = %q, want %q", h.ErrorMsg, "high latency")
	}
}

func TestMarkBanned(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)
	cb := newMockCB(StateClosed)

	acct := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), cb)
	pool.AddAccount(acct)

	pool.MarkBanned("115:acct1")

	h := acct.Health.Load().(types.HealthState)
	if h.State != "banned" {
		t.Errorf("MarkBanned: Health.State = %q, want %q", h.State, "banned")
	}
	if cb.State() != StateOpen {
		t.Errorf("MarkBanned: CB.State = %d, want %d (StateOpen)", cb.State(), StateOpen)
	}
}

func TestMarkBanned_nonexistentAccount_noop(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	// Should not panic
	pool.MarkBanned("115:nonexistent")
}

func TestMarkBanned_noCB_stillWorks(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	acct := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), nil)
	pool.AddAccount(acct)

	pool.MarkBanned("115:acct1")

	h := acct.Health.Load().(types.HealthState)
	if h.State != "banned" {
		t.Errorf("MarkBanned (no CB): Health.State = %q, want %q", h.State, "banned")
	}
}

func TestAddAccount_replacesExisting(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	a1 := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 3.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	a1.VendorWeight = 3.0
	pool.AddAccount(a1)

	a2 := newAccount(string(types.Vendor115), "acct1", mock.NewMockDriver(types.Vendor115, mock.MockDriverConfig{}), 5.0, rate.NewLimiter(10, 20), newMockCB(StateClosed))
	a2.VendorWeight = 5.0
	pool.AddAccount(a2)

	if pool.accounts["115:acct1"].VendorWeight != 5.0 {
		t.Errorf("AddAccount replace: weight = %f, want 5.0", pool.accounts["115:acct1"].VendorWeight)
	}
}

func TestNewAccountPool(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)
	if pool == nil {
		t.Fatal("NewAccountPool: got nil")
	}
	if pool.accounts == nil {
		t.Error("NewAccountPool: accounts map not initialized")
	}
	if pool.vendors == nil {
		t.Error("NewAccountPool: vendors map not initialized")
	}
	if pool.metadata != mc {
		t.Error("NewAccountPool: metadata client not set")
	}
}

// ─── ForceCircuit / ForceCloseCircuit tests ───

func TestForceCircuit_openAndClose(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)
	cb := newMockCB(StateClosed)

	acct := newAccount(string(types.VendorBaidu), "acct1", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), cb)
	pool.AddAccount(acct)

	pool.ForceCircuit(string(types.VendorBaidu), "acct1", true)
	if cb.State() != StateOpen {
		t.Errorf("ForceCircuit(open=true): CB.State = %d, want %d (StateOpen)", cb.State(), StateOpen)
	}

	pool.ForceCircuit(string(types.VendorBaidu), "acct1", false)
	if cb.State() != StateClosed {
		t.Errorf("ForceCircuit(open=false): CB.State = %d, want %d (StateClosed)", cb.State(), StateClosed)
	}
}

func TestForceCircuit_nonexistentAccount_noop(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	// Should not panic
	pool.ForceCircuit(string(types.VendorBaidu), "nonexistent", true)
	pool.ForceCircuit(string(types.VendorBaidu), "nonexistent", false)
}

func TestForceCloseCircuit(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)
	cb := newMockCB(StateClosed)

	acct := newAccount(string(types.VendorBaidu), "acct1", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), cb)
	pool.AddAccount(acct)

	pool.MarkBanned("baidu:acct1")
	if cb.State() != StateOpen {
		t.Fatalf("MarkBanned: CB.State = %d, want %d (StateOpen)", cb.State(), StateOpen)
	}

	pool.ForceCloseCircuit(string(types.VendorBaidu), "acct1")
	if cb.State() != StateClosed {
		t.Errorf("ForceCloseCircuit: CB.State = %d, want %d (StateClosed)", cb.State(), StateClosed)
	}
}

func TestForceCloseCircuit_nonexistentAccount_noop(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	// Should not panic
	pool.ForceCloseCircuit(string(types.VendorBaidu), "nonexistent")
}

func TestForceCircuit_nilCB_noop(t *testing.T) {
	mc := &mockBlobLocationClient{}
	pool := NewAccountPool(mc)

	acct := newAccount(string(types.VendorBaidu), "acct1", mock.NewMockDriver(types.VendorBaidu, mock.MockDriverConfig{}), 2.0, rate.NewLimiter(10, 20), nil)
	pool.AddAccount(acct)

	// Should not panic with nil CB
	pool.ForceCircuit(string(types.VendorBaidu), "acct1", true)
	pool.ForceCloseCircuit(string(types.VendorBaidu), "acct1")
}
