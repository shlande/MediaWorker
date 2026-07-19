package dataplane

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mock implementations ───

type mockAccountSelector struct {
	acct *accountpool.Account
	err  error
}

func (m *mockAccountSelector) SelectForRead(_ context.Context, blobHash string) (*accountpool.Account, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.acct, nil
}

type mockLinkFetcher struct {
	link *types.DownloadLink
	err  error
}

func (m *mockLinkFetcher) GetOrFetch(_ context.Context, _ driver.Driver, _ types.Vendor, _, _ string) (*types.DownloadLink, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.link, nil
}

type mockMetadataClient struct {
	locations []types.BlobLocation
	err       error
}

func (m *mockMetadataClient) GetBlobLocations(ctx context.Context, blobHash string) ([]types.BlobLocation, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.locations, nil
}

// mockDriver is a minimal driver.Driver for tests.
type mockDriver struct {
	vendor types.Vendor
}

func (m *mockDriver) Vendor() types.Vendor { return m.vendor }
func (m *mockDriver) List(ctx context.Context, dirID string, page int) ([]types.FileInfo, error) {
	return nil, nil
}
func (m *mockDriver) Get(ctx context.Context, fileID string) (types.FileInfo, error) {
	return types.FileInfo{}, nil
}
func (m *mockDriver) GetLink(ctx context.Context, fileID string) (*types.DownloadLink, error) {
	return nil, nil
}
func (m *mockDriver) Put(ctx context.Context, dirID string, name string, reader io.Reader, size int64) (*types.FileInfo, error) {
	return nil, nil
}
func (m *mockDriver) Remove(ctx context.Context, fileID string) error { return nil }
func (m *mockDriver) Mkdir(ctx context.Context, parentID string, name string) (*types.FileInfo, error) {
	return nil, nil
}
func (m *mockDriver) HealthCheck(ctx context.Context) types.HealthState {
	return types.HealthState{State: "healthy"}
}
func (m *mockDriver) RateLimitConfig() types.RateLimitConfig {
	return types.RateLimitConfig{ConcurrentLimit: 10}
}

// ─── Tests ───

func TestFetchBlobLocal_Success(t *testing.T) {
	// httptest server that returns a fixed body.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello blob data"))
	}))
	defer ts.Close()

	blobHash := "testhash123"
	acct := &accountpool.Account{
		Vendor:    types.Vendor115,
		AccountID: "acct-1",
		Driver:    &mockDriver{vendor: types.Vendor115},
	}
	location := types.BlobLocation{
		BlobHash:  blobHash,
		BackendID: "115:acct-1",
		FileID:    "file-1",
	}

	dp := NewLocalDataPlane(
		&mockAccountSelector{acct: acct},
		&mockLinkFetcher{link: &types.DownloadLink{URL: ts.URL}},
		&mockMetadataClient{locations: []types.BlobLocation{location}},
		http.DefaultClient,
	)

	reader, err := dp.FetchBlobLocal(context.Background(), blobHash)
	if err != nil {
		t.Fatalf("FetchBlobLocal failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}
	if string(data) != "hello blob data" {
		t.Fatalf("unexpected body: got %q, want %q", string(data), "hello blob data")
	}
}

func TestFetchBlobLocal_403ReturnsBanSignal(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	acct := &accountpool.Account{
		Vendor:    types.Vendor115,
		AccountID: "acct-1",
		Driver:    &mockDriver{vendor: types.Vendor115},
	}
	location := types.BlobLocation{
		BlobHash:  "hash-403",
		BackendID: "115:acct-1",
		FileID:    "file-1",
	}

	dp := NewLocalDataPlane(
		&mockAccountSelector{acct: acct},
		&mockLinkFetcher{link: &types.DownloadLink{URL: ts.URL}},
		&mockMetadataClient{locations: []types.BlobLocation{location}},
		http.DefaultClient,
	)

	_, err := dp.FetchBlobLocal(context.Background(), "hash-403")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var banErr *types.BanSignalError
	if !errors.As(err, &banErr) {
		t.Fatalf("expected BanSignalError, got %T: %v", err, err)
	}
	if banErr.Code != 403 {
		t.Fatalf("expected ban code 403, got %d", banErr.Code)
	}
}

func TestFetchBlobLocal_405ReturnsBanSignal(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	acct := &accountpool.Account{
		Vendor:    types.Vendor115,
		AccountID: "acct-1",
		Driver:    &mockDriver{vendor: types.Vendor115},
	}
	location := types.BlobLocation{
		BlobHash:  "hash-405",
		BackendID: "115:acct-1",
		FileID:    "file-1",
	}

	dp := NewLocalDataPlane(
		&mockAccountSelector{acct: acct},
		&mockLinkFetcher{link: &types.DownloadLink{URL: ts.URL}},
		&mockMetadataClient{locations: []types.BlobLocation{location}},
		http.DefaultClient,
	)

	_, err := dp.FetchBlobLocal(context.Background(), "hash-405")
	var banErr *types.BanSignalError
	if !errors.As(err, &banErr) {
		t.Fatalf("expected BanSignalError, got %T: %v", err, err)
	}
	if banErr.Code != 405 {
		t.Fatalf("expected ban code 405, got %d", banErr.Code)
	}
}

func TestFetchBlobLocal_429ReturnsBanSignal(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	acct := &accountpool.Account{
		Vendor:    types.Vendor115,
		AccountID: "acct-1",
		Driver:    &mockDriver{vendor: types.Vendor115},
	}
	location := types.BlobLocation{
		BlobHash:  "hash-429",
		BackendID: "115:acct-1",
		FileID:    "file-1",
	}

	dp := NewLocalDataPlane(
		&mockAccountSelector{acct: acct},
		&mockLinkFetcher{link: &types.DownloadLink{URL: ts.URL}},
		&mockMetadataClient{locations: []types.BlobLocation{location}},
		http.DefaultClient,
	)

	_, err := dp.FetchBlobLocal(context.Background(), "hash-429")
	var banErr *types.BanSignalError
	if !errors.As(err, &banErr) {
		t.Fatalf("expected BanSignalError, got %T: %v", err, err)
	}
}

func TestFetchBlobLocal_NilCtxDoesNotPanic(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	}))
	defer ts.Close()

	acct := &accountpool.Account{
		Vendor:    types.Vendor115,
		AccountID: "acct-1",
		Driver:    &mockDriver{vendor: types.Vendor115},
	}
	location := types.BlobLocation{
		BlobHash:  "hash-nil",
		BackendID: "115:acct-1",
		FileID:    "file-1",
	}

	dp := NewLocalDataPlane(
		&mockAccountSelector{acct: acct},
		&mockLinkFetcher{link: &types.DownloadLink{URL: ts.URL}},
		&mockMetadataClient{locations: []types.BlobLocation{location}},
		http.DefaultClient,
	)

	reader, err := dp.FetchBlobLocal(nil, "hash-nil")
	if err != nil {
		t.Fatalf("FetchBlobLocal with nil ctx failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected body data, got empty")
	}
}

func TestFetchBlobLocal_NoLocations(t *testing.T) {
	dp := NewLocalDataPlane(
		&mockAccountSelector{},
		&mockLinkFetcher{},
		&mockMetadataClient{locations: nil},
		http.DefaultClient,
	)

	_, err := dp.FetchBlobLocal(context.Background(), "missing-hash")
	if err == nil {
		t.Fatal("expected error for missing locations, got nil")
	}
}

func TestFetchBlobLocal_NoMatchingAccountLocation(t *testing.T) {
	blobHash := "hash-nomatch"
	acct := &accountpool.Account{
		Vendor:    types.Vendor115,
		AccountID: "acct-1",
		Driver:    &mockDriver{vendor: types.Vendor115},
	}
	// Location has a different accountID — no match.
	location := types.BlobLocation{
		BlobHash:  blobHash,
		BackendID: "115:acct-2",
		FileID:    "file-1",
	}

	dp := NewLocalDataPlane(
		&mockAccountSelector{acct: acct},
		&mockLinkFetcher{},
		&mockMetadataClient{locations: []types.BlobLocation{location}},
		http.DefaultClient,
	)

	_, err := dp.FetchBlobLocal(context.Background(), blobHash)
	if err == nil {
		t.Fatal("expected error for no matching location, got nil")
	}
}

func TestFetchBlobLocal_Non200Status(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	acct := &accountpool.Account{
		Vendor:    types.Vendor115,
		AccountID: "acct-1",
		Driver:    &mockDriver{vendor: types.Vendor115},
	}
	location := types.BlobLocation{
		BlobHash:  "hash-500",
		BackendID: "115:acct-1",
		FileID:    "file-1",
	}

	dp := NewLocalDataPlane(
		&mockAccountSelector{acct: acct},
		&mockLinkFetcher{link: &types.DownloadLink{URL: ts.URL}},
		&mockMetadataClient{locations: []types.BlobLocation{location}},
		http.DefaultClient,
	)

	_, err := dp.FetchBlobLocal(context.Background(), "hash-500")
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
}

func TestFetchBlobLocal_MetadataError(t *testing.T) {
	dp := NewLocalDataPlane(
		&mockAccountSelector{},
		&mockLinkFetcher{},
		&mockMetadataClient{err: errors.New("metadata down")},
		http.DefaultClient,
	)

	_, err := dp.FetchBlobLocal(context.Background(), "hash")
	if err == nil {
		t.Fatal("expected error for metadata failure, got nil")
	}
}

func TestFetchBlobLocal_RequestHeaders(t *testing.T) {
	// Verify that headers from DownloadLink are set on the HTTP request.
	var capturedHeaders map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = map[string]string{
			"Authorization": r.Header.Get("Authorization"),
			"X-Custom":      r.Header.Get("X-Custom"),
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer ts.Close()

	acct := &accountpool.Account{
		Vendor:    types.Vendor115,
		AccountID: "acct-1",
		Driver:    &mockDriver{vendor: types.Vendor115},
	}
	location := types.BlobLocation{
		BlobHash:  "hash-headers",
		BackendID: "115:acct-1",
		FileID:    "file-1",
	}
	link := &types.DownloadLink{
		URL: ts.URL,
		Headers: map[string]string{
			"Authorization": "Bearer test-token",
			"X-Custom":      "custom-value",
		},
	}

	dp := NewLocalDataPlane(
		&mockAccountSelector{acct: acct},
		&mockLinkFetcher{link: link},
		&mockMetadataClient{locations: []types.BlobLocation{location}},
		http.DefaultClient,
	)

	_, err := dp.FetchBlobLocal(context.Background(), "hash-headers")
	if err != nil {
		t.Fatalf("FetchBlobLocal failed: %v", err)
	}

	if capturedHeaders["Authorization"] != "Bearer test-token" {
		t.Fatalf("expected Authorization header 'Bearer test-token', got %q", capturedHeaders["Authorization"])
	}
	if capturedHeaders["X-Custom"] != "custom-value" {
		t.Fatalf("expected X-Custom header 'custom-value', got %q", capturedHeaders["X-Custom"])
	}
}
