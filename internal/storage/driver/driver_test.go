package driver

import (
	"context"
	"io"
	"testing"

	"github.com/shlande/mediaworker/internal/types"
)

// mockDriver implements Driver for testing.
type mockDriver struct {
	vendor types.Vendor
}

func (m *mockDriver) Vendor() types.Vendor                     { return m.vendor }
func (m *mockDriver) List(_ context.Context, _ string, _ int) ([]types.FileInfo, error) {
	return nil, nil
}
func (m *mockDriver) Get(_ context.Context, _ string) (types.FileInfo, error) {
	return types.FileInfo{}, nil
}
func (m *mockDriver) GetLink(_ context.Context, _ string) (*types.DownloadLink, error) {
	return nil, nil
}
func (m *mockDriver) Put(_ context.Context, _ string, _ string, _ io.Reader, _ int64) (*types.FileInfo, error) {
	return nil, nil
}
func (m *mockDriver) Remove(_ context.Context, _ string) error { return nil }
func (m *mockDriver) Mkdir(_ context.Context, _ string, _ string) (*types.FileInfo, error) {
	return nil, nil
}
func (m *mockDriver) HealthCheck(_ context.Context) types.HealthState {
	return types.HealthState{State: "healthy"}
}
func (m *mockDriver) RateLimitConfig() types.RateLimitConfig {
	return types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5}
}

func TestDriverRegistry_RegisterAndGet_returnsCorrectInstance(t *testing.T) {
	reg := NewDriverRegistry()

	d115 := &mockDriver{vendor: types.Vendor115}
	dBaidu := &mockDriver{vendor: types.VendorBaidu}
	dQuark := &mockDriver{vendor: types.VendorQuark}

	reg.Register(types.Vendor115, d115)
	reg.Register(types.VendorBaidu, dBaidu)
	reg.Register(types.VendorQuark, dQuark)

	got, err := reg.Get(types.Vendor115)
	if err != nil {
		t.Fatalf("Get(Vendor115) unexpected error: %v", err)
	}
	if got.Vendor() != types.Vendor115 {
		t.Errorf("expected vendor 115, got %v", got.Vendor())
	}

	got, err = reg.Get(types.VendorBaidu)
	if err != nil {
		t.Fatalf("Get(VendorBaidu) unexpected error: %v", err)
	}
	if got.Vendor() != types.VendorBaidu {
		t.Errorf("expected vendor baidu, got %v", got.Vendor())
	}

	got, err = reg.Get(types.VendorQuark)
	if err != nil {
		t.Fatalf("Get(VendorQuark) unexpected error: %v", err)
	}
	if got.Vendor() != types.VendorQuark {
		t.Errorf("expected vendor quark, got %v", got.Vendor())
	}
}

func TestDriverRegistry_Get_unregisteredReturnsError(t *testing.T) {
	reg := NewDriverRegistry()

	// Register only two vendors, leave one unregistered.
	reg.Register(types.Vendor115, &mockDriver{vendor: types.Vendor115})
	reg.Register(types.VendorBaidu, &mockDriver{vendor: types.VendorBaidu})

	_, err := reg.Get(types.VendorQuark)
	if err == nil {
		t.Fatal("expected error for unregistered vendor, got nil")
	}
}

func TestDriverRegistry_Get_afterReplace(t *testing.T) {
	reg := NewDriverRegistry()

	first := &mockDriver{vendor: types.Vendor115}
	second := &mockDriver{vendor: types.Vendor115}

	reg.Register(types.Vendor115, first)
	reg.Register(types.Vendor115, second)

	got, err := reg.Get(types.Vendor115)
	if err != nil {
		t.Fatalf("Get(Vendor115) unexpected error: %v", err)
	}
	// The second registration should replace the first.
	if got != second {
		t.Error("expected the second registered driver to replace the first")
	}
}

func TestDriverRegistry_emptyRegistry(t *testing.T) {
	reg := NewDriverRegistry()

	_, err := reg.Get(types.Vendor115)
	if err == nil {
		t.Fatal("expected error on empty registry, got nil")
	}
}