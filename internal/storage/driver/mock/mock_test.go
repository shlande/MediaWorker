package mock

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

func TestMockDriver_PutGetGetLink_roundtrip(t *testing.T) {
	tests := []struct {
		name      string
		vendor    types.Vendor
		ctor      func(MockDriverConfig) *MockDriver
		ipBound   bool
		urlPrefix string
	}{
		{"115", types.Vendor115, func(cfg MockDriverConfig) *MockDriver { return NewMockDriver(types.Vendor115, cfg) }, false, "https://mock.example.com/115/"},
		{"quark", types.VendorQuark, NewQuarkDriver, true, "https://mock.example.com/quark/"},
		{"aliyundrive", types.VendorAliyundrive, NewAliyundriveDriver, false, "https://mock.example.com/aliyundrive/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := tt.ctor(MockDriverConfig{})
			ctx := context.Background()

			// Put
			content := "hello world"
			fi, err := d.Put(ctx, "dir1", "file1.txt", strings.NewReader(content), int64(len(content)))
			if err != nil {
				t.Fatalf("Put: unexpected error: %v", err)
			}
			if fi.Name != "file1.txt" {
				t.Errorf("Put name = %q, want %q", fi.Name, "file1.txt")
			}
			if fi.Size != int64(len(content)) {
				t.Errorf("Put size = %d, want %d", fi.Size, len(content))
			}
			if fi.Modified.IsZero() {
				t.Error("Put: Modified time should not be zero")
			}

			// Get
			got, err := d.Get(ctx, fi.ID)
			if err != nil {
				t.Fatalf("Get: unexpected error: %v", err)
			}
			if got.ID != fi.ID {
				t.Errorf("Get ID = %q, want %q", got.ID, fi.ID)
			}
			if got.Name != fi.Name {
				t.Errorf("Get Name = %q, want %q", got.Name, fi.Name)
			}

			// GetLink
			link, err := d.GetLink(ctx, fi.ID)
			if err != nil {
				t.Fatalf("GetLink: unexpected error: %v", err)
			}
			wantURL := tt.urlPrefix + fi.ID
			if link.URL != wantURL {
				t.Errorf("GetLink URL = %q, want %q", link.URL, wantURL)
			}
			if link.IPBound != tt.ipBound {
				t.Errorf("GetLink IPBound = %v, want %v", link.IPBound, tt.ipBound)
			}
			if link.ExpireAt.Before(time.Now()) {
				t.Error("GetLink ExpireAt should be in the future")
			}
		})
	}
}

func TestMockDriver_Get_nonexistent_returnsError(t *testing.T) {
	d := NewMockDriver(types.Vendor115, MockDriverConfig{})
	ctx := context.Background()

	_, err := d.Get(ctx, "nonexistent")
	if err == nil {
		t.Fatal("Get nonexistent: expected error, got nil")
	}
	var notFound *ErrNotFound
	if !errors.As(err, &notFound) {
		t.Errorf("Get nonexistent: expected *ErrNotFound, got %T: %v", err, err)
	}
	if notFound.FileID != "nonexistent" {
		t.Errorf("ErrNotFound.FileID = %q, want %q", notFound.FileID, "nonexistent")
	}
}

func TestMockDriver_GetLink_nonexistent_returnsError(t *testing.T) {
	d := NewMockDriver(types.Vendor115, MockDriverConfig{})
	ctx := context.Background()

	_, err := d.GetLink(ctx, "nonexistent")
	if err == nil {
		t.Fatal("GetLink nonexistent: expected error, got nil")
	}
}

func TestMockDriver_Remove_nonexistent_returnsError(t *testing.T) {
	d := NewMockDriver(types.Vendor115, MockDriverConfig{})
	ctx := context.Background()

	err := d.Remove(ctx, "nonexistent")
	if err == nil {
		t.Fatal("Remove nonexistent: expected error, got nil")
	}
}

func TestMockDriver_Mkdir(t *testing.T) {
	d := NewMockDriver(types.Vendor115, MockDriverConfig{})
	ctx := context.Background()

	fi, err := d.Mkdir(ctx, "parent", "subdir")
	if err != nil {
		t.Fatalf("Mkdir: unexpected error: %v", err)
	}
	if !fi.IsDir {
		t.Error("Mkdir: IsDir should be true")
	}
	if fi.ID != "parent/subdir" {
		t.Errorf("Mkdir ID = %q, want %q", fi.ID, "parent/subdir")
	}

	// Verify listed
	files, err := d.List(ctx, "parent", 1)
	if err != nil {
		t.Fatalf("List after Mkdir: unexpected error: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("List after Mkdir: got %d files, want 1", len(files))
	}
	if files[0].ID != fi.ID {
		t.Errorf("List returned ID = %q, want %q", files[0].ID, fi.ID)
	}
}

func TestMockDriver_Remove(t *testing.T) {
	d := NewMockDriver(types.Vendor115, MockDriverConfig{})
	ctx := context.Background()

	fi, err := d.Put(ctx, "dir", "remove_me.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}

	err = d.Remove(ctx, fi.ID)
	if err != nil {
		t.Fatalf("Remove: unexpected error: %v", err)
	}

	// Verify gone
	_, err = d.Get(ctx, fi.ID)
	if err == nil {
		t.Fatal("Get after Remove: expected error, got nil")
	}
}

func TestMockDriver_HealthCheck_configurable(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		health types.HealthState
	}{
		{"healthy", types.HealthState{State: "healthy"}},
		{"banned", types.HealthState{State: "banned", ErrorMsg: "rate limited", LastCheck: time.Now(), Latency: 100 * time.Millisecond}},
		{"degraded", types.HealthState{State: "degraded", ErrorMsg: "high latency", Latency: 5 * time.Second}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewMockDriver(types.Vendor115, MockDriverConfig{Health: tt.health})
			got := d.HealthCheck(ctx)
			if got.State != tt.health.State {
				t.Errorf("HealthCheck.State = %q, want %q", got.State, tt.health.State)
			}
			if got.ErrorMsg != tt.health.ErrorMsg {
				t.Errorf("HealthCheck.ErrorMsg = %q, want %q", got.ErrorMsg, tt.health.ErrorMsg)
			}
		})
	}
}

func TestMockDriver_RateLimitConfig_defaults(t *testing.T) {
	tests := []struct {
		name string
		ctor func(MockDriverConfig) *MockDriver
		want types.RateLimitConfig
	}{
		{"115", func(cfg MockDriverConfig) *MockDriver { return NewMockDriver(types.Vendor115, cfg) }, types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5}},
		{"quark", NewQuarkDriver, types.RateLimitConfig{QPS: 0.5, Burst: 1, ConcurrentLimit: 5}},
		{"aliyundrive", NewAliyundriveDriver, types.RateLimitConfig{QPS: 5.0, Burst: 10, ConcurrentLimit: 10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := tt.ctor(MockDriverConfig{})
			got := d.RateLimitConfig()
			if got != tt.want {
				t.Errorf("RateLimitConfig = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestMockDriver_RateLimitConfig_override(t *testing.T) {
	cfg := types.RateLimitConfig{QPS: 99.0, Burst: 99, ConcurrentLimit: 99}
	d := NewMockDriver(types.Vendor115, MockDriverConfig{RateLimit: cfg})
	got := d.RateLimitConfig()
	if got != cfg {
		t.Errorf("RateLimitConfig = %+v, want %+v", got, cfg)
	}
}

func TestMockDriver_List_byDirID(t *testing.T) {
	d := NewMockDriver(types.Vendor115, MockDriverConfig{})
	ctx := context.Background()

	d.Put(ctx, "a", "f1.txt", strings.NewReader(""), 0)
	d.Put(ctx, "a", "f2.txt", strings.NewReader(""), 0)
	d.Put(ctx, "b", "f3.txt", strings.NewReader(""), 0)

	// List under "a" — should get 2 files
	files, err := d.List(ctx, "a", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("List('a') = %d files, want 2", len(files))
	}

	// List under "b" — should get 1 file
	files, err = d.List(ctx, "b", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("List('b') = %d files, want 1", len(files))
	}
}

func TestMockDriver_List_emptyDir(t *testing.T) {
	d := NewMockDriver(types.Vendor115, MockDriverConfig{})
	ctx := context.Background()

	files, err := d.List(ctx, "nonexistent", 0)
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("List nonexistent dir = %d files, want 0", len(files))
	}
}

func TestMockDriver_concurrentAccess(t *testing.T) {
	d := NewMockDriver(types.Vendor115, MockDriverConfig{})
	ctx := context.Background()

	// Put from multiple goroutines
	const n = 10
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			r := strings.NewReader("data")
			_, err := d.Put(ctx, "concurrent", string(rune('A'+i)), r, 4)
			errs <- err
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent Put: unexpected error: %v", err)
		}
	}

	// Verify all files can be listed
	files, err := d.List(ctx, "concurrent", 0)
	if err != nil {
		t.Fatalf("List after concurrent puts: %v", err)
	}
	if len(files) != n {
		t.Errorf("List after concurrent puts = %d files, want %d", len(files), n)
	}
}

func TestMockDriver_Vendor(t *testing.T) {
	tests := []struct {
		name   string
		vendor types.Vendor
		ctor   func(MockDriverConfig) *MockDriver
	}{
		{"115", types.Vendor115, func(cfg MockDriverConfig) *MockDriver { return NewMockDriver(types.Vendor115, cfg) }},
		{"quark", types.VendorQuark, NewQuarkDriver},
		{"aliyundrive", types.VendorAliyundrive, NewAliyundriveDriver},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := tt.ctor(MockDriverConfig{})
			if d.Vendor() != tt.vendor {
				t.Errorf("Vendor() = %q, want %q", d.Vendor(), tt.vendor)
			}
		})
	}
}

func TestMockDriver_registerToRegistry(t *testing.T) {
	reg := driver.NewDriverRegistry()

	d115 := NewMockDriver(types.Vendor115, MockDriverConfig{})
	dQuark := NewQuarkDriver(MockDriverConfig{})
	dAliyun := NewAliyundriveDriver(MockDriverConfig{})

	reg.Register(types.Vendor115, d115)
	reg.Register(types.VendorQuark, dQuark)
	reg.Register(types.VendorAliyundrive, dAliyun)

	got115, err := reg.Get(types.Vendor115)
	if err != nil {
		t.Fatalf("Get(Vendor115): %v", err)
	}
	if got115.Vendor() != types.Vendor115 {
		t.Errorf("registered 115 driver vendor = %q", got115.Vendor())
	}

	gotQuark, err := reg.Get(types.VendorQuark)
	if err != nil {
		t.Fatalf("Get(VendorQuark): %v", err)
	}
	if gotQuark.Vendor() != types.VendorQuark {
		t.Errorf("registered quark driver vendor = %q", gotQuark.Vendor())
	}

	gotAliyun, err := reg.Get(types.VendorAliyundrive)
	if err != nil {
		t.Fatalf("Get(VendorAliyundrive): %v", err)
	}
	if gotAliyun.Vendor() != types.VendorAliyundrive {
		t.Errorf("registered aliyundrive driver vendor = %q", gotAliyun.Vendor())
	}
}

func TestMockDriver_interface(t *testing.T) {
	// compile-time interface check — mock must implement all 9 methods
	var d driver.Driver = NewMockDriver(types.Vendor115, MockDriverConfig{})
	_ = d.Vendor()
	_, _ = d.List(context.Background(), "", 0)
	_, _ = d.Get(context.Background(), "")
	_, _ = d.GetLink(context.Background(), "")
	_, _ = d.Put(context.Background(), "", "", io.LimitReader(strings.NewReader(""), 0), 0)
	_ = d.Remove(context.Background(), "")
	_, _ = d.Mkdir(context.Background(), "", "")
	_ = d.HealthCheck(context.Background())
	_ = d.RateLimitConfig()
}

func TestErrNotFound_Error(t *testing.T) {
	e := &ErrNotFound{FileID: "test123"}
	got := e.Error()
	want := "mock: file not found: test123"
	if got != want {
		t.Errorf("ErrNotFound.Error() = %q, want %q", got, want)
	}
}
