package mock

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

// ErrNotFound is returned when a file or directory is not found in the mock filesystem.
type ErrNotFound struct {
	FileID string
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("mock: file not found: %s", e.FileID)
}

// MockDriverConfig defines the configurable state for a mock driver.
type MockDriverConfig struct {
	Filesystem map[string]types.FileInfo
	Health     types.HealthState
	RateLimit  types.RateLimitConfig
}

// MockDriver is an in-memory implementation of driver.Driver for testing.
type MockDriver struct {
	vendor types.Vendor
	mu     sync.RWMutex
	fs     map[string]types.FileInfo
	health types.HealthState
	lim    types.RateLimitConfig
}

func defaultRateLimit(vendor types.Vendor) types.RateLimitConfig {
	switch vendor {
	case types.Vendor115:
		return types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5}
	case types.VendorQuark:
		return types.RateLimitConfig{QPS: 0.5, Burst: 1, ConcurrentLimit: 5}
	case types.VendorAliyundrive:
		return types.RateLimitConfig{QPS: 5.0, Burst: 10, ConcurrentLimit: 10}
	default:
		return types.RateLimitConfig{QPS: 1.0, Burst: 2, ConcurrentLimit: 5}
	}
}

func NewMockDriver(vendor types.Vendor, cfg MockDriverConfig) *MockDriver {
	fs := cfg.Filesystem
	if fs == nil {
		fs = make(map[string]types.FileInfo)
	}
	lim := cfg.RateLimit
	if lim == (types.RateLimitConfig{}) {
		lim = defaultRateLimit(vendor)
	}
	return &MockDriver{
		vendor: vendor,
		fs:     fs,
		health: cfg.Health,
		lim:    lim,
	}
}

func (m *MockDriver) Vendor() types.Vendor { return m.vendor }

func (m *MockDriver) List(_ context.Context, dirID string, _ int) ([]types.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []types.FileInfo
	for _, fi := range m.fs {
		if len(dirID) == 0 || (len(fi.ID) >= len(dirID) && fi.ID[:len(dirID)] == dirID) {
			result = append(result, fi)
		}
	}
	return result, nil
}

func (m *MockDriver) Get(_ context.Context, fileID string) (types.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fi, ok := m.fs[fileID]
	if !ok {
		return types.FileInfo{}, &ErrNotFound{FileID: fileID}
	}
	return fi, nil
}

func (m *MockDriver) GetLink(_ context.Context, fileID string) (*types.DownloadLink, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.fs[fileID]; !ok {
		return nil, &ErrNotFound{FileID: fileID}
	}
	ipBound := m.vendor == types.VendorQuark
	return &types.DownloadLink{
		URL:      "https://mock.example.com/" + string(m.vendor) + "/" + fileID,
		ExpireAt: time.Now().Add(8 * time.Hour),
		IPBound:  ipBound,
		Headers:  nil,
	}, nil
}

func (m *MockDriver) Put(_ context.Context, dirID, name string, reader io.Reader, size int64) (*types.FileInfo, error) {
	buf := make([]byte, size)
	if size > 0 {
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
	}
	id := dirID + "/" + name
	fi := types.FileInfo{
		ID:       id,
		Name:     name,
		Size:     size,
		IsDir:    false,
		Modified: time.Now(),
	}
	m.mu.Lock()
	m.fs[id] = fi
	m.mu.Unlock()
	return &fi, nil
}

func (m *MockDriver) Remove(_ context.Context, fileID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.fs[fileID]; !ok {
		return &ErrNotFound{FileID: fileID}
	}
	delete(m.fs, fileID)
	return nil
}

func (m *MockDriver) Mkdir(_ context.Context, parentID, name string) (*types.FileInfo, error) {
	id := parentID + "/" + name
	fi := types.FileInfo{
		ID:       id,
		Name:     name,
		IsDir:    true,
		Modified: time.Now(),
	}
	m.mu.Lock()
	m.fs[id] = fi
	m.mu.Unlock()
	return &fi, nil
}

func (m *MockDriver) HealthCheck(_ context.Context) types.HealthState { return m.health }

func (m *MockDriver) RateLimitConfig() types.RateLimitConfig { return m.lim }

// ensure MockDriver implements driver.Driver
var _ driver.Driver = (*MockDriver)(nil)