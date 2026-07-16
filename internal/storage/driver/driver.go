// Package driver defines the unified Driver interface for all cloud drive vendors
// and a DriverRegistry for registering and retrieving vendor-specific implementations.
package driver

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/shlande/mediaworker/internal/types"
)

// Driver is the unified abstraction for a cloud drive vendor.
// Each vendor (115, Baidu, Quark, OneDrive, AliyunDrive) implements this interface.
type Driver interface {
	// Vendor returns the vendor identifier.
	Vendor() types.Vendor

	// File operations.
	List(ctx context.Context, dirID string, page int) ([]types.FileInfo, error)
	Get(ctx context.Context, fileID string) (types.FileInfo, error)

	// GetLink returns a temporary download URL for a file.
	GetLink(ctx context.Context, fileID string) (*types.DownloadLink, error)

	// Write operations.
	Put(ctx context.Context, dirID string, name string, reader io.Reader, size int64) (*types.FileInfo, error)
	Remove(ctx context.Context, fileID string) error
	Mkdir(ctx context.Context, parentID string, name string) (*types.FileInfo, error)

	// Account management.
	HealthCheck(ctx context.Context) types.HealthState
	RateLimitConfig() types.RateLimitConfig
}

// DriverRegistry manages registered Driver implementations keyed by vendor.
type DriverRegistry struct {
	mu      sync.RWMutex
	drivers map[types.Vendor]Driver
}

// NewDriverRegistry creates a new empty DriverRegistry.
func NewDriverRegistry() *DriverRegistry {
	return &DriverRegistry{
		drivers: make(map[types.Vendor]Driver),
	}
}

// Register registers a Driver for the given vendor.
// If a driver is already registered for this vendor it is silently replaced.
func (r *DriverRegistry) Register(vendor types.Vendor, d Driver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drivers[vendor] = d
}

// Get retrieves the Driver registered for the given vendor.
// Returns an error if no driver is registered for this vendor.
func (r *DriverRegistry) Get(vendor types.Vendor) (Driver, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.drivers[vendor]
	if !ok {
		return nil, fmt.Errorf("driver: no driver registered for vendor %q", vendor)
	}
	return d, nil
}