// Package accountpool — constructor: BuildFromConfig builds an *AccountPool
// from a StorageConfig (cloud_accounts/vendor_profiles/rate_limits), creating
// per-vendor drivers, rate limiters, and circuit breakers.
//
// Extracted from cmd/ingest-worker/main.go:buildAccountPool (T14) so both
// the ingest-worker and the janitor share the same construction logic —
// eliminating duplication and ensuring driver/CB/limiter wiring stays in
// lockstep across services.
package accountpool

import (
	"log/slog"
	"time"

	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/storage/circuitbreaker"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/storage/driver/baidu"
	"github.com/shlande/mediaworker/internal/storage/driver/onedrive"
	"github.com/shlande/mediaworker/internal/types"
)

// StorageConfig is the narrow view of a service's storage config needed to
// build an AccountPool. *config.IngestStorageConfig satisfies this via the
// adapter in this file (FromIngestStorage). Defined here to avoid an import
// cycle (accountpool ← config would be a backward edge).
type StorageConfig struct {
	CloudAccounts  []CloudAccount
	VendorProfiles map[string]VendorProfile
	RateLimits     map[string]RateLimit
}

// CloudAccount is the per-account config (vendor + credentials + enabled flag).
type CloudAccount struct {
	Vendor       string
	AccountID    string
	ClientID     string
	ClientSecret string
	Region       string
	Enabled      bool
}

// VendorProfile is the per-vendor weight (used for load-aware selection).
type VendorProfile struct {
	Weight float64
}

// RateLimit is the per-vendor rate limit override (qps/burst/concurrent).
// Zero values mean "use driver default".
type RateLimit struct {
	QPS        float64
	Burst      int
	Concurrent int
}

// BuildFromConfig creates an AccountPool from a StorageConfig, creates
// per-vendor drivers, and adds them to the pool with rate limiters and
// circuit breakers (same pattern as the edge-node integration tests and
// the original ingest-worker buildAccountPool).
//
// blobLocations may be nil — pass a BlobLocationClient if the pool will be
// used for reads (SelectForRead); nil is fine for upload-only or GC-only
// callers (the janitor never calls SelectForRead).
//
// Unknown vendors are skipped with a Warn log. Disabled accounts (Enabled=false)
// are skipped silently. The returned pool is ready for use; if no accounts
// were added the pool is empty and SelectK/SelectForRead will return an error.
func BuildFromConfig(cfg StorageConfig, blobLocations BlobLocationClient) *AccountPool {
	pool := NewAccountPool(blobLocations)
	tokenMgr := auth.NewTokenManager(nil)

	for _, acctCfg := range cfg.CloudAccounts {
		if !acctCfg.Enabled {
			continue
		}
		vendor := types.Vendor(acctCfg.Vendor)

		var drv driver.Driver
		switch vendor {
		case types.VendorBaidu:
			drv = baidu.NewBaiduDriver(tokenMgr, acctCfg.AccountID, acctCfg.ClientID, acctCfg.ClientSecret, nil)
		case types.VendorOneDrive:
			drv = onedrive.NewOneDriveDriver(tokenMgr, acctCfg.AccountID, acctCfg.Region, nil)
		default:
			slog.Warn("accountpool: unknown vendor, skipping", "vendor", acctCfg.Vendor, "account_id", acctCfg.AccountID)
			continue
		}

		rateCfg := drv.RateLimitConfig()
		if override, ok := cfg.RateLimits[acctCfg.Vendor]; ok {
			if override.QPS > 0 {
				rateCfg.QPS = override.QPS
			}
			if override.Burst > 0 {
				rateCfg.Burst = override.Burst
			}
			if override.Concurrent > 0 {
				rateCfg.ConcurrentLimit = override.Concurrent
			}
		}

		vendorWeight := 2.0
		if vp, ok := cfg.VendorProfiles[acctCfg.Vendor]; ok {
			vendorWeight = vp.Weight
		}

		key := string(vendor) + ":" + acctCfg.AccountID
		acct := &Account{
			Vendor:       vendor,
			AccountID:    acctCfg.AccountID,
			Driver:       drv,
			Limiter:      rate.NewLimiter(rate.Limit(rateCfg.QPS), rateCfg.Burst),
			CB:           circuitbreaker.New(key, 5, 100*time.Millisecond),
			VendorWeight: vendorWeight,
		}
		acct.Health.Store(types.HealthState{State: "healthy"})
		pool.AddAccount(acct)
		slog.Info("accountpool: account added", "key", key, "vendor", acctCfg.Vendor)
	}
	return pool
}
