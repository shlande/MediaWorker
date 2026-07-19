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
	RefreshToken string
	RedirectURI  string
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

		tokenURL := ""
		if vendor == types.VendorOneDrive {
			tokenURL = auth.OneDriveTokenURL(acctCfg.Region)
		}

		tokenMgr.Register(vendor, acctCfg.AccountID, auth.OAuth2Config{
			ClientID:     acctCfg.ClientID,
			ClientSecret: acctCfg.ClientSecret,
			RefreshToken: acctCfg.RefreshToken,
			RedirectURI:  acctCfg.RedirectURI,
			TokenURL:     tokenURL,
		})

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

// BuildFromSnapshot creates an AccountPool from an ACCOUNT_SNAPSHOT payload
// ([]types.AccountSnapshotEntry), following the same assembly pattern as
// BuildFromConfig. The snapshot carries ClientConfig (static OAuth2 client
// material) + Credential (refresh_token/cookies), so TokenManager.Register's
// forced first refresh works without any seeded access token.
//
// Vendors without a real driver (115/quark/aliyundrive) are skipped with a
// Warn log. Disabled entries are skipped silently. Accounts whose ClientConfig
// is incomplete still enter the pool (initial health "healthy") with a Warn —
// the first token refresh will fail and the health-check loop will degrade
// them; BuildFromSnapshot never panics on malformed snapshots.
func BuildFromSnapshot(accounts []types.AccountSnapshotEntry, blobLocations BlobLocationClient) *AccountPool {
	return buildFromSnapshot(accounts, blobLocations, auth.NewTokenManager(nil))
}

// buildFromSnapshot is the injectable core of BuildFromSnapshot; tests pass a
// TokenManager with a stubbed http.Client to observe the registered
// OAuth2Config four elements without network access.
func buildFromSnapshot(accounts []types.AccountSnapshotEntry, blobLocations BlobLocationClient, tokenMgr *auth.TokenManager) *AccountPool {
	pool := NewAccountPool(blobLocations)

	for _, entry := range accounts {
		if !entry.Enabled {
			continue
		}
		vendor := entry.Vendor
		cc := entry.ClientConfig
		cred := entry.Credential

		tokenURL := ""
		if vendor == types.VendorOneDrive {
			tokenURL = auth.OneDriveTokenURL(cc.Region)
		}

		tokenMgr.Register(vendor, entry.AccountID, auth.OAuth2Config{
			ClientID:     cc.ClientID,
			ClientSecret: cc.ClientSecret,
			RefreshToken: cred.RefreshToken,
			RedirectURI:  cc.RedirectURI,
			TokenURL:     tokenURL,
		})

		var drv driver.Driver
		switch vendor {
		case types.VendorBaidu:
			drv = baidu.NewBaiduDriver(tokenMgr, entry.AccountID, cc.ClientID, cc.ClientSecret, nil)
		case types.VendorOneDrive:
			drv = onedrive.NewOneDriveDriver(tokenMgr, entry.AccountID, cc.Region, nil)
		default:
			slog.Warn("accountpool: snapshot vendor has no real driver, skipping", "vendor", entry.Vendor, "account_id", entry.AccountID)
			continue
		}

		if missing := missingClientMaterial(vendor, cc, cred); len(missing) > 0 {
			slog.Warn("accountpool: snapshot account missing OAuth2 client material, first refresh will fail",
				"vendor", entry.Vendor, "account_id", entry.AccountID, "missing", missing)
		}

		rateCfg := drv.RateLimitConfig()
		if entry.RateLimitCfg.QPS > 0 {
			rateCfg.QPS = entry.RateLimitCfg.QPS
		}
		if entry.RateLimitCfg.Burst > 0 {
			rateCfg.Burst = entry.RateLimitCfg.Burst
		}
		if entry.RateLimitCfg.ConcurrentLimit > 0 {
			rateCfg.ConcurrentLimit = entry.RateLimitCfg.ConcurrentLimit
		}

		vendorWeight := 2.0
		if entry.VendorProfile.Weight > 0 {
			vendorWeight = entry.VendorProfile.Weight
		}

		key := string(vendor) + ":" + entry.AccountID
		acct := &Account{
			Vendor:       vendor,
			AccountID:    entry.AccountID,
			Driver:       drv,
			Limiter:      rate.NewLimiter(rate.Limit(rateCfg.QPS), rateCfg.Burst),
			CB:           circuitbreaker.New(key, 5, 100*time.Millisecond),
			VendorWeight: vendorWeight,
		}
		acct.Health.Store(types.HealthState{State: "healthy"})
		pool.AddAccount(acct)
		slog.Info("accountpool: account added from snapshot", "key", key, "vendor", entry.Vendor)
	}
	return pool
}

// missingClientMaterial lists the OAuth2 fields the first token refresh needs
// but the snapshot does not provide (empty). RedirectURI is required only for
// OneDrive; region falls back to "global" and is never reported missing.
func missingClientMaterial(vendor types.Vendor, cc types.ClientConfig, cred types.Credential) []string {
	var missing []string
	if cc.ClientID == "" {
		missing = append(missing, "client_id")
	}
	if cc.ClientSecret == "" {
		missing = append(missing, "client_secret")
	}
	if vendor == types.VendorOneDrive && cc.RedirectURI == "" {
		missing = append(missing, "redirect_uri")
	}
	if cred.RefreshToken == "" {
		missing = append(missing, "refresh_token")
	}
	return missing
}
