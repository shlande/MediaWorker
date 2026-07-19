// Package accounttester implements the B3 account connection test
// (docs/account-backend-adjustments.md:122-135): it builds a throwaway
// TokenManager + vendor driver from candidate (draft) or stored credentials
// and runs driver.HealthCheck, returning the resulting types.HealthState
// verbatim (state/latency/error_msg — the error text is the operator-facing
// diagnostic that distinguishes a wrong client_secret from an expired
// refresh_token, so it is never rewritten).
//
// MANAGEMENT-PLANE ONLY: per the CP responsibility matrix the control plane
// does not deploy drivers in its data path. This package exists solely to
// back the admin connection-test endpoint and must NEVER be wired into any
// backhaul path. Mock vendors (115/quark/aliyundrive) have no driver: the
// tester returns ErrDriverNotImplemented without building anything.
package accounttester

import (
	"context"
	"errors"
	"net/http"

	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/storage/driver/baidu"
	"github.com/shlande/mediaworker/internal/storage/driver/onedrive"
	"github.com/shlande/mediaworker/internal/types"
)

// draftAccountID is the placeholder account id used for unsaved form content.
// The token manager keys entries "vendor:accountID", and driver/token error
// text embeds it, so "draft" makes draft-mode diagnostics self-describing.
const draftAccountID = "draft"

// ErrDriverNotImplemented is returned for vendors without a driver
// implementation (115, quark, aliyundrive). The handler maps it to 501.
var ErrDriverNotImplemented = errors.New("driver not implemented")

// SecretReader is the narrow registry surface for stored-mode tests
// (= accountregistry.GetAccountSecret, todo 6(d)). INTERNAL ONLY: the
// returned secret material feeds driver construction and is never exposed
// through any HTTP response.
type SecretReader interface {
	GetAccountSecret(ctx context.Context, vendor types.Vendor, accountID string) (types.Credential, types.ClientConfig, error)
}

// ValidateFunc split-normalizes draft auth material into credential +
// clientConfig, mirroring adminapi.ValidateAuth (todo 26). It is injected
// rather than imported: adminapi imports accounttester for route mounting,
// so accounttester cannot import adminapi (import cycle). fieldErrors empty
// means the material is valid; warnings never block.
type ValidateFunc func(vendor types.Vendor, auth map[string]any) (credential types.Credential, clientConfig types.ClientConfig, fieldErrors map[string]string, warnings []string)

// ValidationError carries per-field draft validation failures; the handler
// maps it to 400 with the field_errors body (B4 shape).
type ValidationError struct {
	FieldErrors map[string]string
}

// Error implements error.
func (e *ValidationError) Error() string { return "accounttester: invalid auth material" }

// Tester runs account connection tests. It is safe for concurrent use: every
// call builds its own TokenManager and driver, so no state is shared.
type Tester struct {
	registry SecretReader
	validate ValidateFunc
	httpc    *http.Client // nil → http.DefaultClient (driver constructors' own default)
}

// NewTester creates a Tester. registry backs stored-mode reads; validate is
// the todo 26 auth normalizer (production: adminapi.ValidateAuth); httpc may
// be nil (production) or a mock transport (tests).
func NewTester(registry SecretReader, validate ValidateFunc, httpc *http.Client) *Tester {
	return &Tester{registry: registry, validate: validate, httpc: httpc}
}

// TestDraft validates unsaved form auth material via the injected
// ValidateFunc, then builds a temporary driver and probes it. Validation
// failures return *ValidationError (handler → 400); mock vendors return
// ErrDriverNotImplemented (handler → 501) without any driver construction.
func (t *Tester) TestDraft(ctx context.Context, vendor types.Vendor, auth map[string]any) (types.HealthState, error) {
	cred, cc, fieldErrors, _ := t.validate(vendor, auth)
	if len(fieldErrors) > 0 {
		return types.HealthState{}, &ValidationError{FieldErrors: fieldErrors}
	}
	return t.probe(ctx, probeTarget{vendor: vendor, accountID: draftAccountID, cred: cred, cc: cc})
}

// TestStored reads the stored credential + client config via SecretReader and
// probes a temporary driver built from them. Registry errors (including
// accountregistry.ErrAccountNotFound) are passed through unchanged so the
// handler can map them.
func (t *Tester) TestStored(ctx context.Context, vendor types.Vendor, accountID string) (types.HealthState, error) {
	cred, cc, err := t.registry.GetAccountSecret(ctx, vendor, accountID)
	if err != nil {
		return types.HealthState{}, err
	}
	return t.probe(ctx, probeTarget{vendor: vendor, accountID: accountID, cred: cred, cc: cc})
}

type probeTarget struct {
	vendor    types.Vendor
	accountID string
	cred      types.Credential
	cc        types.ClientConfig
}

// probe mirrors the accountpool.BuildFromConfig vendor→driver template
// (build.go:82-100) for one account — keep the two in sync. Mock vendors are
// rejected BEFORE any TokenManager/driver construction (spec MUST-NOT).
func (t *Tester) probe(ctx context.Context, tgt probeTarget) (types.HealthState, error) {
	switch tgt.vendor {
	case types.VendorBaidu, types.VendorOneDrive:
		// supported below
	default:
		return types.HealthState{}, ErrDriverNotImplemented
	}

	tokenURL := ""
	if tgt.vendor == types.VendorOneDrive {
		tokenURL = auth.OneDriveTokenURL(tgt.cc.Region)
	}
	tokenMgr := auth.NewTokenManager(t.httpc)
	tokenMgr.Register(tgt.vendor, tgt.accountID, auth.OAuth2Config{
		ClientID:     tgt.cc.ClientID,
		ClientSecret: tgt.cc.ClientSecret,
		RefreshToken: tgt.cred.RefreshToken,
		RedirectURI:  tgt.cc.RedirectURI,
		TokenURL:     tokenURL,
	})

	var drv driver.Driver
	switch tgt.vendor {
	case types.VendorBaidu:
		drv = baidu.NewBaiduDriver(tokenMgr, tgt.accountID, tgt.cc.ClientID, tgt.cc.ClientSecret, t.httpc)
	case types.VendorOneDrive:
		drv = onedrive.NewOneDriveDriver(tokenMgr, tgt.accountID, tgt.cc.Region, t.httpc)
	}
	return drv.HealthCheck(ctx), nil
}
