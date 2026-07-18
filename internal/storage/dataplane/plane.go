// Package dataplane implements the L4 local data plane — fetching blob data
// from cloud drive storage via the account/link/metadata stack.
package dataplane

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/shlande/mediaworker/internal/storage/accountpool"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

// BlobLocationClient provides blob location lookup for FetchBlobLocal.
// accountpool.BlobLocationClient satisfies this interface.
type BlobLocationClient interface {
	GetBlobLocations(ctx context.Context, blobHash string) ([]types.BlobLocation, error)
}

// AccountSelector selects an account for reading a blob.
// *accountpool.AccountPool implements this interface.
type AccountSelector interface {
	SelectForRead(ctx context.Context, blobHash string) (*accountpool.Account, error)
}

// LinkFetcher returns a download link for a file, using cached values when possible.
// *linkpool.LinkPool implements this interface.
type LinkFetcher interface {
	GetOrFetch(ctx context.Context, d driver.Driver, vendor types.Vendor, accountID, fileID string) (*types.DownloadLink, error)
}

// LocalDataPlane fetches blob data from cloud drive storage via the account pool,
// link pool, and metadata client stack. It satisfies the backhaul.DataPlane interface
// (FetchBlobLocal) without importing backhaul (which would create a cycle).
type LocalDataPlane struct {
	selector AccountSelector
	fetcher  LinkFetcher
	mc       BlobLocationClient
	hc       *http.Client
}

// NewLocalDataPlane creates a LocalDataPlane with the given dependencies.
func NewLocalDataPlane(selector AccountSelector, fetcher LinkFetcher, mc BlobLocationClient, hc *http.Client) *LocalDataPlane {
	return &LocalDataPlane{
		selector: selector,
		fetcher:  fetcher,
		mc:       mc,
		hc:       hc,
	}
}

// FetchBlobLocal fetches blob data from local cloud drive storage.
// ctx is interface{} to match the backhaul.DataPlane interface; if nil,
// context.Background() is used.
//
// The method signature matches backhaul.DataPlane without importing that
// package (which would create a circular dependency).
func (dp *LocalDataPlane) FetchBlobLocal(ctx interface{}, blobHash string) (io.ReadCloser, error) {
	// Resolve context: interface{} from backhaul.DataPlane, nil-safe.
	var cctx context.Context
	if ctx == nil {
		cctx = context.Background()
	} else {
		var ok bool
		cctx, ok = ctx.(context.Context)
		if !ok {
			cctx = context.Background()
		}
	}

	// 1. Look up blob locations from metadata.
	locations, err := dp.mc.GetBlobLocations(cctx, blobHash)
	if err != nil {
		return nil, fmt.Errorf("dataplane: get locations for %q: %w", blobHash, err)
	}
	if len(locations) == 0 {
		return nil, fmt.Errorf("dataplane: no locations for blob %q", blobHash)
	}

	// 2. Select the best account for reading this blob.
	acct, err := dp.selector.SelectForRead(cctx, blobHash)
	if err != nil {
		return nil, fmt.Errorf("dataplane: select account for %q: %w", blobHash, err)
	}

	// 3. Find the matching BlobLocation for the selected account.
	var location *types.BlobLocation
	for i := range locations {
		expectedKey := string(acct.Vendor) + ":" + acct.AccountID
		if locations[i].BackendID == expectedKey {
			location = &locations[i]
			break
		}
	}
	if location == nil {
		return nil, fmt.Errorf("dataplane: no location matching account %s/%s for blob %q", acct.Vendor, acct.AccountID, blobHash)
	}

	// 4. Get a download link via the link pool (cached or fresh).
	link, err := dp.fetcher.GetOrFetch(cctx, acct.Driver, acct.Vendor, acct.AccountID, location.FileID)
	if err != nil {
		return nil, fmt.Errorf("dataplane: get link for %q: %w", blobHash, err)
	}

	// 5. Issue the HTTP GET request.
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, link.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("dataplane: create request for %q: %w", blobHash, err)
	}
	for k, v := range link.Headers {
		req.Header.Set(k, v)
	}

	resp, err := dp.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dataplane: http get for %q: %w", blobHash, err)
	}

	// 6. Check for ban/throttle signals.
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusTooManyRequests {
		resp.Body.Close()
		return nil, &types.BanSignalError{Code: resp.StatusCode, Msg: fmt.Sprintf("dataplane: ban signal fetching %q", blobHash)}
	}

	// 7. Reject non-200 status codes.
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("dataplane: unexpected status %d fetching %q", resp.StatusCode, blobHash)
	}

	return resp.Body, nil
}
