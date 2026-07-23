// Package baidu implements the driver.Driver interface for Baidu Netdisk (百度网盘)
// using the PCS API (Pan Cloud Storage) with OAuth2 access tokens.
package baidu

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

const (
	defaultBaseURL  = "https://pan.baidu.com"
	defaultPageSize = 100
	baiduUploadHost = "https://d.pcs.baidu.com"
	chunkSize       = 4 * 1024 * 1024
)

// BaiduDriver is the Baidu Netdisk implementation of driver.Driver.
type BaiduDriver struct {
	tokenMgr      *auth.TokenManager
	accountID     string
	clientID      string
	clientSecret  string
	baseURL       string
	uploadBaseURL string
	httpc         *http.Client
}

// NewBaiduDriver creates a new BaiduDriver. If httpc is nil, http.DefaultClient is used.
func NewBaiduDriver(
	tokenMgr *auth.TokenManager,
	accountID, clientID, clientSecret string,
	httpc *http.Client,
) *BaiduDriver {
	if httpc == nil {
		httpc = http.DefaultClient
	}
	return &BaiduDriver{
		tokenMgr:      tokenMgr,
		accountID:     accountID,
		clientID:      clientID,
		clientSecret:  clientSecret,
		baseURL:       defaultBaseURL,
		uploadBaseURL: baiduUploadHost,
		httpc:         httpc,
	}
}

// Vendor returns the baidu vendor identifier.
func (d *BaiduDriver) Vendor() types.Vendor { return types.VendorBaidu }

func (d *BaiduDriver) accessToken(ctx context.Context) (string, error) {
	return d.tokenMgr.GetAccessToken(types.VendorBaidu, d.accountID)
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

func (d *BaiduDriver) List(ctx context.Context, dirID string, page int) ([]types.FileInfo, error) {
	token, err := d.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("baidu list: token: %w", err)
	}

	resp, err := d.pcsGet(ctx, "/rest/2.0/xpan/file", map[string]string{
		"method": "list", "dir": dirID, "order": "name", "desc": "0",
		"page": strconv.Itoa(page), "num": strconv.Itoa(defaultPageSize),
		"access_token": token,
	})
	if err != nil {
		return nil, fmt.Errorf("baidu list: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var body pcsListResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("baidu list: decode: %w", err)
	}
	if err := banOrErr(resp, body.Errno, body.Errmsg); err != nil {
		return nil, err
	}

	out := make([]types.FileInfo, 0, len(body.List))
	for _, e := range body.List {
		out = append(out, mapEntry(e))
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

func (d *BaiduDriver) Get(ctx context.Context, fileID string) (types.FileInfo, error) {
	token, err := d.accessToken(ctx)
	if err != nil {
		return types.FileInfo{}, fmt.Errorf("baidu get: token: %w", err)
	}

	resp, err := d.pcsGet(ctx, "/rest/2.0/xpan/file", map[string]string{
		"method": "filemetas", "fsids": "[" + fileID + "]", "dlink": "1",
		"access_token": token,
	})
	if err != nil {
		return types.FileInfo{}, fmt.Errorf("baidu get: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var body pcsMetaResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return types.FileInfo{}, fmt.Errorf("baidu get: decode: %w", err)
	}
	if err := banOrErr(resp, body.Errno, ""); err != nil {
		return types.FileInfo{}, err
	}
	if len(body.List) == 0 {
		return types.FileInfo{}, fmt.Errorf("baidu get: file not found: %s", fileID)
	}
	return mapMetaEntry(body.List[0]), nil
}

// ---------------------------------------------------------------------------
// GetLink
// ---------------------------------------------------------------------------

func (d *BaiduDriver) GetLink(ctx context.Context, fileID string) (*types.DownloadLink, error) {
	token, err := d.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("baidu getlink: token: %w", err)
	}

	resp, err := d.pcsGet(ctx, "/rest/2.0/xpan/file", map[string]string{
		"method": "filemetas", "fsids": "[" + fileID + "]", "dlink": "1",
		"access_token": token,
	})
	if err != nil {
		return nil, fmt.Errorf("baidu getlink: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var body pcsMetaResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("baidu getlink: decode: %w", err)
	}
	if err := banOrErr(resp, body.Errno, ""); err != nil {
		return nil, err
	}
	if len(body.List) == 0 || body.List[0].Dlink == "" {
		return nil, fmt.Errorf("baidu getlink: no dlink for file %s", fileID)
	}

	cdnURL, err := d.resolveCDNURL(ctx, body.List[0].Dlink, token)
	if err != nil {
		return nil, fmt.Errorf("baidu getlink: %w", err)
	}
	return buildDownloadLink(cdnURL), nil
}

// ---------------------------------------------------------------------------
// Put
// ---------------------------------------------------------------------------

func (d *BaiduDriver) Put(ctx context.Context, dirID, name string, reader io.Reader, size int64) (*types.FileInfo, error) {
	token, err := d.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("baidu put: token: %w", err)
	}

	path := dirID + "/" + name
	blockList := buildBlockList(size)

	uploadID, err := d.precreate(ctx, token, path, size, blockList)
	if err != nil {
		return nil, fmt.Errorf("baidu put: precreate: %w", err)
	}

	if err := d.uploadChunks(ctx, token, path, uploadID, reader, size, blockList); err != nil {
		return nil, fmt.Errorf("baidu put: upload: %w", err)
	}

	result, err := d.createFile(ctx, token, path, size, blockList, uploadID)
	if err != nil {
		return nil, fmt.Errorf("baidu put: create: %w", err)
	}
	return fileInfoFromResult(result, name), nil
}

// ---------------------------------------------------------------------------
// Remove
// ---------------------------------------------------------------------------

func (d *BaiduDriver) Remove(ctx context.Context, fileID string) error {
	token, err := d.accessToken(ctx)
	if err != nil {
		return fmt.Errorf("baidu remove: token: %w", err)
	}

	u := d.baseURL + "/api/filemanager?method=delete&access_token=" + token
	form := map[string]string{"fsids": "[" + fileID + "]"}

	req, err := buildFormRequest(ctx, http.MethodPost, u, form)
	if err != nil {
		return fmt.Errorf("baidu remove: build request: %w", err)
	}

	resp, err := d.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("baidu remove: do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var body pcsDeleteResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("baidu remove: decode: %w", err)
	}
	return banOrErr(resp, body.Errno, "delete failed")
}

// ---------------------------------------------------------------------------
// Mkdir
// ---------------------------------------------------------------------------

func (d *BaiduDriver) Mkdir(ctx context.Context, parentID, name string) (*types.FileInfo, error) {
	token, err := d.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("baidu mkdir: token: %w", err)
	}

	path := parentID + "/" + name
	result, err := d.doMkdir(ctx, token, path)
	if err != nil {
		return nil, fmt.Errorf("baidu mkdir: %w", err)
	}
	if result == nil {
		return &types.FileInfo{ID: path, Name: name, IsDir: true}, nil
	}
	return fileInfoFromResult(result, name), nil
}

// ---------------------------------------------------------------------------
// HealthCheck
// ---------------------------------------------------------------------------

func (d *BaiduDriver) HealthCheck(ctx context.Context) types.HealthState {
	start := time.Now()

	token, err := d.accessToken(ctx)
	if err != nil {
		return degradedState(start, fmt.Sprintf("token: %v", err))
	}

	u := d.baseURL + "/rest/2.0/xpan/nas?method=uinfo&access_token=" + token
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return degradedState(start, fmt.Sprintf("build request: %v", err))
	}

	resp, err := d.httpc.Do(req)
	if err != nil {
		return degradedState(start, fmt.Sprintf("request: %v", err))
	}
	defer func() { _ = resp.Body.Close() }()

	latency := time.Since(start)

	if resp.StatusCode == 403 {
		return types.HealthState{State: "banned", LastCheck: start, Latency: latency, ErrorMsg: "banned (HTTP 403)"}
	}
	if resp.StatusCode != http.StatusOK {
		return degradedState(start, fmt.Sprintf("HTTP %d", resp.StatusCode))
	}

	var body pcsUInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return degradedState(start, fmt.Sprintf("decode: %v", err))
	}
	if body.Errno != 0 {
		return degradedState(start, fmt.Sprintf("errno=%d", body.Errno))
	}
	if latency < 2*time.Second {
		return types.HealthState{State: "healthy", LastCheck: start, Latency: latency}
	}
	return types.HealthState{State: "degraded", LastCheck: start, Latency: latency, ErrorMsg: fmt.Sprintf("latency %v > threshold 2s", latency)}
}

func degradedState(start time.Time, msg string) types.HealthState {
	return types.HealthState{State: "degraded", LastCheck: start, Latency: time.Since(start), ErrorMsg: msg}
}

// ---------------------------------------------------------------------------
// RateLimitConfig
// ---------------------------------------------------------------------------

func (d *BaiduDriver) RateLimitConfig() types.RateLimitConfig {
	return types.RateLimitConfig{QPS: 2.0, Burst: 4, ConcurrentLimit: 8}
}

var _ driver.Driver = (*BaiduDriver)(nil)
