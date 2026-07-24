// Package onedrive implements the driver.Driver interface for OneDrive
// (personal / OneDrive for Business) via Microsoft Graph API v1.0.
package onedrive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shlande/mediaworker/internal/storage/auth"
	"github.com/shlande/mediaworker/internal/storage/driver"
	"github.com/shlande/mediaworker/internal/types"
)

// graphHosts maps OneDrive region to Graph API host.
var graphHosts = map[string]string{
	"global": "graph.microsoft.com",
	"cn":     "microsoftgraph.chinacloudapi.cn",
	"us":     "dod-graph.microsoft.us",
	"de":     "graph.microsoft.de",
}

// uploadChunkSize is the chunk size for large file uploads (5 MiB).
const uploadChunkSize = 5 * 1024 * 1024

// OneDriveDriver implements driver.Driver for OneDrive via Microsoft Graph API.
type OneDriveDriver struct {
	tokenMgr  *auth.TokenManager
	accountID string
	region    string
	httpc     *http.Client
	baseHost  string // base host + scheme for testing; empty means derive from graphHosts
}

// NewOneDriveDriver creates a new OneDriveDriver. region defaults to "global"
// if empty or unrecognized.
func NewOneDriveDriver(
	tokenMgr *auth.TokenManager,
	accountID string,
	region string,
	httpc *http.Client,
) *OneDriveDriver {
	if region == "" {
		region = "global"
	}
	if httpc == nil {
		httpc = http.DefaultClient
	}
	return &OneDriveDriver{
		tokenMgr:  tokenMgr,
		accountID: accountID,
		region:    region,
		httpc:     httpc,
	}
}

// newOneDriveDriverWithHost is used in tests to override the API host.
func newOneDriveDriverWithHost(
	tokenMgr *auth.TokenManager,
	accountID string,
	baseHost string,
	httpc *http.Client,
) *OneDriveDriver {
	d := NewOneDriveDriver(tokenMgr, accountID, "global", httpc)
	d.baseHost = baseHost
	return d
}

// baseURL returns the API base URL. Uses baseHost (test override) if set,
// otherwise derives from the region's graphHosts entry.
func (d *OneDriveDriver) baseURL() string {
	if d.baseHost != "" {
		return d.baseHost + "/v1.0/me/drive"
	}
	host, ok := graphHosts[d.region]
	if !ok {
		host = graphHosts["global"]
	}
	return "https://" + host + "/v1.0/me/drive"
}

// Vendor returns the vendor identifier.
func (d *OneDriveDriver) Vendor() types.Vendor {
	return types.VendorOneDrive
}

// ─── JSON types for Graph API responses ───────────────────────────────

type graphListResponse struct {
	Value    []graphDriveItem `json:"value"`
	NextLink string           `json:"@odata.nextLink,omitempty"`
}

type graphDriveItem struct {
	ID              string               `json:"id"`
	Name            string               `json:"name"`
	Size            int64                `json:"size"`
	File            *graphFileFacet      `json:"file,omitempty"`
	FileSystemInfo  *graphFileSystemInfo `json:"fileSystemInfo,omitempty"`
	DownloadURL     string               `json:"@microsoft.graph.downloadUrl,omitempty"`
	ParentReference *graphParentRef      `json:"parentReference,omitempty"`
}

type graphFileFacet struct {
	// empty struct — presence signals "this is a file, not a folder"
}

type graphFileSystemInfo struct {
	LastModifiedDateTime string `json:"lastModifiedDateTime"`
}

type graphParentRef struct {
	ID string `json:"id"`
}

type graphCreateUploadSessionResponse struct {
	UploadURL string `json:"uploadUrl"`
}

// itemURL builds a Graph API URL for addressing an item by dirID + suffix.
// "root" or "" → "/root" + suffix
// "root:<path>" → "/root:<path>" + suffix (path-style addressing)
// anything else → "/items/<dirID>" + suffix (real item ID)
func itemURL(baseURL, dirID, suffix string) string {
	switch {
	case dirID == "root" || dirID == "":
		return baseURL + "/root" + suffix
	case strings.HasPrefix(dirID, "root:"):
		return baseURL + "/" + dirID + suffix
	default:
		return baseURL + "/items/" + dirID + suffix
	}
}

// ─── helpers ──────────────────────────────────────────────────────────

func (d *OneDriveDriver) newRequest(ctx context.Context, method, urlStr string, body io.Reader) (*http.Request, error) {
	token, err := d.tokenMgr.GetAccessToken(types.VendorOneDrive, d.accountID)
	if err != nil {
		return nil, fmt.Errorf("onedrive: get access token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	return req, nil
}

func (d *OneDriveDriver) do(req *http.Request) (*http.Response, error) {
	resp, err := d.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusForbidden ||
		resp.StatusCode == http.StatusMethodNotAllowed ||
		resp.StatusCode == http.StatusTooManyRequests {
		_ = resp.Body.Close()
		return nil, &types.BanSignalError{
			Code: resp.StatusCode,
			Msg:  "onedrive: api returned " + resp.Status,
		}
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("onedrive: api status %d: %s", resp.StatusCode, string(body))
	}
	return resp, nil
}

func toFileInfo(item graphDriveItem) types.FileInfo {
	fi := types.FileInfo{
		ID:   item.ID,
		Name: item.Name,
		Size: item.Size,
	}
	if item.File == nil {
		fi.IsDir = true
	}
	if item.FileSystemInfo != nil && item.FileSystemInfo.LastModifiedDateTime != "" {
		if t, err := time.Parse(time.RFC3339, item.FileSystemInfo.LastModifiedDateTime); err == nil {
			fi.Modified = t
		}
	}
	return fi
}

// ─── Driver interface implementation ──────────────────────────────────

// List returns the children of the given directory. dirID may be "root"
// to list the drive root. pagination via @odata.nextLink is handled;
// the page parameter selects the page.
func (d *OneDriveDriver) List(ctx context.Context, dirID string, page int) ([]types.FileInfo, error) {
	var urlStr string
	if dirID == "root" || dirID == "" {
		urlStr = d.baseURL() + "/root/children"
	} else {
		urlStr = d.baseURL() + "/items/" + dirID + "/children"
	}
	urlStr += "?$top=200&$select=id,name,size,fileSystemInfo,file,parentReference"

	// Walk to the requested page.
	var currentPage int
	for currentPage < page {
		req, err := d.newRequest(ctx, http.MethodGet, urlStr, nil)
		if err != nil {
			return nil, err
		}
		resp, err := d.do(req)
		if err != nil {
			return nil, err
		}
		var listResp graphListResponse
		if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("onedrive: list decode: %w", err)
		}
		_ = resp.Body.Close()

		currentPage++
		if currentPage == page {
			// This is the target page — build and return the result.
			result := make([]types.FileInfo, 0, len(listResp.Value))
			for _, item := range listResp.Value {
				result = append(result, toFileInfo(item))
			}
			return result, nil
		}
		// Advance to next page.
		if listResp.NextLink == "" {
			return nil, nil // no more pages
		}
		urlStr = listResp.NextLink
	}

	return nil, nil
}

// Get returns the FileInfo for a single file by ID.
func (d *OneDriveDriver) Get(ctx context.Context, fileID string) (types.FileInfo, error) {
	urlStr := d.baseURL() + "/items/" + fileID + "?$select=id,name,size,fileSystemInfo,file"
	req, err := d.newRequest(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return types.FileInfo{}, err
	}
	resp, err := d.do(req)
	if err != nil {
		return types.FileInfo{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	var item graphDriveItem
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return types.FileInfo{}, fmt.Errorf("onedrive: get decode: %w", err)
	}
	return toFileInfo(item), nil
}

// GetLink returns a temporary download URL. OneDrive's anonymous download
// URLs are NOT IP-bound and do not require authentication headers.
func (d *OneDriveDriver) GetLink(ctx context.Context, fileID string) (*types.DownloadLink, error) {
	urlStr := d.baseURL() + "/items/" + fileID + "?$select=@microsoft.graph.downloadUrl"
	req, err := d.newRequest(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var item graphDriveItem
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return nil, fmt.Errorf("onedrive: getlink decode: %w", err)
	}
	if item.DownloadURL == "" {
		return nil, fmt.Errorf("onedrive: no download URL in response for file %s", fileID)
	}
	return &types.DownloadLink{
		URL:      item.DownloadURL,
		ExpireAt: time.Now().Add(1 * time.Hour),
		IPBound:  false,
		Headers:  map[string]string{},
	}, nil
}

// Put uploads a file. Files ≤ 4 MiB use a single PUT; larger files use
// the resumable upload session (createUploadSession + chunked PUT).
func (d *OneDriveDriver) Put(ctx context.Context, dirID string, name string, reader io.Reader, size int64) (*types.FileInfo, error) {
	if size <= 4*1024*1024 {
		return d.putSmall(ctx, dirID, name, reader)
	}
	return d.putLarge(ctx, dirID, name, reader, size)
}

func (d *OneDriveDriver) putSmall(ctx context.Context, dirID, name string, reader io.Reader) (*types.FileInfo, error) {
	urlStr := itemURL(d.baseURL(), dirID, "/"+url.PathEscape(name)+":/content")
	req, err := d.newRequest(ctx, http.MethodPut, urlStr, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := d.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var item graphDriveItem
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return nil, fmt.Errorf("onedrive: put decode: %w", err)
	}
	fi := toFileInfo(item)
	return &fi, nil
}

func (d *OneDriveDriver) putLarge(ctx context.Context, dirID, name string, reader io.Reader, size int64) (*types.FileInfo, error) {
	// Step 1: Create upload session.
	sessionURL := itemURL(d.baseURL(), dirID, "/"+url.PathEscape(name)+":/createUploadSession")
	createBody := bytes.NewReader([]byte(`{}`))
	req, err := d.newRequest(ctx, http.MethodPost, sessionURL, createBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.do(req)
	if err != nil {
		return nil, err
	}
	var sessionResp graphCreateUploadSessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&sessionResp); err != nil {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("onedrive: createUploadSession decode: %w", err)
	}
	_ = resp.Body.Close()

	// Step 2: Upload in chunks.
	buf := make([]byte, uploadChunkSize)
	var start int64
	for start < size {
		n, readErr := io.ReadFull(reader, buf)
		if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
			return nil, fmt.Errorf("onedrive: read chunk: %w", readErr)
		}
		if n == 0 {
			break
		}
		chunk := buf[:n]
		end := start + int64(n) - 1

		chunkReq, err := http.NewRequestWithContext(ctx, http.MethodPut, sessionResp.UploadURL, bytes.NewReader(chunk))
		if err != nil {
			return nil, err
		}
		chunkReq.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		chunkReq.Header.Set("Content-Length", strconv.Itoa(n))

		chunkResp, err := d.httpc.Do(chunkReq)
		if err != nil {
			return nil, fmt.Errorf("onedrive: chunk upload: %w", err)
		}
		if chunkResp.StatusCode != http.StatusOK && chunkResp.StatusCode != http.StatusAccepted && chunkResp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(io.LimitReader(chunkResp.Body, 4096))
			_ = chunkResp.Body.Close()
			return nil, fmt.Errorf("onedrive: chunk upload status %d: %s", chunkResp.StatusCode, string(body))
		}

		// On the last chunk, the server returns the completed file item.
		if end == size-1 || n < uploadChunkSize {
			var item graphDriveItem
			if err := json.NewDecoder(chunkResp.Body).Decode(&item); err != nil {
				_ = chunkResp.Body.Close()
				return nil, fmt.Errorf("onedrive: final chunk decode: %w", err)
			}
			_ = chunkResp.Body.Close()
			fi := toFileInfo(item)
			return &fi, nil
		}
		_ = chunkResp.Body.Close()
		start += int64(n)
	}

	return nil, fmt.Errorf("onedrive: upload ended unexpectedly")
}

// Remove deletes a file or folder by ID.
func (d *OneDriveDriver) Remove(ctx context.Context, fileID string) error {
	urlStr := d.baseURL() + "/items/" + fileID
	req, err := d.newRequest(ctx, http.MethodDelete, urlStr, nil)
	if err != nil {
		return err
	}
	resp, err := d.do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// Mkdir creates a new folder inside parentID. Name collisions are handled
// by the server via rename conflict behavior.
func (d *OneDriveDriver) Mkdir(ctx context.Context, parentID string, name string) (*types.FileInfo, error) {
	urlStr := fmt.Sprintf("%s/items/%s/children", d.baseURL(), parentID)
	bodyJSON := fmt.Sprintf(`{"name":%q,"folder":{},"@microsoft.graph.conflictBehavior":"rename"}`, name)
	req, err := d.newRequest(ctx, http.MethodPost, urlStr, strings.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var item graphDriveItem
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return nil, fmt.Errorf("onedrive: mkdir decode: %w", err)
	}
	fi := toFileInfo(item)
	return &fi, nil
}

// HealthCheck probes the Graph API root drive endpoint. 200 + latency < 1s
// is healthy; 429 returns BanSignalError.
func (d *OneDriveDriver) HealthCheck(ctx context.Context) types.HealthState {
	urlStr := d.baseURL()
	start := time.Now()
	req, err := d.newRequest(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return types.HealthState{
			State:     "error",
			LastCheck: time.Now(),
			ErrorMsg:  err.Error(),
		}
	}
	resp, err := d.httpc.Do(req)
	latency := time.Since(start)
	if err != nil {
		return types.HealthState{
			State:     "error",
			LastCheck: time.Now(),
			Latency:   latency,
			ErrorMsg:  err.Error(),
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusTooManyRequests {
		return types.HealthState{
			State:     "banned",
			LastCheck: time.Now(),
			Latency:   latency,
			ErrorMsg:  "429 Too Many Requests",
		}
	}
	if resp.StatusCode == http.StatusOK {
		// 延迟仅作观测值记录，不判降级 —— 网盘类服务延迟不影响可用性
		return types.HealthState{
			State:     "healthy",
			LastCheck: time.Now(),
			Latency:   latency,
		}
	}
	// Any other status: degraded.
	return types.HealthState{
		State:     "degraded",
		LastCheck: time.Now(),
		Latency:   latency,
		ErrorMsg:  resp.Status,
	}
}

// RateLimitConfig returns OneDrive's rate limiting configuration.
func (d *OneDriveDriver) RateLimitConfig() types.RateLimitConfig {
	return types.RateLimitConfig{
		QPS:             10.0,
		Burst:           20,
		ConcurrentLimit: 16,
	}
}

// Ensure OneDriveDriver implements driver.Driver.
var _ driver.Driver = (*OneDriveDriver)(nil)
