package baidu

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// mapEntry converts a pcsFileEntry into types.FileInfo.
func mapEntry(e pcsFileEntry) types.FileInfo {
	return types.FileInfo{
		ID:       strconv.FormatInt(e.FsID, 10),
		Name:     e.Filename,
		Size:     e.Size,
		IsDir:    e.IsDir == 1,
		Modified: time.Unix(e.ServerMtime, 0),
		Hash:     e.MD5,
	}
}

func mapMetaEntry(e pcsMetaEntry) types.FileInfo {
	return types.FileInfo{
		ID:       strconv.FormatInt(e.FsID, 10),
		Name:     e.Filename,
		Size:     e.Size,
		IsDir:    e.IsDir == 1,
		Modified: time.Unix(e.ServerMtime, 0),
		Hash:     e.MD5,
	}
}

func fileInfoFromResult(r *fileInfoResult, name string) *types.FileInfo {
	return &types.FileInfo{
		ID:       strconv.FormatInt(r.fsID, 10),
		Name:     name,
		Size:     r.size,
		IsDir:    r.isDir,
		Modified: time.Unix(r.serverMtime, 0),
		Hash:     r.md5,
	}
}

func nameFromPath(path string) string {
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		return path[idx+1:]
	}
	return path
}

// banOrErr returns BanSignalError for 403/405/429, or error for errno != 0.
func banOrErr(resp *http.Response, errno int, msg string) error {
	switch resp.StatusCode {
	case 403, 405, 429:
		return &types.BanSignalError{Code: resp.StatusCode, Msg: "baidu: " + msg}
	}
	if errno != 0 {
		return &pcsErr{errno: errno, msg: msg}
	}
	return nil
}

type pcsErr struct {
	errno int
	msg   string
}

func (e *pcsErr) Error() string {
	if e.msg != "" {
		return "baidu: " + e.msg
	}
	return "baidu: errno=" + strconv.Itoa(e.errno)
}

// buildFormRequest creates a POST request with x-www-form-urlencoded body.
func buildFormRequest(ctx context.Context, method, urlStr string, form map[string]string) (*http.Request, error) {
	vals := make(url.Values, len(form))
	for k, v := range form {
		vals.Set(k, v)
	}
	req, err := http.NewRequestWithContext(ctx, method, urlStr, strings.NewReader(vals.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req, nil
}

// stringsJoin is a local alias for strings.Join to avoid an import alias.
func stringsJoin(elems []string, sep string) string {
	return strings.Join(elems, sep)
}

// pcsGet is a helper for GET requests to the PCS API.
func (d *BaiduDriver) pcsGet(ctx context.Context, path string, params map[string]string) (*http.Response, error) {
	u, _ := url.Parse(d.baseURL + path)
	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	return d.httpc.Do(req)
}
