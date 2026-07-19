package baidu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
)

func (d *BaiduDriver) precreate(ctx context.Context, token, path string, size int64, blockList string) (string, error) {
	u := d.baseURL + "/rest/2.0/xpan/file?method=precreate&access_token=" + token
	form := map[string]string{
		"path":        path,
		"size":        strconv.FormatInt(size, 10),
		"isdir":       "0",
		"block_list":  blockList,
		"content_md5": "",
	}

	req, err := buildFormRequest(ctx, http.MethodPost, u, form)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}

	resp, err := d.httpc.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	var body pcsPrecreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if err := banOrErr(resp, body.Errno, "precreate failed"); err != nil {
		return "", err
	}
	if body.UploadID == "" {
		return "", fmt.Errorf("empty uploadid")
	}
	return body.UploadID, nil
}

func (d *BaiduDriver) uploadChunks(
	ctx context.Context, token, path, uploadID string,
	reader io.Reader, size int64, blockList string,
) error {
	numBlocks := 0
	if size > 0 {
		numBlocks = int(math.Ceil(float64(size) / float64(chunkSize)))
	}

	for i := 0; i < numBlocks; i++ {
		chunkSizeActual := int64(chunkSize)
		if remainder := size - int64(i)*int64(chunkSize); remainder < chunkSizeActual {
			chunkSizeActual = remainder
		}

		lr := io.LimitReader(reader, chunkSizeActual)
		var buf bytes.Buffer
		writer := newMultipartForm(&buf, "file", "blob", "application/octet-stream", lr, chunkSizeActual)

		uploadURL := fmt.Sprintf(
			"%s/rest/2.0/pcs/superfile2?method=upload&access_token=%s&type=tmpfile&path=%s&uploadid=%s&partseq=%d",
			d.uploadBaseURL, token, path, uploadID, i,
		)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, &buf)
		if err != nil {
			return fmt.Errorf("chunk %d: build request: %w", i, err)
		}
		req.Header.Set("Content-Type", writer.formDataContentType())

		resp, err := d.httpc.Do(req)
		if err != nil {
			return fmt.Errorf("chunk %d: do request: %w", i, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("chunk %d: HTTP %d", i, resp.StatusCode)
		}

		var upResp pcsUploadResponse
		if err := json.NewDecoder(resp.Body).Decode(&upResp); err != nil {
			return fmt.Errorf("chunk %d: decode: %w", i, err)
		}
		if upResp.Errno != 0 {
			return fmt.Errorf("chunk %d: errno=%d", i, upResp.Errno)
		}
	}
	return nil
}

func (d *BaiduDriver) createFile(ctx context.Context, token, path string, size int64, blockList, uploadID string) (*fileInfoResult, error) {
	u := d.baseURL + "/rest/2.0/xpan/file?method=create&access_token=" + token
	form := map[string]string{
		"path":       path,
		"size":       strconv.FormatInt(size, 10),
		"isdir":      "0",
		"block_list": blockList,
		"uploadid":   uploadID,
	}

	req, err := buildFormRequest(ctx, http.MethodPost, u, form)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := d.httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	var body pcsCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if err := banOrErr(resp, body.Errno, "create failed"); err != nil {
		return nil, err
	}
	if body.Info == nil {
		return nil, fmt.Errorf("empty info in create response")
	}

	return &fileInfoResult{
		fsID:        body.Info.FsID,
		path:        body.Info.Path,
		size:        body.Info.Size,
		isDir:       body.Info.IsDir == 1,
		md5:         body.Info.MD5,
		serverMtime: body.Info.ServerMtime,
	}, nil
}

func (d *BaiduDriver) doMkdir(ctx context.Context, token, path string) (*fileInfoResult, error) {
	u := d.baseURL + "/rest/2.0/xpan/file?method=create&access_token=" + token
	form := map[string]string{
		"path":  path,
		"isdir": "1",
		"type":  "2",
	}

	req, err := buildFormRequest(ctx, http.MethodPost, u, form)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := d.httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	var body pcsCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if err := banOrErr(resp, body.Errno, "mkdir failed"); err != nil {
		return nil, err
	}
	if body.Info == nil {
		return nil, nil
	}
	return &fileInfoResult{
		fsID:  body.Info.FsID,
		path:  body.Info.Path,
		isDir: body.Info.IsDir == 1,
	}, nil
}

type fileInfoResult struct {
	fsID        int64
	path        string
	size        int64
	isDir       bool
	md5         string
	serverMtime int64
}

// ---------------------------------------------------------------------------
// multipart form-data
// ---------------------------------------------------------------------------

type multipartForm struct {
	buf      *bytes.Buffer
	boundary string
}

func newMultipartForm(buf *bytes.Buffer, fieldname, filename, contentType string, reader io.Reader, size int64) *multipartForm {
	boundary := "baiduUploadBoundary"
	buf.WriteString("--" + boundary + "\r\n")
	fmt.Fprintf(buf,
		"Content-Disposition: form-data; name=\"%s\"; filename=\"%s\"\r\n",
		fieldname, filename,
	)
	buf.WriteString("Content-Type: " + contentType + "\r\n\r\n")
	_, _ = io.Copy(buf, reader)
	buf.WriteString("\r\n--" + boundary + "--\r\n")
	return &multipartForm{buf: buf, boundary: boundary}
}

func (m *multipartForm) formDataContentType() string {
	return "multipart/form-data; boundary=" + m.boundary
}

// buildBlockList returns a JSON array string of placeholder MD5 hashes for
// each 4MB block; PCS API accepts these for non-rapidupload uploads.
func buildBlockList(size int64) string {
	if size == 0 {
		return "[]"
	}
	numBlocks := int(math.Ceil(float64(size) / float64(chunkSize)))
	parts := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		parts[i] = `"0"`
	}
	return "[" + stringsJoin(parts, ",") + "]"
}
