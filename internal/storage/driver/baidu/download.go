package baidu

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

// resolveCDNURL follows the dlink redirect and returns the real CDN download URL.
func (d *BaiduDriver) resolveCDNURL(ctx context.Context, dlink, token string) (string, error) {
	dlinkURL := dlink
	if strings.Contains(dlink, "?") {
		dlinkURL += "&access_token=" + token
	} else {
		dlinkURL += "?access_token=" + token
	}

	redirectClient := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, dlinkURL, nil)
	if err != nil {
		return "", fmt.Errorf("build head request: %w", err)
	}
	headReq.Header.Set("User-Agent", "pan.baidu.com")

	headResp, err := redirectClient.Do(headReq)
	if err != nil {
		return "", fmt.Errorf("head request: %w", err)
	}
	defer func() { _ = headResp.Body.Close() }()

	cdnURL := headResp.Header.Get("Location")
	if cdnURL == "" {
		return "", fmt.Errorf("no Location header in 302 from dlink")
	}
	return cdnURL, nil
}

func buildDownloadLink(cdnURL string) *types.DownloadLink {
	return &types.DownloadLink{
		URL:      cdnURL,
		ExpireAt: time.Now().Add(15 * time.Minute),
		IPBound:  true,
		Headers:  map[string]string{"User-Agent": "pan.baidu.com"},
	}
}
