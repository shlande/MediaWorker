// Package adminapi provides the control-plane admin API primitives.
package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

var (
	ErrPromDisabled = errors.New("adminapi: prometheus client disabled (empty baseURL)")
	ErrPromRequest  = errors.New("adminapi: prometheus request failed")
)

// ---------------------------------------------------------------------------
// PromClient — minimal instant-query client
// ---------------------------------------------------------------------------

// PromClient is a minimal Prometheus instant-query client for the control plane.
// It uses net/http directly — no prometheus/client_golang dependency.
type PromClient struct {
	baseURL string
	hc      *http.Client
}

// NewPromClient creates a PromClient for the given Prometheus base URL.
// If baseURL is empty, Enabled() returns false and all query methods return
// (0, false).
func NewPromClient(baseURL string) *PromClient {
	return &PromClient{
		baseURL: baseURL,
		hc: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Enabled reports whether this client has a configured Prometheus endpoint.
func (c *PromClient) Enabled() bool {
	return c.baseURL != ""
}

// QueryScalar executes an instant PromQL query and returns the first scalar
// sample value. Returns (0, false, nil) when the result is empty or
// resultType is not "vector". Returns an error on transport/timeout failures.
func (c *PromClient) QueryScalar(ctx context.Context, promQL string) (float64, bool, error) {
	if !c.Enabled() {
		return 0, false, nil
	}

	u, err := url.Parse(c.baseURL)
	if err != nil {
		return 0, false, fmt.Errorf("adminapi: parse prometheus baseURL: %w", err)
	}
	u = u.JoinPath("/api/v1/query")
	q := u.Query()
	q.Set("query", promQL)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, false, fmt.Errorf("adminapi: build request: %w", err)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("%w: %w", ErrPromRequest, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, fmt.Errorf("adminapi: read response body: %w", err)
	}

	if resp.StatusCode >= 500 {
		return 0, false, fmt.Errorf("%w: HTTP %d: %s", ErrPromRequest, resp.StatusCode, string(body))
	}

	var result struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, false, fmt.Errorf("adminapi: unmarshal prometheus response: %w", err)
	}
	if result.Status != "success" {
		return 0, false, fmt.Errorf("adminapi: prometheus returned status=%q", result.Status)
	}
	if result.Data.ResultType != "vector" || len(result.Data.Result) == 0 {
		return 0, false, nil
	}

	valueArr := result.Data.Result[0].Value
	if len(valueArr) < 2 {
		return 0, false, fmt.Errorf("adminapi: prometheus value array too short: %d elements", len(valueArr))
	}

	valStr, ok := valueArr[1].(string)
	if !ok {
		return 0, false, fmt.Errorf("adminapi: prometheus value is not a string, got %T", valueArr[1])
	}

	f, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, false, fmt.Errorf("adminapi: parse prometheus value %q: %w", valStr, err)
	}

	return f, true, nil
}

// ---------------------------------------------------------------------------
// Pre-canned queries
// ---------------------------------------------------------------------------

// CacheHitRate returns the cache hit rate over the last 5 minutes.
func (c *PromClient) CacheHitRate(ctx context.Context) (float64, bool, error) {
	return c.QueryScalar(ctx,
		`sum(rate(edge_cache_hit_total[5m]))/sum(rate(edge_cache_request_total[5m]))`,
	)
}

// TTFBP95 returns the P95 time-to-first-byte in seconds.
func (c *PromClient) TTFBP95(ctx context.Context) (float64, bool, error) {
	return c.QueryScalar(ctx,
		`histogram_quantile(0.95, sum(rate(edge_ttfb_seconds_bucket[5m])) by (le))`,
	)
}

// BackhaulBandwidthBps returns the current backhaul bandwidth in bits per second.
func (c *PromClient) BackhaulBandwidthBps(ctx context.Context) (float64, bool, error) {
	return c.QueryScalar(ctx,
		`sum(edge_backhaul_bandwidth_bytes)*8`,
	)
}
