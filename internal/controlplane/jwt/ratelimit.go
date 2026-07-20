package jwt

import (
	"sync"
	"time"
)

// RateLimiter tracks per-IP request times to enforce a simple rate limit.
// Not designed for high-throughput (>10k concurrent) — for a control-plane
// JWT endpoint that receives one request per node per refresh cadence, a
// mutex is perfectly adequate.
type RateLimiter struct {
	mu       sync.Mutex
	lastSeen map[string]time.Time
	interval time.Duration
}

// NewRateLimiter returns a RateLimiter with the given per-IP interval.
func NewRateLimiter(interval time.Duration) *RateLimiter {
	return &RateLimiter{
		lastSeen: make(map[string]time.Time),
		interval: interval,
	}
}

// Allow reports whether the given IP is allowed to make a request now.
// Returns true if this is the first request, or if the interval has elapsed
// since the last request. Returns false if still within the window.
func (r *RateLimiter) Allow(ip string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	last, ok := r.lastSeen[ip]
	now := time.Now()
	if !ok || now.Sub(last) >= r.interval {
		r.lastSeen[ip] = now
		return true
	}
	return false
}

// DefaultRateLimitInterval is the default control-plane per-IP rate limit.
// Nodes refresh their JWT every 5 minutes (see cmd/edge-node
// runJWTRefreshLoop), so the interval must sit well under that cadence or
// every renewal after the first issuance is starved with HTTP 429 (F4a: a
// 1h interval against the 5m cadence broke all renewals in the test
// cluster). 1/min per IP allows the documented cadence with ample jitter
// headroom while still bounding abuse to 60 req/hour per source IP.
// Operators can tune it via jwt_http.rate_limit_interval.
const DefaultRateLimitInterval = 1 * time.Minute
