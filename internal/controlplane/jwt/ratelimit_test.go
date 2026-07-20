package jwt_test

import (
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/jwt"
)

// TestDefaultRateLimitInterval_AllowsDocumentedRefreshCadence pins the
// default per-IP rate limit against the documented node refresh cadence:
// nodes refresh every 5 minutes (cmd/edge-node runJWTRefreshLoop falls back
// to 5m), so the default interval must be well under 5m or every renewal
// after the first issuance is starved with HTTP 429 (F4a live incident:
// 1h interval vs 5m cadence → all renewals rate_limited since 06:05Z).
//
// Given the shipped default, when compared to the 5-minute refresh cadence,
// then it must leave ample headroom (<= 1 minute) for jitter and retries
// while still bounding abuse to 60 req/hour per IP.
func TestDefaultRateLimitInterval_AllowsDocumentedRefreshCadence(t *testing.T) {
	const refreshCadence = 5 * time.Minute
	if jwt.DefaultRateLimitInterval > time.Minute {
		t.Errorf("DefaultRateLimitInterval = %v, must be <= 1m to allow the documented %v refresh cadence", jwt.DefaultRateLimitInterval, refreshCadence)
	}
}

// TestRateLimiter_FirstRequestAllowed: Given a fresh limiter, when an IP
// makes its first request, then it is allowed.
func TestRateLimiter_FirstRequestAllowed(t *testing.T) {
	r := jwt.NewRateLimiter(time.Minute)
	if !r.Allow("10.0.0.1") {
		t.Fatal("first request for 10.0.0.1 was denied, want allowed")
	}
}

// TestRateLimiter_SecondRequestWithinIntervalDenied: Given an IP that just
// made a request, when it requests again inside the interval, then it is
// denied (abuse protection is preserved).
func TestRateLimiter_SecondRequestWithinIntervalDenied(t *testing.T) {
	r := jwt.NewRateLimiter(time.Minute)
	if !r.Allow("10.0.0.1") {
		t.Fatal("first request denied, want allowed")
	}
	if r.Allow("10.0.0.1") {
		t.Fatal("second request within interval was allowed, want denied")
	}
}

// TestRateLimiter_RequestAllowedAfterInterval: Given an IP whose last
// request is older than the interval, when it requests again, then it is
// allowed.
func TestRateLimiter_RequestAllowedAfterInterval(t *testing.T) {
	r := jwt.NewRateLimiter(time.Millisecond)
	if !r.Allow("10.0.0.1") {
		t.Fatal("first request denied, want allowed")
	}
	time.Sleep(2 * time.Millisecond)
	if !r.Allow("10.0.0.1") {
		t.Fatal("request after interval denied, want allowed")
	}
}

// TestRateLimiter_PerIPIndependence: Given two distinct source IPs (k8s pod
// IPs, per the live CP audit log), when one IP is inside its window, then
// the other IP is unaffected.
func TestRateLimiter_PerIPIndependence(t *testing.T) {
	r := jwt.NewRateLimiter(time.Minute)
	if !r.Allow("10.0.0.1") {
		t.Fatal("first request for 10.0.0.1 denied, want allowed")
	}
	if !r.Allow("10.0.0.2") {
		t.Fatal("first request for 10.0.0.2 denied, want allowed (per-IP independence)")
	}
	if r.Allow("10.0.0.1") {
		t.Fatal("second request for 10.0.0.1 within interval allowed, want denied")
	}
}
