package app

import (
	"context"
	"crypto/ed25519"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/host"

	nodejwt "github.com/shlande/mediaworker/internal/node/jwt"
	"github.com/shlande/mediaworker/internal/node/monitor"
	"github.com/shlande/mediaworker/internal/node/peerstore"

	"github.com/shlande/mediaworker/internal/config"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
)

// runJWTRefreshLoop periodically re-requests a capability JWT from the control
// plane. The next refresh fires at min(refreshInterval, exp-now-refreshBeforeExpiry):
//   - refreshInterval: caller-configured steady cadence (e.g. 5m)
//   - exp-now-refreshBeforeExpiry: time-to-expiry minus a safety margin so we
//     renew before the CP TTL lapses; if the CP-issued JWT has a shorter TTL
//     than the interval, we refresh sooner to avoid drift into an unauthenticated
//     window.
//
// The cadence pair is read from the shared durations holder EVERY round so a
// hot reload (POST /v1/admin/reload-config, todo 47) takes effect on the next
// cycle without restarting the loop.
//
// The CP-side RefreshBefore hint (jwtResp.RefreshBefore) is honoured indirectly
// through refreshBeforeExpiry from the node config: the CP informs the node of
// its desired lead time via the `refresh_before_expiry` YAML field, which the
// operator is expected to align with the CP's `RefreshBeforeSeconds` policy.
//
// On request failure: logs at Error, increments edge_jwt_refresh_failure_total,
// and continues the loop (NO panic/Fatal — consistent with the initial-failure
// degraded mode). On success, increments edge_jwt_refresh_success_total and
// updates edge_jwt_refresh_last_success_timestamp. After a successful refresh
// the new JWT is pushed to every connected peer that has already completed
// mutual auth (has a non-expired JWT entry in the PeerEntryStore). Per-peer
// 5s timeout; failures at Debug only, never block the refresh loop.
// The goroutine exits when ctx is cancelled (process shutdown via rootCtx).
func runJWTRefreshLoop(
	ctx context.Context,
	jwtClient *nodejwt.JWTClient,
	cpPubKey ed25519.PublicKey,
	durations *config.RefreshDurations,
	logger *slog.Logger,
	metrics *monitor.Metrics,
	h host.Host,
	ps *peerstore.PeerEntryStore,
) {
	for {
		// Read the live cadence each round; zero (e.g. loader path bypassed)
		// falls back to 5m.
		refreshInterval, refreshBeforeExpiry := durations.Load()
		if refreshInterval <= 0 {
			refreshInterval = 5 * time.Minute
		}
		if refreshBeforeExpiry <= 0 {
			refreshBeforeExpiry = 5 * time.Minute
		}

		wait := refreshInterval

		// If we have a cached JWT, refine the wait so we renew at
		// exp-now-refreshBeforeExpiry (but never later than refreshInterval).
		if jwt := jwtClient.CurrentJWT(); jwt != "" {
			if payload, err := sjwt.VerifyJWTAnyPeerID(jwt, cpPubKey); err == nil {
				exp := time.Unix(payload.Exp, 0)
				remaining := time.Until(exp)
				if remaining > refreshBeforeExpiry {
					wait = remaining - refreshBeforeExpiry
					if wait > refreshInterval {
						wait = refreshInterval
					}
				} else {
					// Already within the safety margin — refresh now.
					wait = 0
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		if _, err := jwtClient.RequestJWT(ctx); err != nil {
			logger.Error("jwt refresh request failed", "err", err)
			if metrics != nil {
				metrics.RecordJWTRefreshFailure()
			}
			continue
		}
		logger.Info("jwt refreshed")
		if metrics != nil {
			metrics.RecordJWTRefreshSuccess()
			metrics.SetJWTRefreshLastTS(time.Now().Unix())
		}

		// Push refreshed JWT to already-authenticated connected peers.
		pushRefreshedJWT(ctx, jwtClient, h, ps, logger)
	}
}

// pushRefreshedJWT sends the current JWT to every connected peer that has a
// non-expired JWT entry in the PeerEntryStore (i.e. mutual auth previously
// completed). Unauthenticated peers (JWTExp==0) and expired peers are skipped.
// Per-peer 5s timeout; failures at Debug only. Each peer is pushed at most once
// per refresh cycle — HandleJWTPush already deduplicates by Exp on the
// receiving side.
func pushRefreshedJWT(
	ctx context.Context,
	jwtClient *nodejwt.JWTClient,
	h host.Host,
	ps *peerstore.PeerEntryStore,
	logger *slog.Logger,
) {
	jwt := jwtClient.CurrentJWT()
	if jwt == "" {
		return
	}

	now := time.Now().Unix()
	for _, pid := range h.Network().Peers() {
		entry, ok := ps.Get(peerstore.PeerIdFromPeerID(pid))
		if !ok {
			continue
		}
		// JWTExp==0 means never authenticated; JWTExp <= now means expired.
		if entry.JWTExp == 0 || entry.JWTExp <= now {
			continue
		}

		pushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := nodejwt.PushJWT(h, pid, jwt); err != nil {
			logger.Debug("jwt-push to peer failed",
				"peer", pid.ShortString(),
				"err", err,
			)
		}
		cancel()
		<-pushCtx.Done()
	}

}
