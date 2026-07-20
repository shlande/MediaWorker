package jwt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	cpmetrics "github.com/shlande/mediaworker/internal/controlplane/metrics"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// JWTHTTPServer serves POST /v1/node/jwt for JWT credential signing.
// It can additionally host an optional, separately-registered handler for
// GET /v1/blob-locations/{hash} (T9) and GET /metrics (T20). Reusing the
// same mux avoids a new listening port (plan line 176 / 275).
type JWTHTTPServer struct {
	service         *JWTService
	locationHandler http.Handler
	metricsHandler  http.Handler
	metrics         *cpmetrics.Metrics
}

// NewJWTHTTPServer creates a JWTHTTPServer backed by the given JWTService.
func NewJWTHTTPServer(service *JWTService) *JWTHTTPServer {
	return &JWTHTTPServer{service: service}
}

// RegisterLocationHandler registers a handler for GET /v1/blob-locations/{hash}.
// The handler must perform its own JWT authentication and capability checks.
// If never called, the route is simply not mounted. Calling it more than once
// replaces the previously-registered handler. Serve must not have started yet
// when this is called — mounting happens at Serve-time.
func (s *JWTHTTPServer) RegisterLocationHandler(h http.Handler) {
	s.locationHandler = h
}

// RegisterMetricsHandler mounts GET /metrics on the JWT server's mux (T20).
// Pass the same Metrics instance that is wired into the PinOrchestrator so
// the /metrics scrape reflects counters incremented across the CP. If never
// called, /metrics is not mounted.
func (s *JWTHTTPServer) RegisterMetricsHandler(metrics *cpmetrics.Metrics) {
	s.metrics = metrics
	if metrics != nil {
		s.metricsHandler = metrics.HTTPHandler()
	}
}

// handleMetrics wraps s.metricsHandler as a named http.Handler for swag annotation.
//
//	@Summary		Prometheus 指标
//	@Description	Prometheus 文本格式指标端点
//	@Tags			ops
//	@Produce		plain
//	@Success		200	{string}	string
//	@Router			/metrics [get]
func (s *JWTHTTPServer) handleMetrics() http.Handler {
	return s.metricsHandler
}

// Serve starts the HTTP server on listenAddr and blocks until ctx is
// cancelled, at which point it performs a graceful shutdown.
//
// A zero readTimeout or writeTimeout disables that timeout (matching
// net/http semantics); main.go normalises empty config strings to
// DefaultJWTHTTPTimeout (10s) before calling this.
func (s *JWTHTTPServer) Serve(ctx context.Context, listenAddr string, readTimeout, writeTimeout time.Duration) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/node/jwt", s.handleJWTRequest)
	if s.locationHandler != nil {
		mux.Handle("GET /v1/blob-locations/{hash}", s.locationHandler)
	}
	if s.metricsHandler != nil {
		// No auth — /metrics is intended for the operator/scraper network
		// behind the same ACL as the JWT port (plan line 275 — intranet).
		mux.Handle("GET /metrics", s.handleMetrics())
	}

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("jwt http serve: %w", err)
		}
		close(errCh)
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("jwt http shutdown: %w", err)
	}

	return <-errCh
}

const shutdownTimeout = 5 * 1e9 // 5 seconds (time.Duration literal)

// DefaultJWTHTTPTimeout is the fallback for empty/unparseable JWTHTTPConfig
// ReadTimeout / WriteTimeout strings. Matches the documented config default.
const DefaultJWTHTTPTimeout = 10 * time.Second

// extractRemoteIP derives the client IP from X-Forwarded-For or RemoteAddr.
func extractRemoteIP(req *http.Request) string {
	if fwd := req.Header.Get("X-Forwarded-For"); fwd != "" {
		// Take the first entry (client origin).
		if idx := strings.IndexByte(fwd, ','); idx != -1 {
			return strings.TrimSpace(fwd[:idx])
		}
		return strings.TrimSpace(fwd)
	}
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		// RemoteAddr has no port; use as-is.
		return req.RemoteAddr
	}
	return host
}

// handleJWTRequest 签发节点能力 JWT。
//
//	@Summary		签发节点能力 JWT
//	@Description	验证节点提供的 Ed25519 签名请求并签发能力 JWT
//	@Tags			node-auth
//	@Accept			json
//	@Produce		json
//	@Param			request	body		types.JWTRequest	true	"JWT 签发请求"
//	@Success		200		{object}	types.JWTResponse
//	@Failure		400		{object}	types.ErrorResponse	"无效 JSON 或非法 PeerID"
//	@Failure		403		{object}	types.ErrorResponse	"签名验证失败"
//	@Failure		405		{object}	types.ErrorResponse	"方法不允许"
//	@Failure		429		{object}	types.ErrorResponse	"速率限制"
//	@Failure		500		{object}	types.ErrorResponse	"内部错误"
//	@Router			/v1/node/jwt [post]
func (s *JWTHTTPServer) handleJWTRequest(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeHTTPError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var jwtReq types.JWTRequest
	if err := json.NewDecoder(req.Body).Decode(&jwtReq); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid JSON body")
		if s.metrics != nil {
			s.metrics.RecordCPJWTIssued(cpmetrics.CPJWTOutcomeInternalError)
		}
		return
	}

	remoteIP := extractRemoteIP(req)

	resp, err := s.service.HandleJWTRequest(jwtReq, remoteIP)
	if err != nil {
		if s.metrics != nil {
			switch {
			case errors.Is(err, sjwt.ErrInvalidPeerID):
				s.metrics.RecordCPJWTIssued(cpmetrics.CPJWTOutcomeInvalidPeerID)
			case errors.Is(err, sjwt.ErrInvalidSignature):
				s.metrics.RecordCPJWTIssued(cpmetrics.CPJWTOutcomeInvalidSig)
			case errors.Is(err, sjwt.ErrRateLimited):
				s.metrics.RecordCPJWTIssued(cpmetrics.CPJWTOutcomeRateLimited)
			default:
				s.metrics.RecordCPJWTIssued(cpmetrics.CPJWTOutcomeInternalError)
			}
		}
		switch {
		case errors.Is(err, sjwt.ErrInvalidPeerID):
			writeHTTPError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, sjwt.ErrInvalidSignature):
			writeHTTPError(w, http.StatusForbidden, err.Error())
		case errors.Is(err, sjwt.ErrRateLimited):
			writeHTTPError(w, http.StatusTooManyRequests, err.Error())
		default:
			writeHTTPError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	if s.metrics != nil {
		s.metrics.RecordCPJWTIssued(cpmetrics.CPJWTOutcomeSuccess)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp) // client may have disconnected; not actionable
}

func writeHTTPError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg}) // client may have disconnected; not actionable
}
