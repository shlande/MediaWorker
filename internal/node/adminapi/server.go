// Package adminapi implements the node-local admin HTTP server: an
// independent listener (NOT the :8080 client mux) where every mounted route
// requires the X-Admin-Token header. Operators use it to inspect node
// identity/JWT/cache/pin/peer/backhaul state; the only write operation is
// pin retry. The token comes from admin_api.token / NODE_ADMIN_TOKEN and is
// never logged.
package adminapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// TokenHeader is the request header carrying the node admin token.
const TokenHeader = "X-Admin-Token"

// adminShutdownDrainTimeout bounds graceful shutdown: in-flight requests are
// drained, not cut off (mirrors the CP admin server pattern).
const adminShutdownDrainTimeout = 10 * time.Second

// Server is the node-local admin HTTP server skeleton. Routes are mounted via
// Handle; downstream todos (42-48, consolidated in 49) register business
// handlers.
type Server struct {
	mux *http.ServeMux

	// mu guards token: todo 47's config reload hot-updates it via SetToken
	// while in-flight requests read it under RLock.
	mu    sync.RWMutex
	token string
}

// NewServer creates a node admin Server. token is the expected X-Admin-Token
// value; it is never logged.
func NewServer(token string) *Server {
	return &Server{mux: http.NewServeMux(), token: token}
}

// SetToken hot-updates the expected admin token. Safe for concurrent use with
// in-flight requests.
func (s *Server) SetToken(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = token
}

// Handle mounts h on the mux under pattern (Go 1.22+ ServeMux syntax, e.g.
// "GET /v1/healthz"). Every route is wrapped in the X-Admin-Token middleware.
// Use HandleUnauthenticated for routes that must be reachable without a token
// (e.g. k8s health probes).
func (s *Server) Handle(pattern string, h http.HandlerFunc) {
	s.mux.Handle(pattern, s.tokenAuth(h))
}

// HandleUnauthenticated mounts h on the mux without the X-Admin-Token middleware.
// Use only for routes that must be reachable without a token (e.g. GET /v1/healthz).
// NOTE: the mux is a plain http.ServeMux — the last Handle/HandleUnauthenticated
// call for a given pattern wins, so there is no risk of double-registration.
func (s *Server) HandleUnauthenticated(pattern string, h http.HandlerFunc) {
	s.mux.Handle(pattern, h)
}

// tokenAuth enforces the X-Admin-Token header. Comparison is constant-time
// (crypto/subtle) so the token cannot be probed byte-by-byte via timing. The
// failure body stays generic so token internals are not leaked.
func (s *Server) tokenAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		expected := s.token
		s.mu.RUnlock()
		if subtle.ConstantTimeCompare([]byte(r.Header.Get(TokenHeader)), []byte(expected)) != 1 {
			WriteError(w, http.StatusUnauthorized, "invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	}
}

// Serve starts the HTTP server on listenAddr and blocks until ctx is
// cancelled, then drains in-flight requests with a bounded graceful shutdown
// (mirrors the CP admin server Serve with a 10s drain window).
func (s *Server) Serve(ctx context.Context, listenAddr string) error {
	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("node/adminapi: serve: %w", err)
		}
		close(errCh)
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), adminShutdownDrainTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("node/adminapi: shutdown: %w", err)
	}

	return <-errCh
}

// ---------------------------------------------------------------------------
// Handler helpers
// ---------------------------------------------------------------------------

// WriteJSON encodes v as the response body with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // client may have disconnected; not actionable
}

// WriteError emits the unified admin error body {"error": msg}.
func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, map[string]string{"error": msg})
}
