package jwt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// JWTHTTPServer serves POST /v1/node/jwt for JWT credential signing.
type JWTHTTPServer struct {
	service *JWTService
}

// NewJWTHTTPServer creates a JWTHTTPServer backed by the given JWTService.
func NewJWTHTTPServer(service *JWTService) *JWTHTTPServer {
	return &JWTHTTPServer{service: service}
}

// Serve starts the HTTP server on listenAddr and blocks until ctx is
// cancelled, at which point it performs a graceful shutdown.
func (s *JWTHTTPServer) Serve(ctx context.Context, listenAddr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/node/jwt", s.handleJWTRequest)

	srv := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
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

func (s *JWTHTTPServer) handleJWTRequest(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeHTTPError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var jwtReq types.JWTRequest
	if err := json.NewDecoder(req.Body).Decode(&jwtReq); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	remoteIP := extractRemoteIP(req)

	resp, err := s.service.HandleJWTRequest(jwtReq, remoteIP)
	if err != nil {
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func writeHTTPError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
