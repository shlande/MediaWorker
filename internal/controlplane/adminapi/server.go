package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Audit recording (interface only — implementation + instrumentation: todo 33)
// ---------------------------------------------------------------------------

// AuditEntry is one admin-plane audit record. Kind groups related entries
// (e.g. "auth", "accounts"); Result is "ok"/"fail"-style outcome text.
// Detail carries structured context into the admin_audit.detail JSONB column
// (ban reason, rate_limit, changed-field flags). It MUST never contain
// credential material — instrumentation tests lock the exclusion.
type AuditEntry struct {
	TS     time.Time
	Kind   string
	Actor  string
	Action string
	Target string
	IP     string
	Result string
	Detail map[string]any
}

// AuditRecorder consumes audit entries. Implementations must tolerate
// concurrent use. The nil receiver path is always safe: Server treats a nil
// AuditRecorder as "auditing disabled".
type AuditRecorder interface {
	Record(ctx context.Context, entry AuditEntry)
}

// ---------------------------------------------------------------------------
// Context plumbing
// ---------------------------------------------------------------------------

// userCtxKey is the private context key under which the bearer middleware
// stores the authenticated admin identity.
type userCtxKey struct{}

// ctxUser carries the verified token claims into downstream handlers.
type ctxUser struct {
	userID   string
	username string
	roles    []string
}

// UserFromCtx returns the admin identity injected by the bearer middleware.
// ok is false when the request did not pass through authenticated middleware.
func UserFromCtx(ctx context.Context) (userID, username string, roles []string, ok bool) {
	u, ok := ctx.Value(userCtxKey{}).(*ctxUser)
	if !ok {
		return "", "", nil, false
	}
	return u.userID, u.username, u.roles, true
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// adminShutdownDrainTimeout bounds graceful shutdown: in-flight requests
// (reload-config / flush-cache can be long) are drained, not cut off.
const adminShutdownDrainTimeout = 10 * time.Second

// Server is the control-plane admin HTTP server skeleton. Routes are mounted
// via Handle; downstream todos (18, 20, 22, ...) register business handlers.
type Server struct {
	mux    *http.ServeMux
	secret []byte
	audit  AuditRecorder
}

// NewServer creates an admin Server. secret is the HMAC key for user tokens
// (see usertoken.go); it is never logged.
func NewServer(secret []byte) *Server {
	return &Server{mux: http.NewServeMux(), secret: secret}
}

// SetAuditRecorder installs the audit sink. Passing nil disables auditing.
// Safe to leave unset until todo 33 wires the real recorder.
func (s *Server) SetAuditRecorder(a AuditRecorder) {
	s.audit = a
}

// Handle mounts h on the mux under pattern. pattern uses Go 1.22+ ServeMux
// syntax (e.g. "POST /v1/auth/login", "GET /v1/auth/me"). When auth is true,
// the handler is wrapped in the bearer middleware: a valid admin user token
// is required and its claims are injected into the request context.
func (s *Server) Handle(pattern string, h http.Handler, auth bool) {
	if auth {
		h = s.bearerAuth(h)
	}
	s.mux.Handle(pattern, h)
}

// bearerAuth enforces `Authorization: Bearer <UserToken>`: missing or invalid
// tokens get 401, valid tokens without the "admin" role get 403. Failure
// bodies stay generic so token-validation internals are not leaked.
func (s *Server) bearerAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, prefix) {
			WriteError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		token := strings.TrimSpace(header[len(prefix):])
		if token == "" {
			WriteError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		payload, err := VerifyUserToken(token, s.secret)
		if err != nil {
			WriteError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		if !hasAdminRole(payload.Roles) {
			WriteError(w, http.StatusForbidden, "forbidden")
			return
		}
		u := &ctxUser{userID: payload.UserID, username: payload.Username, roles: payload.Roles}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userCtxKey{}, u)))
	})
}

func hasAdminRole(roles []string) bool {
	for _, role := range roles {
		if role == "admin" {
			return true
		}
	}
	return false
}

// Serve starts the HTTP server on listenAddr and blocks until ctx is
// cancelled, then drains in-flight requests with a bounded graceful
// shutdown (mirrors jwt/httpserver.go Serve with a 10s drain window).
func (s *Server) Serve(ctx context.Context, listenAddr string) error {
	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("adminapi: serve: %w", err)
		}
		close(errCh)
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), adminShutdownDrainTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("adminapi: shutdown: %w", err)
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

// Pagination bounds shared by all admin list endpoints.
const (
	defaultPage     = 1
	defaultPageSize = 20
	maxPageSize     = 100
)

// ParsePage extracts ?page / ?page_size with defaults 1/20 and a page-size
// ceiling of 100. Non-numeric or non-positive values fall back to defaults.
func ParsePage(r *http.Request) (page, pageSize int) {
	page, pageSize = defaultPage, defaultPageSize
	q := r.URL.Query()
	if v, err := strconv.Atoi(q.Get("page")); err == nil && v > 0 {
		page = v
	}
	if v, err := strconv.Atoi(q.Get("page_size")); err == nil && v > 0 {
		pageSize = v
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return page, pageSize
}
