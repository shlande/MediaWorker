package adminapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/shlande/mediaworker/internal/storage/metadata"
)

// ─── Narrow metadata dependency (interface → testable) ────────────────────

// AdminUserStore is the user-table surface the auth handlers and the admin
// seed need from the metadata layer. *metadata.PGMetadataClient satisfies it
// (todo 2 accessors in metadata_app_user.go).
type AdminUserStore interface {
	GetUserByUsername(ctx context.Context, username string) (userID, passwordHash string, roles []string, disabled bool, err error)
	CountUsers(ctx context.Context) (int, error)
	CreateUser(ctx context.Context, username, passwordHash string, roles []string) error
}

// userTokenTTL is the login token lifetime (8h per auth contract).
const userTokenTTL = 8 * time.Hour

// ─── Wire types ────────────────────────────────────────────────────────────

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token     string   `json:"token"`
	ExpiresAt string   `json:"expires_at"` // RFC3339
	Roles     []string `json:"roles"`
}

type meResponse struct {
	UserID   string   `json:"user_id"`
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
}

// ─── Handlers ──────────────────────────────────────────────────────────────

// loginHandler serves POST /v1/auth/login.
//
// Anti-enumeration: an unknown username and a wrong password produce the
// IDENTICAL 401 body ({"error":"invalid credentials"}); neither the error
// text nor the status distinguishes them. A disabled account gets 403.
//
// v1 deliberately does NOT rate-limit login attempts: the admin API is
// bound to the intranet (default 127.0.0.1). Re-evaluate before any
// external exposure — this is the documented risk acceptance.
func loginHandler(users AdminUserStore, secret []byte, audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req loginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		userID, passwordHash, roles, disabled, err := users.GetUserByUsername(r.Context(), req.Username)
		if err != nil {
			if !errors.Is(err, metadata.ErrUserNotFound) {
				recordAuthAudit(r, audit, req.Username, "login", "fail")
				WriteError(w, http.StatusInternalServerError, "internal error")
				return
			}
			recordAuthAudit(r, audit, req.Username, "login", "fail")
			WriteError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		if disabled {
			recordAuthAudit(r, audit, req.Username, "login", "fail")
			WriteError(w, http.StatusForbidden, "forbidden")
			return
		}
		if bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)) != nil {
			recordAuthAudit(r, audit, req.Username, "login", "fail")
			WriteError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}

		now := time.Now()
		exp := now.Add(userTokenTTL)
		token, err := SignUserToken(UserTokenPayload{
			UserID:   userID,
			Username: req.Username,
			Roles:    roles,
			Iat:      now.Unix(),
			Exp:      exp.Unix(),
		}, secret)
		if err != nil {
			recordAuthAudit(r, audit, req.Username, "login", "fail")
			WriteError(w, http.StatusInternalServerError, "internal error")
			return
		}

		recordAuthAudit(r, audit, req.Username, "login", "ok")
		WriteJSON(w, http.StatusOK, loginResponse{
			Token:     token,
			ExpiresAt: exp.UTC().Format(time.RFC3339),
			Roles:     roles,
		})
	})
}

// meHandler serves GET /v1/auth/me: the authenticated identity from the
// bearer-middleware context.
func meHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, username, roles, ok := UserFromCtx(r.Context())
		if !ok {
			WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		WriteJSON(w, http.StatusOK, meResponse{UserID: userID, Username: username, Roles: roles})
	})
}

// logoutHandler serves POST /v1/auth/logout. Tokens are stateless JWTs, so
// there is NO server-side revocation: logout is audit-only and the client
// discards the token. A stolen token remains valid until its 8h expiry —
// documented v1 limitation, no refresh tokens exist to rotate.
func logoutHandler(audit AuditRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, username, _, ok := UserFromCtx(r.Context())
		if !ok {
			WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		recordAuthAudit(r, audit, username, "logout", "ok")
		w.WriteHeader(http.StatusNoContent)
	})
}

// recordAuthAudit emits one kind="auth" audit entry. AuditRecorder is
// nil-tolerant (implementation lands in todo 33); the IP is best-effort.
func recordAuthAudit(r *http.Request, audit AuditRecorder, actor, action, result string) {
	if audit == nil {
		return
	}
	audit.Record(r.Context(), AuditEntry{
		TS:     time.Now(),
		Kind:   "auth",
		Actor:  actor,
		Action: action,
		Target: actor,
		IP:     r.RemoteAddr,
		Result: result,
	})
}

// ─── Route registration ───────────────────────────────────────────────────

// RegisterAuthRoutes mounts the three auth endpoints on srv. Login is
// unauthenticated (it ISSUES the credential); me/logout require a valid
// admin bearer token.
func RegisterAuthRoutes(srv *Server, users AdminUserStore) {
	srv.Handle("POST /v1/auth/login", loginHandler(users, srv.secret, srv.audit), false)
	srv.Handle("GET /v1/auth/me", meHandler(), true)
	srv.Handle("POST /v1/auth/logout", logoutHandler(srv.audit), true)
}

// ─── Bootstrap seed ───────────────────────────────────────────────────────

// bootstrapPasswordEnv supplies the initial admin password.
const bootstrapPasswordEnv = "ADMIN_BOOTSTRAP_PASSWORD"

// SeedAdminIfEmpty creates the initial "admin" user (roles={admin}) when —
// and only when — the user table is empty, so restarts are idempotent. The
// password comes from ADMIN_BOOTSTRAP_PASSWORD; when the env is unset, 16
// random bytes are generated and the hex-encoded plaintext is Warn-logged
// ONCE (startup only, never persisted anywhere else).
func SeedAdminIfEmpty(ctx context.Context, users AdminUserStore) error {
	n, err := users.CountUsers(ctx)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}

	password := os.Getenv(bootstrapPasswordEnv)
	if password == "" {
		buf := make([]byte, 16)
		if _, err := rand.Read(buf); err != nil {
			return err
		}
		password = hex.EncodeToString(buf)
		slog.Warn("admin bootstrap: generated random initial password (set ADMIN_BOOTSTRAP_PASSWORD to choose your own)",
			"username", "admin", "password", password)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	if err := users.CreateUser(ctx, "admin", string(hash), []string{"admin"}); err != nil {
		return err
	}
	slog.Info("admin bootstrap: seeded initial admin user", "username", "admin")
	return nil
}
