package adminapi

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/shlande/mediaworker/internal/config"
)

// Reloader performs hot reloads of the whitelisted config subset for
// POST /v1/admin/reload-config. It is the SIGHUP-style entry point over HTTP:
// jwt_service.refresh_interval / refresh_before_expiry are published into the
// shared RefreshDurations the JWT loop reads every round, admin_api.token is
// hot-updated via Server.SetToken, and anything else is reported as
// not_applied (no full reload — listen addresses, identity key, cache paths
// and hash_ring.replicas all require a restart).
type Reloader struct {
	path      string
	durations *config.RefreshDurations

	mu       sync.Mutex // serializes reloads; guards running + setToken
	running  *config.Config
	setToken func(string)
}

// NewReloader wires a Reloader. configPath is re-loaded on every request;
// running is the effective runtime config used as the diff baseline (it
// advances as whitelist fields are applied); durations is the shared holder
// the JWT refresh loop reads from.
func NewReloader(configPath string, running *config.Config, durations *config.RefreshDurations) *Reloader {
	return &Reloader{path: configPath, running: running, durations: durations}
}

// RegisterReloadRoutes mounts POST /v1/admin/reload-config on srv. The route
// inherits the server-wide X-Admin-Token middleware like every admin route.
func (rl *Reloader) RegisterReloadRoutes(srv *Server) {
	rl.mu.Lock()
	rl.setToken = srv.SetToken
	rl.mu.Unlock()
	srv.Handle("POST /v1/admin/reload-config", rl.handleReload)
}

func (rl *Reloader) handleReload(w http.ResponseWriter, _ *http.Request) {
	fresh, err := config.LoadConfig(rl.path)
	if err != nil {
		// Running config stays untouched: nothing was parsed, nothing applied.
		WriteError(w, http.StatusUnprocessableEntity, fmt.Sprintf("reload config: %v", err))
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	diff := config.DiffForReload(rl.running, fresh)
	report := &config.ReloadReport{NotApplied: diff.NotApplied}

	if diff.RefreshInterval != nil || diff.RefreshBeforeExpiry != nil {
		interval, beforeExpiry := rl.durations.Load()
		if diff.RefreshInterval != nil {
			interval = *diff.RefreshInterval
			report.Applied = append(report.Applied, config.ReloadFieldJWTRefreshInterval)
		}
		if diff.RefreshBeforeExpiry != nil {
			beforeExpiry = *diff.RefreshBeforeExpiry
			report.Applied = append(report.Applied, config.ReloadFieldJWTRefreshBeforeExpiry)
		}
		rl.durations.Store(interval, beforeExpiry)
	}
	if diff.AdminToken != nil && rl.setToken != nil {
		// The in-flight request already authenticated with the previous token
		// (middleware runs before the handler), so rotating here is safe.
		rl.setToken(*diff.AdminToken)
		report.Applied = append(report.Applied, config.ReloadFieldAdminToken)
	}

	rl.running = config.AdvanceReloadBaseline(rl.running, fresh, diff)

	WriteJSON(w, http.StatusOK, report)
}
