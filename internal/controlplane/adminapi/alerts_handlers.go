package adminapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/shlande/mediaworker/internal/storage/metadata"
)

// ---------------------------------------------------------------------------
// Alert pipeline (ui-admin-apis todo 51)
//
// Alertmanager --webhook--> POST /v1/admin/alerts/webhook --> alert_events
// table; the dashboard reads the current view via GET /v1/admin/alerts.
//
// The webhook is NOT bearer-authed (Alertmanager cannot hold a user token);
// it validates the X-Alert-Token header against cfg.AdminAPI.AlertWebhookToken
// instead. When the token is not configured the webhook route is not mounted
// at all (requests get the mux's plain 404).
// ---------------------------------------------------------------------------

// AlertEventStore is the narrow persistence dependency of the alerts
// endpoints. *metadata.PGMetadataClient satisfies it; tests use a fake.
type AlertEventStore interface {
	InsertAlertEvent(ctx context.Context, row metadata.AlertEventRow) error
	ListAlertEvents(ctx context.Context, status string, limit int) ([]metadata.AlertEventRow, error)
}

// maxAlertWebhookBody bounds the webhook payload (a large flapping group is
// still far below 1 MiB).
const maxAlertWebhookBody = 1 << 20

// alertsListLimit is the v1 firing-view ceiling (no resolved auto-cleanup).
const alertsListLimit = 100

// RegisterAlertsRoutes mounts the alert pipeline endpoints on srv. The GET
// endpoint is always mounted behind the bearer middleware; the webhook
// endpoint is only mounted when webhookToken is configured, and authenticates
// via X-Alert-Token instead of the bearer middleware.
func RegisterAlertsRoutes(srv *Server, mc AlertEventStore, webhookToken string) {
	if webhookToken != "" {
		srv.Handle("POST /v1/admin/alerts/webhook", alertWebhookHandler(mc, webhookToken), false)
	}
	srv.Handle("GET /v1/admin/alerts", listAlertsHandler(mc), true)
}

// ---------------------------------------------------------------------------
// Alertmanager v4 webhook payload (labels/annotations parsed tolerantly)
// ---------------------------------------------------------------------------

// stringMap decodes a JSON object whose values are normally strings, keeping
// non-string scalars as their compact JSON form instead of failing the whole
// payload (Alertmanager label values are strings by contract, but receivers
// must not choke on exotic senders).
type stringMap map[string]string

func (m *stringMap) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	out := make(map[string]string, len(raw))
	for k, rv := range raw {
		var s string
		if err := json.Unmarshal(rv, &s); err == nil {
			out[k] = s
		} else {
			out[k] = string(rv)
		}
	}
	*m = out
	return nil
}

type amAlert struct {
	Status      string    `json:"status"`
	Fingerprint string    `json:"fingerprint"`
	StartsAt    time.Time `json:"startsAt"`
	Labels      stringMap `json:"labels"`
	Annotations stringMap `json:"annotations"`
}

// amWebhookPayload mirrors the Alertmanager v4 webhook body. Alerts is a
// pointer so a missing (or null) "alerts" field is distinguishable from an
// empty list: missing is a malformed payload (400), empty is received:0.
type amWebhookPayload struct {
	Status string     `json:"status"`
	Alerts *[]amAlert `json:"alerts"`
}

// ---------------------------------------------------------------------------
// POST /v1/admin/alerts/webhook
// ---------------------------------------------------------------------------

func alertWebhookHandler(mc AlertEventStore, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Alert-Token") != token {
			WriteError(w, http.StatusUnauthorized, "invalid alert token")
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxAlertWebhookBody)
		var payload amWebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid alertmanager payload")
			return
		}
		if payload.Alerts == nil {
			WriteError(w, http.StatusBadRequest, `missing "alerts" field`)
			return
		}

		for _, a := range *payload.Alerts {
			row := alertRowFromWebhook(a, payload.Status)
			if err := mc.InsertAlertEvent(r.Context(), row); err != nil {
				// Alertmanager retries non-2xx; the (fingerprint, since) upsert
				// makes a resend safe even if earlier alerts in this batch
				// were already persisted.
				WriteError(w, http.StatusInternalServerError, "persist alert event")
				return
			}
		}
		WriteJSON(w, http.StatusAccepted, map[string]int{"received": len(*payload.Alerts)})
	})
}

// alertRowFromWebhook maps one webhook alert to a storage row. The
// fingerprint falls back to an alertname+startsAt hash when Alertmanager's
// native fingerprint is absent; target prefers labels.instance over
// labels.peer_id.
func alertRowFromWebhook(a amAlert, payloadStatus string) metadata.AlertEventRow {
	name := a.Labels["alertname"]

	fingerprint := a.Fingerprint
	if fingerprint == "" {
		sum := sha256.Sum256([]byte(name + "|" + a.StartsAt.UTC().Format(time.RFC3339Nano)))
		fingerprint = hex.EncodeToString(sum[:16])
	}

	var severity *string
	if s := a.Labels["severity"]; s != "" {
		severity = &s
	}

	target := a.Labels["instance"]
	if target == "" {
		target = a.Labels["peer_id"]
	}
	var targetPtr *string
	if target != "" {
		targetPtr = &target
	}

	status := a.Status
	if status == "" {
		status = payloadStatus
	}

	var since *time.Time
	if !a.StartsAt.IsZero() {
		t := a.StartsAt
		since = &t
	}

	detail, err := json.Marshal(map[string]any{
		"labels":      map[string]string(a.Labels),
		"annotations": map[string]string(a.Annotations),
	})
	if err != nil {
		detail = nil // unreachable for map[string]string values; detail stays NULL
	}

	return metadata.AlertEventRow{
		Fingerprint: fingerprint,
		Name:        name,
		Severity:    severity,
		Target:      targetPtr,
		Detail:      detail,
		Status:      status,
		Since:       since,
	}
}

// ---------------------------------------------------------------------------
// GET /v1/admin/alerts
// ---------------------------------------------------------------------------

// alertItem is the wire shape of GET /v1/admin/alerts (ui-api-requirements
// §3.1 alerts[] contract: {name, severity, target, since, detail}).
type alertItem struct {
	Name     string          `json:"name"`
	Severity string          `json:"severity"`
	Target   string          `json:"target"`
	Since    *time.Time      `json:"since"`
	Detail   json.RawMessage `json:"detail"`
}

func listAlertsHandler(mc AlertEventStore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Query().Get("status")
		if status == "" {
			status = "firing"
		}
		limit := alertsListLimit
		if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v < limit {
			limit = v
		}

		rows, err := mc.ListAlertEvents(r.Context(), status, limit)
		if err != nil {
			WriteError(w, http.StatusInternalServerError, "list alert events")
			return
		}

		items := make([]alertItem, 0, len(rows))
		for _, row := range rows {
			items = append(items, alertItem{
				Name:     row.Name,
				Severity: deref(row.Severity),
				Target:   deref(row.Target),
				Since:    row.Since,
				Detail:   row.Detail,
			})
		}
		WriteJSON(w, http.StatusOK, items)
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
