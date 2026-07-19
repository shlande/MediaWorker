CREATE TABLE IF NOT EXISTS alert_events (
    id          BIGSERIAL PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    name        TEXT NOT NULL,
    severity    TEXT,
    target      TEXT,
    detail      JSONB,
    status      TEXT NOT NULL,
    since       TIMESTAMPTZ,
    received_at TIMESTAMPTZ DEFAULT now()
);

-- Alertmanager repeat_interval resend dedup: one logical alert instance is
-- (fingerprint, since). NOTE: NULL since values never conflict (SQL NULL
-- semantics); Alertmanager always sends startsAt for firing alerts, so this
-- is safe in practice.
CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_events_fp_since
    ON alert_events (fingerprint, since);

CREATE INDEX IF NOT EXISTS idx_alert_events_status
    ON alert_events (status, received_at DESC);
