CREATE TABLE IF NOT EXISTS admin_audit (
    id     BIGSERIAL PRIMARY KEY,
    ts     TIMESTAMPTZ DEFAULT now(),
    kind   TEXT NOT NULL,
    actor  TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT,
    ip     TEXT,
    result TEXT NOT NULL,
    detail JSONB
);
CREATE INDEX IF NOT EXISTS idx_admin_audit_ts ON admin_audit(ts DESC);
CREATE INDEX IF NOT EXISTS idx_admin_audit_kind ON admin_audit(kind, ts DESC);
