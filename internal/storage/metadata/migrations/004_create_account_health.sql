CREATE TABLE IF NOT EXISTS account_health (
    vendor       TEXT NOT NULL,
    account_id   TEXT NOT NULL,
    state        TEXT NOT NULL,
    last_check   TIMESTAMPTZ NOT NULL,
    latency_ms   INTEGER,
    error_msg    TEXT,
    ban_until    TIMESTAMPTZ,
    PRIMARY KEY (vendor, account_id)
);