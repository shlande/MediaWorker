CREATE TABLE IF NOT EXISTS node_status_history (
    id           BIGSERIAL PRIMARY KEY,
    peer_id      TEXT NOT NULL,
    node_id      TEXT,
    healthy      BOOLEAN NOT NULL,
    prefix_used  BIGINT,
    prefix_total BIGINT,
    warm_used    BIGINT,
    warm_total   BIGINT,
    conn_count   INT,
    region       TEXT,
    version      TEXT,
    reported_at  TIMESTAMPTZ NOT NULL,
    received_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_node_status_history_peer
    ON node_status_history (peer_id, received_at DESC);
