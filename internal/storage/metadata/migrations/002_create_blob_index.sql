CREATE TABLE IF NOT EXISTS blob_index (
    content_id   UUID NOT NULL REFERENCES content(content_id),
    blob_hash    TEXT NOT NULL,
    role         TEXT NOT NULL,
    sort_order   INTEGER DEFAULT 0,
    size_bytes   BIGINT NOT NULL,
    checksum     TEXT NOT NULL,
    PRIMARY KEY (content_id, blob_hash)
);