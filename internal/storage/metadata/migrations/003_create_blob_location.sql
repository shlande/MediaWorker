CREATE TABLE IF NOT EXISTS blob_location (
    content_id   UUID NOT NULL,
    blob_hash    TEXT NOT NULL,
    vendor       TEXT NOT NULL,
    account_id   TEXT NOT NULL,
    file_id      TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (content_id, blob_hash, vendor, account_id),
    FOREIGN KEY (content_id, blob_hash)
        REFERENCES blob_index(content_id, blob_hash)
);