-- 新建 blob_location_v2: 跨 content 共享, 无 content_id 列, backend_id 替代 vendor+account_id
CREATE TABLE IF NOT EXISTS blob_location_v2 (
    blob_hash   TEXT NOT NULL REFERENCES blob(blob_hash),
    backend_id  TEXT NOT NULL,              -- "vendor:account_id", e.g. "115:acct_03"
    file_id     TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (blob_hash, backend_id)
);