CREATE TABLE IF NOT EXISTS content (
    content_id      UUID PRIMARY KEY,
    content_type    TEXT NOT NULL,
    type_metadata   JSONB,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);