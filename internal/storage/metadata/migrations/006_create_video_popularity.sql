-- 此表逻辑属于分发域, 此处创建为依赖 stub
CREATE TABLE IF NOT EXISTS video_popularity (
    content_id   UUID PRIMARY KEY,
    window_24h   BIGINT DEFAULT 0,
    updated_at   TIMESTAMPTZ DEFAULT now()
);