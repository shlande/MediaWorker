-- 热度表重命名: video_popularity → content_popularity (content 维度热度)
-- 字段不变: content_id (PK) + window_24h + updated_at
ALTER TABLE IF EXISTS video_popularity RENAME TO content_popularity;