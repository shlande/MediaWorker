-- 热度表重命名: video_popularity → content_popularity (content 维度热度)
-- 字段不变: content_id (PK) + window_24h + updated_at
-- 幂等: 如果 content_popularity 已存在（部分迁移遗留），直接删除旧的 video_popularity。
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'video_popularity') THEN
        IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'content_popularity') THEN
            -- content_popularity 已存在，说明之前部分迁移已建表，删除旧的 video_popularity
            DROP TABLE video_popularity;
        ELSE
            -- 正常路径：重命名
            ALTER TABLE video_popularity RENAME TO content_popularity;
        END IF;
    END IF;
END $$;