-- 编排层: content_blob 关联表 (blob 在某 content 内的 role/sort_order/business_meta)
-- role / sort_order / business_meta 都是关联属性, 不是 blob 本身的属性
CREATE TABLE IF NOT EXISTS content_blob (
    content_id    UUID NOT NULL REFERENCES content(content_id),
    blob_hash     TEXT NOT NULL REFERENCES blob(blob_hash),
    role          TEXT NOT NULL,            -- "init" | "media" | "original" | "thumbnail" | "page" | ...
    sort_order    INTEGER DEFAULT 0,        -- 段序号 / 页码 / 缩略图尺寸升序
    business_meta JSONB,                    -- {"representation_id":"720p","bitrate":1500000} 等
    PRIMARY KEY (content_id, blob_hash)
);

CREATE INDEX IF NOT EXISTS idx_content_blob_role ON content_blob (role);
CREATE INDEX IF NOT EXISTS idx_content_blob_sort ON content_blob (content_id, role, sort_order);