-- 内容寻址层: blob 主表 (blob_hash = SHA-256, 全局唯一, 跨 content 去重)
CREATE TABLE IF NOT EXISTS blob (
    blob_hash   TEXT PRIMARY KEY,
    blob_type   TEXT NOT NULL,              -- 二进制产出类型: "mp4_init_segment" | "m4s_media_segment" | "jpeg_original" | ...
    size_bytes  BIGINT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);