-- 管理后台内容列表标题 + 软删除标记 (E4): content.title + content.deleted_at
-- title 由 ingest 透传 (opts.Metadata["title"]); deleted_at 由 SoftDeleteContent 置位,
-- blob 不硬删 (content_blob 解除关联后由 janitor MarkOrphans 回收)
-- 幂等: ADD COLUMN IF NOT EXISTS
ALTER TABLE content ADD COLUMN IF NOT EXISTS title TEXT;
ALTER TABLE content ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
