-- janitor 两阶段软删除: blob 上的软删除时间戳
-- 阶段一 MarkOrphans 置 deleted_at = now(); 阶段二 Sweep 复核 content_blob 后硬删
-- 幂等: ADD COLUMN IF NOT EXISTS
ALTER TABLE blob ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
