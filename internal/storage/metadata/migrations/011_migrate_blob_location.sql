-- 数据迁移: blob_location → blob_location_v2
-- 合并 vendor + account_id 为 backend_id ("vendor:account_id" 格式)
-- 幂等: 如果 blob_location 表没有 vendor 列（已迁移或 schema 已是新形态），跳过。
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'blob_location' AND column_name = 'vendor'
    ) THEN
        INSERT INTO blob_location_v2 (blob_hash, backend_id, file_id)
        SELECT DISTINCT blob_hash, vendor || ':' || account_id, file_id
        FROM blob_location
        WHERE blob_hash IS NOT NULL
        ON CONFLICT (blob_hash, backend_id) DO NOTHING;
    END IF;
END $$;