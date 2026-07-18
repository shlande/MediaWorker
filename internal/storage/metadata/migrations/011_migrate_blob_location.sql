-- 数据迁移: blob_location → blob_location_v2
-- 合并 vendor + account_id 为 backend_id ("vendor:account_id" 格式)
INSERT INTO blob_location_v2 (blob_hash, backend_id, file_id)
SELECT DISTINCT blob_hash, vendor || ':' || account_id, file_id
FROM blob_location
WHERE blob_hash IS NOT NULL
ON CONFLICT (blob_hash, backend_id) DO NOTHING;