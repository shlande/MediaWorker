-- 删除老 blob_location, 重命名 v2 为 blob_location
DROP TABLE IF EXISTS blob_location;
ALTER TABLE blob_location_v2 RENAME TO blob_location;