-- B1 凭据结构归一化: cloud_account.client_config 承载静态授权材料
-- (client_id/client_secret/redirect_uri/region — 管理员维护, 经 ACCOUNT_SNAPSHOT 下发)
-- 可空, 存量行不受影响; Cookie 系厂商 (115/quark) 该列保持 NULL
-- 幂等: ADD COLUMN IF NOT EXISTS
ALTER TABLE cloud_account ADD COLUMN IF NOT EXISTS client_config JSONB;
