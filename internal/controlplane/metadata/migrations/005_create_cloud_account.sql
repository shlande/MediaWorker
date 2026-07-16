CREATE TABLE IF NOT EXISTS cloud_account (
    vendor             TEXT NOT NULL,
    account_id         TEXT NOT NULL,
    credential         JSONB,
    rate_limit_config  JSONB,
    vendor_profile     JSONB,
    enabled            BOOLEAN DEFAULT true,
    created_at         TIMESTAMPTZ DEFAULT now(),
    updated_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (vendor, account_id)
);