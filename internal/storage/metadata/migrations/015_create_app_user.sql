CREATE TABLE IF NOT EXISTS app_user (
    user_id       UUID PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    roles         TEXT[] NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    disabled      BOOLEAN DEFAULT false
);
