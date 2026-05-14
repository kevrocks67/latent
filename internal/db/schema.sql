-- Schema for the Latent Cache Metadata
-- We use TIMESTAMPTZ to ensure consistent time handling across timezones.

DO $$ BEGIN
    CREATE TYPE cache_state AS ENUM ('missing', 'filling', 'ready', 'error', 'stale');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS cache_records (
    cache_key    TEXT PRIMARY KEY,
    object_key   TEXT NOT NULL,
    owner_node   TEXT NOT NULL,
    state        cache_state NOT NULL DEFAULT 'filling',
    etag         TEXT NOT NULL DEFAULT '',
    size_bytes   BIGINT NOT NULL DEFAULT -1,
    fresh_until  TIMESTAMPTZ NOT NULL,
    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for background cleanup of expired artifacts
CREATE INDEX IF NOT EXISTS idx_cache_fresh_until ON cache_records(fresh_until);

-- Index for finding "stuck" fills (heartbeat check)
CREATE INDEX IF NOT EXISTS idx_cache_updated_state ON cache_records(state, updated_at);
