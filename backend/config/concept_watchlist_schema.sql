CREATE TABLE IF NOT EXISTS {schema}.{table} (
    concept_name TEXT PRIMARY KEY,
    concept_code TEXT NOT NULL,
    last_synced_at TIMESTAMPTZ NULL,
    is_watched BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS is_watched BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS {index_updated}
    ON {schema}.{table} (updated_at DESC);
