CREATE TABLE IF NOT EXISTS {schema}.{table} (
    concept_name TEXT PRIMARY KEY,
    concept_code TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS {index_code}
    ON {schema}.{table} (concept_code);
