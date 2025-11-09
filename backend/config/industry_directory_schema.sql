CREATE TABLE IF NOT EXISTS {schema}.{table} (
    industry_name TEXT PRIMARY KEY,
    industry_code TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS {index_code}
    ON {schema}.{table} (industry_code);
