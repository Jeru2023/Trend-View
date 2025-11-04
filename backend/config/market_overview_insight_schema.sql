CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id BIGSERIAL PRIMARY KEY,
    generated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    summary_json JSONB NOT NULL,
    raw_response TEXT,
    model TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS {generated_at_idx}
    ON {schema}.{table} (generated_at DESC);
