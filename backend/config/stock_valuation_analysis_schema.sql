CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id SERIAL PRIMARY KEY,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    summary_json JSONB NOT NULL,
    raw_text TEXT NOT NULL,
    model TEXT,
    context_json JSONB,
    generated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS {index_name}
    ON {schema}.{table} (stock_code, generated_at DESC);
