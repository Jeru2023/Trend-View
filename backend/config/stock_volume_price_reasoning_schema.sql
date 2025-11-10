CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id SERIAL PRIMARY KEY,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    lookback_days INTEGER NOT NULL,
    summary_json JSONB NOT NULL,
    raw_text TEXT NOT NULL,
    model TEXT,
    generated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS {index_name}
    ON {schema}.{table} (stock_code, generated_at DESC);
