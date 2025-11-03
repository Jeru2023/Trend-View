CREATE TABLE IF NOT EXISTS {schema}.{table} (
    code TEXT PRIMARY KEY,
    name TEXT,
    latest_price NUMERIC,
    change_amount NUMERIC,
    change_percent NUMERIC,
    prev_close NUMERIC,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    volume NUMERIC,
    turnover NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS {table_updated_at_idx}
    ON {schema}.{table} (updated_at DESC);
