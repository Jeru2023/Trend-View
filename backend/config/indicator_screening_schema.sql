CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id BIGSERIAL PRIMARY KEY,
    indicator_code TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    captured_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    rank INTEGER,
    stock_code TEXT NOT NULL,
    stock_code_full TEXT,
    stock_name TEXT,
    price_change_percent NUMERIC,
    stage_change_percent NUMERIC,
    last_price NUMERIC,
    volume_shares BIGINT,
    volume_text TEXT,
    baseline_volume_shares BIGINT,
    baseline_volume_text TEXT,
    volume_days INTEGER,
    industry TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (indicator_code, stock_code)
);

CREATE INDEX IF NOT EXISTS {table}_indicator_rank_idx
    ON {schema}.{table} (indicator_code, rank);
