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
    volume_shares NUMERIC,
    volume_text TEXT,
    baseline_volume_shares NUMERIC,
    baseline_volume_text TEXT,
    volume_days INTEGER,
    turnover_percent NUMERIC,
    turnover_rate NUMERIC,
    turnover_amount NUMERIC,
    turnover_amount_text TEXT,
    high_price NUMERIC,
    low_price NUMERIC,
    industry TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (indicator_code, stock_code)
);

CREATE INDEX IF NOT EXISTS {indicator_rank_idx}
    ON {schema}.{table} (indicator_code, rank);

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS turnover_percent NUMERIC;

ALTER TABLE {schema}.{table}
    ALTER COLUMN volume_shares TYPE NUMERIC USING volume_shares::numeric;

ALTER TABLE {schema}.{table}
    ALTER COLUMN baseline_volume_shares TYPE NUMERIC USING baseline_volume_shares::numeric;

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS turnover_rate NUMERIC;

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS turnover_amount NUMERIC;

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS turnover_amount_text TEXT;

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS high_price NUMERIC;

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS low_price NUMERIC;
