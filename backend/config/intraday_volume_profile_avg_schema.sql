CREATE TABLE IF NOT EXISTS {schema}.{table} (
    stock_code TEXT NOT NULL,
    minute_index SMALLINT NOT NULL,
    ratio_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    cumulative_ratio_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    sample_count INTEGER NOT NULL DEFAULT 0,
    avg_ratio DOUBLE PRECISION,
    avg_cumulative_ratio DOUBLE PRECISION,
    is_frozen BOOLEAN NOT NULL DEFAULT FALSE,
    last_trade_date DATE,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code, minute_index)
);

CREATE INDEX IF NOT EXISTS {table_stock_idx}
    ON {schema}.{table} (stock_code);

CREATE INDEX IF NOT EXISTS {table_frozen_idx}
    ON {schema}.{table} (stock_code, is_frozen);
