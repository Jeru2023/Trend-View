CREATE TABLE IF NOT EXISTS {schema}.{table} (
    stock_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    minute_index SMALLINT NOT NULL,
    volume_ratio DOUBLE PRECISION,
    cumulative_ratio DOUBLE PRECISION,
    minute_volume BIGINT,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code, trade_date, minute_index)
);

CREATE INDEX IF NOT EXISTS {table_trade_idx}
    ON {schema}.{table} (trade_date DESC, stock_code);
