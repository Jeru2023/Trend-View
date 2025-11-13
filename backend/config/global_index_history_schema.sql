CREATE TABLE IF NOT EXISTS {schema}.{table} (
    code TEXT NOT NULL,
    name TEXT,
    trade_date DATE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    prev_close DOUBLE PRECISION,
    change_amount DOUBLE PRECISION,
    change_percent DOUBLE PRECISION,
    currency TEXT,
    timezone TEXT,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date)
);

CREATE INDEX IF NOT EXISTS {trade_date_index}
    ON {schema}.{table} (code, trade_date DESC);
