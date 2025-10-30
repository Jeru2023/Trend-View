CREATE TABLE IF NOT EXISTS {schema}.{table} (
    trade_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    trade_price NUMERIC,
    trade_volume BIGINT,
    trade_amount NUMERIC,
    trade_side TEXT,
    price_change_percent NUMERIC,
    price_change NUMERIC,
    fetched_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_time, stock_code, trade_side, trade_volume, trade_amount)
);
