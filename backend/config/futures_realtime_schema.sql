CREATE TABLE IF NOT EXISTS {schema}.{table} (
    name TEXT PRIMARY KEY,
    code TEXT,
    last_price DOUBLE PRECISION,
    price_cny DOUBLE PRECISION,
    change_amount DOUBLE PRECISION,
    change_percent DOUBLE PRECISION,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    prev_settlement DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    bid_price DOUBLE PRECISION,
    ask_price DOUBLE PRECISION,
    quote_time TEXT,
    trade_date DATE,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);
