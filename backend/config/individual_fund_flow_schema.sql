CREATE TABLE IF NOT EXISTS {schema}.{table} (
    symbol TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    rank INTEGER,
    latest_price NUMERIC,
    price_change_percent NUMERIC,
    stage_change_percent NUMERIC,
    turnover_rate NUMERIC,
    continuous_turnover_rate NUMERIC,
    inflow NUMERIC,
    outflow NUMERIC,
    net_amount NUMERIC,
    net_inflow NUMERIC,
    turnover_amount NUMERIC,
    fetched_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, stock_code)
);
