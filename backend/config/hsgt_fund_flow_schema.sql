CREATE TABLE IF NOT EXISTS {schema}.{table} (
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    net_buy_amount NUMERIC,
    buy_amount NUMERIC,
    sell_amount NUMERIC,
    net_buy_amount_cumulative NUMERIC,
    fund_inflow NUMERIC,
    balance NUMERIC,
    market_value NUMERIC,
    leading_stock TEXT,
    leading_stock_change_percent NUMERIC,
    hs300_index NUMERIC,
    hs300_change_percent NUMERIC,
    leading_stock_code TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS {trade_date_idx}
    ON {schema}.{table} (trade_date DESC);
