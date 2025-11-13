CREATE TABLE IF NOT EXISTS {schema}.{table} (
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    net_buy_amount NUMERIC,
    fund_inflow NUMERIC,
    net_buy_amount_cumulative NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS {trade_date_idx}
    ON {schema}.{table} (trade_date DESC);
