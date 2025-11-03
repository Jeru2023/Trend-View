CREATE TABLE IF NOT EXISTS {schema}.{table} (
    index_code TEXT NOT NULL,
    index_name TEXT,
    trade_date DATE NOT NULL,
    open NUMERIC,
    close NUMERIC,
    high NUMERIC,
    low NUMERIC,
    volume NUMERIC,
    amount NUMERIC,
    amplitude NUMERIC,
    pct_change NUMERIC,
    change_amount NUMERIC,
    turnover NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (index_code, trade_date)
);

CREATE INDEX IF NOT EXISTS {table_trade_date_idx}
    ON {schema}.{table} (trade_date DESC);
