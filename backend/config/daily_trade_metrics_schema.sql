CREATE TABLE IF NOT EXISTS {schema}.{table} (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    close NUMERIC,
    pct_change_1y NUMERIC,
    pct_change_6m NUMERIC,
    pct_change_3m NUMERIC,
    pct_change_1m NUMERIC,
    pct_change_2w NUMERIC,
    pct_change_1w NUMERIC,
    ma_20 NUMERIC,
    ma_10 NUMERIC,
    ma_5 NUMERIC,
    volume_spike NUMERIC,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date)
);
