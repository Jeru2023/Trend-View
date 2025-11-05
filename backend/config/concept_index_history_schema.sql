CREATE TABLE IF NOT EXISTS {schema}.{table} (
    ts_code TEXT NOT NULL,
    concept_name TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    pre_close NUMERIC,
    change NUMERIC,
    pct_chg NUMERIC,
    vol NUMERIC,
    amount NUMERIC,
    fetched_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date)
);
