CREATE TABLE IF NOT EXISTS {schema}.{table} (
    symbol TEXT NOT NULL,
    ts_code TEXT,
    stock_name TEXT,
    report_count INTEGER,
    rating_buy DOUBLE PRECISION,
    rating_add DOUBLE PRECISION,
    rating_neutral DOUBLE PRECISION,
    rating_reduce DOUBLE PRECISION,
    rating_sell DOUBLE PRECISION,
    forecast_year INTEGER NOT NULL,
    forecast_eps DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (symbol, forecast_year)
);
