CREATE TABLE IF NOT EXISTS {schema}.{table} (
    symbol TEXT NOT NULL,
    ts_code TEXT,
    stock_name TEXT,
    report_period DATE NOT NULL,
    forecast_metric TEXT,
    change_description TEXT,
    forecast_value NUMERIC,
    change_rate NUMERIC,
    change_reason TEXT,
    forecast_type TEXT,
    last_year_value NUMERIC,
    announcement_date DATE,
    row_number INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, report_period, forecast_metric, forecast_type)
);
