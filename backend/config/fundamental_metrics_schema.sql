CREATE TABLE IF NOT EXISTS {schema}.{table} (
    ts_code TEXT PRIMARY KEY,
    net_income_end_date_latest DATE,
    net_income_end_date_prev1 DATE,
    net_income_end_date_prev2 DATE,
    revenue_end_date_latest DATE,
    roe_end_date_latest DATE,
    net_income_yoy_latest NUMERIC,
    net_income_yoy_prev1 NUMERIC,
    net_income_yoy_prev2 NUMERIC,
    net_income_qoq_latest NUMERIC,
    revenue_yoy_latest NUMERIC,
    revenue_qoq_latest NUMERIC,
    roe_yoy_latest NUMERIC,
    roe_qoq_latest NUMERIC,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
