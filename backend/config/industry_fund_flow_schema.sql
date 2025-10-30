CREATE TABLE IF NOT EXISTS {schema}.{table} (
    symbol TEXT NOT NULL,
    industry TEXT NOT NULL,
    rank INTEGER,
    industry_index NUMERIC,
    price_change_percent NUMERIC,
    stage_change_percent NUMERIC,
    inflow NUMERIC,
    outflow NUMERIC,
    net_amount NUMERIC,
    company_count INTEGER,
    leading_stock TEXT,
    leading_stock_change_percent NUMERIC,
    current_price NUMERIC,
    fetched_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, industry)
);
