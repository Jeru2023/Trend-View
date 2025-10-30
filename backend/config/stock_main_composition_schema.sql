CREATE TABLE IF NOT EXISTS {schema}.{table} (
    symbol TEXT NOT NULL,
    report_date DATE NOT NULL,
    category_type TEXT,
    composition TEXT,
    revenue NUMERIC,
    revenue_ratio NUMERIC,
    cost NUMERIC,
    cost_ratio NUMERIC,
    profit NUMERIC,
    profit_ratio NUMERIC,
    gross_margin NUMERIC,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, report_date, category_type, composition)
);

