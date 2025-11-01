CREATE TABLE IF NOT EXISTS {schema}.{table} (
    period_date DATE NOT NULL,
    period_label TEXT,
    household_ratio DOUBLE PRECISION,
    non_financial_corporate_ratio DOUBLE PRECISION,
    government_ratio DOUBLE PRECISION,
    central_government_ratio DOUBLE PRECISION,
    local_government_ratio DOUBLE PRECISION,
    real_economy_ratio DOUBLE PRECISION,
    financial_assets_ratio DOUBLE PRECISION,
    financial_liabilities_ratio DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (period_date)
);
