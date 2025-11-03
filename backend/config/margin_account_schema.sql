CREATE TABLE IF NOT EXISTS {schema}.{table} (
    trade_date DATE NOT NULL,
    financing_balance NUMERIC,
    securities_lending_balance NUMERIC,
    financing_purchase_amount NUMERIC,
    securities_lending_sell_amount NUMERIC,
    securities_company_count NUMERIC,
    business_department_count NUMERIC,
    individual_investor_count NUMERIC,
    institutional_investor_count NUMERIC,
    participating_investor_count NUMERIC,
    liability_investor_count NUMERIC,
    collateral_value NUMERIC,
    average_collateral_ratio NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date)
);

CREATE INDEX IF NOT EXISTS {trade_date_idx}
    ON {schema}.{table} (trade_date DESC);
