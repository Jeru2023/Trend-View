CREATE TABLE IF NOT EXISTS {schema}.{table} (
    period_date DATE NOT NULL,
    period_label TEXT,
    total_financing DOUBLE PRECISION,
    renminbi_loans DOUBLE PRECISION,
    entrusted_and_fx_loans DOUBLE PRECISION,
    entrusted_loans DOUBLE PRECISION,
    trust_loans DOUBLE PRECISION,
    undiscounted_bankers_acceptance DOUBLE PRECISION,
    corporate_bonds DOUBLE PRECISION,
    domestic_equity_financing DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (period_date)
);
