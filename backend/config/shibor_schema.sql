CREATE TABLE IF NOT EXISTS {schema}.{table} (
    period_date DATE NOT NULL,
    period_label TEXT,
    on_rate DOUBLE PRECISION,
    rate_1w DOUBLE PRECISION,
    rate_2w DOUBLE PRECISION,
    rate_1m DOUBLE PRECISION,
    rate_3m DOUBLE PRECISION,
    rate_6m DOUBLE PRECISION,
    rate_9m DOUBLE PRECISION,
    rate_1y DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (period_date)
);
