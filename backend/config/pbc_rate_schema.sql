CREATE TABLE IF NOT EXISTS {schema}.{table} (
    period_date DATE NOT NULL,
    period_label TEXT,
    actual_value DOUBLE PRECISION,
    forecast_value DOUBLE PRECISION,
    previous_value DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (period_date)
);
