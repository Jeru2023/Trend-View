CREATE TABLE IF NOT EXISTS {schema}.{table} (
    series TEXT NOT NULL,
    period_label TEXT NOT NULL,
    period_date DATE NOT NULL,
    actual_value DOUBLE PRECISION,
    forecast_value DOUBLE PRECISION,
    previous_value DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (series, period_label)
);
