CREATE TABLE IF NOT EXISTS {schema}.{table} (
    period_label TEXT PRIMARY KEY,
    period_date DATE NOT NULL,
    actual_value DOUBLE PRECISION,
    forecast_value DOUBLE PRECISION,
    previous_value DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);
