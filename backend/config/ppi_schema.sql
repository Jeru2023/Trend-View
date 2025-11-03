CREATE TABLE IF NOT EXISTS {schema}.{table} (
    period_date DATE NOT NULL,
    period_label TEXT,
    current_index DOUBLE PRECISION,
    yoy_change DOUBLE PRECISION,
    cumulative_index DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (period_date)
);
