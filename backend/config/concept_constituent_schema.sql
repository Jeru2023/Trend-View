CREATE TABLE IF NOT EXISTS {schema}.{table} (
    concept_name TEXT NOT NULL,
    concept_code TEXT NOT NULL,
    symbol TEXT NOT NULL,
    stock_name TEXT,
    rank INTEGER,
    last_price DOUBLE PRECISION,
    change_percent DOUBLE PRECISION,
    change_amount DOUBLE PRECISION,
    speed_percent DOUBLE PRECISION,
    turnover_rate DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION,
    amplitude_percent DOUBLE PRECISION,
    turnover_amount DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (concept_name, symbol)
);

CREATE INDEX IF NOT EXISTS {index_concept}
    ON {schema}.{table} (concept_name, updated_at DESC);
