CREATE TABLE IF NOT EXISTS {schema}.{table} (
    code TEXT NOT NULL,
    seq INTEGER,
    name TEXT,
    latest_price DOUBLE PRECISION,
    change_amount DOUBLE PRECISION,
    change_percent DOUBLE PRECISION,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    prev_close DOUBLE PRECISION,
    amplitude DOUBLE PRECISION,
    last_quote_time TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (code)
);

CREATE INDEX IF NOT EXISTS {table}_updated_at_idx
    ON {schema}.{table} (updated_at DESC);
