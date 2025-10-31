CREATE TABLE IF NOT EXISTS {schema}.{table} (
    trade_date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    open_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    amplitude DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (code, trade_date)
);
