CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id SERIAL PRIMARY KEY,
    stock_code TEXT NOT NULL,
    keyword TEXT,
    title TEXT NOT NULL,
    content TEXT,
    source TEXT,
    url TEXT,
    normalized_url TEXT,
    published_at TIMESTAMP,
    raw_payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS normalized_url TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS {unique_idx}
    ON {schema}.{table} (stock_code, COALESCE(normalized_url, ''), COALESCE(title, ''));

CREATE INDEX IF NOT EXISTS {stock_idx}
    ON {schema}.{table} (stock_code, published_at DESC);
