CREATE TABLE IF NOT EXISTS {schema}.{table} (
    article_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_item_id TEXT,
    title TEXT NOT NULL,
    summary TEXT,
    content TEXT,
    content_type TEXT,
    published_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    url TEXT,
    language TEXT,
    content_fetched BOOLEAN DEFAULT FALSE,
    content_fetched_at TIMESTAMP WITHOUT TIME ZONE,
    processing_status TEXT DEFAULT 'pending',
    relevance_attempts INTEGER DEFAULT 0,
    impact_attempts INTEGER DEFAULT 0,
    last_error TEXT,
    raw_payload TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS {table_source_item_idx}
    ON {schema}.{table} (source, COALESCE(source_item_id, url));

CREATE INDEX IF NOT EXISTS {table_status_idx}
    ON {schema}.{table} (processing_status, published_at DESC);
