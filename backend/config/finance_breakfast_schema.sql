CREATE TABLE IF NOT EXISTS {schema}.{table} (
    title TEXT NOT NULL,
    summary TEXT,
    content TEXT,
    ai_extract TEXT,
    ai_extract_summary TEXT,
    ai_extract_detail TEXT,
    published_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    url TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (title, published_at)
);
