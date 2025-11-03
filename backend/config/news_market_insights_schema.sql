CREATE TABLE IF NOT EXISTS {schema}.{table} (
    summary_id TEXT PRIMARY KEY,
    generated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    window_start TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    window_end TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    headline_count INTEGER NOT NULL,
    summary_json TEXT,
    raw_response TEXT,
    referenced_articles TEXT,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_tokens INTEGER,
    elapsed_ms INTEGER,
    model_used TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS {table_generated_idx}
    ON {schema}.{table} (generated_at DESC);

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP;
