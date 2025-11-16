CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id SERIAL PRIMARY KEY,
    ts_code TEXT NOT NULL,
    symbol TEXT,
    report_id TEXT NOT NULL,
    title TEXT NOT NULL,
    report_type TEXT,
    publish_date DATE,
    org TEXT,
    analysts TEXT,
    detail_url TEXT,
    content_html TEXT,
    content_text TEXT,
    distillation JSONB,
    distillation_model TEXT,
    distillation_generated_at TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (report_id)
);

CREATE INDEX IF NOT EXISTS {index_ts_code_date}
    ON {schema}.{table} (ts_code, publish_date DESC);

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS distillation JSONB;

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS distillation_model TEXT;

ALTER TABLE {schema}.{table}
    ADD COLUMN IF NOT EXISTS distillation_generated_at TIMESTAMP WITHOUT TIME ZONE;
