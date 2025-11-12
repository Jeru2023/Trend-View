CREATE TABLE IF NOT EXISTS {schema}.{table} (
    entry_date DATE PRIMARY KEY,
    review_html TEXT,
    plan_html TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
