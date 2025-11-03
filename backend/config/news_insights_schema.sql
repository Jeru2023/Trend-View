CREATE TABLE IF NOT EXISTS {schema}.{table} (
    article_id TEXT PRIMARY KEY,
    is_relevant BOOLEAN,
    relevance_confidence DOUBLE PRECISION,
    relevance_reason TEXT,
    relevance_checked_at TIMESTAMP WITHOUT TIME ZONE,
    impact_levels TEXT,
    impact_markets TEXT,
    impact_industries TEXT,
    impact_sectors TEXT,
    impact_themes TEXT,
    impact_stocks TEXT,
    impact_summary TEXT,
    impact_analysis TEXT,
    impact_confidence DOUBLE PRECISION,
    impact_checked_at TIMESTAMP WITHOUT TIME ZONE,
    extra_metadata TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_news_article FOREIGN KEY(article_id)
        REFERENCES {schema}.news_articles(article_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS {table_relevance_idx}
    ON {schema}.{table} (is_relevant, relevance_checked_at DESC);
