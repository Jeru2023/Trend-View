CREATE TABLE IF NOT EXISTS {schema}.{table} (
    symbol TEXT PRIMARY KEY,
    ts_code TEXT,
    main_business TEXT,
    product_type TEXT,
    product_name TEXT,
    business_scope TEXT,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

