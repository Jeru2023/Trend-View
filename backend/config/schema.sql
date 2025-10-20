CREATE TABLE IF NOT EXISTS {schema}.{table} (
    ts_code TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    area TEXT,
    industry TEXT,
    fullname TEXT,
    enname TEXT,
    market TEXT,
    exchange TEXT,
    curr_type TEXT,
    list_status TEXT,
    list_date DATE,
    delist_date DATE,
    is_hs TEXT,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
