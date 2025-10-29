CREATE TABLE IF NOT EXISTS {schema}.{table} (
    ts_code TEXT NOT NULL,
    ann_date DATE,
    end_date DATE NOT NULL,
    type TEXT,
    p_change_min NUMERIC,
    p_change_max NUMERIC,
    net_profit_min NUMERIC,
    net_profit_max NUMERIC,
    last_parent_net NUMERIC,
    first_ann_date DATE,
    summary TEXT,
    change_reason TEXT,
    update_flag TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, end_date, ann_date)
);
