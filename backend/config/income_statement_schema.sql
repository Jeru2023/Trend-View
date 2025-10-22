CREATE TABLE IF NOT EXISTS {schema}.{table} (
    ts_code TEXT NOT NULL,
    ann_date DATE,
    f_ann_date DATE,
    end_date DATE NOT NULL,
    report_type TEXT,
    comp_type TEXT,
    basic_eps DOUBLE PRECISION,
    diluted_eps DOUBLE PRECISION,
    oper_exp DOUBLE PRECISION,
    total_revenue DOUBLE PRECISION,
    revenue DOUBLE PRECISION,
    operate_profit DOUBLE PRECISION,
    total_profit DOUBLE PRECISION,
    n_income DOUBLE PRECISION,
    ebitda DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, end_date)
);
