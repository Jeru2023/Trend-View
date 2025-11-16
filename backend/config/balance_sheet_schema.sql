CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id SERIAL PRIMARY KEY,
    ts_code TEXT NOT NULL,
    ann_date DATE,
    end_date DATE,
    money_cap NUMERIC,
    accounts_receiv NUMERIC,
    inventories NUMERIC,
    fix_assets NUMERIC,
    total_cur_assets NUMERIC,
    total_nca NUMERIC,
    total_assets NUMERIC,
    st_borr NUMERIC,
    lt_borr NUMERIC,
    acct_payable NUMERIC,
    total_cur_liab NUMERIC,
    total_ncl NUMERIC,
    total_liab NUMERIC,
    total_share NUMERIC,
    cap_rese NUMERIC,
    surplus_rese NUMERIC,
    undistr_porfit NUMERIC,
    total_hldr_eqy_exc_min_int NUMERIC,
    total_liab_hldr_eqy NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, end_date, ann_date)
);

CREATE INDEX IF NOT EXISTS {index_ts_code}
    ON {schema}.{table} (ts_code, end_date DESC, ann_date DESC);
