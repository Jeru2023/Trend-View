CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id SERIAL PRIMARY KEY,
    ts_code TEXT NOT NULL,
    ann_date DATE,
    end_date DATE,
    c_fr_sale_sg NUMERIC,
    c_paid_goods_s NUMERIC,
    c_paid_to_for_empl NUMERIC,
    n_cashflow_act NUMERIC,
    c_pay_acq_const_fiolta NUMERIC,
    n_cashflow_inv_act NUMERIC,
    c_recp_borrow NUMERIC,
    c_prepay_amt_borr NUMERIC,
    c_pay_dist_dpcp_int_exp NUMERIC,
    n_cash_flows_fnc_act NUMERIC,
    n_incr_cash_cash_equ NUMERIC,
    c_cash_equ_beg_period NUMERIC,
    c_cash_equ_end_period NUMERIC,
    free_cashflow NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, end_date, ann_date)
);

CREATE INDEX IF NOT EXISTS {index_ts_code}
    ON {schema}.{table} (ts_code, end_date DESC, ann_date DESC);
