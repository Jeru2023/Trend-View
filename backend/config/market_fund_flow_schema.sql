CREATE TABLE IF NOT EXISTS {schema}.{table} (
    trade_date DATE PRIMARY KEY,
    shanghai_close NUMERIC,
    shanghai_change_percent NUMERIC,
    shenzhen_close NUMERIC,
    shenzhen_change_percent NUMERIC,
    main_net_inflow_amount NUMERIC,
    main_net_inflow_ratio NUMERIC,
    huge_order_net_inflow_amount NUMERIC,
    huge_order_net_inflow_ratio NUMERIC,
    large_order_net_inflow_amount NUMERIC,
    large_order_net_inflow_ratio NUMERIC,
    medium_order_net_inflow_amount NUMERIC,
    medium_order_net_inflow_ratio NUMERIC,
    small_order_net_inflow_amount NUMERIC,
    small_order_net_inflow_ratio NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS {trade_date_idx}
    ON {schema}.{table} (trade_date DESC);
