from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.src.services.hsgt_fund_flow_service import _merge_summary_into_history


def _build_history_row(trade_date: date) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["北向资金"],
            "trade_date": [trade_date],
            "net_buy_amount": [np.nan],
            "buy_amount": [np.nan],
            "sell_amount": [np.nan],
            "net_buy_amount_cumulative": [np.nan],
            "fund_inflow": [np.nan],
            "balance": [np.nan],
            "market_value": [0.0],
            "leading_stock": ["--"],
            "leading_stock_change_percent": [np.nan],
            "hs300_index": [np.nan],
            "hs300_change_percent": [np.nan],
            "leading_stock_code": ["--"],
        }
    )


SUMMARY_SAMPLE = pd.DataFrame(
    [
        {
            "trade_date": date(2025, 11, 4),
            "channel_type": "沪港通",
            "board_name": "沪股通",
            "funds_direction": "北向",
            "trading_status": 3,
            "net_buy_amount": 12.5,
            "fund_inflow": 45.2,
            "balance": 5100.0,
            "rising_count": 300,
            "flat_count": 40,
            "falling_count": 900,
            "index_name": "上证指数",
            "index_change_percent": -0.41,
        },
        {
            "trade_date": date(2025, 11, 4),
            "channel_type": "深港通",
            "board_name": "深股通",
            "funds_direction": "北向",
            "trading_status": 3,
            "net_buy_amount": 8.4,
            "fund_inflow": 32.1,
            "balance": 5050.0,
            "rising_count": 280,
            "flat_count": 30,
            "falling_count": 950,
            "index_name": "深证成指",
            "index_change_percent": -1.71,
        },
        {
            "trade_date": date(2025, 11, 4),
            "channel_type": "沪港通",
            "board_name": "港股通(沪)",
            "funds_direction": "南向",
            "trading_status": 3,
            "net_buy_amount": -4.2,
            "fund_inflow": 18.6,
            "balance": 4200.0,
            "rising_count": 120,
            "flat_count": 15,
            "falling_count": 360,
            "index_name": "恒生指数",
            "index_change_percent": -0.79,
        },
    ]
)


def test_merge_summary_into_history_aggregates_northbound():
    history = _build_history_row(date(2025, 11, 4))
    merged = _merge_summary_into_history(history.copy(), SUMMARY_SAMPLE, "北向资金")

    row = merged.iloc[0]
    assert pytest.approx(row["net_buy_amount"], rel=1e-6) == 12.5 + 8.4
    assert pytest.approx(row["fund_inflow"], rel=1e-6) == 45.2 + 32.1
    assert pytest.approx(row["balance"], rel=1e-6) == 5100.0 + 5050.0


def test_merge_summary_into_history_filters_channel():
    history = _build_history_row(date(2025, 11, 4))
    merged = _merge_summary_into_history(history.copy(), SUMMARY_SAMPLE, "沪股通")

    row = merged.iloc[0]
    assert pytest.approx(row["net_buy_amount"], rel=1e-6) == 12.5
    assert pytest.approx(row["fund_inflow"], rel=1e-6) == 45.2
    assert pytest.approx(row["balance"], rel=1e-6) == 5100.0


def test_merge_summary_inserts_missing_trade_date():
    history = _build_history_row(date(2025, 11, 3))
    merged = _merge_summary_into_history(history.iloc[0:0].copy(), SUMMARY_SAMPLE, "北向资金")

    assert len(merged) == 1
    row = merged.iloc[0]
    assert row["trade_date"] == date(2025, 11, 4)
    assert pytest.approx(row["net_buy_amount"], rel=1e-6) == 12.5 + 8.4
    assert pytest.approx(row["fund_inflow"], rel=1e-6) == 45.2 + 32.1
    assert pytest.approx(row["balance"], rel=1e-6) == 5100.0 + 5050.0
