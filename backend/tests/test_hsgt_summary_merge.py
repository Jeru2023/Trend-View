from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.src.services.hsgt_fund_flow_service import (
    _apply_cumulative_values,
    _build_standardized_frame,
)


def test_build_standardized_frame_normalizes_trade_dates():
    raw = pd.DataFrame(
        {
            "trade_date": ["20240103", "20240101", "20240102"],
            "north_money": [200.0, 100.0, 150.0],
        }
    )
    frame = _build_standardized_frame(raw, "北向资金")
    assert list(frame.columns) == [
        "trade_date",
        "net_buy_amount",
        "fund_inflow",
        "net_buy_amount_cumulative",
    ]
    assert frame.loc[0, "trade_date"].isoformat() == "2024-01-01"
    assert frame.loc[1, "trade_date"].isoformat() == "2024-01-02"
    assert frame.loc[2, "trade_date"].isoformat() == "2024-01-03"
    assert frame.loc[0, "net_buy_amount"] == 100.0
    assert frame.loc[0, "fund_inflow"] == 100.0


def test_build_standardized_frame_handles_missing_columns():
    raw = pd.DataFrame(
        {
            "trade_date": ["20240101"],
            "south_money": [250.5],
        }
    )
    frame = _build_standardized_frame(raw, "南向资金")
    assert frame.loc[0, "net_buy_amount"] == pytest.approx(250.5)
    assert frame.loc[0, "fund_inflow"] == pytest.approx(250.5)
    assert frame.loc[0, "net_buy_amount_cumulative"] is None


@pytest.mark.parametrize(
    "starting_value,expected",
    [
        (None, [10.0, 15.0, 5.0]),
        (100.0, [110.0, 115.0, 105.0]),
    ],
)
def test_apply_cumulative_values(starting_value, expected):
    frame = pd.DataFrame(
        {
            "net_buy_amount": [10.0, 5.0, -10.0],
        }
    )
    _apply_cumulative_values(frame, starting_value)
    assert frame["net_buy_amount_cumulative"].tolist() == pytest.approx(expected)


def test_apply_cumulative_values_skips_null_rows():
    frame = pd.DataFrame({"net_buy_amount": [None, 5.0, None, -2.0]})
    _apply_cumulative_values(frame, None)
    result = frame["net_buy_amount_cumulative"].tolist()
    assert pd.isna(result[0])
    assert result[1] == pytest.approx(5.0)
    assert pd.isna(result[2])
    assert result[3] == pytest.approx(3.0)
