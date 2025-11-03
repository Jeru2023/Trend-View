import pandas as pd

from backend.src.services.trade_calendar_service import _prepare_calendar_frame


def test_prepare_calendar_frame_normalises_columns():
    raw = pd.DataFrame(
        [
            ["SSE", "20250101", 1],
            ["SSE", "20250102", 0],
            [None, "20250103", "1"],
            ["", "invalid", 1],
        ],
        columns=["exchange", "cal_date", "is_open"],
    )

    prepared = _prepare_calendar_frame(raw)

    assert prepared.columns.tolist() == ["cal_date", "exchange", "is_open"]
    assert len(prepared) == 3
    assert prepared.iloc[0]["cal_date"].isoformat() == "2025-01-01"
    assert prepared.iloc[0]["exchange"] == "SSE"
    assert prepared.iloc[0]["is_open"] is True
    assert prepared.iloc[1]["is_open"] is False
    assert prepared.iloc[2]["exchange"] == "SSE"
