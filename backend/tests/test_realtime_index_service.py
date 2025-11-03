import pandas as pd

from backend.src.services.realtime_index_service import _prepare_realtime_index_frame, REALTIME_INDEX_COLUMN_MAP


def test_prepare_realtime_index_frame_basic():
    frame = pd.DataFrame(
        {
            "代码": ["sh000001", ""],
            "名称": ["上证指数", ""],
            "最新价": ["3000.55", "100"],
            "涨跌额": ["-10.23", ""],
            "涨跌幅": ["-0.34%", "1.0"],
            "昨收": ["3010.78", None],
            "今开": ["2998.00", ""],
            "最高": ["3012.34", None],
            "最低": ["2987.90", None],
            "成交量": ["123456789", None],
            "成交额": ["987654321", None],
        }
    )

    prepared = _prepare_realtime_index_frame(frame)

    assert list(prepared.columns) == list(REALTIME_INDEX_COLUMN_MAP.values())
    assert prepared.shape[0] == 1
    row = prepared.iloc[0]
    assert row["code"] == "sh000001"
    assert row["name"] == "上证指数"
    assert row["latest_price"] == 3000.55
    assert row["change_amount"] == -10.23
    assert row["change_percent"] == -0.34
    assert row["prev_close"] == 3010.78
    assert row["open_price"] == 2998.0
    assert row["high_price"] == 3012.34
    assert row["low_price"] == 2987.9
    assert row["volume"] == 123456789
    assert row["turnover"] == 987654321
