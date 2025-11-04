import unittest
from datetime import datetime

import pandas as pd

from backend.src.services.market_activity_service import _prepare_market_activity_frame


class MarketActivityServiceTests(unittest.TestCase):
    def test_prepare_market_activity_frame_parses_numeric_and_timestamp(self) -> None:
        raw = pd.DataFrame(
            [
                {"metric": "上涨", "value": "4,770"},
                {"metric": "活跃度", "value": "93.53%"},
                {"metric": "统计日期", "value": "2024-10-14 15:00:00"},
            ]
        )

        prepared = _prepare_market_activity_frame(raw)

        self.assertEqual(len(prepared), 3)
        up_row = prepared.loc[prepared["metric"] == "上涨"].iloc[0]
        self.assertAlmostEqual(up_row["value_number"], 4770)

        active_row = prepared.loc[prepared["metric"] == "活跃度"].iloc[0]
        self.assertAlmostEqual(active_row["value_number"], 93.53)

        ts_values = prepared["dataset_timestamp"].dropna().unique()
        self.assertEqual(len(ts_values), 1)
        self.assertIsInstance(ts_values[0], datetime)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

