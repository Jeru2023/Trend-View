import unittest
from datetime import date

import pandas as pd

from backend.src.dao.performance_forecast_dao import PERFORMANCE_FORECAST_FIELDS
from backend.src.services._akshare_utils import latest_report_period
from backend.src.services.performance_forecast_service import _prepare_frame


class PerformanceForecastServiceTests(unittest.TestCase):
    def test_latest_report_period_selects_previous_quarter(self) -> None:
        resolved = latest_report_period(date(2025, 10, 30))
        self.assertEqual(resolved, date(2025, 9, 30))

    def test_latest_report_period_wraps_year(self) -> None:
        resolved = latest_report_period(date(2025, 1, 1))
        self.assertEqual(resolved, date(2024, 12, 31))

    def test_prepare_frame_normalizes_and_deduplicates(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "row_number": 1,
                    "symbol": "28",
                    "stock_name": "示例股份",
                    "forecast_metric": "净利润",
                    "change_description": "预计净利润同比上升",
                    "forecast_value": "1000",
                    "change_rate": "25.5",
                    "change_reason": None,
                    "forecast_type": "预增",
                    "last_year_value": "800",
                    "announcement_date": date(2024, 5, 15),
                },
                {
                    "row_number": 2,
                    "symbol": "688750",
                    "stock_name": "金天钛业",
                    "forecast_metric": "净利润",
                    "change_description": None,
                    "forecast_value": 2000,
                    "change_rate": None,
                    "change_reason": "原材料价格波动",
                    "forecast_type": None,
                    "last_year_value": None,
                    "announcement_date": "2024-05-20",
                },
                {
                    "row_number": 3,
                    "symbol": "688750",
                    "stock_name": "金天钛业",
                    "forecast_metric": "净利润",
                    "change_description": "区间上调",
                    "forecast_value": 2500,
                    "change_rate": 30,
                    "change_reason": "",
                    "forecast_type": None,
                    "last_year_value": 1500,
                    "announcement_date": "2024-05-21",
                },
            ]
        )

        report_period = date(2024, 3, 31)
        prepared = _prepare_frame(raw, report_period)

        self.assertEqual(list(prepared.columns), list(PERFORMANCE_FORECAST_FIELDS))
        self.assertEqual(len(prepared), 2)

        first = prepared.loc[0]
        self.assertEqual(first["symbol"], "000028")
        self.assertEqual(first["ts_code"], "000028.SZ")
        self.assertEqual(first["forecast_type"], "预增")
        self.assertEqual(first["change_reason"], "")
        self.assertAlmostEqual(first["forecast_value"], 1000.0)
        self.assertAlmostEqual(first["change_rate"], 25.5)
        self.assertEqual(first["report_period"], report_period)

        second = prepared.loc[1]
        self.assertEqual(second["symbol"], "688750")
        self.assertEqual(second["ts_code"], "688750.SH")
        self.assertEqual(second["forecast_type"], "未披露")
        self.assertEqual(second["change_description"], "区间上调")
        self.assertAlmostEqual(second["forecast_value"], 2500.0)
        self.assertAlmostEqual(second["change_rate"], 30.0)
        self.assertAlmostEqual(second["last_year_value"], 1500.0)
        self.assertEqual(second["report_period"], report_period)
        self.assertTrue(pd.notna(second["announcement_date"]))
        self.assertEqual(int(second["row_number"]), 3)


if __name__ == "__main__":
    unittest.main()
