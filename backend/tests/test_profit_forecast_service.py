import unittest

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.src.dao.profit_forecast_dao import PROFIT_FORECAST_FIELDS
from backend.src.services.profit_forecast_service import _prepare_profit_forecast_frame


class ProfitForecastServiceTests(unittest.TestCase):
    def test_prepare_profit_forecast_frame_expands_years_and_normalises(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "symbol": "600519",
                    "stock_name": "贵州茅台",
                    "report_count": "57",
                    "rating_buy": "10",
                    "rating_add": "20",
                    "rating_neutral": 15,
                    "rating_reduce": None,
                    "rating_sell": 0,
                    "row_number": 1,
                    "forecast_year": 2024,
                    "forecast_eps": "69.944737",
                },
                {
                    "symbol": "600519",
                    "stock_name": "贵州茅台",
                    "report_count": "57",
                    "rating_buy": "10",
                    "rating_add": "20",
                    "rating_neutral": 15,
                    "rating_reduce": None,
                    "rating_sell": 0,
                    "row_number": 1,
                    "forecast_year": 2025,
                    "forecast_eps": None,
                },
                {
                    "symbol": "28",
                    "stock_name": "测试股份",
                    "report_count": "5",
                    "rating_buy": "1",
                    "rating_add": "2",
                    "rating_neutral": "3",
                    "rating_reduce": "4",
                    "rating_sell": "5",
                    "row_number": 2,
                    "forecast_year": 2024,
                    "forecast_eps": "1.234",
                },
            ]
        )

        prepared = _prepare_profit_forecast_frame(raw)

        self.assertEqual(list(prepared.columns), list(PROFIT_FORECAST_FIELDS))
        self.assertEqual(len(prepared), 3)

        first = prepared.loc[0]
        self.assertEqual(first["symbol"], "600519")
        self.assertEqual(first["ts_code"], "600519.SH")
        self.assertAlmostEqual(float(first["forecast_eps"]), 69.944737, places=6)
        self.assertEqual(int(first["forecast_year"]), 2024)
        self.assertEqual(int(first["report_count"]), 57)
        self.assertAlmostEqual(float(first["rating_add"]), 20.0)

        third = prepared.loc[2]
        self.assertEqual(third["symbol"], "000028")
        self.assertEqual(third["ts_code"], "000028.SZ")
        self.assertAlmostEqual(float(third["forecast_eps"]), 1.234)
        self.assertEqual(int(third["forecast_year"]), 2024)


if __name__ == "__main__":
    unittest.main()
