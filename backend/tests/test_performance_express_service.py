import unittest
from datetime import date

import pandas as pd

from backend.src.dao.performance_express_dao import PERFORMANCE_EXPRESS_FIELDS
from backend.src.services._akshare_utils import latest_report_period
from backend.src.services.performance_express_service import _prepare_frame


class PerformanceExpressServiceTests(unittest.TestCase):
    def test_resolve_report_period_uses_previous_quarter(self) -> None:
        resolved = latest_report_period(date(2025, 10, 30))
        self.assertEqual(resolved, date(2025, 9, 30))

    def test_resolve_report_period_wraps_to_previous_year(self) -> None:
        resolved = latest_report_period(date(2025, 1, 2))
        self.assertEqual(resolved, date(2024, 12, 31))

    def test_prepare_frame_normalizes_symbols_and_ts_codes(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "row_number": 1,
                    "symbol": "28",
                    "stock_name": "示例股份",
                    "eps": "0.12",
                    "revenue": "1000",
                    "revenue_prev": "900",
                    "revenue_yoy": "11.5",
                    "revenue_qoq": "-5.2",
                    "net_profit": "120",
                    "net_profit_prev": "80",
                    "net_profit_yoy": "50",
                    "net_profit_qoq": "10",
                    "net_assets_per_share": "3.2",
                    "return_on_equity": "5.7",
                    "industry": "医药商业",
                    "announcement_date": date(2024, 10, 15),
                },
                {
                    "row_number": 2,
                    "symbol": "688727",
                    "stock_name": "恒坤新材",
                    "eps": None,
                    "revenue": 391183800.0,
                    "revenue_prev": None,
                    "revenue_yoy": None,
                    "revenue_qoq": None,
                    "net_profit": 70043900.0,
                    "net_profit_prev": None,
                    "net_profit_yoy": None,
                    "net_profit_qoq": None,
                    "net_assets_per_share": None,
                    "return_on_equity": None,
                    "industry": None,
                    "announcement_date": date(2025, 10, 30),
                },
            ]
        )

        report_period = date(2024, 9, 30)
        prepared = _prepare_frame(raw, report_period)

        self.assertEqual(list(prepared.columns), list(PERFORMANCE_EXPRESS_FIELDS))
        self.assertEqual(len(prepared), 2)
        first = prepared.loc[0]
        self.assertEqual(first["symbol"], "000028")
        self.assertEqual(first["ts_code"], "000028.SZ")
        self.assertEqual(first["report_period"], report_period)
        self.assertAlmostEqual(first["revenue"], 1000.0)
        self.assertEqual(first["row_number"], 1)

        second = prepared.loc[1]
        self.assertEqual(second["symbol"], "688727")
        self.assertEqual(second["ts_code"], "688727.SH")
        self.assertEqual(second["report_period"], report_period)
        self.assertTrue(pd.isna(second["eps"]))


if __name__ == "__main__":
    unittest.main()
