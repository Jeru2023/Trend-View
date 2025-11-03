import unittest
from datetime import date

import pandas as pd

from backend.src.services.hsgt_fund_flow_service import _prepare_hsgt_frame


class HsgtFundFlowServiceTests(unittest.TestCase):
    def test_prepare_hsgt_frame_normalizes_types(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "trade_date": "2025-11-03",
                    "net_buy_amount": "12.34",
                    "buy_amount": "100.11",
                    "sell_amount": "87.77",
                    "net_buy_amount_cumulative": "2.345",
                    "fund_inflow": "56.78",
                    "balance": "890.12",
                    "market_value": "1234567890",
                    "leading_stock": " 贵州茅台 ",
                    "leading_stock_change_percent": "2.34%",
                    "leading_stock_code": "600519.SH ",
                    "hs300_index": "3500.12",
                    "hs300_change_percent": "-0.45%",
                }
            ]
        )

        prepared = _prepare_hsgt_frame(raw)

        self.assertEqual(prepared.loc[0, "trade_date"], date(2025, 11, 3))
        self.assertAlmostEqual(prepared.loc[0, "net_buy_amount"], 12.34)
        self.assertAlmostEqual(prepared.loc[0, "buy_amount"], 100.11)
        self.assertAlmostEqual(prepared.loc[0, "sell_amount"], 87.77)
        self.assertAlmostEqual(prepared.loc[0, "net_buy_amount_cumulative"], 2.345)
        self.assertAlmostEqual(prepared.loc[0, "fund_inflow"], 56.78)
        self.assertAlmostEqual(prepared.loc[0, "balance"], 890.12)
        self.assertAlmostEqual(prepared.loc[0, "market_value"], 1234567890.0)
        self.assertEqual(prepared.loc[0, "leading_stock"], "贵州茅台")
        self.assertEqual(prepared.loc[0, "leading_stock_code"], "600519.SH")
        self.assertAlmostEqual(prepared.loc[0, "leading_stock_change_percent"], 2.34)
        self.assertAlmostEqual(prepared.loc[0, "hs300_index"], 3500.12)
        self.assertAlmostEqual(prepared.loc[0, "hs300_change_percent"], -0.45)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
