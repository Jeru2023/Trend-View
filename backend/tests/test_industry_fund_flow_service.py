import unittest

import pandas as pd

from backend.src.services.industry_fund_flow_service import _prepare_frame


class IndustryFundFlowServiceTests(unittest.TestCase):
    def test_prepare_frame_normalizes_numeric_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "rank": "1",
                    "industry": "黑色家电",
                    "industry_index": "2572.370",
                    "price_change_percent": "3.49",
                    "stage_change_percent": "-1.23%",
                    "inflow": "7.13",
                    "outflow": "7.41",
                    "net_amount": "-0.28",
                    "company_count": "9",
                    "leading_stock": "辰奕智能",
                    "leading_stock_change_percent": "19.99%",
                    "current_price": "42.50",
                }
            ]
        )

        prepared = _prepare_frame(raw, "即时")

        self.assertEqual(prepared.loc[0, "symbol"], "即时")
        self.assertEqual(int(prepared.loc[0, "rank"]), 1)
        self.assertAlmostEqual(prepared.loc[0, "industry_index"], 2572.37)
        self.assertAlmostEqual(prepared.loc[0, "price_change_percent"], 3.49)
        self.assertAlmostEqual(prepared.loc[0, "stage_change_percent"], -1.23)
        self.assertAlmostEqual(prepared.loc[0, "leading_stock_change_percent"], 19.99)
        self.assertEqual(prepared.loc[0, "company_count"], 9)


if __name__ == "__main__":
    unittest.main()
