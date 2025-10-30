import unittest

import pandas as pd

from backend.src.services.concept_fund_flow_service import _prepare_frame


class ConceptFundFlowServiceTests(unittest.TestCase):
    def test_prepare_frame_normalizes_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "rank": "1",
                    "concept": "华为海思概念股",
                    "concept_index": "1750.270",
                    "price_change_percent": "6.17",
                    "stage_change_percent": "11.45%",
                    "inflow": "45.26",
                    "outflow": "41.72",
                    "net_amount": "3.54",
                    "company_count": "36",
                    "leading_stock": "力源信息",
                    "leading_stock_change_percent": "20.04%",
                    "current_price": "6.11",
                }
            ]
        )

        prepared = _prepare_frame(raw, "即时")

        self.assertEqual(prepared.loc[0, "symbol"], "即时")
        self.assertEqual(prepared.loc[0, "concept"], "华为海思概念股")
        self.assertEqual(int(prepared.loc[0, "rank"]), 1)
        self.assertAlmostEqual(prepared.loc[0, "concept_index"], 1750.27)
        self.assertAlmostEqual(prepared.loc[0, "price_change_percent"], 6.17)
        self.assertAlmostEqual(prepared.loc[0, "stage_change_percent"], 11.45)
        self.assertAlmostEqual(prepared.loc[0, "leading_stock_change_percent"], 20.04)
        self.assertEqual(prepared.loc[0, "company_count"], 36)


if __name__ == "__main__":
    unittest.main()
