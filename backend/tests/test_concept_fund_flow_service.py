import unittest

import pandas as pd

from backend.src.services.concept_fund_flow_service import _prepare_frame


class ConceptFundFlowServiceTests(unittest.TestCase):
    def test_prepare_frame_normalizes_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "序号": "1",
                    "行业": "算力租赁",
                    "行业指数": "2030.55",
                    "行业-涨跌幅": "4.65",
                    "流入资金": "127.17亿",
                    "流出资金": "9.51亿",
                    "净额": "-5094.96万",
                    "公司家数": "43",
                    "领涨股": "东岳硅材",
                    "领涨股-涨跌幅": "20.04%",
                    "当前价": "11.38",
                }
            ]
        )

        prepared = _prepare_frame(raw, "即时")

        self.assertEqual(prepared.loc[0, "symbol"], "即时")
        self.assertEqual(prepared.loc[0, "concept"], "算力租赁")
        self.assertEqual(int(prepared.loc[0, "rank"]), 1)
        self.assertAlmostEqual(prepared.loc[0, "concept_index"], 2030.55)
        self.assertAlmostEqual(prepared.loc[0, "price_change_percent"], 4.65)
        self.assertIsNone(prepared.loc[0, "stage_change_percent"])
        self.assertAlmostEqual(prepared.loc[0, "leading_stock_change_percent"], 20.04)
        self.assertEqual(prepared.loc[0, "company_count"], 43)
        self.assertAlmostEqual(prepared.loc[0, "inflow"], 127.17e8)
        self.assertAlmostEqual(prepared.loc[0, "net_amount"], -5094.96e4)


if __name__ == "__main__":
    unittest.main()
