import unittest

import pandas as pd

from backend.src.services.individual_fund_flow_service import _prepare_frame


class IndividualFundFlowServiceTests(unittest.TestCase):
    def test_prepare_frame_normalizes_and_parses_amounts(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "rank": "1",
                    "stock_code": "300256",
                    "stock_name": "星星科技",
                    "latest_price": "3.40",
                    "price_change_percent": "6.17%",
                    "stage_change_percent": "-1.23%",
                    "turnover_rate": "15.2%",
                    "continuous_turnover_rate": "45.1%",
                    "inflow": "6.49亿",
                    "outflow": "5.01亿",
                    "net_amount": "1.48亿",
                    "net_inflow": "-571.77万",
                    "turnover_amount": "11.50亿",
                }
            ]
        )

        prepared = _prepare_frame(raw, "即时")

        self.assertEqual(prepared.loc[0, "symbol"], "即时")
        self.assertEqual(prepared.loc[0, "stock_code"], "300256")
        self.assertAlmostEqual(prepared.loc[0, "latest_price"], 3.40)
        self.assertAlmostEqual(prepared.loc[0, "price_change_percent"], 6.17)
        self.assertAlmostEqual(prepared.loc[0, "stage_change_percent"], -1.23)
        self.assertAlmostEqual(prepared.loc[0, "turnover_rate"], 15.2)
        self.assertAlmostEqual(prepared.loc[0, "continuous_turnover_rate"], 45.1)
        self.assertAlmostEqual(prepared.loc[0, "inflow"], 6.49e8)
        self.assertAlmostEqual(prepared.loc[0, "net_inflow"], -571.77e4)


if __name__ == "__main__":
    unittest.main()
