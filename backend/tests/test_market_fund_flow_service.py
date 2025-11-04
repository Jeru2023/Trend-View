import unittest
from datetime import date

import pandas as pd

from backend.src.api_clients import MARKET_FUND_FLOW_COLUMN_MAP
from backend.src.services.market_fund_flow_service import _prepare_market_fund_flow_frame


class MarketFundFlowServiceTests(unittest.TestCase):
    def test_prepare_market_fund_flow_frame_normalizes_numeric_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "trade_date": "2024-01-02",
                    "shanghai_close": "3,000.15",
                    "shanghai_change_percent": "0.55%",
                    "shenzhen_close": "9,800.20",
                    "shenzhen_change_percent": "-0.30%",
                    "main_net_inflow_amount": "12,345.67",
                    "main_net_inflow_ratio": "5.00%",
                    "huge_order_net_inflow_amount": "2,000",
                    "huge_order_net_inflow_ratio": "1.00%",
                    "large_order_net_inflow_amount": "3,000",
                    "large_order_net_inflow_ratio": "1.50%",
                    "medium_order_net_inflow_amount": "--",
                    "medium_order_net_inflow_ratio": "--",
                    "small_order_net_inflow_amount": "1,500",
                    "small_order_net_inflow_ratio": "0.25%",
                },
                {
                    "trade_date": "2024-01-01",
                    "shanghai_close": "2,990.12",
                    "shanghai_change_percent": "1.00%",
                    "shenzhen_close": "9,700.00",
                    "shenzhen_change_percent": "0.10%",
                    "main_net_inflow_amount": "8,000",
                    "main_net_inflow_ratio": "2.50%",
                    "huge_order_net_inflow_amount": "1,000",
                    "huge_order_net_inflow_ratio": "0.40%",
                    "large_order_net_inflow_amount": "2,000",
                    "large_order_net_inflow_ratio": "0.80%",
                    "medium_order_net_inflow_amount": "500",
                    "medium_order_net_inflow_ratio": "0.20%",
                    "small_order_net_inflow_amount": "300",
                    "small_order_net_inflow_ratio": "0.05%",
                },
            ]
        )

        prepared = _prepare_market_fund_flow_frame(raw)

        expected_columns = list(MARKET_FUND_FLOW_COLUMN_MAP.values())
        self.assertListEqual(list(prepared.columns), expected_columns)
        self.assertEqual(len(prepared), 2)
        self.assertEqual(prepared.loc[0, "trade_date"], date(2024, 1, 1))
        self.assertAlmostEqual(prepared.loc[0, "shanghai_close"], 2990.12)
        self.assertAlmostEqual(prepared.loc[0, "shanghai_change_percent"], 0.01)
        self.assertAlmostEqual(prepared.loc[0, "main_net_inflow_ratio"], 0.025)
        self.assertEqual(prepared.loc[1, "trade_date"], date(2024, 1, 2))
        self.assertTrue(pd.isna(prepared.loc[1, "medium_order_net_inflow_amount"]))
        self.assertTrue(pd.isna(prepared.loc[1, "medium_order_net_inflow_ratio"]))
        self.assertAlmostEqual(prepared.loc[1, "small_order_net_inflow_ratio"], 0.0025)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
