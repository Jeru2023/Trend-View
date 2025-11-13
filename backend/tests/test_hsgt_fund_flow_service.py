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
                    "net_buy_amount_cumulative": "2.345",
                    "fund_inflow": "56.78",
                }
            ]
        )

        prepared = _prepare_hsgt_frame(raw)

        self.assertEqual(prepared.loc[0, "trade_date"], date(2025, 11, 3))
        self.assertAlmostEqual(prepared.loc[0, "net_buy_amount"], 12.34)
        self.assertAlmostEqual(prepared.loc[0, "net_buy_amount_cumulative"], 2.345)
        self.assertAlmostEqual(prepared.loc[0, "fund_inflow"], 56.78)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
