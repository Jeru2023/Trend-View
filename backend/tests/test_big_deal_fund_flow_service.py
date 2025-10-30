import unittest

import pandas as pd

from backend.src.services.big_deal_fund_flow_service import _prepare_frame, _normalize_query_codes


class BigDealFundFlowServiceTests(unittest.TestCase):
    def test_prepare_frame_parses_numeric_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "trade_time": "2024-08-19 15:00:01",
                    "stock_code": "601668",
                    "stock_name": "中国建筑",
                    "trade_price": "5.67",
                    "trade_volume": "100000",
                    "trade_amount": "111.98万",
                    "trade_side": "买盘",
                    "price_change_percent": "0.53%",
                    "price_change": "0.03",
                }
            ]
        )

        prepared = _prepare_frame(raw)

        self.assertEqual(prepared.loc[0, "stock_code"], "601668")
        self.assertAlmostEqual(prepared.loc[0, "trade_price"], 5.67)
        self.assertEqual(prepared.loc[0, "trade_volume"], 100000)
        self.assertAlmostEqual(prepared.loc[0, "trade_amount"], 111.98 * 1e4)
        self.assertAlmostEqual(prepared.loc[0, "price_change_percent"], 0.53)
        self.assertAlmostEqual(prepared.loc[0, "price_change"], 0.03)
        self.assertFalse(pd.isna(prepared.loc[0, "trade_time"]))

    def test_prepare_frame_drops_duplicates_and_nulls(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "trade_time": "2024-08-19 15:00:01",
                    "stock_code": "601668",
                    "trade_amount": "111.98万",
                    "stock_name": "中国建筑",
                    "trade_price": "5.67",
                    "trade_volume": "100000",
                    "trade_side": " 买盘 ",
                    "price_change_percent": "0.53%",
                    "price_change": "0.03",
                },
                {
                    "trade_time": "2024-08-19 15:00:01",
                    "stock_code": "601668",
                    "stock_name": "中国建筑",
                    "trade_price": "5.67",
                    "trade_volume": "100000",
                    "trade_amount": "111.98万",
                    "trade_side": "买盘",
                    "price_change_percent": "0.53%",
                    "price_change": "0.03",
                },
                {
                    "trade_time": None,
                    "stock_code": "601668",
                    "stock_name": "中国建筑",
                    "trade_price": "5.67",
                    "trade_volume": "100000",
                    "trade_amount": "111.98万",
                    "trade_side": "买盘",
                    "price_change_percent": "0.53%",
                    "price_change": "0.03",
                },
            ]
        )

        prepared = _prepare_frame(raw)

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared.iloc[0]["stock_code"], "601668")
        self.assertEqual(prepared.iloc[0]["trade_side"], "买盘")
        self.assertEqual(prepared.iloc[0]["trade_amount"], round(111.98 * 1e4, 2))
        self.assertEqual(prepared.iloc[0]["trade_volume"], 100000)

    def test_normalize_query_codes(self) -> None:
        result = _normalize_query_codes("000063.SZ")
        self.assertIn("000063", result)
        self.assertIn("000063.SZ", result)
        self.assertNotIn("", result)


if __name__ == "__main__":
    unittest.main()
