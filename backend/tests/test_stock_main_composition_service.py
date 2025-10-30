import unittest

import pandas as pd

from backend.src.services.stock_main_composition_service import (
    _extract_symbol,
    _prepare_frame,
    _to_eastmoney_symbol,
)


class StockMainCompositionServiceTests(unittest.TestCase):
    def test_to_eastmoney_symbol_handles_multiple_formats(self) -> None:
        self.assertEqual(_to_eastmoney_symbol("000001"), "SZ000001")
        self.assertEqual(_to_eastmoney_symbol("000001.SZ"), "SZ000001")
        self.assertEqual(_to_eastmoney_symbol("sh600519"), "SH600519")
        self.assertIsNone(_to_eastmoney_symbol(""))

    def test_extract_symbol_normalizes_code(self) -> None:
        self.assertEqual(_extract_symbol("SZ000001"), "000001")
        self.assertEqual(_extract_symbol("000001.SH"), "000001")
        self.assertIsNone(_extract_symbol(None))

    def test_prepare_frame_sanitizes_and_filters_rows(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "symbol": "600519",
                    "report_date": "2024-06-30",
                    "category_type": "按产品分类",
                    "composition": "白酒",
                    "revenue": "123456789",
                    "revenue_ratio": 0.75,
                    "cost": "34567890",
                    "cost_ratio": 0.25,
                    "profit": "88888888",
                    "profit_ratio": 0.8,
                    "gross_margin": 0.64,
                },
                {
                    "symbol": "600519",
                    "report_date": "2024-06-30",
                    "category_type": "按地区分类",
                    "composition": None,
                    "revenue": None,
                    "revenue_ratio": None,
                    "cost": None,
                    "cost_ratio": None,
                    "profit": None,
                    "profit_ratio": None,
                    "gross_margin": None,
                },
            ]
        )

        prepared = _prepare_frame(frame, fallback_symbol="600519")

        self.assertEqual(len(prepared), 1)
        row = prepared.iloc[0]
        self.assertEqual(row["symbol"], "600519")
        self.assertEqual(str(row["report_date"]), "2024-06-30")
        self.assertEqual(row["category_type"], "按产品分类")
        self.assertEqual(row["composition"], "白酒")
        self.assertAlmostEqual(row["revenue"], 123456789.0)
        self.assertAlmostEqual(row["revenue_ratio"], 0.75)
        self.assertAlmostEqual(row["gross_margin"], 0.64)


if __name__ == "__main__":
    unittest.main()

