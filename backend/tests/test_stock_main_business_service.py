import unittest

import pandas as pd

from backend.src.services.stock_main_business_service import (
    _extract_symbol,
    _prepare_business_frame,
)


class StockMainBusinessServiceTests(unittest.TestCase):
    def test_extract_symbol_normalizes_various_codes(self) -> None:
        self.assertEqual(_extract_symbol("000066"), "000066")
        self.assertEqual(_extract_symbol("000066.SZ"), "000066")
        self.assertEqual(_extract_symbol("sz000066"), "000066")
        self.assertEqual(_extract_symbol("  300750  "), "300750")
        self.assertIsNone(_extract_symbol(None))
        self.assertIsNone(_extract_symbol(""))

    def test_prepare_business_frame_fills_missing_fields(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "symbol": "000066",
                    "main_business": "软件开发、系统集成",
                    "product_type": "软件",
                    "product_name": "核心软件",
                    "business_scope": "信息技术服务",
                }
            ]
        )

        prepared = _prepare_business_frame(frame, fallback_symbol="000066")

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared.loc[0, "symbol"], "000066")
        self.assertEqual(prepared.loc[0, "ts_code"], "000066.SZ")
        self.assertEqual(prepared.loc[0, "main_business"], "软件开发、系统集成")
        self.assertEqual(prepared.loc[0, "product_type"], "软件")
        self.assertEqual(prepared.loc[0, "business_scope"], "信息技术服务")

    def test_prepare_business_frame_uses_fallback_symbol_when_missing(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "symbol": None,
                    "main_business": None,
                    "product_type": "--",
                    "product_name": "",
                    "business_scope": "主营业务涵盖 --",
                }
            ]
        )

        prepared = _prepare_business_frame(frame, fallback_symbol="600000")

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared.loc[0, "symbol"], "600000")
        self.assertEqual(prepared.loc[0, "ts_code"], "600000.SH")
        self.assertIsNone(prepared.loc[0, "main_business"])
        self.assertIsNone(prepared.loc[0, "product_type"])
        self.assertEqual(prepared.loc[0, "business_scope"], "主营业务涵盖 --")


if __name__ == "__main__":
    unittest.main()

