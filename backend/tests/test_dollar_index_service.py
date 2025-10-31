import sys
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.src.services.dollar_index_service import _prepare_dollar_index_frame


class DollarIndexServiceTests(unittest.TestCase):
    def test_prepare_dollar_index_frame_normalizes_numeric_and_dates(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "trade_date": "2025-03-07",
                    "code": "UDI",
                    "name": "美元指数",
                    "open_price": "104.23",
                    "close_price": "103.63",
                    "high_price": "104.25",
                    "low_price": "103.55",
                    "amplitude": "0.67",
                }
            ]
        )

        prepared = _prepare_dollar_index_frame(raw)

        self.assertEqual(len(prepared), 1)
        record = prepared.loc[0]
        self.assertEqual(record["code"], "UDI")
        self.assertEqual(record["trade_date"], date(2025, 3, 7))
        self.assertAlmostEqual(record["close_price"], 103.63)
        self.assertAlmostEqual(record["amplitude"], 0.67)


if __name__ == "__main__":
    unittest.main()
