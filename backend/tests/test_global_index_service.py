import sys
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.src.services.global_index_service import _prepare_global_index_frame


class GlobalIndexServiceTests(unittest.TestCase):
    def test_prepare_global_index_frame_normalizes_numeric_and_dates(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "seq": "1",
                    "code": "DJI",
                    "name": "道琼斯",
                    "latest_price": "38000.12",
                    "change_amount": "120.5",
                    "change_percent": "0.31",
                    "open_price": "37900",
                    "high_price": "38100",
                    "low_price": "37800",
                    "prev_close": "37879.62",
                    "amplitude": "0.76",
                    "last_quote_time": "2025-10-31 15:30:00",
                },
                {
                    "seq": "2",
                    "code": "IXIC",
                    "name": "纳斯达克",
                    "latest_price": "15000.55",
                    "change_amount": "-50.25",
                    "change_percent": "-0.33",
                    "open_price": "15040",
                    "high_price": "15120",
                    "low_price": "14980",
                    "prev_close": "15050.80",
                    "amplitude": "0.95",
                    "last_quote_time": "2025-10-31 15:30:00",
                },
            ]
        )

        prepared = _prepare_global_index_frame(raw)

        self.assertEqual(len(prepared), 2)
        first = prepared.loc[0]
        self.assertEqual(first["code"], "DJI")
        self.assertAlmostEqual(first["latest_price"], 38000.12)
        self.assertEqual(int(first["seq"]), 1)
        self.assertIsInstance(first["last_quote_time"], (pd.Timestamp, datetime))


if __name__ == "__main__":
    unittest.main()
