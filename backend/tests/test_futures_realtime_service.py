import sys
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.src.services.futures_realtime_service import _prepare_futures_realtime_frame


class FuturesRealtimeServiceTests(unittest.TestCase):
    def test_prepare_futures_realtime_frame_coerces_values(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "name": "LME镍3个月",
                    "last_price": "20000",
                    "price_cny": "150000",
                    "change_amount": "100",
                    "change_percent": "0.50%",
                    "open_price": "19900",
                    "high_price": "20100",
                    "low_price": "19800",
                    "prev_settlement": "19900",
                    "open_interest": "1000",
                    "bid_price": "19980",
                    "ask_price": "20010",
                    "quote_time": "10:00:00",
                    "trade_date": "2025-03-07",
                }
            ]
        )

        prepared = _prepare_futures_realtime_frame(raw)

        self.assertEqual(len(prepared), 1)
        record = prepared.loc[0]
        self.assertEqual(record["name"], "LME镍3个月")
        self.assertEqual(record["code"], "NID")
        self.assertEqual(record["trade_date"], date(2025, 3, 7))
        self.assertAlmostEqual(record["change_percent"], 0.5)


if __name__ == "__main__":
    unittest.main()
