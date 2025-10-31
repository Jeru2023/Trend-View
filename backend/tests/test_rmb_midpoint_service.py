import sys
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.src.services.rmb_midpoint_service import _prepare_rmb_midpoint_frame


class RmbMidpointServiceTests(unittest.TestCase):
    def test_prepare_rmb_midpoint_frame_normalizes_numeric_and_dates(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "trade_date": "2025-03-07",
                    "usd": "717.86",
                    "eur": "768.65",
                    "jpy": "4.9064",
                    "hkd": "91.30",
                },
                {
                    "trade_date": "invalid",
                    "usd": "abc",
                },
            ]
        )

        prepared = _prepare_rmb_midpoint_frame(raw)

        self.assertEqual(len(prepared), 1)
        record = prepared.loc[0]
        self.assertEqual(record["trade_date"], date(2025, 3, 7))
        self.assertAlmostEqual(record["usd"], 717.86)
        self.assertAlmostEqual(record["jpy"], 4.9064)


if __name__ == "__main__":
    unittest.main()
