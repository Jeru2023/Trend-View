import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.src.services.global_index_service import (  # noqa: E402
    _build_snapshot_records,
    _prepare_global_index_frame,
    _normalize_history_rows,
    list_global_index_history,
)


class GlobalIndexServiceTests(unittest.TestCase):
    def test_normalize_history_rows_maps_yahoo_fields(self) -> None:
        rows = [
            {
                "trade_date": date(2025, 11, 12),
                "open": 10.0,
                "high": 11.5,
                "low": 9.8,
                "close": 11.0,
                "volume": 123456.0,
            }
        ]

        normalized = _normalize_history_rows(rows)
        latest = normalized[0]
        self.assertAlmostEqual(latest["open_price"], 10.0)
        self.assertAlmostEqual(latest["high_price"], 11.5)
        self.assertAlmostEqual(latest["low_price"], 9.8)
        self.assertAlmostEqual(latest["close_price"], 11.0)
        self.assertEqual(latest["volume"], 123456.0)

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

    def test_build_snapshot_records_calculates_change_fields(self) -> None:
        now = datetime(2025, 11, 12, tzinfo=timezone.utc)
        rows = {
            "^DJI": [
                {
                    "close_price": 100.0,
                    "open_price": 99.0,
                    "high_price": 102.0,
                    "low_price": 98.0,
                    "prev_close": 100.0,
                    "trade_date": now,
                },
                {
                    "close_price": 98.0,
                    "open_price": 97.5,
                    "high_price": 99.0,
                    "low_price": 96.5,
                    "prev_close": 97.0,
                    "trade_date": now.replace(day=11),
                },
            ],
            "^GSPC": [
                {
                    "close_price": 50.0,
                    "open_price": 49.0,
                    "high_price": 51.0,
                    "low_price": 48.5,
                    "prev_close": None,
                    "trade_date": now,
                },
                {
                    "close_price": 49.5,
                    "open_price": 49.2,
                    "high_price": 50.3,
                    "low_price": 48.8,
                    "prev_close": 48.7,
                    "trade_date": now.replace(day=11),
                },
            ],
        }

        records = _build_snapshot_records(rows)
        codes = {record["code"] for record in records}
        self.assertIn("^DJI", codes)
        self.assertIn("^GSPC", codes)

        dji = next(record for record in records if record["code"] == "^DJI")
        self.assertAlmostEqual(dji["change_amount"], 2.0)
        self.assertAlmostEqual(dji["change_percent"], (2.0 / 98.0) * 100.0)
        self.assertAlmostEqual(dji["amplitude"], ((102.0 - 98.0) / 98.0) * 100.0)
        self.assertEqual(dji["latest_price"], 100.0)
        self.assertEqual(dji["prev_close"], 98.0)

        spx = next(record for record in records if record["code"] == "^GSPC")
        expected_prev = 49.5
        self.assertAlmostEqual(spx["change_amount"], 0.5)
        self.assertAlmostEqual(spx["change_percent"], (0.5 / expected_prev) * 100.0)
        self.assertAlmostEqual(spx["amplitude"], ((51.0 - 48.5) / expected_prev) * 100.0)
        self.assertEqual(spx["latest_price"], 50.0)

    def test_list_global_index_history_backfills_change_values(self) -> None:
        rows = [
            {
                "code": "^DJI",
                "name": "Dow",
                "trade_date": datetime(2025, 11, 12).date(),
                "open_price": 48015.0,
                "high_price": 48431.0,
                "low_price": 48015.0,
                "close_price": 48254.82,
                "volume": 123.0,
                "prev_close": 48254.82,
                "change_amount": 0.0,
                "change_percent": 0.0,
                "currency": "USD",
                "timezone": "America/New_York",
            },
            {
                "code": "^DJI",
                "name": "Dow",
                "trade_date": datetime(2025, 11, 11).date(),
                "open_price": 47384.5,
                "high_price": 47974.3,
                "low_price": 47384.5,
                "close_price": 47927.96,
                "volume": 120.0,
                "prev_close": 47368.62,
                "change_amount": 559.33,
                "change_percent": 1.18,
                "currency": "USD",
                "timezone": "America/New_York",
            },
        ]

        with patch("backend.src.services.global_index_service.load_settings") as mock_settings, patch(
            "backend.src.services.global_index_service.GlobalIndexHistoryDAO"
        ) as mock_dao:
            mock_settings.return_value = SimpleNamespace(postgres=SimpleNamespace())
            instance = MagicMock()
            instance.list_history.return_value = rows
            mock_dao.return_value = instance

            result = list_global_index_history(code="^DJI", limit=5)

        self.assertEqual(result["code"], "^DJI")
        latest = result["items"][0]
        self.assertAlmostEqual(latest["change_amount"], 326.86, places=2)
        self.assertGreater(latest["change_percent"], 0.0)

    def test_list_global_index_history_triggers_tushare_fallback_for_ftse(self) -> None:
        fallback_rows = [
            {
                "code": "XIN9.FGI",
                "name": "FTSE",
                "trade_date": datetime(2025, 1, 2).date(),
                "open_price": 1.0,
                "high_price": 1.2,
                "low_price": 0.9,
                "close_price": 1.1,
                "volume": 100.0,
                "prev_close": 0.95,
                "change_amount": 0.15,
                "change_percent": 15.0,
                "currency": "CNY",
                "timezone": "Asia/Shanghai",
            }
        ]
        fallback_df = pd.DataFrame(fallback_rows)

        with patch("backend.src.services.global_index_service.load_settings") as mock_settings, patch(
            "backend.src.services.global_index_service.GlobalIndexHistoryDAO"
        ) as mock_dao, patch(
            "backend.src.services.global_index_service._fetch_ftse_a50_history_from_tushare"
        ) as mock_fetch:
            mock_settings.return_value = SimpleNamespace(
                tushare=SimpleNamespace(token="dummy"),
                postgres=SimpleNamespace(),
            )
            dao_instance = MagicMock()
            dao_instance.list_history.side_effect = [[], fallback_rows]
            mock_dao.return_value = dao_instance
            mock_fetch.return_value = fallback_df

            result = list_global_index_history(code="XIN9.FGI", limit=5)

        self.assertEqual(result["code"], "XIN9.FGI")
        self.assertEqual(len(result["items"]), 1)
        mock_fetch.assert_called_once()
        dao_instance.upsert.assert_called_once()

    def test_list_global_index_history_backfills_missing_changes(self) -> None:
        rows = [
            {
                "code": "^DJI",
                "name": "Dow",
                "trade_date": datetime(2025, 1, 2).date(),
                "open_price": 100.0,
                "high_price": 102.0,
                "low_price": 98.0,
                "close_price": 101.0,
                "volume": 10.0,
                "prev_close": 99.0,
                "change_amount": None,
                "change_percent": None,
                "currency": "USD",
                "timezone": "America/New_York",
            }
        ]

        with patch("backend.src.services.global_index_service.load_settings") as mock_settings, patch(
            "backend.src.services.global_index_service.GlobalIndexHistoryDAO"
        ) as mock_dao:
            mock_settings.return_value = SimpleNamespace(postgres=SimpleNamespace())
            dao_instance = MagicMock()
            dao_instance.list_history.return_value = rows
            mock_dao.return_value = dao_instance

            result = list_global_index_history(code="^DJI", limit=10)

        self.assertEqual(result["code"], "^DJI")
        first = result["items"][0]
        self.assertAlmostEqual(first["change_amount"], 2.0)
        self.assertAlmostEqual(first["change_percent"], (2.0 / 99.0) * 100.0)
        dao_instance.upsert.assert_called_once()


if __name__ == "__main__":
    unittest.main()
