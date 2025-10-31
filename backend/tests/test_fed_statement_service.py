import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.src.services.fed_statement_service import _prepare_fed_statement_frame  # noqa: E402


class FedStatementServiceTests(unittest.TestCase):
    def test_prepare_frame_normalizes_dates_and_strings(self) -> None:
        entries = [
            {
                "title": "  Statement A ",
                "url": "https://example.com/a ",
                "publishedDate": "10/30/2025",
                "content": " First line ",
                "rawText": " Full text ",
                "position": "1",
            },
            {
                "title": "Statement B",
                "url": "https://example.com/b",
                "publishedDate": date(2025, 10, 29),
                "content": None,
                "rawText": None,
                "position": None,
            },
        ]

        frame = _prepare_fed_statement_frame(entries)

        self.assertEqual(list(frame.columns), ["url", "title", "statement_date", "content", "raw_text", "position"])
        self.assertEqual(len(frame), 2)
        first = frame.iloc[0]
        self.assertEqual(first["title"], "Statement A")
        self.assertEqual(first["url"], "https://example.com/a")
        self.assertEqual(first["content"], "First line")
        self.assertEqual(first["raw_text"], "Full text")
        self.assertEqual(first["position"], 1)
        self.assertIsInstance(first["statement_date"], date)
        self.assertEqual(first["statement_date"], date(2025, 10, 30))

        second = frame.iloc[1]
        self.assertEqual(second["position"], 0)
        self.assertEqual(second["statement_date"], date(2025, 10, 29))


if __name__ == "__main__":
    unittest.main()
