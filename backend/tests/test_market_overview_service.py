import unittest

from backend.src.services.market_overview_service import build_market_overview_payload


class MarketOverviewServiceTests(unittest.TestCase):
    def test_build_market_overview_payload_structure(self) -> None:
        payload = build_market_overview_payload()

        self.assertIn("generatedAt", payload)
        self.assertIn("realtimeIndices", payload)
        self.assertIsInstance(payload["realtimeIndices"], list)

        index_history = payload.get("indexHistory")
        self.assertIsInstance(index_history, dict)
        for series in index_history.values():
            self.assertLessEqual(len(series), 10)

        self.assertIn("marketFundFlow", payload)
        self.assertLessEqual(len(payload.get("marketFundFlow", [])), 10)
        self.assertLessEqual(len(payload.get("marginAccount", [])), 10)

        self.assertIn("marketInsight", payload)
        self.assertIn("macroInsight", payload)
        self.assertIn("peripheralInsight", payload)
        self.assertIn("marketActivity", payload)

if __name__ == "__main__":  # pragma: no cover
    unittest.main()
