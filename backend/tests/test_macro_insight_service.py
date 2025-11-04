import unittest
from datetime import date, datetime

from backend.src.services.macro_insight_service import generate_macro_insight


class MacroInsightServiceTests(unittest.TestCase):
    def test_generate_macro_insight_without_llm(self) -> None:
        result = generate_macro_insight(run_llm=False)

        self.assertIsInstance(result.get("snapshot_date"), date)
        self.assertIsInstance(result.get("generated_at"), datetime)

        datasets = result.get("datasets", [])
        self.assertTrue(datasets, "Expected at least one macro dataset to be returned")

        for dataset in datasets:
            series = dataset.get("series", [])
            self.assertLessEqual(len(series), 11)
            if series:
                first = series[0]
                self.assertIn("period_date", first)
                self.assertIn("period_label", first)

        self.assertIsNone(result.get("summary"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
