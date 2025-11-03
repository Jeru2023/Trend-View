import unittest
from datetime import date

import pandas as pd

from backend.src.services.margin_account_service import _prepare_margin_frame


class MarginAccountServiceTests(unittest.TestCase):
    def test_prepare_margin_frame_normalizes_numeric_columns(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "trade_date": "2024-06-13",
                    "financing_balance": "14769.50",
                    "securities_lending_balance": "338.84",
                    "financing_purchase_amount": "123.45",
                    "securities_lending_sell_amount": "67.89",
                    "securities_company_count": "140",
                    "business_department_count": "800",
                    "individual_investor_count": "1,548,858",
                    "institutional_investor_count": "200",
                    "participating_investor_count": "1234567",
                    "liability_investor_count": "1230000",
                    "collateral_value": "44328.60",
                    "average_collateral_ratio": "249.3",
                }
            ]
        )

        prepared = _prepare_margin_frame(raw)

        self.assertEqual(len(prepared), 1)
        row = prepared.loc[0]
        self.assertEqual(row["trade_date"], date(2024, 6, 13))
        self.assertAlmostEqual(row["financing_balance"], 14769.5)
        self.assertAlmostEqual(row["securities_lending_balance"], 338.84)
        self.assertAlmostEqual(row["financing_purchase_amount"], 123.45)
        self.assertAlmostEqual(row["securities_lending_sell_amount"], 67.89)
        self.assertAlmostEqual(row["securities_company_count"], 140.0)
        self.assertAlmostEqual(row["business_department_count"], 800.0)
        self.assertAlmostEqual(row["individual_investor_count"], 1548858.0)
        self.assertAlmostEqual(row["institutional_investor_count"], 200.0)
        self.assertAlmostEqual(row["participating_investor_count"], 1234567.0)
        self.assertAlmostEqual(row["liability_investor_count"], 1230000.0)
        self.assertAlmostEqual(row["collateral_value"], 44328.6)
        self.assertAlmostEqual(row["average_collateral_ratio"], 249.3)

    def test_prepare_margin_frame_drops_invalid_dates(self) -> None:
        raw = pd.DataFrame(
            [
                {"trade_date": "invalid-date", "financing_balance": "123"},
                {"trade_date": None, "financing_balance": "456"},
            ]
        )

        prepared = _prepare_margin_frame(raw)
        self.assertEqual(len(prepared), 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
