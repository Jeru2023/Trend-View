"""Quick helper to verify Tushare moneyflow_hsgt connectivity and data."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

import pandas as pd
import tushare as ts

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.src.config.settings import load_settings


def _get_date_range(window_days: int = 30) -> tuple[str, str]:
    """Return a tuple of (start_date, end_date) formatted as YYYYMMDD."""
    end = date.today()
    start = end - timedelta(days=max(1, window_days))
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def main() -> None:
    """Fetch recent HSGT moneyflow data and print a compact preview."""
    settings = load_settings()
    token = getattr(settings.tushare, "token", "") or ""
    if not token.strip():
        raise RuntimeError("No Tushare token configured. Please set settings.tushare.token first.")

    pro = ts.pro_api(token.strip())
    start_date, end_date = _get_date_range(window_days=600)
    print(f"Fetching Tushare moneyflow_hsgt from {start_date} to {end_date} ...")

    df = pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        print("Tushare returned no rows for the requested window.")
        return

    columns = ["trade_date", "north_money", "south_money", "hgt", "sgt", "ggt_ss", "ggt_sz"]
    preview = df.loc[:, [col for col in columns if col in df.columns]].copy()
    preview["trade_date"] = pd.to_datetime(preview["trade_date"], errors="coerce")
    preview = preview.sort_values("trade_date", ascending=False).reset_index(drop=True)

    print(preview.head(10).to_string(index=False))
    print(f"\nTotal rows fetched: {len(df)}")
    print(f"Date range covered: {preview['trade_date'].min().date()} -> {preview['trade_date'].max().date()}")


if __name__ == "__main__":  # pragma: no cover
    main()
