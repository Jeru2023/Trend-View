"""
Service layer for synchronising macro leverage ratio data.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from ..api_clients import MACRO_LEVERAGE_COLUMN_MAP, fetch_macro_leverage_ratios
from ..config.settings import load_settings
from ..dao import MacroLeverageDAO

logger = logging.getLogger(__name__)


def _prepare_macro_leverage_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        columns = ["period_date"] + list(MACRO_LEVERAGE_COLUMN_MAP.values())
        return pd.DataFrame(columns=columns)

    frame = dataframe.copy()

    # Ensure all expected columns exist
    for column in MACRO_LEVERAGE_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    # Build period_date column from label
    frame["period_label"] = frame["period_label"].astype(str)
    with pd.option_context("mode.chained_assignment", None):
        period_series = pd.to_datetime(frame["period_label"], errors="coerce")
        period_series = period_series + MonthEnd(0)
        frame["period_date"] = period_series.dt.date

    numeric_columns = [
        "household_ratio",
        "non_financial_corporate_ratio",
        "government_ratio",
        "central_government_ratio",
        "local_government_ratio",
        "real_economy_ratio",
        "financial_assets_ratio",
        "financial_liabilities_ratio",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    columns = ["period_date", "period_label", *numeric_columns]
    prepared = frame.loc[:, columns].dropna(subset=["period_date"]).sort_values("period_date").reset_index(drop=True)
    return prepared


def sync_macro_leverage_ratios(*, settings_path: Optional[str] = None) -> dict[str, object]:
    """Fetch macro leverage ratios and upsert into PostgreSQL."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = MacroLeverageDAO(settings.postgres)

    raw = fetch_macro_leverage_ratios()
    prepared = _prepare_macro_leverage_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Macro leverage sync skipped: no data returned.")
        return {"rows": 0, "elapsedSeconds": elapsed}

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    return {"rows": int(affected), "elapsedSeconds": elapsed}


def list_macro_leverage_ratios(
    *,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MacroLeverageDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset)
    stats = dao.stats()
    return {
        "total": int(result.get("total", 0) or 0),
        "items": result.get("items", []),
        "lastSyncedAt": stats.get("updated_at") if isinstance(stats, dict) else None,
    }


__all__ = [
    "sync_macro_leverage_ratios",
    "list_macro_leverage_ratios",
    "_prepare_macro_leverage_frame",
]
