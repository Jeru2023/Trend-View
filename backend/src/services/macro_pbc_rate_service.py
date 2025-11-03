"""
Service helpers for synchronising People's Bank of China interest rate decisions.
"""

from __future__ import annotations

import logging
import math
import time
from typing import Optional

import pandas as pd

from ..api_clients import MACRO_PBC_RATE_COLUMN_MAP, fetch_macro_pbc_interest_rates
from ..config.settings import load_settings
from ..dao import MacroPbcRateDAO

logger = logging.getLogger(__name__)


def _prepare_macro_pbc_rate_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        columns = ["period_date"] + list(MACRO_PBC_RATE_COLUMN_MAP.values())
        return pd.DataFrame(columns=columns)

    frame = dataframe.copy()

    for column in MACRO_PBC_RATE_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    frame["period_date"] = pd.to_datetime(frame["period_label"], errors="coerce").dt.date

    numeric_columns = ["actual_value", "forecast_value", "previous_value"]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    prepared = (
        frame.loc[:, ["period_date", "period_label", *numeric_columns]]
        .dropna(subset=["period_date"])
        .sort_values("period_date")
        .drop_duplicates(subset=["period_date"], keep="last")
        .reset_index(drop=True)
    )
    return prepared


def sync_macro_pbc_rate(*, settings_path: Optional[str] = None) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = MacroPbcRateDAO(settings.postgres)

    raw = fetch_macro_pbc_interest_rates()
    prepared = _prepare_macro_pbc_rate_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("PBC rate sync skipped: no data returned.")
        return {"rows": 0, "elapsedSeconds": elapsed}

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    return {"rows": int(affected), "elapsedSeconds": elapsed}


def list_macro_pbc_rate(
    *,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MacroPbcRateDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset)
    stats = dao.stats()
    items = []
    for entry in result.get("items", []):
        sanitized = {}
        for key, value in entry.items():
            if isinstance(value, float) and not math.isfinite(value):
                sanitized[key] = None
            else:
                sanitized[key] = value
        items.append(sanitized)
    return {
        "total": int(result.get("total", 0) or 0),
        "items": items,
        "lastSyncedAt": stats.get("updated_at") if isinstance(stats, dict) else None,
    }


__all__ = ["sync_macro_pbc_rate", "list_macro_pbc_rate", "_prepare_macro_pbc_rate_frame"]
