"""
Service layer for synchronising monthly PMI data.
"""

from __future__ import annotations

import logging
import math
import time
from typing import Optional

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from ..api_clients import (
    MACRO_PMI_COLUMN_MAP,
    fetch_macro_non_man_pmi,
    fetch_macro_pmi_yearly,
)
from ..config.settings import load_settings
from ..dao import MacroPmiDAO

logger = logging.getLogger(__name__)

MANUFACTURING_SERIES = "manufacturing"
NON_MANUFACTURING_SERIES = "non_manufacturing"
NUMERIC_COLUMNS = ["actual_value", "forecast_value", "previous_value"]


def _prepare_macro_pmi_frame(dataframe: pd.DataFrame, series: str) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        columns = ["series", "period_label", "period_date", *NUMERIC_COLUMNS]
        return pd.DataFrame(columns=columns)

    frame = dataframe.copy()

    for column in MACRO_PMI_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    with pd.option_context("mode.chained_assignment", None):
        period_series = pd.to_datetime(frame["period_label"], errors="coerce")
        period_series = period_series + MonthEnd(0)
        frame["period_date"] = period_series.dt.date

    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["series"] = series

    columns = ["series", "period_label", "period_date", *NUMERIC_COLUMNS]
    prepared = (
        frame.loc[:, columns]
        .dropna(subset=["period_label", "period_date"])
        .sort_values(["series", "period_date", "period_label"])
        .drop_duplicates(subset=["series", "period_label"], keep="last")
        .reset_index(drop=True)
    )
    return prepared


def sync_macro_pmi(*, settings_path: Optional[str] = None) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = MacroPmiDAO(settings.postgres)

    frames = [
        _prepare_macro_pmi_frame(fetch_macro_pmi_yearly(), MANUFACTURING_SERIES),
        _prepare_macro_pmi_frame(fetch_macro_non_man_pmi(), NON_MANUFACTURING_SERIES),
    ]
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    if not non_empty:
        elapsed = time.perf_counter() - started
        logger.warning("PMI sync skipped: no data returned.")
        return {"rows": 0, "elapsedSeconds": elapsed}

    prepared = (
        pd.concat(non_empty, ignore_index=True)
        .sort_values(["series", "period_date", "period_label"])
        .reset_index(drop=True)
    )

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    return {"rows": int(affected), "elapsedSeconds": elapsed}


def list_macro_pmi(
    *,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MacroPmiDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset)
    stats = dao.stats()
    items = []
    for entry in result.get("items", []):
        sanitised = {}
        for key, value in entry.items():
            if isinstance(value, float) and not math.isfinite(value):
                sanitised[key] = None
            else:
                sanitised[key] = value
        sanitised.setdefault("series", MANUFACTURING_SERIES)
        items.append(sanitised)
    return {
        "total": int(result.get("total", 0) or 0),
        "items": items,
        "lastSyncedAt": stats.get("updated_at") if isinstance(stats, dict) else None,
    }


__all__ = ["sync_macro_pmi", "list_macro_pmi", "_prepare_macro_pmi_frame"]
