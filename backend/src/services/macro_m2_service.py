"""
Service layer for synchronising monthly money supply data (M0/M1/M2).
"""

from __future__ import annotations

import logging
import math
import time
from typing import Optional

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from ..api_clients import MACRO_M2_COLUMN_MAP, fetch_macro_m2_yearly
from ..config.settings import load_settings
from ..dao import MacroM2DAO

logger = logging.getLogger(__name__)

MONEY_SUPPLY_FIELDS = [
    "m0",
    "m0_yoy",
    "m0_mom",
    "m1",
    "m1_yoy",
    "m1_mom",
    "m2",
    "m2_yoy",
    "m2_mom",
]


def _prepare_macro_m2_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        columns = ["period_date", "period_label", *MONEY_SUPPLY_FIELDS]
        return pd.DataFrame(columns=columns)

    frame = dataframe.copy()

    for column in MACRO_M2_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    with pd.option_context("mode.chained_assignment", None):
        labels = frame["period_label"].astype(str).str.strip()
        period_series = pd.to_datetime(labels, format="%Y%m", errors="coerce")
        needs_second_pass = period_series.isna()
        if needs_second_pass.any():
            period_series.loc[needs_second_pass] = pd.to_datetime(
                labels.loc[needs_second_pass], format="%Y-%m", errors="coerce"
            )
        period_series = period_series + MonthEnd(0)
        frame["period_date"] = period_series.dt.date

    for column in MONEY_SUPPLY_FIELDS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    columns = ["period_date", "period_label", *MONEY_SUPPLY_FIELDS]
    prepared = (
        frame.loc[:, columns]
        .dropna(subset=["period_date"])
        .sort_values(["period_date", "period_label"])
        .reset_index(drop=True)
    )
    return prepared


def _month_range_strings(years: int = 3) -> tuple[str, str]:
    today = pd.Timestamp.today().normalize()
    end_month = today.strftime("%Y%m")
    start_month = (today - pd.DateOffset(years=years)).strftime("%Y%m")
    return start_month, end_month


def sync_macro_m2(*, settings_path: Optional[str] = None) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = MacroM2DAO(settings.postgres)
    start_month, end_month = _month_range_strings()

    raw = fetch_macro_m2_yearly(
        token=settings.tushare.token,
        start_month=start_month,
        end_month=end_month,
    )
    prepared = _prepare_macro_m2_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("M2 sync skipped: no data returned.")
        return {"rows": 0, "elapsedSeconds": elapsed}

    affected = dao.upsert(prepared)
    dao.purge_empty_rows()
    elapsed = time.perf_counter() - started
    return {"rows": int(affected), "elapsedSeconds": elapsed}


def list_macro_m2(
    *,
    limit: int = 120,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MacroM2DAO(settings.postgres)
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


__all__ = ["sync_macro_m2", "list_macro_m2", "_prepare_macro_m2_frame"]
