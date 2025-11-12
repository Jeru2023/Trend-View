"""
Service helpers for synchronising Loan Prime Rate (LPR) data.
"""

from __future__ import annotations

import logging
import math
import time
from typing import Optional

import pandas as pd

from ..api_clients import LPR_COLUMN_MAP, fetch_lpr_rates
from ..config.settings import load_settings
from ..dao import MacroLprDAO

logger = logging.getLogger(__name__)


def _date_range_strings(years: int = 5) -> tuple[str, str]:
    today = pd.Timestamp.today().normalize()
    end_date = today.strftime("%Y%m%d")
    start_date = (today - pd.DateOffset(years=years)).strftime("%Y%m%d")
    return start_date, end_date


def _prepare_lpr_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        columns = ["period_date", "period_label", "rate_1y", "rate_5y"]
        return pd.DataFrame(columns=columns)

    frame = dataframe.copy()
    for column in LPR_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    with pd.option_context("mode.chained_assignment", None):
        frame["period_label"] = frame["period_label"].astype(str).str.strip()
        frame["period_date"] = (
            pd.to_datetime(frame["period_label"], format="%Y%m%d", errors="coerce").dt.date
        )

    for column in ("rate_1y", "rate_5y"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    prepared = (
        frame.loc[:, ["period_date", "period_label", "rate_1y", "rate_5y"]]
        .dropna(subset=["period_date"])
        .sort_values("period_date")
        .drop_duplicates(subset=["period_date"], keep="last")
        .reset_index(drop=True)
    )
    return prepared


def sync_macro_lpr(*, settings_path: Optional[str] = None) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = MacroLprDAO(settings.postgres)
    start_date, end_date = _date_range_strings()

    raw = fetch_lpr_rates(token=settings.tushare.token, start_date=start_date, end_date=end_date)
    prepared = _prepare_lpr_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("LPR sync skipped: no data returned.")
        return {"rows": 0, "elapsedSeconds": elapsed}

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    return {"rows": int(affected), "elapsedSeconds": elapsed}


def list_macro_lpr(
    *,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MacroLprDAO(settings.postgres)
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


__all__ = ["sync_macro_lpr", "list_macro_lpr", "_prepare_lpr_frame"]
