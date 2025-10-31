"""
Service layer for synchronising RMB central parity rates from SAFE.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Optional

import pandas as pd

from ..api_clients import RMB_MIDPOINT_COLUMN_MAP, fetch_rmb_midpoint_rates
from ..config.settings import load_settings
from ..dao import RmbMidpointDAO

logger = logging.getLogger(__name__)


def _prepare_rmb_midpoint_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=list(RMB_MIDPOINT_COLUMN_MAP.values()))

    frame = dataframe.copy()

    numeric_columns = [column for column in RMB_MIDPOINT_COLUMN_MAP.values() if column != "trade_date"]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date

    for column in RMB_MIDPOINT_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    prepared = frame.loc[:, list(RMB_MIDPOINT_COLUMN_MAP.values())].copy()
    prepared = prepared.dropna(subset=["trade_date"])
    prepared = prepared.sort_values("trade_date").reset_index(drop=True)
    return prepared


def sync_rmb_midpoint_rates(*, settings_path: Optional[str] = None) -> dict[str, object]:
    """Fetch the SAFE midpoint dataset and persist into PostgreSQL."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = RmbMidpointDAO(settings.postgres)

    raw = fetch_rmb_midpoint_rates()
    prepared = _prepare_rmb_midpoint_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("RMB midpoint sync skipped: no data returned.")
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
        }

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "dateCount": prepared["trade_date"].nunique(),
    }


def list_rmb_midpoint_rates(
    *,
    limit: int = 200,
    offset: int = 0,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = RmbMidpointDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset, start_date=start_date, end_date=end_date)
    stats = dao.stats()
    return {
        "total": int(result.get("total", 0) or 0),
        "items": result.get("items", []),
        "lastSyncedAt": stats.get("updated_at"),
    }


__all__ = [
    "sync_rmb_midpoint_rates",
    "list_rmb_midpoint_rates",
    "_prepare_rmb_midpoint_frame",
]
