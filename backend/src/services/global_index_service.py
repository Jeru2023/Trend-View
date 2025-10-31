"""
Service layer for synchronising global index spot data from AkShare.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd

from ..api_clients import GLOBAL_INDEX_COLUMN_MAP, fetch_global_indices
from ..config.settings import load_settings
from ..dao import GlobalIndexDAO

logger = logging.getLogger(__name__)


def _prepare_global_index_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.copy()
    numeric_columns = [
        "latest_price",
        "change_amount",
        "change_percent",
        "open_price",
        "high_price",
        "low_price",
        "prev_close",
        "amplitude",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if "seq" in frame.columns:
        frame["seq"] = pd.to_numeric(frame["seq"], errors="coerce").astype("Int64")

    if "last_quote_time" in frame.columns:
        frame["last_quote_time"] = pd.to_datetime(frame["last_quote_time"], errors="coerce")

    frame["code"] = frame["code"].astype(str).str.strip()
    frame["name"] = frame.get("name", "").astype(str).str.strip()

    prepared = frame.loc[:, list(GLOBAL_INDEX_COLUMN_MAP.values())].copy()
    prepared = prepared.dropna(subset=["code"])
    return prepared.reset_index(drop=True)


def sync_global_indices(*, settings_path: Optional[str] = None) -> dict[str, object]:
    """
    Fetch the global index snapshot from AkShare and upsert into PostgreSQL.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = GlobalIndexDAO(settings.postgres)

    raw_frame = fetch_global_indices()
    prepared = _prepare_global_index_frame(raw_frame)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Global index sync skipped: no data returned.")
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "codes": [],
            "codeCount": 0,
        }

    dao.clear_table()
    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started

    codes = prepared["code"].dropna().tolist()
    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "codes": codes[:10],
        "codeCount": len(codes),
    }


def list_global_indices(
    *,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = GlobalIndexDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset)
    stats = dao.stats()
    return {
        "total": result.get("total", 0),
        "items": result.get("items", []),
        "lastSyncedAt": stats.get("updated_at"),
    }


__all__ = ["sync_global_indices", "list_global_indices", "_prepare_global_index_frame"]
