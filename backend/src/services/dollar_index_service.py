"""
Service layer for synchronising Dollar Index historical data from AkShare.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Optional

import pandas as pd

from ..api_clients import DOLLAR_INDEX_COLUMN_MAP, fetch_dollar_index_history
from ..config.settings import load_settings
from ..dao import DollarIndexDAO

logger = logging.getLogger(__name__)


def _prepare_dollar_index_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=list(DOLLAR_INDEX_COLUMN_MAP.values()))

    frame = dataframe.copy()

    numeric_columns = [
        "open_price",
        "close_price",
        "high_price",
        "low_price",
        "amplitude",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date

    frame["code"] = frame.get("code", "").astype(str).str.strip()
    frame["name"] = frame.get("name", "").astype(str).str.strip()

    prepared = frame.loc[:, list(DOLLAR_INDEX_COLUMN_MAP.values())].copy()
    prepared = prepared.dropna(subset=["trade_date", "code"])
    prepared = prepared.sort_values("trade_date").reset_index(drop=True)
    return prepared


def sync_dollar_index(
    *,
    symbol: str = "美元指数",
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    """
    Fetch the Dollar Index historical dataset and upsert into PostgreSQL.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = DollarIndexDAO(settings.postgres)

    raw = fetch_dollar_index_history(symbol=symbol)
    prepared = _prepare_dollar_index_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Dollar index sync skipped: no data returned for %s", symbol)
        return {
            "rows": 0,
            "symbol": symbol,
            "codes": [],
            "codeCount": 0,
            "elapsedSeconds": elapsed,
        }

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    codes = sorted(set(prepared["code"].dropna().astype(str)))
    return {
        "rows": int(affected),
        "symbol": symbol,
        "codes": codes,
        "codeCount": len(codes),
        "elapsedSeconds": elapsed,
    }


def list_dollar_index(
    *,
    limit: int = 200,
    offset: int = 0,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = DollarIndexDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset, start_date=start_date, end_date=end_date)
    stats = dao.stats()
    return {
        "total": int(result.get("total", 0) or 0),
        "items": result.get("items", []),
        "lastSyncedAt": stats.get("updated_at"),
    }


__all__ = ["sync_dollar_index", "list_dollar_index", "_prepare_dollar_index_frame"]
