"""
Service layer for synchronising foreign commodity futures realtime quotes.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable, Optional, Sequence

import pandas as pd

from ..api_clients import (
    FUTURES_REALTIME_COLUMN_MAP,
    FUTURES_TARGET_CODES,
    fetch_futures_realtime,
)
from ..config.settings import load_settings
from ..dao import FuturesRealtimeDAO

logger = logging.getLogger(__name__)


def _prepare_futures_realtime_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=list(FUTURES_REALTIME_COLUMN_MAP.values()))

    frame = dataframe.copy()
    if "change_percent" in frame.columns:
        frame["change_percent"] = (
            frame["change_percent"].astype(str).str.replace("%", "", regex=False)
        )

    numeric_columns = [
        "last_price",
        "price_cny",
        "change_amount",
        "change_percent",
        "open_price",
        "high_price",
        "low_price",
        "prev_settlement",
        "open_interest",
        "bid_price",
        "ask_price",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date

    for column in FUTURES_REALTIME_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    prepared = frame.loc[:, list(FUTURES_REALTIME_COLUMN_MAP.values())].copy()
    prepared["name"] = prepared["name"].astype(str)
    mapped_codes = prepared["name"].map(FUTURES_TARGET_CODES)
    if "code" in prepared.columns:
        prepared["code"] = mapped_codes.where(mapped_codes.notna(), prepared["code"])
    else:
        prepared["code"] = mapped_codes
    prepared = prepared.dropna(subset=["name"])
    return prepared.reset_index(drop=True)


def sync_futures_realtime(
    *,
    symbols: Optional[Sequence[str]] = None,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = FuturesRealtimeDAO(settings.postgres)

    target_symbols: Iterable[str]
    if symbols:
        target_symbols = symbols
    else:
        target_symbols = FUTURES_TARGET_CODES.values()

    raw = fetch_futures_realtime(symbols=list(target_symbols))
    prepared = _prepare_futures_realtime_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Futures realtime sync skipped: no data returned.")
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
        }

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
    }


def list_futures_realtime(
    *,
    limit: int = 50,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = FuturesRealtimeDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset)
    stats = dao.stats()
    return {
        "total": int(result.get("total", 0) or 0),
        "items": result.get("items", []),
        "lastSyncedAt": stats.get("updated_at"),
    }


__all__ = [
    "sync_futures_realtime",
    "list_futures_realtime",
    "_prepare_futures_realtime_frame",
]
