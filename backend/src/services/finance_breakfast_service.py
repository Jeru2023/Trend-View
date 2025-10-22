"""
Service module to synchronise AkShare finance breakfast summaries.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

import pandas as pd

from ..api_clients import fetch_finance_breakfast
from ..config.settings import load_settings
from ..dao import FinanceBreakfastDAO

logger = logging.getLogger(__name__)


def _normalize_text(value: object) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return text.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def sync_finance_breakfast(
    *,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """
    Synchronise finance breakfast data into PostgreSQL.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = FinanceBreakfastDAO(settings.postgres)

    if progress_callback:
        progress_callback(0.1, "Fetching finance breakfast feed", 0)

    dataframe = fetch_finance_breakfast()
    if dataframe.empty:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No finance breakfast entries retrieved", 0)
        return {
            "rows": 0,
            "elapsed_seconds": elapsed,
        }

    dataframe["title"] = dataframe["title"].apply(_normalize_text)
    dataframe["summary"] = dataframe["summary"].apply(_normalize_text)
    dataframe["url"] = dataframe["url"].apply(lambda val: str(val).strip() if val else None)
    dataframe["published_at"] = pd.to_datetime(dataframe["published_at"], errors="coerce")
    dataframe = dataframe.dropna(subset=["title", "published_at"]).drop_duplicates(subset=["title", "published_at"])

    if dataframe.empty:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No valid finance breakfast entries after filtering", 0)
        return {
            "rows": 0,
            "elapsed_seconds": elapsed,
        }

    if progress_callback:
        progress_callback(0.6, f"Upserting {len(dataframe.index)} finance breakfast entries", len(dataframe.index))

    affected = dao.upsert(dataframe)
    elapsed = time.perf_counter() - started

    if progress_callback:
        progress_callback(1.0, "Finance breakfast sync completed", affected)

    return {
        "rows": affected,
        "elapsed_seconds": elapsed,
    }

def list_finance_breakfast(
    *,
    limit: int = 50,
    settings_path: Optional[str] = None,
) -> list[dict[str, object]]:
    settings = load_settings(settings_path)
    dao = FinanceBreakfastDAO(settings.postgres)
    limit = max(1, int(limit))
    return dao.list_recent(limit=limit)


__all__ = ["list_finance_breakfast", "sync_finance_breakfast"]
