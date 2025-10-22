"""
Service layer for synchronising Tushare daily indicator (daily_basic) data.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Callable, Optional

import pandas as pd
import tushare as ts

from ..api_clients import get_daily_indicator
from ..config.settings import AppSettings, load_settings
from ..dao import DailyIndicatorDAO, DailyTradeDAO

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y%m%d"


def _resolve_token(token: Optional[str], settings: AppSettings) -> str:
    resolved = token or settings.tushare.token
    if not resolved:
        raise RuntimeError(
            "Tushare token is required. Update the configuration file or provide one explicitly."
        )
    return resolved


def _determine_trade_date(trade_date: Optional[str], daily_trade_dao: DailyTradeDAO) -> str:
    if trade_date:
        return trade_date

    latest_trade_date = daily_trade_dao.latest_trade_date()
    if latest_trade_date:
        return latest_trade_date.strftime(DATE_FORMAT)

    return datetime.utcnow().strftime(DATE_FORMAT)


def sync_daily_indicator(
    token: Optional[str] = None,
    *,
    trade_date: Optional[str] = None,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, float | int | str]:
    """
    Fetch daily indicator information and persist all fields into PostgreSQL.

    Returns summary statistics including rows affected, trade date, and elapsed seconds.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    resolved_token = _resolve_token(token, settings)
    daily_trade_dao = DailyTradeDAO(settings.postgres)
    indicator_dao = DailyIndicatorDAO(settings.postgres)
    resolved_trade_date = _determine_trade_date(trade_date, daily_trade_dao)

    pro_client = ts.pro_api(resolved_token)

    if progress_callback:
        progress_callback(0.1, f"Fetching daily indicators for {resolved_trade_date}", None)

    dataframe = get_daily_indicator(pro_client, resolved_trade_date)
    if dataframe.empty:
        logger.warning("No daily indicator data returned for trade_date=%s", resolved_trade_date)
        if progress_callback:
            progress_callback(1.0, "No daily indicator records to upsert", 0)
        return {
            "trade_date": resolved_trade_date,
            "rows": 0,
            "elapsed_seconds": time.perf_counter() - started,
        }

    dataframe = dataframe.drop_duplicates(subset=["ts_code"])
    dataframe["trade_date"] = pd.to_datetime(dataframe["trade_date"], errors="coerce").dt.date

    numeric_columns = [
        column
        for column in dataframe.columns
        if column not in ("ts_code", "trade_date")
    ]
    for column in numeric_columns:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")

    if progress_callback:
        progress_callback(0.6, "Upserting daily indicator records", len(dataframe.index))

    rows = indicator_dao.upsert(dataframe)

    elapsed = time.perf_counter() - started

    if progress_callback:
        progress_callback(1.0, "Daily indicator sync completed", rows)

    return {
        "trade_date": resolved_trade_date,
        "rows": rows,
        "elapsed_seconds": elapsed,
    }


__all__ = [
    "sync_daily_indicator",
]
