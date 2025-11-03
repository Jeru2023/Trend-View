"\"\"\"Service layer for margin (financing & securities lending) account statistics.\"\"\""

from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime
from typing import Callable, Optional

import pandas as pd

from ..api_clients import MARGIN_ACCOUNT_COLUMN_MAP, fetch_margin_account_info
from ..config.settings import load_settings
from ..dao import MarginAccountDAO

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "financing_balance",
    "securities_lending_balance",
    "financing_purchase_amount",
    "securities_lending_sell_amount",
    "securities_company_count",
    "business_department_count",
    "individual_investor_count",
    "institutional_investor_count",
    "participating_investor_count",
    "liability_investor_count",
    "collateral_value",
    "average_collateral_ratio",
)


def _to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _prepare_margin_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()

    for column in MARGIN_ACCOUNT_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date

    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_to_float)

    prepared = frame.loc[frame["trade_date"].notnull(), ["trade_date", *NUMERIC_COLUMNS]].copy()
    for column in NUMERIC_COLUMNS:
        series = prepared[column]
        prepared[column] = series.astype(object).where(pd.notnull(series), None)

    prepared = prepared.drop_duplicates(subset=["trade_date"], keep="last")
    return prepared.sort_values("trade_date").reset_index(drop=True)


def sync_margin_account_info(
    *,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """Fetch and persist margin account statistics."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = MarginAccountDAO(settings.postgres)

    if progress_callback:
        progress_callback(0.05, "Fetching margin account statistics", None)

    frame = fetch_margin_account_info()
    if frame.empty:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No margin account data returned", 0)
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "tradeDates": [],
            "tradeDateCount": 0,
        }

    prepared = _prepare_margin_frame(frame)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Margin account frame is empty after preparation.")
        if progress_callback:
            progress_callback(1.0, "No valid margin account rows after filtering", 0)
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "tradeDates": [],
            "tradeDateCount": 0,
        }

    if progress_callback:
        progress_callback(0.4, f"Upserting {len(prepared)} margin account rows", len(prepared))

    affected = dao.upsert(prepared)

    elapsed = time.perf_counter() - started
    trade_dates = sorted(
        {
            value.isoformat()
            for value in prepared["trade_date"].dropna().unique()
            if isinstance(value, datetime) or hasattr(value, "isoformat")
        }
    )

    if progress_callback:
        progress_callback(1.0, f"Upserted {affected} margin account rows", int(affected))

    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "tradeDates": trade_dates,
        "tradeDateCount": len(trade_dates),
    }


def list_margin_account_info(
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MarginAccountDAO(settings.postgres)

    parsed_start: Optional[date] = None
    parsed_end: Optional[date] = None

    if start_date:
        try:
            parsed_start = datetime.fromisoformat(str(start_date)).date()
        except ValueError:
            parsed_start = None

    if end_date:
        try:
            parsed_end = datetime.fromisoformat(str(end_date)).date()
        except ValueError:
            parsed_end = None

    try:
        parsed_limit = int(limit)
    except (TypeError, ValueError):
        parsed_limit = 200

    try:
        parsed_offset = int(offset)
    except (TypeError, ValueError):
        parsed_offset = 0

    safe_limit = max(1, min(parsed_limit, 2000))
    safe_offset = max(0, parsed_offset)

    return dao.list_entries(
        start_date=parsed_start,
        end_date=parsed_end,
        limit=safe_limit,
        offset=safe_offset,
    )


__all__ = [
    "sync_margin_account_info",
    "list_margin_account_info",
    "_prepare_margin_frame",
]
