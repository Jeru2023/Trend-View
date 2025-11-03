"""
Service helpers for synchronising and querying the A-share trading calendar.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Dict, Optional

import pandas as pd

from ..api_clients import TRADE_CALENDAR_FIELDS, fetch_trade_calendar
from ..config.settings import load_settings
from ..dao import TradeCalendarDAO

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 180
DEFAULT_LOOKAHEAD_DAYS = 365
CALENDAR_CACHE_TTL_SECONDS = 3600

_calendar_cache: Dict[date, bool] = {}
_cache_expiry: Optional[datetime] = None


def _normalize_date_string(value: date | datetime | str) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if len(stripped) == 8 and stripped.isdigit():
            return stripped
        parsed = pd.to_datetime(stripped, errors="coerce")
        if pd.isna(parsed):
            raise ValueError(f"Invalid date value: {value}")
        return parsed.strftime("%Y%m%d")
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid date value: {value}")
    return parsed.strftime("%Y%m%d")


def _prepare_calendar_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=TRADE_CALENDAR_FIELDS)

    frame = dataframe.copy()
    for column in TRADE_CALENDAR_FIELDS:
        if column not in frame.columns:
            frame[column] = None

    def _to_bool(value: object) -> Optional[bool]:
        if value in (None, ""):
            return None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        try:
            return bool(int(value))
        except (TypeError, ValueError):
            return bool(value)

    with pd.option_context("mode.chained_assignment", None):
        frame["cal_date"] = pd.to_datetime(frame["cal_date"], errors="coerce").dt.date
        frame["is_open"] = frame["is_open"].apply(_to_bool)
        frame["exchange"] = frame["exchange"].fillna("SSE")

    prepared = (
        frame.loc[:, ["cal_date", "exchange", "is_open"]]
        .dropna(subset=["cal_date", "is_open"])
        .drop_duplicates(subset=["cal_date"], keep="last")
        .sort_values("cal_date")
        .reset_index(drop=True)
    )
    return prepared


def sync_trade_calendar(
    *,
    start_date: Optional[date | datetime | str] = None,
    end_date: Optional[date | datetime | str] = None,
    exchange: str = "SSE",
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    """Synchronise the trading calendar for the provided date range."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = TradeCalendarDAO(settings.postgres)

    today = date.today()
    start_value = start_date or (today - timedelta(days=DEFAULT_LOOKBACK_DAYS))
    end_value = end_date or (today + timedelta(days=DEFAULT_LOOKAHEAD_DAYS))

    start_string = _normalize_date_string(start_value)
    end_string = _normalize_date_string(end_value)

    frame = fetch_trade_calendar(
        settings.tushare.token,
        start_date=start_string,
        end_date=end_string,
        exchange=exchange,
    )
    prepared = _prepare_calendar_frame(frame)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning(
            "Trade calendar sync skipped: no data returned for range %s-%s.",
            start_string,
            end_string,
        )
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "startDate": start_string,
            "endDate": end_string,
        }

    affected = dao.upsert(prepared)
    # Invalidate cache so subsequent queries reload latest values.
    global _calendar_cache, _cache_expiry
    _calendar_cache = {}
    _cache_expiry = None

    elapsed = time.perf_counter() - started
    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "startDate": start_string,
        "endDate": end_string,
    }


def _refresh_cache_if_needed(target_date: date, *, settings_path: Optional[str] = None) -> None:
    global _calendar_cache, _cache_expiry
    now = datetime.utcnow()
    if _cache_expiry and now < _cache_expiry and target_date in _calendar_cache:
        return

    settings = load_settings(settings_path)
    dao = TradeCalendarDAO(settings.postgres)
    window_start = target_date - timedelta(days=30)
    window_end = target_date + timedelta(days=60)

    entries = list(dao.list_between(window_start, window_end))
    if not entries:
        sync_trade_calendar(start_date=window_start, end_date=window_end, settings_path=settings_path)
        entries = list(dao.list_between(window_start, window_end))

    for entry in entries:
        cal_date_value = entry.get("cal_date")
        if isinstance(cal_date_value, date):
            _calendar_cache[cal_date_value] = bool(entry.get("is_open"))

    _cache_expiry = now + timedelta(seconds=CALENDAR_CACHE_TTL_SECONDS)


def is_trading_day(target_date: date | datetime, *, settings_path: Optional[str] = None) -> Optional[bool]:
    """Return True if the provided date is an A-share trading day."""
    parsed_date = pd.to_datetime(target_date, errors="coerce")
    if pd.isna(parsed_date):
        raise ValueError(f"Invalid date value: {target_date}")
    effective_date = parsed_date.date()

    _refresh_cache_if_needed(effective_date, settings_path=settings_path)
    cached = _calendar_cache.get(effective_date)
    if cached is not None:
        return cached

    settings = load_settings(settings_path)
    dao = TradeCalendarDAO(settings.postgres)
    result = dao.is_trading_day(effective_date)
    if result is not None:
        _calendar_cache[effective_date] = bool(result)
        return bool(result)

    sync_trade_calendar(
        start_date=effective_date - timedelta(days=30),
        end_date=effective_date + timedelta(days=60),
        settings_path=settings_path,
    )
    result = dao.is_trading_day(effective_date)
    if result is not None:
        _calendar_cache[effective_date] = bool(result)
        return bool(result)
    return None


__all__ = ["sync_trade_calendar", "is_trading_day", "_prepare_calendar_frame"]
