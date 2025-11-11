"""
Service layer for generating derived metrics from daily trade prices.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import date, timedelta
from typing import Callable, Optional, Sequence

import pandas as pd

from ..config.settings import load_settings
from ..dao import DailyTradeDAO, DailyTradeMetricsDAO

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y%m%d"
MIN_HISTORY_DAYS = 400

PCT_WINDOWS = (
    ("pct_change_1y", 365),
    ("pct_change_6m", 182),
    ("pct_change_3m", 90),
    ("pct_change_1m", 30),
    ("pct_change_2w", 14),
    ("pct_change_1w", 7),
)

MA_WINDOWS = (
    ("ma_20", 20),
    ("ma_10", 10),
    ("ma_5", 5),
)

VOLUME_SPIKE_WINDOW = 10


def _sanitize_history(frame: pd.DataFrame) -> pd.DataFrame:
    history = frame.copy()
    history["trade_date"] = pd.to_datetime(history["trade_date"], errors="coerce")
    history["close"] = pd.to_numeric(history["close"], errors="coerce")
    if "volume" in history.columns:
        history["volume"] = pd.to_numeric(history["volume"], errors="coerce")
    history = history.dropna(subset=["ts_code", "trade_date", "close"])
    history = history.sort_values(["ts_code", "trade_date"])
    return history


def _compute_metrics_for_group(group: pd.DataFrame) -> Optional[dict[str, object]]:
    if group.empty:
        return None

    latest = group.iloc[-1]
    trade_ts_raw = latest["trade_date"]
    close_value = latest["close"]

    try:
        trade_ts = pd.Timestamp(trade_ts_raw)
    except (TypeError, ValueError):
        return None

    if pd.isna(trade_ts) or pd.isna(close_value):
        return None

    trade_date: date = trade_ts.date()

    latest_close = float(close_value)
    if not math.isfinite(latest_close):
        return None

    result: dict[str, object] = {
        "ts_code": str(latest["ts_code"]),
        "trade_date": trade_date,
        "close": latest_close,
    }

    for field, window_days in PCT_WINDOWS:
        threshold = trade_ts - timedelta(days=window_days)
        historical = group[group["trade_date"] <= threshold]
        if historical.empty:
            result[field] = None
            continue
        base_close = historical.iloc[-1]["close"]
        if pd.isna(base_close):
            result[field] = None
            continue
        base_value = float(base_close)
        if not math.isfinite(base_value) or base_value == 0:
            result[field] = None
            continue
        result[field] = (latest_close - base_value) / base_value

    closes = group["close"]
    for field, window in MA_WINDOWS:
        tail = closes.tail(window)
        if len(tail) < window:
            result[field] = None
            continue
        result[field] = float(tail.mean())

    volumes = group.get("volume")
    if volumes is not None:
        latest_volume_value = pd.to_numeric(latest.get("volume"), errors="coerce")
        if latest_volume_value is not None and not pd.isna(latest_volume_value):
            previous = volumes.iloc[:-1].tail(VOLUME_SPIKE_WINDOW)
            previous = pd.to_numeric(previous, errors="coerce").dropna()
            if len(previous) >= VOLUME_SPIKE_WINDOW:
                average_volume = float(previous.mean())
                latest_volume = float(latest_volume_value)
                if math.isfinite(average_volume) and average_volume > 0 and math.isfinite(latest_volume):
                    result["volume_spike"] = latest_volume / average_volume
                else:
                    result["volume_spike"] = None
            else:
                result["volume_spike"] = None
        else:
            result["volume_spike"] = None
    else:
        result["volume_spike"] = None

    return result


def sync_daily_trade_metrics(
    *,
    history_window_days: int = MIN_HISTORY_DAYS,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """
    Generate derived trade metrics for each security based on daily prices.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    daily_trade_dao = DailyTradeDAO(settings.postgres)
    metrics_dao = DailyTradeMetricsDAO(settings.postgres)

    latest_trade_date = daily_trade_dao.latest_trade_date()
    if not latest_trade_date:
        message = "Daily trade table is empty; cannot compute metrics."
        logger.warning(message)
        if progress_callback:
            progress_callback(1.0, message, 0)
        return {
            "trade_date": None,
            "rows": 0,
            "elapsed_seconds": time.perf_counter() - started,
        }

    if progress_callback:
        progress_callback(0.05, "Loading price history for derived metrics", None)

    window_days = max(history_window_days, MIN_HISTORY_DAYS)
    start_date = latest_trade_date - timedelta(days=window_days)
    frame = daily_trade_dao.fetch_close_prices(start_date=start_date, end_date=latest_trade_date)

    if frame.empty:
        message = "No price history available for derived metrics window."
        logger.warning(message)
        if progress_callback:
            progress_callback(1.0, message, 0)
        return {
            "trade_date": latest_trade_date.strftime(DATE_FORMAT),
            "rows": 0,
            "elapsed_seconds": time.perf_counter() - started,
        }

    if progress_callback:
        progress_callback(0.2, "Preparing price history", len(frame.index))

    history = _sanitize_history(frame)
    if history.empty:
        message = "Price history contained no valid close values."
        logger.warning(message)
        if progress_callback:
            progress_callback(1.0, message, 0)
        return {
            "trade_date": latest_trade_date.strftime(DATE_FORMAT),
            "rows": 0,
            "elapsed_seconds": time.perf_counter() - started,
        }

    if progress_callback:
        progress_callback(0.5, "Calculating derived metrics", None)

    records: list[dict[str, object]] = []
    for ts_code, group in history.groupby("ts_code", sort=False):
        metrics = _compute_metrics_for_group(group)
        if metrics:
            records.append(metrics)

    if not records:
        message = "Derived metrics calculation produced no records."
        logger.warning(message)
        if progress_callback:
            progress_callback(1.0, message, 0)
        return {
            "trade_date": latest_trade_date.strftime(DATE_FORMAT),
            "rows": 0,
            "elapsed_seconds": time.perf_counter() - started,
        }

    metrics_frame = pd.DataFrame.from_records(records)
    if "volume_spike" not in metrics_frame.columns:
        metrics_frame["volume_spike"] = None

    if progress_callback:
        progress_callback(0.75, "Persisting derived metrics", len(metrics_frame.index))

    affected = metrics_dao.upsert(metrics_frame)
    elapsed = time.perf_counter() - started

    if progress_callback:
        progress_callback(1.0, "Daily trade metrics sync completed", affected)

    return {
        "trade_date": latest_trade_date.strftime(DATE_FORMAT),
        "rows": affected,
        "elapsed_seconds": elapsed,
    }


def recompute_trade_metrics_for_codes(
    codes: Sequence[str],
    *,
    include_intraday: bool = False,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    if not codes:
        return {"rows": 0}

    unique_codes = list(dict.fromkeys(code for code in codes if code))
    if not unique_codes:
        return {"rows": 0}

    settings = load_settings(settings_path)
    daily_trade_dao = DailyTradeDAO(settings.postgres)
    metrics_dao = DailyTradeMetricsDAO(settings.postgres)

    records: list[dict[str, object]] = []
    for ts_code in unique_codes:
        history = daily_trade_dao.fetch_price_history(
            ts_code,
            limit=MIN_HISTORY_DAYS,
            include_intraday=include_intraday,
        )
        if not history:
            continue
        frame = pd.DataFrame(history)
        frame["ts_code"] = ts_code
        sanitized = _sanitize_history(frame)
        if sanitized.empty:
            continue
        metrics = _compute_metrics_for_group(sanitized)
        if metrics:
            records.append(metrics)

    if not records:
        return {"rows": 0}

    metrics_frame = pd.DataFrame.from_records(records)
    if "volume_spike" not in metrics_frame.columns:
        metrics_frame["volume_spike"] = None

    affected = metrics_dao.upsert_partial(metrics_frame)
    return {"rows": affected}


__all__ = [
    "sync_daily_trade_metrics",
    "recompute_trade_metrics_for_codes",
]
