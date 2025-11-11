"""Build minute-level intraday volume profiles from AkShare tick data."""

from __future__ import annotations

import logging
from datetime import date, datetime, time as dtime
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import pandas as pd

import akshare as ak
from zoneinfo import ZoneInfo

from ..config.settings import load_settings
from ..dao import (
    IntradayVolumeProfileAverageDAO,
    IntradayVolumeProfileDailyDAO,
    StockBasicDAO,
)

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

MORNING_START = dtime(9, 30)
MORNING_END = dtime(11, 30)
AFTERNOON_START = dtime(13, 0)
AFTERNOON_END = dtime(15, 0)

TOTAL_MINUTES = 240  # 120 AM + 120 PM


def _normalize_ts_code(value: str) -> str | None:
    if not value:
        return None
    text = value.strip().upper()
    if not text:
        return None
    if "." in text:
        symbol, exchange = text.split(".", 1)
    else:
        symbol, exchange = text, ""
    exchange = exchange.lower()
    if exchange in {"sh", "sz", "bj"}:
        prefix = exchange
    else:
        prefix = "sh" if symbol.startswith(("6", "9", "5")) else "sz"
    return f"{prefix}{symbol.zfill(6)}"


def _minute_index_from_time(value: dtime) -> Optional[int]:
    if MORNING_START <= value < MORNING_END:
        return (value.hour - 9) * 60 + (value.minute - 30)
    if AFTERNOON_START <= value <= AFTERNOON_END:
        if value == AFTERNOON_END:
            return TOTAL_MINUTES - 1
        return 120 + (value.hour - 13) * 60 + value.minute
    return None


def _fetch_tick_frame(symbol: str) -> pd.DataFrame:
    fetchers: List[Callable[..., pd.DataFrame]] = []
    if hasattr(ak, "stock_zh_a_tick_tx_js"):
        fetchers.append(getattr(ak, "stock_zh_a_tick_tx_js"))
    if hasattr(ak, "stock_zh_a_tick_163"):
        fetchers.append(getattr(ak, "stock_zh_a_tick_163"))

    errors: List[str] = []
    for fetcher in fetchers:
        try:
            frame = fetcher(symbol=symbol)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch tick data for %s via %s: %s", symbol, fetcher.__name__, exc)
            errors.append(str(exc))
            continue
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            return frame
    if errors:
        raise RuntimeError(f"Unable to fetch tick data for {symbol}: {'; '.join(errors)}")
    return pd.DataFrame()


def _build_minute_entries(frame: pd.DataFrame) -> List[dict[str, float]]:
    minute_volumes = [0.0] * TOTAL_MINUTES
    if "成交时间" not in frame.columns or "成交量" not in frame.columns:
        return []

    for raw_time, raw_volume in zip(frame["成交时间"], frame["成交量"]):
        if not raw_time:
            continue
        try:
            time_obj = datetime.strptime(str(raw_time).strip(), "%H:%M:%S").time()
        except ValueError:
            continue
        minute_index = _minute_index_from_time(time_obj)
        if minute_index is None:
            continue
        try:
            volume_hands = float(raw_volume)
        except (TypeError, ValueError):
            continue
        minute_volumes[minute_index] += volume_hands * 100  # hands -> shares

    total_volume = sum(minute_volumes)
    if total_volume <= 0:
        return []

    entries: List[dict[str, float]] = []
    cumulative = 0.0
    for idx, volume in enumerate(minute_volumes):
        ratio = volume / total_volume if total_volume else 0.0
        cumulative = min(1.0, cumulative + ratio)
        entries.append(
            {
                "minute_index": idx,
                "minute_volume": volume,
                "volume_ratio": ratio,
                "cumulative_ratio": cumulative,
            }
        )
    return entries


def sync_intraday_volume_profiles(
    symbols: Optional[Sequence[str]] = None,
    *,
    trade_date: Optional[date] = None,
    freeze_after_days: int = 20,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> Dict[str, int]:
    """Fetch tick data for all (or specified) stocks and build per-minute volume ratios."""
    settings = load_settings(settings_path)
    stock_dao = StockBasicDAO(settings.postgres)
    daily_dao = IntradayVolumeProfileDailyDAO(settings.postgres)
    avg_dao = IntradayVolumeProfileAverageDAO(settings.postgres)

    resolved_codes = list(symbols) if symbols else stock_dao.list_codes()
    resolved_codes = [code for code in resolved_codes if code]
    total = len(resolved_codes)
    if total == 0:
        return {"total": 0, "success": 0, "skipped": 0, "failed": 0, "frozen": 0}

    trade_day = trade_date or datetime.now(LOCAL_TZ).date()
    frozen_codes = set(avg_dao.list_frozen_codes())

    stats = {"total": total, "success": 0, "skipped": 0, "failed": 0, "frozen": len(frozen_codes)}

    for index, ts_code in enumerate(resolved_codes, start=1):
        if progress_callback:
            progress_callback((index - 1) / total, f"Processing {ts_code}", None)

        ak_symbol = _normalize_ts_code(ts_code)
        if not ak_symbol:
            stats["skipped"] += 1
            continue

        try:
            frame = _fetch_tick_frame(ak_symbol)
            entries = _build_minute_entries(frame)
        except Exception as exc:  # noqa: BLE001
            logger.warning("intraday volume profile failed for %s: %s", ts_code, exc)
            stats["failed"] += 1
            continue

        if not entries:
            stats["skipped"] += 1
            continue

        daily_dao.replace_profile(ts_code, trade_day, entries)

        if ts_code not in frozen_codes:
            avg_dao.upsert_running_average(ts_code, trade_day, entries)
            samples = avg_dao.get_sample_count(ts_code)
            if samples >= freeze_after_days:
                avg_dao.mark_frozen(ts_code)
                frozen_codes.add(ts_code)
                stats["frozen"] = len(frozen_codes)

        stats["success"] += 1

    if progress_callback:
        progress_callback(1.0, "Intraday volume profile sync completed", stats["success"])
    return stats


def load_average_profile_map(
    stock_codes: Sequence[str],
    *,
    settings_path: Optional[str] = None,
) -> Dict[str, Dict[int, float]]:
    settings = load_settings(settings_path)
    avg_dao = IntradayVolumeProfileAverageDAO(settings.postgres)
    return avg_dao.fetch_profiles(stock_codes)


def estimate_full_day_volume(
    ts_code: str,
    trade_time: str,
    current_volume: float,
    *,
    profile_map: Optional[Dict[str, Dict[int, float]]] = None,
    settings_path: Optional[str] = None,
) -> tuple[float, float]:
    if not ts_code or current_volume is None:
        return 0.0, 0.0
    try:
        time_obj = datetime.strptime(trade_time.strip(), "%H:%M:%S").time()
    except (ValueError, AttributeError):
        return float(current_volume), 1.0
    minute_index = _minute_index_from_time(time_obj)
    if minute_index is None:
        return float(current_volume), 1.0

    profile = None
    if profile_map and ts_code in profile_map:
        profile = profile_map[ts_code]
    elif profile_map is None:
        profile = load_average_profile_map([ts_code], settings_path=settings_path).get(ts_code)

    ratio = None
    if profile and minute_index in profile:
        ratio = profile[minute_index]

    if not ratio or ratio <= 0:
        elapsed_minutes = minute_index + 1
        ratio = elapsed_minutes / TOTAL_MINUTES

    ratio = min(max(ratio, 1e-4), 1.0)
    estimated = float(current_volume) / ratio
    return estimated, ratio


__all__ = [
    "sync_intraday_volume_profiles",
    "load_average_profile_map",
    "estimate_full_day_volume",
]
