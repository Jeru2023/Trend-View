"""Service for syncing HSGT fund flow history data via Tushare."""

from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime, timedelta
from typing import Callable, Optional

import pandas as pd
import tushare as ts

from ..api_clients import (
    HSGT_MONEYFLOW_FIELDS,
    fetch_moneyflow_hsgt,
)
from ..config.settings import load_settings
from ..dao import HSGTFundFlowDAO

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "net_buy_amount",
    "fund_inflow",
    "net_buy_amount_cumulative",
)

STORED_COLUMNS: tuple[str, ...] = (
    "trade_date",
    "net_buy_amount",
    "fund_inflow",
    "net_buy_amount_cumulative",
)

DEFAULT_SYMBOL = "北向资金"
DEFAULT_START_DATE = date(2018, 1, 1)

SYMBOL_COLUMN_MAP: dict[str, str] = {
    "北向资金": "north_money",
    "南向资金": "south_money",
    "沪股通": "hgt",
    "深股通": "sgt",
    "港股通(沪)": "ggt_ss",
    "港股通(深)": "ggt_sz",
}

SUPPORTED_SYMBOLS: tuple[str, ...] = tuple(SYMBOL_COLUMN_MAP.keys())


def _to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"nan", "none", "null", "--"}:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _to_percent(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("%"):
        text = text[:-1]
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _prepare_hsgt_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()

    for column in STORED_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date

    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_to_float)

    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            series = frame[column]
            frame[column] = series.astype(object).where(pd.notnull(series), None)

    frame = frame.loc[:, list(STORED_COLUMNS)]

    return frame


def _normalize_symbol(value: Optional[str]) -> str:
    symbol = (value or DEFAULT_SYMBOL).strip() or DEFAULT_SYMBOL
    if symbol not in SUPPORTED_SYMBOLS:
        raise ValueError(f"Unsupported HSGT symbol: {symbol}")
    return symbol


def _parse_date_input(value: Optional[object]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _resolve_token(explicit_token: Optional[str], settings) -> str:
    token = (explicit_token or getattr(settings.tushare, "token", "") or "").strip()
    if not token:
        raise RuntimeError("Tushare token is required for syncing HSGT fund flow data.")
    return token


def _build_standardized_frame(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=list(STORED_COLUMNS))

    column = SYMBOL_COLUMN_MAP[symbol]
    frame = raw.copy()
    for field in HSGT_MONEYFLOW_FIELDS:
        if field not in frame.columns:
            frame[field] = None

    frame = frame.loc[:, ["trade_date", column]].rename(columns={column: "net_buy_amount"})
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
    frame["fund_inflow"] = frame["net_buy_amount"]

    for column_name in STORED_COLUMNS:
        if column_name not in frame.columns:
            frame[column_name] = None

    return frame.loc[:, list(STORED_COLUMNS)]




def _apply_cumulative_values(frame: pd.DataFrame, starting_value: Optional[float]) -> None:
    cumulative = _to_float(starting_value)
    if cumulative is None:
        cumulative = 0.0

    values: list[Optional[float]] = []
    for record in frame["net_buy_amount"]:
        net_value = _to_float(record)
        if net_value is None:
            values.append(None)
            continue
        cumulative += net_value
        values.append(cumulative)

    frame["net_buy_amount_cumulative"] = values


def _iter_date_windows(
    start: date,
    end: date,
    window_days: int = 180,
) -> list[tuple[date, date]]:
    """
    Generate inclusive date windows to avoid Tushare's 300-row limit per request.
    """
    if window_days <= 0:
        window_days = 30

    windows: list[tuple[date, date]] = []
    current = start
    step = timedelta(days=window_days)
    one_day = timedelta(days=1)

    while current <= end:
        window_end = min(end, current + step - one_day)
        windows.append((current, window_end))
        current = window_end + one_day

    return windows


def sync_hsgt_fund_flow(
    *,
    symbol: str = DEFAULT_SYMBOL,
    token: Optional[str] = None,
    start_date: Optional[object] = None,
    end_date: Optional[object] = None,
    full_refresh: bool = False,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """Fetch and persist the HSGT fund flow history for the configured symbol."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    resolved_symbol = _normalize_symbol(symbol)
    resolved_token = _resolve_token(token, settings)
    dao = HSGTFundFlowDAO(settings.postgres)

    requested_start = _parse_date_input(start_date)
    requested_end = _parse_date_input(end_date) or date.today()
    performed_full_refresh = False
    status_message: Optional[str] = None

    with dao.connect() as conn:
        dao.ensure_table(conn)
        latest_snapshot = dao.get_latest_snapshot(symbol=resolved_symbol, conn=conn)
        legacy_detected = dao.has_legacy_metrics(symbol=resolved_symbol, conn=conn)
        should_full_refresh = bool(full_refresh) or legacy_detected
        if legacy_detected and not full_refresh:
            logger.info(
                "Detected legacy HSGT fund flow rows for %s; forcing one-time rebuild",
                resolved_symbol,
            )
        if should_full_refresh:
            removed = dao.clear_table(symbol=resolved_symbol, conn=conn)
            if removed:
                logger.info("Cleared %s existing %s rows prior to re-sync", removed, resolved_symbol)
            latest_snapshot = None
            performed_full_refresh = True
        conn.commit()

    snapshot_raw = latest_snapshot.get("trade_date") if latest_snapshot else None
    if isinstance(snapshot_raw, datetime):
        snapshot_date = snapshot_raw.date()
    else:
        snapshot_date = snapshot_raw

    if performed_full_refresh:
        effective_start = requested_start or DEFAULT_START_DATE
    elif requested_start:
        effective_start = requested_start
    elif snapshot_date:
        effective_start = snapshot_date + timedelta(days=1)
    else:
        effective_start = DEFAULT_START_DATE

    effective_end = requested_end

    if effective_start > effective_end:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No new HSGT fund flow dates to fetch", 0)
        status_message = "HSGT fund flow already up-to-date"
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "tradeDates": [],
            "tradeDateCount": 0,
            "symbol": resolved_symbol,
            "statusMessage": status_message,
            "performedFullRefresh": performed_full_refresh,
        }

    start_str = effective_start.strftime("%Y%m%d")
    end_str = effective_end.strftime("%Y%m%d")

    if progress_callback:
        progress_callback(
            0.05,
            f"Fetching Tushare moneyflow data for {resolved_symbol} ({start_str} - {end_str})",
            None,
        )

    pro_client = ts.pro_api(resolved_token)
    windows = _iter_date_windows(effective_start, effective_end, window_days=180)
    frames: list[pd.DataFrame] = []
    window_count = len(windows)
    for idx, (window_start, window_end) in enumerate(windows):
        window_frame = fetch_moneyflow_hsgt(
            pro=pro_client,
            start_date=window_start.strftime("%Y%m%d"),
            end_date=window_end.strftime("%Y%m%d"),
        )
        if not window_frame.empty:
            frames.append(window_frame)
        if progress_callback and window_count:
            progress_callback(
                0.05 + 0.25 * ((idx + 1) / window_count),
                f"Fetched {idx + 1}/{window_count} windows for {resolved_symbol}",
                None,
            )

    if not frames:
        elapsed = time.perf_counter() - started
        logger.info("No HSGT fund flow data returned for %s (%s-%s)", resolved_symbol, start_str, end_str)
        if progress_callback:
            progress_callback(1.0, "No HSGT fund flow data returned", 0)
        status_message = "Tushare returned no HSGT fund flow data for the requested window"
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "tradeDates": [],
            "tradeDateCount": 0,
            "symbol": resolved_symbol,
            "statusMessage": status_message,
            "performedFullRefresh": performed_full_refresh,
        }

    combined_frame = pd.concat(frames, ignore_index=True).drop_duplicates()
    standardized = _build_standardized_frame(combined_frame, resolved_symbol)
    if standardized.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Standardized HSGT fund flow frame is empty after filtering.")
        if progress_callback:
            progress_callback(1.0, "No valid HSGT fund flow rows after filtering", 0)
        status_message = "No valid HSGT fund flow rows after filtering"
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "tradeDates": [],
            "tradeDateCount": 0,
            "symbol": resolved_symbol,
            "statusMessage": status_message,
            "performedFullRefresh": performed_full_refresh,
        }

    prepared = _prepare_hsgt_frame(standardized)
    _apply_cumulative_values(
        prepared,
        starting_value=latest_snapshot.get("net_buy_amount_cumulative") if latest_snapshot else None,
    )
    prepared.insert(0, "symbol", resolved_symbol)
    prepared = prepared.drop_duplicates(subset=["symbol", "trade_date"]).reset_index(drop=True)

    if progress_callback:
        progress_callback(0.4, f"Upserting {len(prepared)} HSGT rows", len(prepared))

    with dao.connect() as conn:
        dao.ensure_table(conn)
        affected = dao.upsert(prepared, conn=conn)
        conn.commit()

    elapsed = time.perf_counter() - started
    trade_dates = sorted(
        {
            value.isoformat()
            for value in prepared["trade_date"].dropna().unique()
            if isinstance(value, datetime) or hasattr(value, "isoformat")
        }
    )

    if progress_callback:
        progress_callback(1.0, f"Upserted {affected} HSGT fund flow rows", int(affected))
    status_message = f"Upserted {int(affected)} HSGT fund flow rows for {resolved_symbol}"
    if performed_full_refresh:
        status_message += " (full refresh)"

    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "tradeDates": trade_dates,
        "tradeDateCount": len(trade_dates),
        "symbol": resolved_symbol,
        "statusMessage": status_message,
        "performedFullRefresh": performed_full_refresh,
    }


def list_hsgt_fund_flow(
    *,
    symbol: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = HSGTFundFlowDAO(settings.postgres)

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

    selected_symbol = (symbol or DEFAULT_SYMBOL).strip()

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
        symbol=selected_symbol,
        start_date=parsed_start,
        end_date=parsed_end,
        limit=safe_limit,
        offset=safe_offset,
    )


__all__ = ["sync_hsgt_fund_flow", "list_hsgt_fund_flow", "_prepare_hsgt_frame", "_build_standardized_frame"]
