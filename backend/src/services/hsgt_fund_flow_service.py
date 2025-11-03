"""Service for syncing HSGT fund flow history data from AkShare."""

from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime
from typing import Callable, Optional

import pandas as pd

from ..api_clients import HSGT_FUND_FLOW_COLUMN_MAP, fetch_hsgt_fund_flow_history
from ..config.settings import load_settings
from ..dao import HSGTFundFlowDAO

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "net_buy_amount",
    "buy_amount",
    "sell_amount",
    "net_buy_amount_cumulative",
    "fund_inflow",
    "balance",
    "market_value",
    "hs300_index",
)

PERCENT_COLUMNS: tuple[str, ...] = (
    "leading_stock_change_percent",
    "hs300_change_percent",
)

DEFAULT_SYMBOL = "北向资金"


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

    for column in HSGT_FUND_FLOW_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date

    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_to_float)

    for column in PERCENT_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_to_percent)

    for column in (*NUMERIC_COLUMNS, *PERCENT_COLUMNS):
        if column in frame.columns:
            series = frame[column]
            frame[column] = series.astype(object).where(pd.notnull(series), None)

    for column in ("leading_stock", "leading_stock_code"):
        if column in frame.columns:
            frame[column] = frame[column].astype(str).str.strip()

    ordered_columns = list(HSGT_FUND_FLOW_COLUMN_MAP.values())
    frame = frame.loc[:, ordered_columns]

    if "net_buy_amount" in frame.columns:
        frame = frame[frame["net_buy_amount"].notna()].copy()

    return frame


def sync_hsgt_fund_flow(
    *,
    symbol: str = DEFAULT_SYMBOL,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """Fetch and persist the HSGT fund flow history for the configured symbol."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = HSGTFundFlowDAO(settings.postgres)

    if progress_callback:
        progress_callback(0.05, f"Fetching HSGT fund flow history for {symbol}", None)

    frame = fetch_hsgt_fund_flow_history(symbol=symbol)
    if frame.empty:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No HSGT fund flow data returned", 0)
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "tradeDates": [],
            "tradeDateCount": 0,
        }

    prepared = _prepare_hsgt_frame(frame)
    prepared.insert(0, "symbol", symbol)

    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Filtered HSGT fund flow frame is empty after removing NaN rows.")
        if progress_callback:
            progress_callback(1.0, "No valid HSGT fund flow rows after filtering", 0)
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "tradeDates": [],
            "tradeDateCount": 0,
            "symbol": symbol,
        }

    if progress_callback:
        progress_callback(0.4, f"Upserting {len(prepared)} HSGT history rows", len(prepared))

    with dao.connect() as conn:
        dao.ensure_table(conn)
        purged = dao.purge_rows_without_net_buy(conn)
        if purged:
            logger.info("Removed %s HSGT rows without net buy metrics", purged)
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

    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "tradeDates": trade_dates,
        "tradeDateCount": len(trade_dates),
        "symbol": symbol,
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


__all__ = ["sync_hsgt_fund_flow", "list_hsgt_fund_flow", "_prepare_hsgt_frame"]
