"""Service for syncing HSGT fund flow history data from AkShare."""

from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime
from typing import Callable, Optional

import pandas as pd

from ..api_clients import (
    HSGT_FUND_FLOW_COLUMN_MAP,
    fetch_hsgt_fund_flow_history,
    fetch_hsgt_fund_flow_summary,
)
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

SUMMARY_SYMBOL_MAPPINGS: dict[str, str] = {
    "沪股通": "沪股通",
    "深股通": "深股通",
    "港股通(沪)": "港股通(沪)",
    "港股通(深)": "港股通(深)",
}


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

    return frame


def _summary_rows_for_symbol(summary: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if summary.empty:
        return summary
    if symbol == "北向资金":
        return summary[summary["funds_direction"] == "北向"]
    if symbol == "南向资金":
        return summary[summary["funds_direction"] == "南向"]
    board_name = SUMMARY_SYMBOL_MAPPINGS.get(symbol, symbol)
    return summary[summary["board_name"] == board_name]


def _merge_summary_into_history(
    history: pd.DataFrame, summary: pd.DataFrame, symbol: str
) -> pd.DataFrame:
    if summary.empty:
        return history

    if history.empty:
        base_columns = list(history.columns)
        if not base_columns:
            base_columns = ["symbol"] + list(HSGT_FUND_FLOW_COLUMN_MAP.values())
        if "symbol" not in base_columns:
            base_columns = ["symbol"] + [column for column in base_columns if column != "symbol"]
        history = pd.DataFrame(columns=base_columns)

    subset = _summary_rows_for_symbol(summary, symbol)
    if subset.empty:
        return history

    aggregated = (
        subset.groupby("trade_date", as_index=False)[["net_buy_amount", "fund_inflow", "balance"]]
        .sum(min_count=1)
        .dropna(how="all", subset=["net_buy_amount", "fund_inflow", "balance"])
    )
    if aggregated.empty:
        return history

    for _, row in aggregated.iterrows():
        trade_date = row["trade_date"]
        mask = history["trade_date"] == trade_date
        if not mask.any():
            new_row = {column: None for column in history.columns}
            new_row["symbol"] = symbol
            new_row["trade_date"] = trade_date
            history = pd.concat([history, pd.DataFrame([new_row])], ignore_index=True)
            mask = history["trade_date"] == trade_date

        for source_column, target_column in [
            ("net_buy_amount", "net_buy_amount"),
            ("fund_inflow", "fund_inflow"),
            ("balance", "balance"),
        ]:
            value = row.get(source_column)
            if value is None or pd.isna(value):
                continue
            history.loc[mask, target_column] = float(value)
            logger.debug(
                "Patched %s summary value %s=%s for trade_date=%s",
                symbol,
                target_column,
                value,
                trade_date,
            )

    history = history.sort_values(["trade_date"], ignore_index=True)
    return history


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

    summary_snapshot = fetch_hsgt_fund_flow_summary()
    if not summary_snapshot.empty:
        prepared = _merge_summary_into_history(prepared, summary_snapshot, symbol)

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
