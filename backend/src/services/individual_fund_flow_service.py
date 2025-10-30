"""Service for syncing AkShare individual stock fund flow data."""

from __future__ import annotations

import logging
import re
import time
from typing import Callable, Optional, Sequence

import pandas as pd

from ..api_clients import fetch_individual_fund_flow
from ..config.settings import load_settings
from ..dao import IndividualFundFlowDAO
from ..dao.individual_fund_flow_dao import INDIVIDUAL_FUND_FLOW_FIELDS

logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS: tuple[str, ...] = ("即时", "3日排行", "5日排行", "10日排行", "20日排行")

NUMERIC_COLUMNS: tuple[str, ...] = (
    "latest_price",
    "inflow",
    "outflow",
    "net_amount",
    "net_inflow",
    "turnover_amount",
)

PERCENT_COLUMNS: tuple[str, ...] = (
    "price_change_percent",
    "stage_change_percent",
    "turnover_rate",
    "continuous_turnover_rate",
)

_UNIT_PATTERN = re.compile(r"([+-]?\d+(?:\.\d+)?)([万亿兆]?)(?:元)?", re.IGNORECASE)
_UNIT_MULTIPLIER = {
    "": 1.0,
    "万": 1e4,
    "亿": 1e8,
    "兆": 1e12,
}


def _parse_numeric(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"--", "-"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_percent(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"--", "-"}:
        return None
    if text.endswith("%"):
        text = text[:-1]
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_amount(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"--", "-"}:
        return None
    text = text.replace("人民币", "").replace("元", "")
    match = _UNIT_PATTERN.match(text)
    if not match:
        return _parse_numeric(text)
    number, unit = match.groups()
    try:
        numeric = float(number)
    except ValueError:
        return None
    multiplier = _UNIT_MULTIPLIER.get(unit, 1.0)
    return numeric * multiplier


def _normalize_stock_code(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return text.zfill(6) if len(text) <= 6 else text
    return text


def _prepare_frame(dataframe: pd.DataFrame, symbol: str) -> pd.DataFrame:
    frame = dataframe.copy()
    if "stock_code" not in frame.columns:
        frame["stock_code"] = dataframe.get("股票代码")
    if "stock_name" not in frame.columns:
        frame["stock_name"] = dataframe.get("股票简称")

    frame = frame.dropna(subset=["stock_code"])
    frame["stock_code"] = frame["stock_code"].map(_normalize_stock_code)
    frame = frame.dropna(subset=["stock_code"])

    frame["symbol"] = symbol
    frame["rank"] = pd.to_numeric(frame.get("rank"), errors="coerce").astype("Int64")

    frame = frame.drop_duplicates(subset=["stock_code"], keep="first")

    frame["latest_price"] = frame.get("latest_price").map(_parse_numeric)
    frame["turnover_amount"] = frame.get("turnover_amount").map(_parse_amount)
    frame["net_inflow"] = frame.get("net_inflow").map(_parse_amount)
    for column in ("inflow", "outflow", "net_amount"):
        frame[column] = frame.get(column).map(_parse_amount)

    for column in PERCENT_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_parse_percent)

    required_columns = set(INDIVIDUAL_FUND_FLOW_FIELDS)
    for column in required_columns:
        if column not in frame.columns:
            frame[column] = None

    ordered = list(INDIVIDUAL_FUND_FLOW_FIELDS)
    return frame.loc[:, ordered]


def _normalize_symbols(symbols: Optional[Sequence[str]]) -> list[str]:
    if not symbols:
        return list(DEFAULT_SYMBOLS)
    result: list[str] = []
    for entry in symbols:
        if entry is None:
            continue
        text = str(entry).strip()
        if text:
            result.append(text)
    return result or list(DEFAULT_SYMBOLS)


def sync_individual_fund_flow(
    symbols: Optional[Sequence[str]] = None,
    *,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = IndividualFundFlowDAO(settings.postgres)

    target_symbols = _normalize_symbols(symbols)

    frames: list[pd.DataFrame] = []

    for index, symbol in enumerate(target_symbols, start=1):
        if progress_callback:
            progress_callback((index - 1) / len(target_symbols), f"Fetching individual fund flow for {symbol}", None)
        frame = fetch_individual_fund_flow(symbol)
        if frame.empty:
            logger.info("No individual fund flow data returned for %s", symbol)
            continue
        prepared = _prepare_frame(frame, symbol)
        frames.append(prepared)

    if not frames:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No individual fund flow data fetched", 0)
        return {
            "symbols": [],
            "symbolCount": 0,
            "rows": 0,
            "elapsedSeconds": elapsed,
        }

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["symbol", "rank", "stock_code"], na_position="last")
    combined = combined.drop_duplicates(subset=["symbol", "stock_code"], keep="first").reset_index(drop=True)

    if progress_callback:
        progress_callback(0.8, f"Upserting {len(combined)} individual fund flow rows", len(combined))

    with dao.connect() as conn:
        dao.ensure_table(conn)
        affected = dao.upsert(combined, conn=conn)
        conn.commit()

    elapsed = time.perf_counter() - started
    unique_symbols = sorted({str(symbol) for symbol in combined["symbol"].unique()})

    if progress_callback:
        progress_callback(1.0, f"Upserted {affected} individual fund flow rows", int(affected))

    return {
        "symbols": unique_symbols,
        "symbolCount": len(unique_symbols),
        "rows": int(affected),
        "elapsedSeconds": elapsed,
    }


def list_individual_fund_flow(
    *,
    symbol: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = IndividualFundFlowDAO(settings.postgres)
    return dao.list_entries(symbol=symbol, limit=limit, offset=offset)


__all__ = [
    "DEFAULT_SYMBOLS",
    "sync_individual_fund_flow",
    "list_individual_fund_flow",
]
