"""Service for syncing AkShare concept fund flow data (Tonghuashun)."""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional, Sequence

import pandas as pd

import akshare as ak

from ..config.settings import load_settings
from ..dao import ConceptFundFlowDAO

logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS: tuple[str, ...] = ("即时", "3日排行", "5日排行", "10日排行", "20日排行")

COLUMN_RENAME_MAP = {
    "行业": "concept",
    "行业指数": "concept_index",
    "行业-涨跌幅": "price_change_percent",
    "流入资金": "inflow",
    "流出资金": "outflow",
    "净额": "net_amount",
    "公司家数": "company_count",
    "领涨股": "leading_stock",
    "领涨股-涨跌幅": "leading_stock_change_percent",
    "当前价": "current_price",
    "序号": "rank",
}

FLOAT_COLUMNS = ("concept_index", "inflow", "outflow", "net_amount", "current_price")
PERCENT_COLUMNS = ("price_change_percent", "leading_stock_change_percent")


def _parse_number(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    multiplier = 1.0
    if text.endswith("亿"):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 1e4
        text = text[:-1]
    text = text.replace(",", "")
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _parse_percent(value: object) -> Optional[float]:
    if value is None or value == "":
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


def _prepare_frame(dataframe: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.rename(columns=COLUMN_RENAME_MAP).copy()
    for required in COLUMN_RENAME_MAP.values():
        if required not in frame.columns:
            frame[required] = None

    frame["symbol"] = symbol
    frame["concept"] = frame["concept"].astype(str).str.strip()
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype("Int64")
    frame["company_count"] = pd.to_numeric(frame["company_count"], errors="coerce").astype("Int64")
    frame["stage_change_percent"] = None

    for column in FLOAT_COLUMNS:
        frame[column] = frame[column].map(_parse_number)
    for column in PERCENT_COLUMNS:
        frame[column] = frame[column].map(_parse_percent)

    frame = frame.dropna(subset=["concept"]).drop_duplicates(subset=["concept"], keep="first")

    ordered = [
        "symbol",
        "concept",
        "rank",
        "concept_index",
        "price_change_percent",
        "stage_change_percent",
        "inflow",
        "outflow",
        "net_amount",
        "company_count",
        "leading_stock",
        "leading_stock_change_percent",
        "current_price",
    ]
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


def sync_concept_fund_flow(
    symbols: Optional[Sequence[str]] = None,
    *,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = ConceptFundFlowDAO(settings.postgres)

    target_symbols = _normalize_symbols(symbols)

    frames: list[pd.DataFrame] = []

    for index, symbol in enumerate(target_symbols, start=1):
        if progress_callback:
            progress_callback((index - 1) / len(target_symbols), f"Fetching concept fund flow for {symbol}", None)
        try:
            frame = ak.stock_fund_flow_concept(symbol=symbol)
        except Exception as exc:  # pragma: no cover - external dependency
            logger.warning("Concept fund flow fetch failed for %s: %s", symbol, exc)
            continue
        if frame.empty:
            logger.info("No concept fund flow data returned for %s", symbol)
            continue
        prepared = _prepare_frame(frame, symbol)
        frames.append(prepared)

    if not frames:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No concept fund flow data fetched", 0)
        return {
            "symbols": [],
            "symbolCount": 0,
            "rows": 0,
            "elapsedSeconds": elapsed,
        }

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["symbol", "rank"], na_position="last")
    combined = combined.drop_duplicates(subset=["symbol", "concept"], keep="first").reset_index(drop=True)

    if progress_callback:
        progress_callback(0.8, f"Upserting {len(combined)} concept fund flow rows", len(combined))

    with dao.connect() as conn:
        dao.ensure_table(conn)
        affected = dao.upsert(combined, conn=conn)
        conn.commit()

    elapsed = time.perf_counter() - started
    unique_symbols = sorted({str(symbol) for symbol in combined["symbol"].unique()})

    if progress_callback:
        progress_callback(1.0, f"Upserted {affected} concept fund flow rows", int(affected))

    return {
        "symbols": unique_symbols,
        "symbolCount": len(unique_symbols),
        "rows": int(affected),
        "elapsedSeconds": elapsed,
    }


def list_concept_fund_flow(
    *,
    symbol: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = ConceptFundFlowDAO(settings.postgres)
    return dao.list_entries(symbol=symbol, limit=limit, offset=offset)


__all__ = [
    "DEFAULT_SYMBOLS",
    "sync_concept_fund_flow",
    "list_concept_fund_flow",
]
