"""
Service utilities for Tonghuashun stock main business information.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional, Sequence

import pandas as pd

from ..api_clients import fetch_stock_main_business
from ..dao import StockBasicDAO, StockMainBusinessDAO
from ..dao.stock_main_business_dao import STOCK_MAIN_BUSINESS_FIELDS
from ..config.settings import load_settings
from ._akshare_utils import normalize_symbol, symbol_to_ts_code

logger = logging.getLogger(__name__)

TEXT_COLUMNS: tuple[str, ...] = (
    "main_business",
    "product_type",
    "product_name",
    "business_scope",
)


def _clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"--", "-"}:
        return None
    lowered = text.lower()
    if lowered in {"nan", "none", "null"}:
        return None
    return text


def _extract_symbol(code: object) -> Optional[str]:
    if code is None:
        return None
    text = str(code).strip().upper()
    if not text:
        return None
    if "." in text:
        symbol_part, _suffix = text.split(".", 1)
        text = symbol_part
    if text.startswith(("SH", "SZ", "BJ")) and len(text) > 2:
        digits = text[2:]
        if digits.isdigit():
            return digits.zfill(6)
    if text.isdigit():
        return text.zfill(6)

    normalized = normalize_symbol(text)
    if not normalized:
        return None
    if "." in normalized:
        digits, _suffix = normalized.split(".", 1)
        if digits.isdigit():
            return digits.zfill(6)
    if normalized.isdigit():
        return normalized.zfill(6)
    return normalized


def _prepare_business_frame(dataframe: pd.DataFrame, fallback_symbol: Optional[str]) -> pd.DataFrame:
    frame = dataframe.copy()
    for column in STOCK_MAIN_BUSINESS_FIELDS:
        if column not in frame.columns:
            frame[column] = None

    normalized_fallback = normalize_symbol(fallback_symbol) if fallback_symbol else None

    frame["symbol"] = frame["symbol"].map(normalize_symbol)
    if normalized_fallback:
        frame["symbol"] = frame["symbol"].fillna(normalized_fallback)
        frame["symbol"] = frame["symbol"].map(lambda value: value or normalized_fallback)

    frame = frame.dropna(subset=["symbol"])

    frame["ts_code"] = frame["symbol"].map(symbol_to_ts_code)

    for column in TEXT_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_clean_text)

    prepared = frame.loc[:, list(STOCK_MAIN_BUSINESS_FIELDS)].copy()
    prepared = prepared.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)

    return prepared


def sync_stock_main_business(
    *,
    codes: Optional[Sequence[str]] = None,
    include_list_statuses: Optional[Sequence[str]] = ("L",),
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """
    Fetch Tonghuashun stock main business information and persist it.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    business_dao = StockMainBusinessDAO(settings.postgres)
    stock_basic_dao = StockBasicDAO(settings.postgres)

    existing_symbols = set(business_dao.list_symbols())

    def _dedupe_symbols(raw_codes: Sequence[object]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for entry in raw_codes:
            symbol = _extract_symbol(entry)
            if not symbol:
                continue
            if symbol in seen:
                continue
            seen.add(symbol)
            ordered.append(symbol)
        return ordered

    if codes:
        requested_symbols = _dedupe_symbols(codes)
    else:
        base_codes = stock_basic_dao.list_codes(list_statuses=include_list_statuses or ("L",))
        requested_symbols = _dedupe_symbols(base_codes)

    if not requested_symbols:
        logger.info("No stock codes provided for main business sync; skipping.")
        if progress_callback:
            progress_callback(1.0, "No codes available for main business sync", 0)
        elapsed = time.perf_counter() - started
        return {
            "rows": 0,
            "codes": [],
            "codeCount": 0,
            "elapsedSeconds": elapsed,
            "skippedCount": 0,
        }

    fetch_symbols = [symbol for symbol in requested_symbols if symbol not in existing_symbols]
    skipped_count = len(requested_symbols) - len(fetch_symbols)

    logger.info(
        "Main business sync prepared with %d requested symbols, %d existing, %d pending fetch",
        len(requested_symbols),
        skipped_count,
        len(fetch_symbols),
    )

    if not fetch_symbols:
        message = "No new main business codes to sync"
        logger.info(message)
        if progress_callback:
            progress_callback(1.0, message, 0)
        elapsed = time.perf_counter() - started
        return {
            "rows": 0,
            "codes": [],
            "codeCount": 0,
            "elapsedSeconds": elapsed,
            "skippedCount": skipped_count,
        }

    total = len(fetch_symbols)
    unique_symbols: list[str] = []
    prepared_frames: list[pd.DataFrame] = []

    if progress_callback:
        progress_callback(0.02, f"Preparing to fetch {total} main business profiles", total)

    for index, symbol in enumerate(fetch_symbols, start=1):
        if progress_callback:
            progress_callback(index / total, f"Fetching main business for {symbol}", len(prepared_frames))

        frame = fetch_stock_main_business(symbol)
        if frame.empty:
            logger.debug("No main business data returned for %s", symbol)
            continue

        prepared = _prepare_business_frame(frame, fallback_symbol=symbol)
        if prepared.empty:
            logger.debug("Prepared main business frame empty for %s", symbol)
            continue

        prepared_frames.append(prepared)
        unique_symbols.append(symbol)

    if not prepared_frames:
        if progress_callback:
            progress_callback(1.0, "No main business records fetched", 0)
        elapsed = time.perf_counter() - started
        return {
            "rows": 0,
            "codes": [],
            "codeCount": 0,
            "elapsedSeconds": elapsed,
            "skippedCount": skipped_count,
        }

    combined = pd.concat(prepared_frames, ignore_index=True)

    if progress_callback:
        progress_callback(0.9, f"Persisting {len(combined)} main business rows", len(combined))

    with business_dao.connect() as conn:
        business_dao.ensure_table(conn)
        affected = business_dao.upsert(combined, conn=conn)
        conn.commit()

    elapsed = time.perf_counter() - started
    deduped_codes = sorted({symbol_to_ts_code(symbol) or symbol for symbol in unique_symbols if symbol})

    if progress_callback:
        progress_callback(1.0, f"Upserted {affected} main business rows", int(affected))

    return {
        "rows": int(affected),
        "codes": deduped_codes[:10],
        "codeCount": len(deduped_codes),
        "elapsedSeconds": elapsed,
        "skippedCount": skipped_count,
    }


def get_stock_main_business(
    code: str,
    *,
    settings_path: Optional[str] = None,
) -> Optional[dict[str, object]]:
    """
    Retrieve persisted main business details for a specific stock code.
    """
    symbol = _extract_symbol(code)
    if not symbol:
        return None

    settings = load_settings(settings_path)
    dao = StockMainBusinessDAO(settings.postgres)
    entry = dao.get_entry(symbol)
    if not entry:
        return None

    return {
        "symbol": entry.get("symbol"),
        "ts_code": entry.get("ts_code"),
        "mainBusiness": entry.get("main_business"),
        "productType": entry.get("product_type"),
        "productName": entry.get("product_name"),
        "businessScope": entry.get("business_scope"),
        "updatedAt": entry.get("updated_at"),
    }


__all__ = [
    "sync_stock_main_business",
    "get_stock_main_business",
    "_prepare_business_frame",
    "_extract_symbol",
]
