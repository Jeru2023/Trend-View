"""
Service utilities for Eastmoney stock main composition data.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import date
from typing import Callable, Optional, Sequence

import pandas as pd

from ..api_clients import STOCK_MAIN_COMPOSITION_COLUMN_MAP, fetch_stock_main_composition
from ..config.settings import load_settings
from ..dao import StockBasicDAO, StockMainCompositionDAO
from ..dao.stock_main_composition_dao import STOCK_MAIN_COMPOSITION_FIELDS
from ._akshare_utils import normalize_symbol, symbol_to_ts_code

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "revenue",
    "revenue_ratio",
    "cost",
    "cost_ratio",
    "profit",
    "profit_ratio",
    "gross_margin",
)


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text in {"--", "-"}:
        return ""
    return text


def _clean_numeric(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = str(value).strip()
    if not text or text in {"--", "-", "nan", "NaN"}:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _to_eastmoney_symbol(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if "." in text:
        symbol_part, suffix_part = text.split(".", 1)
        symbol_part = symbol_part.strip()
        suffix_part = suffix_part.strip().upper()
        if symbol_part.isdigit() and suffix_part in {"SH", "SZ", "BJ"}:
            return f"{suffix_part}{symbol_part.zfill(6)}"
        text = symbol_part

    if text.startswith(("SH", "SZ", "BJ")) and len(text) > 2:
        digits = text[2:]
        if digits.isdigit():
            return f"{text[:2]}{digits.zfill(6)}"

    if text.isdigit():
        digits = text.zfill(6)
        ts_code = symbol_to_ts_code(digits)
        if ts_code and "." in ts_code:
            base, suffix = ts_code.split(".", 1)
            return f"{suffix}{base}".upper()
        prefix = ""
        first = digits[0]
        if digits.startswith(("43", "83", "87")) or first in {"4", "8"}:
            prefix = "BJ"
        elif first in {"6", "9", "5"}:
            prefix = "SH"
        elif first in {"0", "2", "3"}:
            prefix = "SZ"
        if prefix:
            return f"{prefix}{digits}"
        return digits

    normalized = normalize_symbol(text)
    if not normalized:
        return None
    if "." in normalized:
        base, suffix = normalized.split(".", 1)
        return f"{suffix}{base}".upper()
    return normalized


def _prepare_frame(dataframe: pd.DataFrame, fallback_symbol: Optional[str]) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=STOCK_MAIN_COMPOSITION_FIELDS)

    frame = dataframe.rename(columns=STOCK_MAIN_COMPOSITION_COLUMN_MAP).copy()
    for column in STOCK_MAIN_COMPOSITION_FIELDS:
        if column not in frame.columns:
            frame[column] = None

    normalized_symbol = normalize_symbol(fallback_symbol) if fallback_symbol else None

    frame["symbol"] = frame["symbol"].map(normalize_symbol)
    if normalized_symbol:
        frame["symbol"] = frame["symbol"].fillna(normalized_symbol)

    frame = frame.dropna(subset=["symbol"])

    frame["report_date"] = pd.to_datetime(frame["report_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["report_date"])

    frame["category_type"] = frame["category_type"].map(_clean_text)
    frame["composition"] = frame["composition"].map(_clean_text)

    if normalized_symbol:
        frame["symbol"] = frame["symbol"].fillna(normalized_symbol)

    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_clean_numeric)

    # Drop rows without composition and without meaningful metrics to avoid duplicates
    mask_empty_composition = frame["composition"] == ""
    if mask_empty_composition.any():
        metrics_columns = ["revenue", "cost", "profit", "revenue_ratio", "cost_ratio", "profit_ratio", "gross_margin"]

        def _row_has_metrics(row: pd.Series) -> bool:
            return any(pd.notna(row[col]) for col in metrics_columns if col in row)

        keep_indices: list[int] = []
        for idx, row in frame.iterrows():
            if row["composition"] != "":
                keep_indices.append(idx)
                continue
            if _row_has_metrics(row):
                frame.at[idx, "composition"] = row["category_type"] or "Total"
                if not frame.at[idx, "composition"]:
                    frame.at[idx, "composition"] = "Total"
                keep_indices.append(idx)

        frame = frame.loc[keep_indices]

    if frame.empty:
        return pd.DataFrame(columns=STOCK_MAIN_COMPOSITION_FIELDS)

    frame["category_type"] = frame["category_type"].replace("", None)
    frame["composition"] = frame["composition"].replace("", None)

    # Ensure no nulls in conflict columns
    frame["category_type"] = frame["category_type"].fillna("Uncategorized")
    frame["composition"] = frame["composition"].fillna("Unnamed")

    prepared = frame.loc[:, list(STOCK_MAIN_COMPOSITION_FIELDS)].copy()
    prepared = prepared.drop_duplicates(subset=list(STOCK_MAIN_COMPOSITION_FIELDS[:-1]), keep="last")
    return prepared.reset_index(drop=True)


def _extract_symbol(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
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


def sync_stock_main_composition(
    *,
    codes: Optional[Sequence[str]] = None,
    include_list_statuses: Optional[Sequence[str]] = ("L",),
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """Fetch and persist stock main composition data."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    composition_dao = StockMainCompositionDAO(settings.postgres)
    stock_basic_dao = StockBasicDAO(settings.postgres)

    def _dedupe_symbols(raw_codes: Sequence[object]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for entry in raw_codes:
            symbol = _extract_symbol(entry)
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            ordered.append(symbol)
        return ordered

    existing_symbols = set(composition_dao.list_symbols())

    if codes:
        target_symbols = _dedupe_symbols(codes)
    else:
        base_codes = stock_basic_dao.list_codes(list_statuses=include_list_statuses or ("L",))
        base_symbols = _dedupe_symbols(base_codes)
        target_symbols = [symbol for symbol in base_symbols if symbol not in existing_symbols]

    if not target_symbols:
        if progress_callback:
            progress_callback(1.0, "No stock codes available for main composition sync", 0)
        elapsed = time.perf_counter() - started
        return {
            "rows": 0,
            "codes": [],
            "codeCount": 0,
            "elapsedSeconds": elapsed,
            "skippedSymbols": 0,
        }

    written_symbols: list[str] = []
    skipped_symbols = 0
    total_rows = 0
    total_symbols = len(target_symbols)

    with composition_dao.connect() as conn:
        composition_dao.ensure_table(conn)
        for index, symbol in enumerate(target_symbols, start=1):
            em_symbol = _to_eastmoney_symbol(symbol)
            if not em_symbol:
                logger.debug("Skipping main composition fetch for %s (unable to derive Eastmoney code)", symbol)
                skipped_symbols += 1
                continue

            if progress_callback:
                progress_callback((index - 1) / total_symbols, f"Fetching main composition for {symbol}", total_rows)

            try:
                frame = fetch_stock_main_composition(em_symbol)
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("Main composition fetch failed for %s: %s", symbol, exc)
                skipped_symbols += 1
                continue

            prepared = _prepare_frame(frame, symbol)
            if symbol in existing_symbols:
                max_report_date = composition_dao.get_max_report_date(symbol)
                if max_report_date:
                    prepared = prepared[prepared["report_date"] > max_report_date]

            if prepared.empty:
                skipped_symbols += 1
                continue

            affected = composition_dao.upsert(prepared, conn=conn)
            conn.commit()
            if affected:
                written_symbols.append(symbol)
                total_rows += affected

            if progress_callback:
                progress_callback(
                    index / total_symbols,
                    f"Saved {affected} main composition rows for {symbol}",
                    total_rows,
                )

    elapsed = time.perf_counter() - started
    deduped_codes = sorted({symbol_to_ts_code(symbol) or symbol for symbol in written_symbols if symbol})

    if progress_callback:
        final_message = (
            f"Upserted {total_rows} main composition rows for {len(deduped_codes)} symbols"
            if total_rows
            else "No new main composition rows to sync"
        )
        progress_callback(1.0, final_message, total_rows)

    return {
        "rows": int(total_rows),
        "codes": deduped_codes[:10],
        "codeCount": len(deduped_codes),
        "elapsedSeconds": elapsed,
        "skippedSymbols": skipped_symbols,
    }


def get_stock_main_composition(
    code: str,
    *,
    settings_path: Optional[str] = None,
    latest_only: bool = True,
) -> Optional[dict[str, object]]:
    symbol = _extract_symbol(code)
    if not symbol:
        return None

    settings = load_settings(settings_path)
    dao = StockMainCompositionDAO(settings.postgres)
    entries = dao.list_entries(symbol, latest_only=latest_only)
    if not entries:
        return None

    processed: list[dict[str, object]] = []
    latest_report_date: Optional[date] = None

    for entry in entries:
        report_date = entry.get("report_date")
        if isinstance(report_date, date):
            if latest_report_date is None or report_date > latest_report_date:
                latest_report_date = report_date
            report_date_iso = report_date.isoformat()
        else:
            report_date_iso = None

        category_type = entry.get("category_type")
        if isinstance(category_type, str) and category_type.lower() == "uncategorized":
            category_type = None

        composition_name = entry.get("composition")
        if isinstance(composition_name, str) and composition_name.lower() in {"unnamed", "total"}:
            if composition_name.lower() == "unnamed":
                composition_name = None

        processed.append(
            {
                "symbol": entry.get("symbol"),
                "reportDate": report_date_iso,
                "categoryType": category_type,
                "composition": composition_name,
                "revenue": _clean_numeric(entry.get("revenue")),
                "revenueRatio": _clean_numeric(entry.get("revenue_ratio")),
                "cost": _clean_numeric(entry.get("cost")),
                "costRatio": _clean_numeric(entry.get("cost_ratio")),
                "profit": _clean_numeric(entry.get("profit")),
                "profitRatio": _clean_numeric(entry.get("profit_ratio")),
                "grossMargin": _clean_numeric(entry.get("gross_margin")),
            }
        )

    groups: dict[str, list[dict[str, object]]] = {}
    for item in processed:
        group_key = item.get("categoryType") or "Uncategorized"
        groups.setdefault(group_key, []).append(item)

    grouped_entries = [
        {"categoryType": category, "entries": sorted(items, key=lambda row: row.get("composition") or "")}
        for category, items in groups.items()
    ]
    grouped_entries.sort(key=lambda group: group["categoryType"])

    return {
        "symbol": symbol,
        "latestReportDate": latest_report_date.isoformat() if latest_report_date else None,
        "groups": grouped_entries,
    }


__all__ = [
    "sync_stock_main_composition",
    "get_stock_main_composition",
]
