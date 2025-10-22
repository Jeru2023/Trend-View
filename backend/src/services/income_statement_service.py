"""
Service module to synchronise Tushare income statements via ``pro.income``.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Iterable, List, Optional, Sequence, Set

import pandas as pd
import tushare as ts

from ..api_clients import INCOME_STATEMENT_FIELDS, fetch_stock_basic, get_income_statements
from ..config.settings import AppSettings, load_settings
from ..dao import IncomeStatementDAO, StockBasicDAO

logger = logging.getLogger(__name__)

INITIAL_PERIOD_COUNT = 8
RATE_LIMIT_PER_MINUTE = 500


def _resolve_token(token: Optional[str], settings: AppSettings) -> str:
    resolved = token or settings.tushare.token
    if not resolved:
        raise RuntimeError(
            "Tushare token is required. Update the configuration file or provide one explicitly."
        )
    return resolved


def _unique_codes(codes: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for code in codes:
        if code and code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


class _RateLimiter:
    def __init__(self, rate_per_minute: int) -> None:
        self._min_interval = 60.0 / max(1, rate_per_minute)
        self._last_call: Optional[float] = None

    def wait(self) -> None:
        now = time.perf_counter()
        if self._last_call is not None:
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
                now = time.perf_counter()
        self._last_call = now


def sync_income_statements(
    token: Optional[str] = None,
    *,
    settings_path: Optional[str] = None,
    codes: Optional[Iterable[str]] = None,
    initial_periods: int = INITIAL_PERIOD_COUNT,
    rate_limit_per_minute: int = RATE_LIMIT_PER_MINUTE,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """
    Synchronise income statement data into PostgreSQL.

    Fetches the latest ``initial_periods`` income statement rows for each security and upserts
    them individually to respect API access controls.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    resolved_token = _resolve_token(token, settings)
    statement_dao = IncomeStatementDAO(settings.postgres)
    stock_dao = StockBasicDAO(settings.postgres)

    available_codes = _unique_codes(codes if codes is not None else stock_dao.list_codes())
    if not available_codes:
        try:
            fallback_frame = fetch_stock_basic(resolved_token)
        except Exception as fallback_exc:  # pragma: no cover - defensive
            logger.warning("Failed to fetch stock codes for income statements: %s", fallback_exc)
            fallback_frame = None
        else:
            if fallback_frame is not None and not fallback_frame.empty:
                available_codes = _unique_codes(fallback_frame["ts_code"].dropna().tolist())
                try:
                    stock_dao.upsert(fallback_frame)
                except Exception as stock_exc:  # pragma: no cover - defensive
                    logger.warning("Failed to upsert fallback stock basics: %s", stock_exc)

    if not available_codes:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No stock codes available for income statements", 0)
        return {
            "codes": [],
            "code_count": 0,
            "total_codes": 0,
            "rows": 0,
            "elapsed_seconds": elapsed,
        }

    pro_client = ts.pro_api(resolved_token)
    limiter = _RateLimiter(rate_limit_per_minute)
    record_limit = max(1, int(initial_periods))

    frames: List[pd.DataFrame] = []
    processed_codes: Set[str] = set()
    total_rows = 0
    total_codes = len(available_codes)

    for idx, code in enumerate(available_codes, start=1):
        if progress_callback:
            progress_callback(
                idx / (total_codes + 1),
                f"Fetching income statements for {code}",
                total_rows,
            )

        limiter.wait()
        try:
            frame = get_income_statements(
                pro_client,
                ts_code=code,
                limit=record_limit,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to fetch income statements for %s: %s", code, exc)
            continue

        if frame.empty:
            continue

        frames.append(frame)
        processed_codes.update(frame["ts_code"].dropna().unique().tolist())
        total_rows += len(frame.index)

    if not frames:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No income statements retrieved", 0)
        return {
            "codes": [],
            "code_count": 0,
            "total_codes": total_codes,
            "rows": 0,
            "elapsed_seconds": elapsed,
        }

    dataframe = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["ts_code", "end_date"], keep="last")
    )

    for column in ("ann_date", "f_ann_date", "end_date"):
        dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce").dt.date

    categorical: Sequence[str] = ("ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type")
    numeric_columns = [col for col in INCOME_STATEMENT_FIELDS if col not in categorical]
    for column in numeric_columns:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")

    if progress_callback:
        progress_callback(
            total_codes / (total_codes + 1),
            f"Upserting {len(dataframe.index)} income statement records",
            len(dataframe.index),
        )

    affected = statement_dao.upsert(dataframe)

    elapsed = time.perf_counter() - started
    if progress_callback:
        progress_callback(1.0, "Income statement sync completed", affected)

    processed_codes_sample = sorted(processed_codes)[:10]
    return {
        "codes": processed_codes_sample,
        "code_count": len(processed_codes),
        "total_codes": total_codes,
        "rows": affected,
        "elapsed_seconds": elapsed,
    }


__all__ = ["sync_income_statements"]
