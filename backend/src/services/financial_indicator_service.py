"""
Service module to synchronise Tushare financial indicators (fina_indicator).
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Iterable, List, Optional, Sequence, Set

import pandas as pd
import tushare as ts

from ..api_clients import (
    FINANCIAL_INDICATOR_FIELDS,
    fetch_stock_basic,
    get_financial_indicators,
)
from ..config.settings import AppSettings, load_settings
from ..dao import FinancialIndicatorDAO, StockBasicDAO

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 8
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


def _ensure_codes(
    resolved_token: str,
    stock_dao: StockBasicDAO,
    explicit_codes: Optional[Iterable[str]],
) -> List[str]:
    codes = _unique_codes(explicit_codes if explicit_codes is not None else stock_dao.list_codes())
    if codes:
        return codes

    try:
        fallback_frame = fetch_stock_basic(resolved_token)
    except Exception as fallback_exc:  # pragma: no cover - defensive
        logger.warning("Failed to fetch stock basics for financial indicators: %s", fallback_exc)
        return []

    if fallback_frame is None or fallback_frame.empty:
        return []

    codes = _unique_codes(fallback_frame["ts_code"].dropna().tolist())
    if not codes:
        return []

    try:
        stock_dao.upsert(fallback_frame)
    except Exception as stock_exc:  # pragma: no cover - defensive
        logger.warning("Failed to upsert fallback stock basics while preparing financial indicators: %s", stock_exc)
    return codes


def sync_financial_indicators(
    token: Optional[str] = None,
    *,
    settings_path: Optional[str] = None,
    codes: Optional[Iterable[str]] = None,
    limit: int = DEFAULT_LIMIT,
    rate_limit_per_minute: int = RATE_LIMIT_PER_MINUTE,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """
    Synchronise financial indicator data (fina_indicator) into PostgreSQL.

    Fetches the latest ``limit`` indicator rows per stock code.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    resolved_token = _resolve_token(token, settings)
    indicator_dao = FinancialIndicatorDAO(settings.postgres)
    stock_dao = StockBasicDAO(settings.postgres)

    available_codes = _ensure_codes(resolved_token, stock_dao, codes)
    if not available_codes:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No stock codes available for financial indicators", 0)
        return {
            "codes": [],
            "code_count": 0,
            "total_codes": 0,
            "rows": 0,
            "elapsed_seconds": elapsed,
        }

    limit = max(1, int(limit))
    total_codes = len(available_codes)
    limiter = _RateLimiter(rate_limit_per_minute)
    pro_client = ts.pro_api(resolved_token)

    frames: List[pd.DataFrame] = []
    processed_codes: Set[str] = set()
    total_rows = 0

    for idx, code in enumerate(available_codes, start=1):
        if progress_callback:
            progress_callback(
                idx / (total_codes + 1),
                f"Fetching financial indicators for {code}",
                total_rows,
            )

        limiter.wait()
        try:
            frame = get_financial_indicators(
                pro_client,
                ts_code=code,
                limit=limit,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to fetch financial indicators for %s: %s", code, exc)
            continue

        if frame.empty:
            continue

        frames.append(frame)
        processed_codes.add(code)
        total_rows += len(frame.index)

    if not frames:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No financial indicators retrieved", 0)
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

    for column in ("ann_date", "end_date"):
        dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce").dt.date

    categorical: Sequence[str] = ("ts_code", "ann_date", "end_date")
    numeric_columns = [col for col in FINANCIAL_INDICATOR_FIELDS if col not in categorical]
    for column in numeric_columns:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")

    if progress_callback:
        progress_callback(
            total_codes / (total_codes + 1),
            f"Upserting {len(dataframe.index)} financial indicator records",
            len(dataframe.index),
        )

    affected = indicator_dao.upsert(dataframe)

    elapsed = time.perf_counter() - started
    if progress_callback:
        progress_callback(1.0, "Financial indicator sync completed", affected)

    processed_codes_sample = sorted(processed_codes)[:10]
    return {
        "codes": processed_codes_sample,
        "code_count": len(processed_codes),
        "total_codes": total_codes,
        "rows": affected,
        "elapsed_seconds": elapsed,
    }


__all__ = ["sync_financial_indicators"]
