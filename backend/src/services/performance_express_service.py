"""
Service for syncing Tushare performance express (业绩快报) data.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Optional, Sequence, Set

import pandas as pd
import tushare as ts

from ..config.settings import AppSettings, load_settings
from ..dao import PerformanceExpressDAO, StockBasicDAO
from ..api_clients import PERFORMANCE_EXPRESS_FIELDS, get_performance_express

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 365 * 3
DEFAULT_RATE_LIMIT = 200
MAX_FETCH_RETRIES = 3


def _resolve_token(token: Optional[str], settings: AppSettings) -> str:
    resolved = token or settings.tushare.token
    if not resolved:
        raise RuntimeError("Tushare token is required for performance express sync.")
    return resolved


def _unique_codes(codes: Iterable[str]) -> list[str]:
    seen: Set[str] = set()
    ordered: list[str] = []
    for code in codes:
        if code and code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


class _RateLimiter:
    def __init__(self, rate_per_minute: int) -> None:
        self._min_interval = 60.0 / max(1, rate_per_minute)
        self._last_call: float | None = None

    def wait(self) -> None:
        now = time.perf_counter()
        if self._last_call is not None:
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
                now = time.perf_counter()
        self._last_call = now


def sync_performance_express(
    token: Optional[str] = None,
    *,
    settings_path: Optional[str] = None,
    codes: Optional[Sequence[str]] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    rate_limit_per_minute: int = DEFAULT_RATE_LIMIT,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    resolved_token = _resolve_token(token, settings)
    express_dao = PerformanceExpressDAO(settings.postgres)
    stock_dao = StockBasicDAO(settings.postgres)

    available_codes = _unique_codes(codes if codes is not None else stock_dao.list_codes())
    total_codes = len(available_codes)
    if total_codes == 0:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No stock codes available for performance express", 0)
        return {
            "codes": [],
            "code_count": 0,
            "total_codes": 0,
            "rows": 0,
            "elapsed_seconds": elapsed,
        }

    pro = ts.pro_api(resolved_token)
    limiter = _RateLimiter(rate_limit_per_minute)
    default_start_date = (datetime.utcnow().date() - timedelta(days=max(1, lookback_days))).strftime("%Y%m%d")

    processed_codes: Set[str] = set()
    total_rows = 0

    with express_dao.connect() as conn:
        express_dao.ensure_table(conn)
        latest_ann_dates = express_dao.latest_ann_dates(available_codes, conn=conn)
        conn.commit()

        for idx, code in enumerate(available_codes, start=1):
            last_ann = latest_ann_dates.get(code)
            if isinstance(last_ann, date):
                start_dt = last_ann + timedelta(days=1)
                start_date = start_dt.strftime("%Y%m%d")
            else:
                start_date = default_start_date

            if progress_callback:
                progress_callback(
                    (idx - 1) / total_codes,
                    f"Fetching express data for {code}",
                    total_rows,
                )

            limiter.wait()
            frame = pd.DataFrame(columns=PERFORMANCE_EXPRESS_FIELDS)
            for attempt in range(1, MAX_FETCH_RETRIES + 1):
                try:
                    frame = get_performance_express(pro, code, start_date=start_date)
                    break
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning(
                        "Attempt %s/%s failed fetching performance express for %s: %s",
                        attempt,
                        MAX_FETCH_RETRIES,
                        code,
                        exc,
                    )
                    time.sleep(min(4.0, attempt))
            else:
                logger.error("Giving up fetching performance express for %s", code)
                continue

            if frame is None or frame.empty:
                continue

            prepared = frame.loc[:, [col for col in PERFORMANCE_EXPRESS_FIELDS if col in frame.columns]].copy()
            for column in ("ann_date", "end_date"):
                if column in prepared.columns:
                    prepared[column] = pd.to_datetime(prepared[column], errors="coerce").dt.date

            affected = express_dao.upsert(prepared, conn=conn)
            conn.commit()
            if affected:
                processed_codes.add(code)
                total_rows += affected

            if progress_callback:
                progress_callback(
                    idx / total_codes,
                    f"Upserted {affected} performance express rows for {code}",
                    total_rows,
                )

    elapsed = time.perf_counter() - started
    if progress_callback:
        progress_callback(1.0, "Performance express sync completed", total_rows)

    return {
        "codes": sorted(processed_codes)[:10],
        "code_count": len(processed_codes),
        "total_codes": total_codes,
        "rows": total_rows,
        "elapsed_seconds": elapsed,
    }


def _parse_date(value: object) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def list_performance_express(
    *,
    limit: int = 100,
    offset: int = 0,
    start_date: Optional[object] = None,
    end_date: Optional[object] = None,
    keyword: Optional[str] = None,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = PerformanceExpressDAO(settings.postgres)
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    return dao.list_entries(limit=limit, offset=offset, start_date=start, end_date=end, keyword=keyword)


__all__ = [
    "sync_performance_express",
    "list_performance_express",
]
