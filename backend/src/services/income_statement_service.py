"""
Service module to synchronise Tushare income statements via ``pro.income``.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Callable, Iterable, List, Optional, Sequence, Set

import pandas as pd
import tushare as ts

from ..api_clients import INCOME_STATEMENT_FIELDS, fetch_stock_basic, get_income_statements
from ..config.settings import AppSettings, load_settings
from ..dao import IncomeStatementDAO, StockBasicDAO

logger = logging.getLogger(__name__)

INITIAL_PERIOD_COUNT = 8
RATE_LIMIT_PER_MINUTE = 200
MAX_FETCH_RETRIES = 3
DB_MAX_RETRIES = 2
MIN_RETRY_SLEEP_SECONDS = 1.0
MAX_RETRY_SLEEP_SECONDS = 16.0
RATE_LIMIT_BACKOFF_SECONDS = 5.0

_DATE_COLUMNS: Sequence[str] = ("ann_date", "f_ann_date", "end_date")
_CATEGORICAL_COLUMNS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "report_type",
    "comp_type",
)
_NUMERIC_COLUMNS: Sequence[str] = tuple(
    column for column in INCOME_STATEMENT_FIELDS if column not in _CATEGORICAL_COLUMNS
)


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
    """
    Resolve the list of stock codes to process, falling back to synchronising stock basics when required.
    """
    codes = _unique_codes(explicit_codes if explicit_codes is not None else stock_dao.list_codes())
    if codes:
        return codes

    try:
        fallback_frame = fetch_stock_basic(resolved_token)
    except Exception as fallback_exc:  # pragma: no cover - defensive
        logger.warning("Failed to fetch stock codes for income statements: %s", fallback_exc)
        return []

    if fallback_frame is None or fallback_frame.empty:
        return []

    codes = _unique_codes(fallback_frame["ts_code"].dropna().tolist())
    if not codes:
        return []

    try:
        stock_dao.upsert(fallback_frame)
    except Exception as stock_exc:  # pragma: no cover - defensive
        logger.warning("Failed to upsert fallback stock basics while preparing income statements: %s", stock_exc)
    return codes


def _format_date_for_request(value: Optional[date]) -> Optional[str]:
    if not value:
        return None
    return value.strftime("%Y%m%d")


def _prepare_income_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise raw income statement data before database persistence.
    """
    if frame.empty:
        return frame

    prepared = (
        frame.loc[:, list(INCOME_STATEMENT_FIELDS)]
        .drop_duplicates(subset=["ts_code", "end_date"], keep="last")
        .copy()
    )

    for column in _DATE_COLUMNS:
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(prepared[column], errors="coerce").dt.date

    for column in _NUMERIC_COLUMNS:
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    return prepared


def _is_rate_limit_exception(exc: Exception) -> bool:
    """
    Heuristically detect Tushare rate limit errors from exception messages.
    """
    message = str(exc)
    lowered = message.lower()
    if any(keyword in lowered for keyword in ("rate limit", "too many requests", "frequency limit", "exceed")):
        return True

    compact = message.replace(" ", "")
    rate_limit_phrases = (
        "\u6700\u591a\u8bbf\u95ee\u8be5\u63a5\u53e3",  # "max visits" Chinese message
        "\u8d85\u8fc7\u9891\u6b21",  # "exceeded frequency"
        "\u8bbf\u95ee\u6b21\u6570\u8d85\u9650",  # "request count exceeded"
        "\u8d85\u8fc7\u6700\u5927\u8c03\u7528\u6b21\u6570",  # "max call count exceeded"
    )
    return any(phrase in compact for phrase in rate_limit_phrases)


def _compute_retry_sleep(attempt: int, rate_limited: bool) -> float:
    """
    Calculate a bounded exponential backoff interval for retries.
    """
    base = RATE_LIMIT_BACKOFF_SECONDS if rate_limited else MIN_RETRY_SLEEP_SECONDS * (2 ** max(0, attempt - 1))
    return float(min(MAX_RETRY_SLEEP_SECONDS, base))


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

    Performs per-code incremental updates using the last known announcement date while fetching
    initial batches for codes that have not been persisted yet.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    resolved_token = _resolve_token(token, settings)
    statement_dao = IncomeStatementDAO(settings.postgres)
    stock_dao = StockBasicDAO(settings.postgres)

    available_codes = _ensure_codes(resolved_token, stock_dao, codes)
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

    processed_codes: Set[str] = set()
    total_rows = 0
    total_codes = len(available_codes)

    with statement_dao.connect() as conn:
        statement_dao.ensure_table(conn)
        latest_ann_dates = statement_dao.latest_ann_dates(available_codes, conn=conn)
        conn.commit()

        for idx, code in enumerate(available_codes, start=1):
            last_ann_date = latest_ann_dates.get(code)
            incremental = last_ann_date is not None
            fetch_kwargs: dict[str, object] = {"ts_code": code}
            fetch_mode = "incremental" if incremental else "initial"
            if incremental:
                start_date = _format_date_for_request(last_ann_date)
                if start_date:
                    fetch_kwargs["start_date"] = start_date
            else:
                fetch_kwargs["limit"] = record_limit

            if progress_callback:
                progress_ratio = (idx - 1) / total_codes if total_codes else 1.0
                progress_callback(
                    progress_ratio,
                    f"Fetching {fetch_mode} income statements for {code}",
                    total_rows,
                )

            frame = pd.DataFrame(columns=INCOME_STATEMENT_FIELDS)
            fetch_succeeded = False
            for attempt in range(1, MAX_FETCH_RETRIES + 1):
                limiter.wait()
                try:
                    frame = get_income_statements(pro_client, **fetch_kwargs)
                    fetch_succeeded = True
                    break
                except Exception as exc:  # pragma: no cover - defensive
                    rate_limited = _is_rate_limit_exception(exc)
                    sleep_seconds = _compute_retry_sleep(attempt, rate_limited)
                    logger.warning(
                        "Attempt %s/%s failed fetching %s income statements for %s: %s",
                        attempt,
                        MAX_FETCH_RETRIES,
                        fetch_mode,
                        code,
                        exc,
                    )
                    time.sleep(sleep_seconds)

            if not fetch_succeeded:
                logger.error(
                    "Giving up fetching income statements for %s after %s attempts",
                    code,
                    MAX_FETCH_RETRIES,
                )
                continue

            if frame.empty:
                if progress_callback:
                    progress_callback(
                        idx / total_codes if total_codes else 1.0,
                        f"No {fetch_mode} income statements returned for {code}",
                        total_rows,
                    )
                continue

            prepared = _prepare_income_frame(frame)
            if prepared.empty:
                if progress_callback:
                    progress_callback(
                        idx / total_codes if total_codes else 1.0,
                        f"No new income statements detected for {code}",
                        total_rows,
                    )
                continue

            affected = 0
            db_success = False
            last_db_exception: Optional[Exception] = None
            for attempt in range(1, DB_MAX_RETRIES + 1):
                try:
                    affected = statement_dao.upsert(prepared, conn=conn)
                    conn.commit()
                    db_success = True
                    break
                except Exception as db_exc:  # pragma: no cover - defensive
                    last_db_exception = db_exc
                    try:
                        conn.rollback()
                    except Exception:  # pragma: no cover - defensive
                        pass
                    sleep_seconds = _compute_retry_sleep(attempt, False)
                    logger.warning(
                        "Attempt %s/%s failed upserting income statements for %s: %s",
                        attempt,
                        DB_MAX_RETRIES,
                        code,
                        db_exc,
                    )
                    time.sleep(sleep_seconds)

            if not db_success:
                logger.error(
                    "Failed to upsert income statements for %s after %s attempts: %s",
                    code,
                    DB_MAX_RETRIES,
                    last_db_exception,
                )
                continue

            processed_codes.update(prepared["ts_code"].dropna().unique().tolist())
            total_rows += affected

            if progress_callback:
                progress_callback(
                    idx / total_codes if total_codes else 1.0,
                    f"Upserted {affected} income statement rows for {code}",
                    total_rows,
                )

    elapsed = time.perf_counter() - started
    if progress_callback:
        progress_callback(1.0, "Income statement sync completed", total_rows)

    processed_codes_sample = sorted(processed_codes)[:10]
    return {
        "codes": processed_codes_sample,
        "code_count": len(processed_codes),
        "total_codes": total_codes,
        "rows": total_rows,
        "elapsed_seconds": elapsed,
    }


__all__ = ["sync_income_statements"]
