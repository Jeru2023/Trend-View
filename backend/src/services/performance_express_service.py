"""
Service for syncing AkShare performance express (业绩快报) data.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Callable, Optional, Sequence

import pandas as pd

from ..api_clients import fetch_performance_express_em
from ..config.settings import load_settings
from ..dao import PerformanceExpressDAO
from ..dao.performance_express_dao import PERFORMANCE_EXPRESS_FIELDS
from ._akshare_utils import normalize_symbol, resolve_report_period, symbol_to_ts_code

logger = logging.getLogger(__name__)

_NUMERIC_COLUMNS: tuple[str, ...] = (
    "eps",
    "revenue",
    "revenue_prev",
    "revenue_yoy",
    "revenue_qoq",
    "net_profit",
    "net_profit_prev",
    "net_profit_yoy",
    "net_profit_qoq",
    "net_assets_per_share",
    "return_on_equity",
)


def _first_non_null(series: pd.Series) -> Optional[object]:
    for value in series:
        if pd.notna(value):
            return value
    return None


def _prepare_frame(dataframe: pd.DataFrame, report_period: date) -> pd.DataFrame:
    frame = dataframe.copy()

    if "symbol" not in frame.columns:
        frame["symbol"] = None

    frame["symbol"] = frame["symbol"].apply(normalize_symbol)
    frame = frame.dropna(subset=["symbol"])

    frame["ts_code"] = frame["symbol"].apply(symbol_to_ts_code)

    for column in _NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if "row_number" in frame.columns:
        frame["row_number"] = pd.to_numeric(frame["row_number"], errors="coerce").astype("Int64")

    frame["report_period"] = report_period

    for column in PERFORMANCE_EXPRESS_FIELDS:
        if column not in frame.columns:
            frame[column] = None

    prepared = frame.loc[:, list(PERFORMANCE_EXPRESS_FIELDS)].copy()
    prepared = prepared.sort_values(
        ["symbol", "report_period", "announcement_date", "row_number"],
        ascending=[True, True, False, False],
        na_position="last",
    )

    agg_spec: dict[str, object] = {
        column: _first_non_null
        for column in PERFORMANCE_EXPRESS_FIELDS
        if column not in {"symbol", "report_period", "row_number"}
    }
    agg_spec["row_number"] = _first_non_null

    grouped = (
        prepared.groupby(["symbol", "report_period"], as_index=False)
        .agg(agg_spec)
        .reindex(columns=PERFORMANCE_EXPRESS_FIELDS)
    )

    grouped["row_number"] = pd.to_numeric(grouped["row_number"], errors="coerce").astype("Int64")

    return grouped.reset_index(drop=True)


def sync_performance_express(
    token: Optional[str] = None,
    *,
    settings_path: Optional[str] = None,
    report_period: Optional[object] = None,
    codes: Optional[Sequence[str]] = None,
    lookback_days: Optional[int] = None,
    rate_limit_per_minute: Optional[int] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    if token:
        logger.debug("Token parameter is ignored for AkShare performance express sync.")
    if lookback_days:
        logger.debug("Lookback window is unused for AkShare performance express sync.")
    if rate_limit_per_minute:
        logger.debug("Rate limiting is handled by AkShare and ignored for this sync.")

    started = time.perf_counter()
    settings = load_settings(settings_path)
    express_dao = PerformanceExpressDAO(settings.postgres)

    target_period = resolve_report_period(report_period)
    period_str = target_period.strftime("%Y%m%d")

    if progress_callback:
        progress_callback(0.0, f"Fetching performance express for {period_str}", None)

    raw_frame = fetch_performance_express_em(period_str)
    if raw_frame.empty:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, f"No performance express data available for {period_str}", 0)
        return {
            "codes": [],
            "code_count": 0,
            "total_codes": 0,
            "rows": 0,
            "report_period": target_period.isoformat(),
            "elapsed_seconds": elapsed,
        }

    prepared = _prepare_frame(raw_frame, target_period)

    if codes:
        normalized_codes = {str(code).strip().upper() for code in codes if str(code).strip()}
        symbol_filters = {
            code.split(".")[0].zfill(6)
            for code in normalized_codes
            if "." in code and code.split(".")[0].isdigit()
        }
        symbol_filters.update({code.zfill(6) for code in normalized_codes if code.isdigit()})
        if symbol_filters or normalized_codes:
            prepared = prepared[
                prepared["symbol"].isin(symbol_filters)
                | prepared["ts_code"].isin(normalized_codes)
            ].reset_index(drop=True)

    if prepared.empty:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, f"No matching performance express rows for {period_str}", 0)
        return {
            "codes": [],
            "code_count": 0,
            "total_codes": 0,
            "rows": 0,
            "report_period": target_period.isoformat(),
            "elapsed_seconds": elapsed,
        }

    if progress_callback:
        progress_callback(
            0.5,
            f"Preparing {len(prepared)} performance express rows for {period_str}",
            len(prepared),
        )

    with express_dao.connect() as conn:
        express_dao.ensure_table(conn)
        affected = express_dao.upsert(prepared, conn=conn)
        conn.commit()

    elapsed = time.perf_counter() - started

    unique_codes = [code for code in prepared["ts_code"] if code]
    if not unique_codes:
        unique_codes = [symbol for symbol in prepared["symbol"] if symbol]
    representative = sorted({code for code in unique_codes})

    if progress_callback:
        progress_callback(
            1.0,
            f"Upserted {affected} performance express rows for {period_str}",
            int(affected),
        )

    return {
        "codes": representative[:10],
        "code_count": len(representative),
        "total_codes": len(prepared),
        "rows": int(affected),
        "report_period": target_period.isoformat(),
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
