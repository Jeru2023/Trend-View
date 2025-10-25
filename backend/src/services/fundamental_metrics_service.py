"""
Service layer for computing derived fundamental metrics.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime
from typing import Callable, Optional

import numpy as np
import pandas as pd

from ..config.runtime_config import load_runtime_config
from ..config.settings import load_settings
from ..dao import FinancialIndicatorDAO, IncomeStatementDAO
from ..dao.fundamental_metrics_dao import FundamentalMetricsDAO, FUNDAMENTAL_METRICS_FIELDS

logger = logging.getLogger(__name__)


def _calc_growth(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous is None:
        return None
    try:
        current_val = float(current)
        previous_val = float(previous)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(previous_val) or previous_val == 0:
        return None
    if not np.isfinite(current_val):
        return None
    return (current_val - previous_val) / abs(previous_val)


def _safe_value(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(numeric):
        return None
    return numeric


def sync_fundamental_metrics(
    *,
    settings_path: Optional[str] = None,
    per_code: int = 8,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, float | int]:
    """Compute YoY/QoQ metrics from fundamentals and persist to PostgreSQL."""
    started = time.perf_counter()
    settings = load_settings(settings_path)

    income_dao = IncomeStatementDAO(settings.postgres)
    financial_dao = FinancialIndicatorDAO(settings.postgres)
    metrics_dao = FundamentalMetricsDAO(settings.postgres)

    if progress_callback:
        progress_callback(0.05, "Loading recent income statements", None)

    income_df = income_dao.fetch_recent(per_code=per_code)
    if income_df.empty:
        logger.warning("Income statements not available; skipping fundamental metrics computation")
        metrics_dao.replace_all(pd.DataFrame(columns=list(FUNDAMENTAL_METRICS_FIELDS)))
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No income statements found", 0)
        return {"rows": 0, "elapsed_seconds": elapsed}

    if progress_callback:
        progress_callback(0.15, "Loading recent financial indicators", None)

    financial_df = financial_dao.fetch_recent(per_code=per_code)

    if progress_callback:
        progress_callback(0.35, "Computing metrics", None)

    income_df = income_df.dropna(subset=["ts_code", "end_date"]).copy()
    financial_df = financial_df.dropna(subset=["ts_code", "end_date"]).copy()

    records: list[dict[str, object]] = []

    income_groups = income_df.groupby("ts_code", sort=False)
    financial_groups = financial_df.groupby("ts_code", sort=False)

    for ts_code, group in income_groups:
        group = group.drop_duplicates(subset=["end_date"], keep="last").sort_values("end_date")
        if group.empty:
            continue

        net_income_map: dict[pd.Timestamp, dict[str, Optional[float]]] = {}
        revenue_map: dict[pd.Timestamp, dict[str, Optional[float]]] = {}

        date_index = {row.end_date: row for row in group.itertuples()}

        for row in group.itertuples():
            end_date: pd.Timestamp = row.end_date
            prev_year = end_date - pd.DateOffset(years=1)
            prev_year_row = date_index.get(prev_year)
            net_income_yoy = _calc_growth(row.n_income, prev_year_row.n_income if prev_year_row else None)
            revenue_yoy = _calc_growth(row.revenue, prev_year_row.revenue if prev_year_row else None)
            net_income_map[end_date] = {
                "value": _safe_value(row.n_income),
                "yoy": net_income_yoy,
            }
            revenue_map[end_date] = {
                "value": _safe_value(row.revenue),
                "yoy": revenue_yoy,
            }

        end_dates = list(group["end_date"].tolist())
        latest_end = end_dates[-1]
        prev_end = end_dates[-2] if len(end_dates) >= 2 else None
        prev2_end = end_dates[-3] if len(end_dates) >= 3 else None

        net_income_yoy_latest = net_income_map.get(latest_end, {}).get("yoy")
        net_income_yoy_prev1 = net_income_map.get(prev_end, {}).get("yoy") if prev_end else None
        net_income_yoy_prev2 = net_income_map.get(prev2_end, {}).get("yoy") if prev2_end else None

        net_income_qoq_latest = None
        revenue_qoq_latest = None
        revenue_yoy_latest = revenue_map.get(latest_end, {}).get("yoy")

        latest_income_val = net_income_map.get(latest_end, {}).get("value")
        latest_revenue_val = revenue_map.get(latest_end, {}).get("value")

        if prev_end is not None:
            prev_income_val = net_income_map.get(prev_end, {}).get("value")
            net_income_qoq_latest = _calc_growth(latest_income_val, prev_income_val)
            prev_revenue_val = revenue_map.get(prev_end, {}).get("value")
            revenue_qoq_latest = _calc_growth(latest_revenue_val, prev_revenue_val)

        # Compare revenue YoY already computed

        financial_group = financial_groups.get_group(ts_code) if ts_code in financial_groups.groups else pd.DataFrame()
        roe_yoy_latest = None
        roe_qoq_latest = None
        roe_end_date = None
        if not financial_group.empty:
            financial_group = financial_group.drop_duplicates(subset=["end_date"], keep="last").sort_values("end_date")
            fin_dates = list(financial_group["end_date"].tolist())
            roe_end_date = fin_dates[-1]
            fin_latest = financial_group.iloc[-1]
            fin_prev = financial_group.iloc[-2] if len(financial_group) >= 2 else None
            prev_year_end = fin_latest.end_date - pd.DateOffset(years=1)
            prev_year_row = financial_group[financial_group["end_date"] == prev_year_end]
            prev_year_val = None
            if not prev_year_row.empty:
                prev_year_val = _safe_value(prev_year_row.iloc[0]["roe"])
            roe_latest_val = _safe_value(fin_latest["roe"])
            roe_yoy_latest = _calc_growth(roe_latest_val, prev_year_val)
            if fin_prev is not None:
                prev_val = _safe_value(fin_prev["roe"])
                roe_qoq_latest = _calc_growth(roe_latest_val, prev_val)
        else:
            roe_latest_val = None

        record = {
            "ts_code": ts_code,
            "net_income_end_date_latest": latest_end.date() if isinstance(latest_end, pd.Timestamp) else latest_end,
            "net_income_end_date_prev1": prev_end.date() if isinstance(prev_end, pd.Timestamp) else prev_end,
            "net_income_end_date_prev2": prev2_end.date() if isinstance(prev2_end, pd.Timestamp) else prev2_end,
            "revenue_end_date_latest": latest_end.date() if isinstance(latest_end, pd.Timestamp) else latest_end,
            "roe_end_date_latest": roe_end_date.date() if isinstance(roe_end_date, pd.Timestamp) else roe_end_date,
            "net_income_yoy_latest": net_income_yoy_latest,
            "net_income_yoy_prev1": net_income_yoy_prev1,
            "net_income_yoy_prev2": net_income_yoy_prev2,
            "net_income_qoq_latest": net_income_qoq_latest,
            "revenue_yoy_latest": revenue_yoy_latest,
            "revenue_qoq_latest": revenue_qoq_latest,
            "roe_yoy_latest": roe_yoy_latest,
            "roe_qoq_latest": roe_qoq_latest,
        }
        records.append(record)

    if not records:
        metrics_dao.replace_all(pd.DataFrame(columns=list(FUNDAMENTAL_METRICS_FIELDS)))
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No fundamental metrics to compute", 0)
        return {"rows": 0, "elapsed_seconds": elapsed}

    metrics_df = pd.DataFrame.from_records(records, columns=list(FUNDAMENTAL_METRICS_FIELDS))

    affected = metrics_dao.replace_all(metrics_df)
    elapsed = time.perf_counter() - started

    if progress_callback:
        progress_callback(1.0, "Fundamental metrics sync completed", affected)

    return {"rows": affected, "elapsed_seconds": elapsed}


def list_fundamental_metrics(
    *,
    keyword: Optional[str] = None,
    market: Optional[str] = None,
    exchange: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    """
    Return paginated fundamental metrics to power the dashboard views.
    """
    settings = load_settings(settings_path)
    runtime_config = load_runtime_config()
    dao = FundamentalMetricsDAO(settings.postgres)
    result = dao.query_metrics(
        keyword=keyword,
        market=market,
        exchange=exchange,
        include_st=runtime_config.include_st,
        include_delisted=runtime_config.include_delisted,
        limit=limit,
        offset=offset,
    )

    def _format_date(value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, date):
            return value.isoformat()
        text = str(value).strip()
        return text or None

    def _safe_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric):
            return None
        return numeric

    items: list[dict[str, object]] = []
    for item in result["items"]:
        items.append(
            {
                "code": item["code"],
                "name": item.get("name"),
                "industry": item.get("industry"),
                "market": item.get("market"),
                "exchange": item.get("exchange"),
                "net_income_end_date_latest": _format_date(item.get("net_income_end_date_latest")),
                "net_income_end_date_prev1": _format_date(item.get("net_income_end_date_prev1")),
                "net_income_end_date_prev2": _format_date(item.get("net_income_end_date_prev2")),
                "revenue_end_date_latest": _format_date(item.get("revenue_end_date_latest")),
                "roe_end_date_latest": _format_date(item.get("roe_end_date_latest")),
                "net_income_yoy_latest": _safe_float(item.get("net_income_yoy_latest")),
                "net_income_yoy_prev1": _safe_float(item.get("net_income_yoy_prev1")),
                "net_income_yoy_prev2": _safe_float(item.get("net_income_yoy_prev2")),
                "net_income_qoq_latest": _safe_float(item.get("net_income_qoq_latest")),
                "revenue_yoy_latest": _safe_float(item.get("revenue_yoy_latest")),
                "revenue_qoq_latest": _safe_float(item.get("revenue_qoq_latest")),
                "roe_yoy_latest": _safe_float(item.get("roe_yoy_latest")),
                "roe_qoq_latest": _safe_float(item.get("roe_qoq_latest")),
            }
        )

    return {"total": result["total"], "items": items}


__all__ = ["sync_fundamental_metrics", "list_fundamental_metrics"]
