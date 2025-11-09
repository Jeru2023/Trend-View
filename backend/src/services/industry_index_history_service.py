"""Service utilities for syncing industry index history via Eastmoney."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import akshare as ak
import pandas as pd

from ..config.settings import load_settings
from ..dao import IndustryIndexHistoryDAO

logger = logging.getLogger(__name__)


def _normalise_dates(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    default_span_days: int = 365,
) -> Tuple[str, str]:
    today = date.today()
    end = datetime.strptime(end_date, "%Y%m%d").date() if end_date else today
    if start_date:
        start = datetime.strptime(start_date, "%Y%m%d").date()
    else:
        start = end - timedelta(days=default_span_days)
    if start > end:
        start = end - timedelta(days=default_span_days)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _fetch_history_from_em(industry_name: str, start: str, end: str) -> pd.DataFrame:
    try:
        frame = ak.stock_board_industry_hist_em(symbol=industry_name, start_date=start, end_date=end)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.warning("Eastmoney industry index fetch failed for %s: %s", industry_name, exc)
        return pd.DataFrame()
    if frame is None or frame.empty:
        return pd.DataFrame()
    rename_map = {
        "日期": "trade_date",
        "开盘": "open",
        "开盘价": "open",
        "最高": "high",
        "最高价": "high",
        "最低": "low",
        "最低价": "low",
        "收盘": "close",
        "收盘价": "close",
        "昨收": "pre_close",
        "涨跌额": "change",
        "涨跌幅": "pct_chg",
        "成交量": "vol",
        "成交额": "amount",
        "换手率": "turnover",
    }
    frame = frame.rename(columns=rename_map)
    if "trade_date" not in frame.columns:
        logger.warning("Eastmoney industry data missing trade_date for %s", industry_name)
        return pd.DataFrame()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.loc[frame["trade_date"].notna()].copy()
    frame["trade_date"] = frame["trade_date"].dt.date
    numeric_columns = ["open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values("trade_date")
    if "pre_close" not in frame.columns or frame["pre_close"].isna().all():
        frame["pre_close"] = frame["close"].shift(1)
    if "change" not in frame.columns:
        frame["change"] = frame["close"] - frame["pre_close"]
    if "pct_chg" not in frame.columns:
        frame["pct_chg"] = frame["change"] / frame["pre_close"] * 100
    frame.loc[frame["pre_close"].isna(), ["change", "pct_chg"]] = None
    frame.loc[frame["pre_close"] == 0, "pct_chg"] = None
    ts_code = f"EM-{industry_name}"
    frame["ts_code"] = ts_code
    frame["industry_name"] = industry_name
    return frame[
        ["ts_code", "industry_name", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]
    ]


def sync_industry_index_history(
    industry_names: Iterable[str],
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    names = [name.strip() for name in industry_names if name and name.strip()]
    if not names:
        return {"startDate": start_date, "endDate": end_date, "totalRows": 0, "errors": []}
    start, end = _normalise_dates(start_date, end_date)
    settings = load_settings(settings_path)
    dao = IndustryIndexHistoryDAO(settings.postgres)
    total_rows = 0
    errors: List[str] = []
    for name in names:
        frame = _fetch_history_from_em(name, start, end)
        if frame.empty:
            errors.append(name)
            continue
        try:
            inserted = dao.upsert(frame)
            total_rows += inserted
        except Exception as exc:  # pragma: no cover - persistence failure
            logger.exception("Failed to upsert industry index history for %s: %s", name, exc)
            errors.append(name)
    return {"startDate": start, "endDate": end, "totalRows": total_rows, "errors": errors}


def list_industry_index_history(
    industry: str,
    *,
    limit: int = 240,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    settings = load_settings(settings_path)
    dao = IndustryIndexHistoryDAO(settings.postgres)
    result = dao.list_entries(industry_name=industry, limit=limit, offset=0)
    items = result.get("items", [])
    rows = [
        {
            "tradeDate": row.get("trade_date"),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "pctChg": row.get("pct_chg"),
            "vol": row.get("vol"),
            "amount": row.get("amount"),
        }
        for row in items
    ]
    return {"industry": industry, "total": result.get("total", 0), "rows": rows}


__all__ = ["sync_industry_index_history", "list_industry_index_history"]
