"""Service layer for syncing index historical prices."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional

import akshare as ak
import pandas as pd

from ..config.settings import load_settings
from ..dao import IndexHistoryDAO

logger = logging.getLogger(__name__)

INDEX_CONFIG: Dict[str, Dict[str, str]] = {
    "000001.SH": {"symbol": "000001", "name": "上证指数"},
    "399001.SZ": {"symbol": "399001", "name": "深证成指"},
    "399006.SZ": {"symbol": "399006", "name": "创业板指数"},
    "588040.SH": {"symbol": "588040", "name": "科创50指数"},
}

DEFAULT_START_DATE = date(2000, 1, 1)


def sync_index_history(*, index_codes: Optional[Iterable[str]] = None) -> Dict[str, object]:
    """Fetch and persist index historical prices."""
    settings = load_settings()
    dao = IndexHistoryDAO(settings.postgres)

    codes = list(index_codes) if index_codes else list(INDEX_CONFIG.keys())
    results: Dict[str, Dict[str, object]] = {}
    total_rows = 0

    for code in codes:
        meta = INDEX_CONFIG.get(code)
        if not meta:
            logger.warning("Unknown index code %s skipped", code)
            continue
        symbol = meta["symbol"]
        name = meta["name"]

        latest = dao.latest_trade_date(code)
        if latest is None:
            start_date = DEFAULT_START_DATE
        else:
            start_date = latest + pd.Timedelta(days=1)

        start_str = start_date.strftime("%Y%m%d")
        end_str = datetime.now().strftime("%Y%m%d")

        if start_str > end_str:
            results[code] = {"rows": 0, "skipped": True, "message": "No new data"}
            continue

        logger.info("Syncing index history %s from %s to %s", code, start_str, end_str)
        try:
            frame = ak.index_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str)
        except Exception as exc:  # pragma: no cover - network call
            logger.exception("Index history fetch failed for %s: %s", code, exc)
            results[code] = {"rows": 0, "error": str(exc)}
            continue

        if frame is None or frame.empty:
            results[code] = {"rows": 0, "message": "No data returned"}
            continue

        prepared = _prepare_frame(frame, index_code=code, index_name=name)
        if prepared.empty:
            results[code] = {"rows": 0, "message": "No usable rows"}
            continue

        affected = dao.upsert(prepared)
        results[code] = {"rows": int(affected)}
        total_rows += int(affected)

    return {
        "rows": total_rows,
        "details": results,
    }


def list_index_history(
    *,
    index_code: str,
    limit: int = 500,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> List[Dict[str, object]]:
    settings = load_settings()
    dao = IndexHistoryDAO(settings.postgres)
    rows = dao.list_history(
        index_code=index_code,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
    )
    rows.reverse()
    return rows


def _prepare_frame(dataframe: pd.DataFrame, *, index_code: str, index_name: str) -> pd.DataFrame:
    frame = dataframe.copy()
    rename_map = {
        "日期": "trade_date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "pct_change",
        "涨跌额": "change_amount",
        "换手率": "turnover",
    }
    frame = frame.rename(columns=rename_map)
    expected_columns = list(rename_map.values())

    for column in expected_columns:
        if column not in frame.columns:
            frame[column] = None

    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    numeric_columns = [
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "pct_change",
        "change_amount",
        "turnover",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["index_code"] = index_code
    frame["index_name"] = index_name
    prepared = (
        frame.loc[frame["trade_date"].notnull(), ["index_code", "index_name", "trade_date", *numeric_columns]]
        .drop_duplicates(subset=["index_code", "trade_date"], keep="last")
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    return prepared


__all__ = ["sync_index_history", "list_index_history", "INDEX_CONFIG"]
