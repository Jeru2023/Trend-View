"""Service layer for syncing index historical prices."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional

import pandas as pd
import tushare as ts

from ..config.settings import load_settings
from ..dao import IndexHistoryDAO

logger = logging.getLogger(__name__)

INDEX_CONFIG: Dict[str, Dict[str, str]] = {
    "000001.SH": {"symbol": "000001", "ts_code": "000001.SH", "name": "上证指数"},
    "399001.SZ": {"symbol": "399001", "ts_code": "399001.SZ", "name": "深证成指"},
    "399006.SZ": {"symbol": "399006", "ts_code": "399006.SZ", "name": "创业板指数"},
    "588040.SH": {"symbol": "000688", "ts_code": "000688.SH", "name": "科创50指数"},
}

DEFAULT_START_DATE = date(2000, 1, 1)
TUSHARE_INDEX_FIELDS = "ts_code,trade_date,open,high,low,close,vol,amount,pct_chg,change"


def sync_index_history(*, index_codes: Optional[Iterable[str]] = None) -> Dict[str, object]:
    """Fetch and persist index historical prices via Tushare."""
    settings = load_settings()
    tushare_config = getattr(settings, "tushare", None)
    token = getattr(tushare_config, "token", None)
    if not token:
        raise RuntimeError("Tushare token is required to sync index history.")

    pro = ts.pro_api(token)
    dao = IndexHistoryDAO(settings.postgres)

    codes = list(index_codes) if index_codes else list(INDEX_CONFIG.keys())
    results: Dict[str, Dict[str, object]] = {}
    total_rows = 0

    for code in codes:
        meta = INDEX_CONFIG.get(code)
        if not meta:
            logger.warning("Unknown index code %s skipped", code)
            continue
        ts_code = meta.get("ts_code") or code
        name = meta.get("name", code)

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

        logger.info("Syncing index history %s (%s) from %s to %s", code, ts_code, start_str, end_str)
        try:
            frame = pro.index_daily(ts_code=ts_code, start_date=start_str, end_date=end_str, fields=TUSHARE_INDEX_FIELDS)
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
        "trade_date": "trade_date",
        "日期": "trade_date",
        "open": "open",
        "开盘": "open",
        "close": "close",
        "收盘": "close",
        "high": "high",
        "最高": "high",
        "low": "low",
        "最低": "low",
        "vol": "volume",
        "成交量": "volume",
        "amount": "amount",
        "成交额": "amount",
        "pct_chg": "pct_change",
        "涨跌幅": "pct_change",
        "change": "change_amount",
        "涨跌额": "change_amount",
        "turnover": "turnover",
        "换手率": "turnover",
        "amplitude": "amplitude",
        "振幅": "amplitude",
    }
    frame = frame.rename(columns={k: v for k, v in rename_map.items() if k in frame.columns})

    expected_columns = [
        "trade_date",
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

    for column in expected_columns:
        if column not in frame.columns:
            frame[column] = None

    frame["trade_date"] = pd.to_datetime(frame["trade_date"].astype(str), errors="coerce").dt.date
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
