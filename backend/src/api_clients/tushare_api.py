"""
Utilities for interacting with the Tushare Pro API.

This module only contains helpers responsible for fetching data from Tushare.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Sequence

import pandas as pd
import tushare as ts

logger = logging.getLogger(__name__)


STOCK_BASIC_FIELDS: Sequence[str] = (
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
)

DATE_COLUMNS: Sequence[str] = ("list_date", "delist_date")

DAILY_TRADE_FIELDS: Sequence[str] = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)


DAILY_INDICATOR_FIELDS: Sequence[str] = (
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)

INCOME_STATEMENT_FIELDS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "report_type",
    "comp_type",
    "basic_eps",
    "diluted_eps",
    "oper_exp",
    "total_revenue",
    "revenue",
    "operate_profit",
    "total_profit",
    "n_income",
    "ebitda",
)

FINANCIAL_INDICATOR_FIELDS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "end_date",
    "eps",
    "gross_margin",
    "current_ratio",
    "quick_ratio",
    "invturn_days",
    "arturn_days",
    "inv_turn",
    "ar_turn",
    "netprofit_margin",
    "grossprofit_margin",
    "profit_to_gr",
    "saleexp_to_gr",
    "adminexp_of_gr",
    "finaexp_of_gr",
    "roe",
    "q_eps",
    "q_netprofit_margin",
    "q_gsprofit_margin",
    "q_roe",
    "basic_eps_yoy",
    "op_yoy",
    "ebt_yoy",
    "netprofit_yoy",
    "q_sales_yoy",
    "q_sales_qoq",
    "q_op_yoy",
    "q_op_qoq",
    "q_profit_yoy",
    "q_profit_qoq",
)

def _fetch_stock_basic_frames(
    pro: ts.pro_api,
    list_statuses: Sequence[str],
    fields: Sequence[str],
    market: Optional[str],
) -> List[pd.DataFrame]:
    """Fetch stock basics for each list status and return non-empty frames."""
    frames: List[pd.DataFrame] = []
    for status in list_statuses:
        params = {
            "exchange": "",
            "list_status": status,
        }
        if market:
            params["market"] = market

        logger.debug("Requesting stock_basic with params=%s", params)
        frame = pro.stock_basic(fields=",".join(fields), **params)
        if frame is None or frame.empty:
            logger.warning("No stock_basic data returned for list_status=%s", status)
            continue

        frames.append(frame)

    return frames


def fetch_stock_basic(
    token: str,
    list_statuses: Sequence[str] = ("L", "D", "P"),
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch A-share stock basics from Tushare and return them as a DataFrame.

    Args:
        token: Tushare authentication token.
        list_statuses: Sequence of list statuses to request from Tushare. By
            default this covers listed (L), delisted (D), and paused (P) A-shares.
        market: Optional Tushare ``market`` filter. Leave empty to include all
            A-share markets.
    """
    if not token:
        raise RuntimeError("Tushare token is required to fetch stock basics.")

    pro = ts.pro_api(token)
    frames = _fetch_stock_basic_frames(
        pro=pro,
        list_statuses=list_statuses,
        fields=STOCK_BASIC_FIELDS,
        market=market,
    )

    if not frames:
        logger.warning("No stock_basic data retrieved.")
        return pd.DataFrame(columns=STOCK_BASIC_FIELDS)

    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"])


def get_daily_trade(
    pro: ts.pro_api,
    code_list: Sequence[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Fetch daily trade data for the provided codes within the given date range.

    Args:
        pro: An authenticated ``tushare.pro_api`` client.
        code_list: Iterable of Tushare ``ts_code`` identifiers.
        start_date: Start date in ``YYYYMMDD`` format.
        end_date: End date in ``YYYYMMDD`` format.
    """
    if not code_list:
        return pd.DataFrame(columns=DAILY_TRADE_FIELDS)

    code_list_str = ",".join(code_list)
    df = pro.daily(ts_code=code_list_str, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        logger.warning("No daily trade data returned for codes: %s", code_list_str)
        return pd.DataFrame(columns=DAILY_TRADE_FIELDS)

    missing_columns = [col for col in DAILY_TRADE_FIELDS if col not in df.columns]
    for column in missing_columns:
        df[column] = None

    return df.loc[:, list(DAILY_TRADE_FIELDS)]


def get_daily_indicator(
    pro: ts.pro_api,
    trade_date: str,
) -> pd.DataFrame:
    """
    Fetch daily indicator (daily_basic) data for the given trade date.
    """
    if not trade_date:
        raise ValueError("trade_date is required to fetch daily indicator data.")

    df = pro.daily_basic(trade_date=trade_date)
    if df is None or df.empty:
        logger.warning("No daily indicator data returned for trade_date=%s", trade_date)
        return pd.DataFrame(columns=DAILY_INDICATOR_FIELDS)

    missing_columns = [col for col in DAILY_INDICATOR_FIELDS if col not in df.columns]
    for column in missing_columns:
        df[column] = None

    return df.loc[:, list(DAILY_INDICATOR_FIELDS)]


def get_income_statements(
    pro: ts.pro_api,
    *,
    ts_code: str,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch income statements for the provided security using ``pro.income``.
    """
    if not ts_code:
        raise ValueError("ts_code is required to fetch income statements.")

    fields = ",".join(INCOME_STATEMENT_FIELDS)
    params: dict[str, object] = {
        "fields": fields,
        "ts_code": ts_code,
    }
    if limit is not None:
        params["limit"] = limit

    df = pro.income(**params)
    if df is None or df.empty:
        logger.warning("No income statement data returned for ts_code=%s", ts_code)
        return pd.DataFrame(columns=INCOME_STATEMENT_FIELDS)

    missing_columns = [col for col in INCOME_STATEMENT_FIELDS if col not in df.columns]
    for column in missing_columns:
        df[column] = None

    return df.loc[:, list(INCOME_STATEMENT_FIELDS)]


def get_financial_indicators(
    pro: ts.pro_api,
    *,
    ts_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch financial indicators for the provided security using ``pro.fina_indicator``.
    """
    if not ts_code:
        raise ValueError("ts_code is required to fetch financial indicators.")

    fields = ",".join(FINANCIAL_INDICATOR_FIELDS)
    params: dict[str, object] = {
        "fields": fields,
        "ts_code": ts_code,
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if limit is not None:
        params["limit"] = limit

    df = pro.fina_indicator(**params)
    if df is None or df.empty:
        logger.warning("No financial indicator data returned for ts_code=%s", ts_code)
        return pd.DataFrame(columns=FINANCIAL_INDICATOR_FIELDS)

    missing_columns = [col for col in FINANCIAL_INDICATOR_FIELDS if col not in df.columns]
    for column in missing_columns:
        df[column] = None

    return df.loc[:, list(FINANCIAL_INDICATOR_FIELDS)]


__all__ = [
    "DATE_COLUMNS",
    "DAILY_TRADE_FIELDS",
    "DAILY_INDICATOR_FIELDS",
    "INCOME_STATEMENT_FIELDS",
    "FINANCIAL_INDICATOR_FIELDS",
    "STOCK_BASIC_FIELDS",
    "fetch_stock_basic",
    "get_daily_trade",
    "get_daily_indicator",
    "get_income_statements",
    "get_financial_indicators",
]
