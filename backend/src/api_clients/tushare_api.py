"""
Utilities for interacting with the Tushare Pro API.

This module only contains helpers responsible for fetching data from Tushare.
"""

from __future__ import annotations

import logging
from typing import Final, List, Optional, Sequence

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
    "is_intraday",
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

PERFORMANCE_EXPRESS_FIELDS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "end_date",
    "revenue",
    "operate_profit",
    "total_profit",
    "n_income",
    "total_assets",
    "total_hldr_eqy_exc_min_int",
    "diluted_eps",
    "diluted_roe",
    "yoy_net_profit",
    "bps",
    "perf_summary",
    "update_flag",
)

PERFORMANCE_FORECAST_FIELDS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "end_date",
    "type",
    "p_change_min",
    "p_change_max",
    "net_profit_min",
    "net_profit_max",
    "last_parent_net",
    "first_ann_date",
    "summary",
    "change_reason",
    "update_flag",
)

TRADE_CALENDAR_FIELDS: Sequence[str] = (
    "exchange",
    "cal_date",
    "is_open",
)

MACRO_M2_COLUMN_MAP: Final[dict[str, str]] = {
    "month": "period_label",
    "m0": "m0",
    "m0_yoy": "m0_yoy",
    "m0_mom": "m0_mom",
    "m1": "m1",
    "m1_yoy": "m1_yoy",
    "m1_mom": "m1_mom",
    "m2": "m2",
    "m2_yoy": "m2_yoy",
    "m2_mom": "m2_mom",
}

LPR_COLUMN_MAP: Final[dict[str, str]] = {
    "date": "period_label",
    "1y": "rate_1y",
    "5y": "rate_5y",
}

SHIBOR_COLUMN_MAP: Final[dict[str, str]] = {
    "date": "period_label",
    "on": "on_rate",
    "1w": "rate_1w",
    "2w": "rate_2w",
    "1m": "rate_1m",
    "3m": "rate_3m",
    "6m": "rate_6m",
    "9m": "rate_9m",
    "1y": "rate_1y",
}

HSGT_MONEYFLOW_FIELDS: Sequence[str] = (
    "trade_date",
    "ggt_ss",
    "ggt_sz",
    "hgt",
    "sgt",
    "north_money",
    "south_money",
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


def get_performance_express(
    pro: ts.pro_api,
    ts_code: str,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch performance express records for a single security."""
    params = {"ts_code": ts_code}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    frame = pro.express(**params)
    if frame is None or frame.empty:
        return pd.DataFrame(columns=PERFORMANCE_EXPRESS_FIELDS)

    return frame.loc[:, [col for col in PERFORMANCE_EXPRESS_FIELDS if col in frame.columns]]


def get_performance_forecast(
    pro: ts.pro_api,
    ts_code: str,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch performance forecast records for a single security."""
    params = {"ts_code": ts_code}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    frame = pro.forecast(**params)
    if frame is None or frame.empty:
        return pd.DataFrame(columns=PERFORMANCE_FORECAST_FIELDS)

    return frame.loc[:, [col for col in PERFORMANCE_FORECAST_FIELDS if col in frame.columns]]


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

    chunk_size = 50
    frames: list[pd.DataFrame] = []
    total = len(code_list)
    for start_idx in range(0, total, chunk_size):
        chunk = code_list[start_idx : start_idx + chunk_size]
        codes_str = ",".join(chunk)
        try:
            df = pro.daily(ts_code=codes_str, start_date=start_date, end_date=end_date)
        except Exception as exc:  # pragma: no cover - network errors
            logger.error(
                "Failed to fetch daily trade data for chunk %s-%s: %s",
                start_idx + 1,
                start_idx + len(chunk),
                exc,
            )
            continue

        if df is None or df.empty:
            logger.debug(
                "No daily trade data returned for chunk %s-%s",
                start_idx + 1,
                start_idx + len(chunk),
            )
            continue

        missing_columns = [col for col in DAILY_TRADE_FIELDS if col not in df.columns]
        for column in missing_columns:
            if column == "is_intraday":
                df[column] = False
            else:
                df[column] = None

        if "is_intraday" in df.columns:
            df["is_intraday"] = df["is_intraday"].fillna(False)

        frames.append(df.loc[:, list(DAILY_TRADE_FIELDS)])

    if not frames:
        return pd.DataFrame(columns=DAILY_TRADE_FIELDS)

    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates(subset=["ts_code", "trade_date"])


def fetch_moneyflow_hsgt(
    pro: ts.pro_api,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch Shanghai/Shenzhen-Hong Kong connect money flow statistics.
    """
    params: dict[str, str] = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    frame = pro.moneyflow_hsgt(**params)
    if frame is None or frame.empty:
        return pd.DataFrame(columns=HSGT_MONEYFLOW_FIELDS)

    missing = [col for col in HSGT_MONEYFLOW_FIELDS if col not in frame.columns]
    for column in missing:
        frame[column] = None

    return frame.loc[:, list(HSGT_MONEYFLOW_FIELDS)]


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


def fetch_macro_m2_yearly(token: str, *, start_month: str, end_month: str) -> pd.DataFrame:
    """
    Fetch M0/M1/M2 money supply statistics from Tushare for the given month range.
    """
    if not token:
        raise RuntimeError("Tushare token is required to fetch M2 data.")

    if not start_month or not end_month:
        raise ValueError("start_month and end_month are required (format YYYYMM).")

    pro = ts.pro_api(token)
    fields = ",".join(MACRO_M2_COLUMN_MAP.keys())
    params = {
        "start_m": start_month,
        "end_m": end_month,
        "fields": fields,
    }
    logger.debug("Requesting cn_m with params=%s", params)
    dataframe = pro.cn_m(**params)
    if dataframe is None or dataframe.empty:
        logger.warning("Tushare returned no M2 data for range %s - %s", start_month, end_month)
        return pd.DataFrame(columns=list(MACRO_M2_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=MACRO_M2_COLUMN_MAP)
    for column in MACRO_M2_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MACRO_M2_COLUMN_MAP.values())]


def fetch_lpr_rates(token: str, *, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch Loan Prime Rate (LPR) data from Tushare."""

    if not token:
        raise RuntimeError("Tushare token is required to fetch LPR data.")

    pro = ts.pro_api(token)
    fields = ",".join(LPR_COLUMN_MAP.keys())
    params = {"start_date": start_date, "end_date": end_date, "fields": fields}
    logger.debug("Requesting shibor_lpr with params=%s", params)
    dataframe = pro.shibor_lpr(**params)
    if dataframe is None or dataframe.empty:
        logger.warning("Tushare returned no LPR data for %s-%s", start_date, end_date)
        return pd.DataFrame(columns=list(LPR_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=LPR_COLUMN_MAP)
    for column in LPR_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(LPR_COLUMN_MAP.values())]


def fetch_shibor_rates(token: str, *, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch SHIBOR quotes from Tushare."""

    if not token:
        raise RuntimeError("Tushare token is required to fetch SHIBOR data.")

    pro = ts.pro_api(token)
    params = {"start_date": start_date, "end_date": end_date}
    logger.debug("Requesting shibor with params=%s", params)
    dataframe = pro.shibor(**params)
    if dataframe is None or dataframe.empty:
        logger.warning("Tushare returned no SHIBOR data for %s-%s", start_date, end_date)
        return pd.DataFrame(columns=list(SHIBOR_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=SHIBOR_COLUMN_MAP)
    for column in SHIBOR_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(SHIBOR_COLUMN_MAP.values())]


def get_income_statements(
    pro: ts.pro_api,
    *,
    ts_code: str,
    limit: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
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
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

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


def get_realtime_quotes(
    ts_codes: Sequence[str],
    *,
    token: Optional[str] = None,
    chunk_size: int = 50,
) -> pd.DataFrame:
    """Fetch realtime quotes for the specified ``ts_code`` list via Tushare realtime API."""
    if not ts_codes:
        return pd.DataFrame(
            columns=[
                "code",
                "name",
                "trade_date",
                "trade_time",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "volume",
                "amount",
            ]
        )

    if token:
        try:
            ts.set_token(token)
        except Exception:  # noqa: BLE001
            logger.warning("Failed to set Tushare token for realtime quotes", exc_info=True)

    unique_codes = list(dict.fromkeys(code.strip().upper() for code in ts_codes if code))
    frames: List[pd.DataFrame] = []
    for start in range(0, len(unique_codes), chunk_size):
        chunk = unique_codes[start : start + chunk_size]
        try:
            df = ts.realtime_quote(ts_code=",".join(chunk))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Realtime quote request failed for chunk %s: %s", chunk, exc)
            continue
        if df is None or df.empty:
            continue
        frames.append(df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "code",
                "name",
                "trade_date",
                "trade_time",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "volume",
                "amount",
            ]
        )

    result = pd.concat(frames, ignore_index=True)
    rename_map = {
        "NAME": "name",
        "TS_CODE": "code",
        "DATE": "trade_date",
        "TIME": "trade_time",
        "OPEN": "open",
        "HIGH": "high",
        "LOW": "low",
        "PRICE": "close",
        "PRE_CLOSE": "pre_close",
        "VOLUME": "volume",
        "AMOUNT": "amount",
    }
    result = result.rename(columns=rename_map)
    for column in rename_map.values():
        if column not in result.columns:
            result[column] = None
    desired_columns = [
        "code",
        "name",
        "trade_date",
        "trade_time",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
    ]
    return result.loc[:, desired_columns]


def fetch_trade_calendar(
    token: str,
    start_date: str,
    end_date: str,
    exchange: str = "SSE",
) -> pd.DataFrame:
    """
    Fetch trade calendar data for the specified date range.
    """
    if not token:
        raise RuntimeError("Tushare token is required to fetch trade calendar data.")
    if not start_date or not end_date:
        raise ValueError("Both start_date and end_date must be provided to fetch trade calendar data.")

    pro = ts.pro_api(token)
    params = {
        "start_date": start_date,
        "end_date": end_date,
    }
    if exchange:
        params["exchange"] = exchange

    logger.debug("Requesting trade_cal with params=%s", params)
    try:
        frame = pro.query("trade_cal", fields=",".join(TRADE_CALENDAR_FIELDS), **params)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch trade calendar via Tushare: %s", exc)
        return pd.DataFrame(columns=TRADE_CALENDAR_FIELDS)

    if frame is None or frame.empty:
        logger.warning("Tushare returned empty trade calendar data for range %s-%s", start_date, end_date)
        return pd.DataFrame(columns=TRADE_CALENDAR_FIELDS)

    return frame.loc[:, list(TRADE_CALENDAR_FIELDS)]


__all__ = [
    "DATE_COLUMNS",
    "DAILY_TRADE_FIELDS",
    "DAILY_INDICATOR_FIELDS",
    "INCOME_STATEMENT_FIELDS",
    "FINANCIAL_INDICATOR_FIELDS",
    "STOCK_BASIC_FIELDS",
    "MACRO_M2_COLUMN_MAP",
    "LPR_COLUMN_MAP",
    "SHIBOR_COLUMN_MAP",
    "fetch_stock_basic",
    "fetch_trade_calendar",
    "fetch_macro_m2_yearly",
    "fetch_lpr_rates",
    "fetch_shibor_rates",
    "get_daily_trade",
    "get_daily_indicator",
    "get_income_statements",
    "get_financial_indicators",
    "TRADE_CALENDAR_FIELDS",
]
