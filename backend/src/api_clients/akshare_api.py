"""
Helpers for interacting with AkShare endpoints.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
from contextlib import suppress
from queue import Empty
from typing import Final, Optional, Tuple

import pandas as pd

import akshare as ak

logger = logging.getLogger(__name__)

FINANCE_BREAKFAST_COLUMNS: Final[dict[str, str]] = {
    "标题": "title",
    "摘要": "summary",
    "发布时间": "published_at",
    "链接": "url",
}

PERFORMANCE_EXPRESS_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "row_number",
    "股票代码": "symbol",
    "股票简称": "stock_name",
    "每股收益": "eps",
    "营业收入-营业收入": "revenue",
    "营业收入-去年同期": "revenue_prev",
    "营业收入-同比增长": "revenue_yoy",
    "营业收入-季度环比增长": "revenue_qoq",
    "净利润-净利润": "net_profit",
    "净利润-去年同期": "net_profit_prev",
    "净利润-同比增长": "net_profit_yoy",
    "净利润-季度环比增长": "net_profit_qoq",
    "每股净资产": "net_assets_per_share",
    "净资产收益率": "return_on_equity",
    "所处行业": "industry",
    "公告日期": "announcement_date",
}

PERFORMANCE_FORECAST_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "row_number",
    "股票代码": "symbol",
    "股票简称": "stock_name",
    "预测指标": "forecast_metric",
    "业绩变动": "change_description",
    "预测数值": "forecast_value",
    "业绩变动幅度": "change_rate",
    "业绩变动原因": "change_reason",
    "预告类型": "forecast_type",
    "上年同期值": "last_year_value",
    "公告日期": "announcement_date",
}

INDUSTRY_FUND_FLOW_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "rank",
    "行业": "industry",
    "行业指数": "industry_index",
    "行业-涨跌幅": "price_change_percent",
    "阶段涨跌幅": "stage_change_percent",
    "流入资金": "inflow",
    "流出资金": "outflow",
    "净额": "net_amount",
    "公司家数": "company_count",
    "领涨股": "leading_stock",
    "领涨股-涨跌幅": "leading_stock_change_percent",
    "当前价": "current_price",
}

CONCEPT_FUND_FLOW_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "rank",
    "行业": "concept",
    "行业指数": "concept_index",
    "行业-涨跌幅": "price_change_percent",
    "阶段涨跌幅": "stage_change_percent",
    "流入资金": "inflow",
    "流出资金": "outflow",
    "净额": "net_amount",
    "公司家数": "company_count",
    "领涨股": "leading_stock",
    "领涨股-涨跌幅": "leading_stock_change_percent",
    "当前价": "current_price",
}

_FINANCE_BREAKFAST_TIMEOUT_SECONDS: Final[float] = 12.0


def _empty_finance_breakfast_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the expected schema."""
    return pd.DataFrame(columns=FINANCE_BREAKFAST_COLUMNS.values())


def _finance_breakfast_worker(queue: mp.Queue) -> None:
    """
    Fetch the finance breakfast feed and send either the DataFrame or an error back.

    Executed in a separate process so that a hung network request cannot block the
    main application thread indefinitely.
    """
    try:
        dataframe = ak.stock_info_cjzc_em()
    except Exception as exc:  # pragma: no cover - external dependency
        with suppress(Exception):
            queue.put(("error", repr(exc)))
        return

    try:
        queue.put(("data", dataframe))
    except Exception as exc:  # pragma: no cover - defensive
        with suppress(Exception):
            queue.put(("error", repr(exc)))


def _run_with_timeout(timeout: float) -> Tuple[str, Optional[pd.DataFrame], Optional[str]]:
    """
    Run the AkShare fetch in a child process with a timeout.

    Returns a tuple (status, dataframe, error_message).
    """
    ctx = mp.get_context("spawn")
    queue: mp.Queue = ctx.Queue(maxsize=1)
    process = ctx.Process(target=_finance_breakfast_worker, args=(queue,))
    process.daemon = True
    process.start()

    try:
        process.join(timeout)
        if process.is_alive():
            process.terminate()
            process.join()
            return "timeout", None, f"Timed out after {timeout:.1f}s"

        status: str = "error"
        dataframe: Optional[pd.DataFrame] = None
        error_message: Optional[str] = None

        try:
            status, payload = queue.get(timeout=1.0)
        except Empty:
            status, payload = "error", None
        except Exception as exc:  # pragma: no cover - defensive
            status, payload = "error", repr(exc)

        if status == "data":
            dataframe = payload
            status = "ok"
        else:
            error_message = str(payload) if payload is not None else "Unknown AkShare error"

        return status, dataframe, error_message
    finally:
        queue.close()
        queue.join_thread()
        with suppress(Exception):
            process.close()


def fetch_finance_breakfast(timeout: float = _FINANCE_BREAKFAST_TIMEOUT_SECONDS) -> pd.DataFrame:
    """
    Fetch finance breakfast summaries from AkShare.
    """
    status, dataframe, error_message = _run_with_timeout(timeout)

    if status == "timeout":
        logger.error(
            "AkShare finance breakfast request exceeded %.1f seconds; skipping update.",
            timeout,
        )
        return _empty_finance_breakfast_frame()

    if status != "ok" or dataframe is None:
        logger.error(
            "Failed to fetch finance breakfast data from AkShare: %s",
            error_message,
        )
        return _empty_finance_breakfast_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no finance breakfast data.")
        return _empty_finance_breakfast_frame()

    renamed = dataframe.rename(columns=FINANCE_BREAKFAST_COLUMNS)
    for column in FINANCE_BREAKFAST_COLUMNS.values():
        if column not in renamed.columns:
            renamed[column] = None

    subset = renamed.loc[:, list(FINANCE_BREAKFAST_COLUMNS.values())]
    subset["published_at"] = pd.to_datetime(subset["published_at"], errors="coerce")
    return subset


def _empty_performance_express_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(PERFORMANCE_EXPRESS_COLUMN_MAP.values()))


def fetch_performance_express_em(period: str) -> pd.DataFrame:
    """
    Fetch performance express (业绩快报) data for the given report period.
    """
    if not period or not str(period).strip():
        raise ValueError("period is required for performance express fetch.")

    try:
        dataframe = ak.stock_yjkb_em(date=str(period))
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch performance express data via AkShare: %s", exc)
        return _empty_performance_express_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no performance express data for %s", period)
        return _empty_performance_express_frame()

    renamed = dataframe.rename(columns=PERFORMANCE_EXPRESS_COLUMN_MAP)
    for column in PERFORMANCE_EXPRESS_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(PERFORMANCE_EXPRESS_COLUMN_MAP.values())]


def _empty_performance_forecast_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(PERFORMANCE_FORECAST_COLUMN_MAP.values()))


def fetch_performance_forecast_em(period: str) -> pd.DataFrame:
    """
    Fetch performance forecast (业绩预告) data for the given report period.
    """
    if not period or not str(period).strip():
        raise ValueError("period is required for performance forecast fetch.")

    try:
        dataframe = ak.stock_yjyg_em(date=str(period))
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch performance forecast data via AkShare: %s", exc)
        return _empty_performance_forecast_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no performance forecast data for %s", period)
        return _empty_performance_forecast_frame()

    renamed = dataframe.rename(columns=PERFORMANCE_FORECAST_COLUMN_MAP)
    for column in PERFORMANCE_FORECAST_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(PERFORMANCE_FORECAST_COLUMN_MAP.values())]


def _empty_industry_fund_flow_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(INDUSTRY_FUND_FLOW_COLUMN_MAP.values()))


def fetch_industry_fund_flow(symbol: str) -> pd.DataFrame:
    """Fetch industry fund flow snapshot for the specified ranking symbol."""
    if not symbol or not str(symbol).strip():
        raise ValueError("symbol is required for industry fund flow fetch.")

    try:
        dataframe = ak.stock_fund_flow_industry(symbol=str(symbol))
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch industry fund flow data via AkShare: %s", exc)
        return _empty_industry_fund_flow_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no industry fund flow data for %s", symbol)
        return _empty_industry_fund_flow_frame()

    renamed = dataframe.rename(columns=INDUSTRY_FUND_FLOW_COLUMN_MAP)
    for column in INDUSTRY_FUND_FLOW_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(INDUSTRY_FUND_FLOW_COLUMN_MAP.values())]


def _empty_concept_fund_flow_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(CONCEPT_FUND_FLOW_COLUMN_MAP.values()))


def fetch_concept_fund_flow(symbol: str) -> pd.DataFrame:
    """Fetch concept fund flow snapshot for the specified ranking symbol."""
    if not symbol or not str(symbol).strip():
        raise ValueError("symbol is required for concept fund flow fetch.")

    try:
        dataframe = ak.stock_fund_flow_concept(symbol=str(symbol))
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch concept fund flow data via AkShare: %s", exc)
        return _empty_concept_fund_flow_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no concept fund flow data for %s", symbol)
        return _empty_concept_fund_flow_frame()

    renamed = dataframe.rename(columns=CONCEPT_FUND_FLOW_COLUMN_MAP)
    for column in CONCEPT_FUND_FLOW_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(CONCEPT_FUND_FLOW_COLUMN_MAP.values())]


__all__ = [
    "FINANCE_BREAKFAST_COLUMNS",
    "PERFORMANCE_EXPRESS_COLUMN_MAP",
    "PERFORMANCE_FORECAST_COLUMN_MAP",
    "INDUSTRY_FUND_FLOW_COLUMN_MAP",
    "CONCEPT_FUND_FLOW_COLUMN_MAP",
    "fetch_finance_breakfast",
    "fetch_performance_express_em",
    "fetch_performance_forecast_em",
    "fetch_industry_fund_flow",
    "fetch_concept_fund_flow",
]
