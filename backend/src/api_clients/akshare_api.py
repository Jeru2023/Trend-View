"""
Helpers for interacting with AkShare endpoints.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import re
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

INDIVIDUAL_FUND_FLOW_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "rank",
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "最新价": "latest_price",
    "涨跌幅": "price_change_percent",
    "阶段涨跌幅": "stage_change_percent",
    "换手率": "turnover_rate",
    "连续换手率": "continuous_turnover_rate",
    "流入资金": "inflow",
    "流出资金": "outflow",
    "净额": "net_amount",
    "资金流入净额": "net_inflow",
    "成交额": "turnover_amount",
}

BIG_DEAL_FUND_FLOW_COLUMN_MAP: Final[dict[str, str]] = {
    "成交时间": "trade_time",
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "成交价格": "trade_price",
    "成交量": "trade_volume",
    "成交额": "trade_amount",
    "大单性质": "trade_side",
    "涨跌幅": "price_change_percent",
    "涨跌额": "price_change",
}

STOCK_MAIN_BUSINESS_COLUMN_MAP: Final[dict[str, str]] = {
    "股票代码": "symbol",
    "主营业务": "main_business",
    "产品类型": "product_type",
    "产品名称": "product_name",
    "经营范围": "business_scope",
}

STOCK_MAIN_COMPOSITION_COLUMN_MAP: Final[dict[str, str]] = {
    "股票代码": "symbol",
    "报告日期": "report_date",
    "分类类型": "category_type",
    "主营构成": "composition",
    "主营收入": "revenue",
    "收入比例": "revenue_ratio",
    "主营成本": "cost",
    "成本比例": "cost_ratio",
    "主营利润": "profit",
    "利润比例": "profit_ratio",
    "毛利率": "gross_margin",
}

PROFIT_FORECAST_BASE_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "row_number",
    "代码": "symbol",
    "股票代码": "symbol",
    "名称": "stock_name",
    "股票简称": "stock_name",
    "研报数": "report_count",
    "机构投资评级(近六个月)-买入": "rating_buy",
    "机构投资评级(近六个月)-增持": "rating_add",
    "机构投资评级(近六个月)-中性": "rating_neutral",
    "机构投资评级(近六个月)-减持": "rating_reduce",
    "机构投资评级(近六个月)-卖出": "rating_sell",
}

_FORECAST_YEAR_PATTERN = re.compile(r"(?P<year>\d{4})预测每股收益")
_PROFIT_FORECAST_OUTPUT_COLUMNS: Final[Tuple[str, ...]] = (
    "symbol",
    "stock_name",
    "report_count",
    "rating_buy",
    "rating_add",
    "rating_neutral",
    "rating_reduce",
    "rating_sell",
    "row_number",
    "forecast_year",
    "forecast_eps",
)

GLOBAL_INDEX_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "seq",
    "代码": "code",
    "名称": "name",
    "最新价": "latest_price",
    "涨跌额": "change_amount",
    "涨跌幅": "change_percent",
    "开盘价": "open_price",
    "最高价": "high_price",
    "最低价": "low_price",
    "昨收价": "prev_close",
    "振幅": "amplitude",
    "最新行情时间": "last_quote_time",
}

DOLLAR_INDEX_COLUMN_MAP: Final[dict[str, str]] = {
    "日期": "trade_date",
    "代码": "code",
    "名称": "name",
    "今开": "open_price",
    "最新价": "close_price",
    "最高": "high_price",
    "最低": "low_price",
    "振幅": "amplitude",
}

RMB_MIDPOINT_COLUMN_MAP: Final[dict[str, str]] = {
    "日期": "trade_date",
    "美元": "usd",
    "欧元": "eur",
    "日元": "jpy",
    "港元": "hkd",
    "英镑": "gbp",
    "澳元": "aud",
    "加元": "cad",
    "新西兰元": "nzd",
    "新加坡元": "sgd",
    "瑞士法郎": "chf",
    "林吉特": "myr",
    "卢布": "rub",
    "兰特": "zar",
    "韩元": "krw",
    "迪拉姆": "aed",
    "里亚尔": "sar",
    "福林": "huf",
    "兹罗提": "pln",
    "丹麦克朗": "dkk",
    "瑞典克朗": "sek",
    "挪威克朗": "nok",
    "里拉": "try",
    "比索": "mxn",
    "泰铢": "thb",
}

FUTURES_REALTIME_COLUMN_MAP: Final[dict[str, str]] = {
    "名称": "name",
    "最新价": "last_price",
    "人民币报价": "price_cny",
    "涨跌额": "change_amount",
    "涨跌幅": "change_percent",
    "开盘价": "open_price",
    "最高价": "high_price",
    "最低价": "low_price",
    "昨日结算价": "prev_settlement",
    "持仓量": "open_interest",
    "买价": "bid_price",
    "卖价": "ask_price",
    "行情时间": "quote_time",
    "日期": "trade_date",
}

FUTURES_TARGET_CODES: Final[dict[str, str]] = {
    "LME镍3个月": "NID",
    "LME铅3个月": "PBD",
    "LME锡3个月": "SND",
    "LME锌3个月": "ZSD",
    "LME铝3个月": "AHD",
    "LME铜3个月": "CAD",
    "COMEX铜": "HG",
    "NYMEX天然气": "NG",
    "NYMEX原油": "CL",
    "COMEX白银": "SI",
    "COMEX黄金": "GC",
    "布伦特原油": "OIL",
    "伦敦金": "XAU",
    "伦敦银": "XAG",
}

_FINANCE_BREAKFAST_TIMEOUT_SECONDS: Final[float] = 20.0


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


def _normalize_finance_breakfast_frame(dataframe: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Rename columns and coerce timestamps for the finance breakfast dataset."""
    if dataframe is None or dataframe.empty:
        return _empty_finance_breakfast_frame()

    renamed = dataframe.rename(columns=FINANCE_BREAKFAST_COLUMNS)
    for column in FINANCE_BREAKFAST_COLUMNS.values():
        if column not in renamed.columns:
            renamed[column] = None

    subset = renamed.loc[:, list(FINANCE_BREAKFAST_COLUMNS.values())]
    subset["published_at"] = pd.to_datetime(subset["published_at"], errors="coerce")
    return subset


def fetch_finance_breakfast(timeout: float = _FINANCE_BREAKFAST_TIMEOUT_SECONDS) -> pd.DataFrame:
    """
    Fetch finance breakfast summaries from AkShare.
    """
    status, dataframe, error_message = _run_with_timeout(timeout)

    if status != "ok" or dataframe is None or dataframe.empty:
        reason = "timeout" if status == "timeout" else error_message or "unknown error"
        if status == "timeout":
            logger.warning(
                "AkShare finance breakfast request exceeded %.1f seconds; attempting direct fallback.",
                timeout,
            )
        else:
            logger.warning(
                "Finance breakfast fetch via worker failed (%s); attempting direct fallback.",
                reason,
            )
        try:
            dataframe = ak.stock_info_cjzc_em()
        except Exception as fallback_exc:  # pragma: no cover - external dependency
            logger.error("Finance breakfast fallback fetch failed: %s", fallback_exc)
            return _empty_finance_breakfast_frame()

    normalized = _normalize_finance_breakfast_frame(dataframe)
    if normalized.empty:
        logger.warning("AkShare returned no finance breakfast data.")
    return normalized


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


def _empty_profit_forecast_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PROFIT_FORECAST_OUTPUT_COLUMNS))


def fetch_profit_forecast_em(symbol: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch profit forecast (盈利预测) snapshot, optionally filtered by industry symbol.
    """
    symbol_param = "" if symbol is None else str(symbol).strip()
    try:
        dataframe = ak.stock_profit_forecast_em(symbol=symbol_param)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch profit forecast data via AkShare: %s", exc)
        return _empty_profit_forecast_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no profit forecast data for symbol '%s'", symbol_param or "ALL")
        return _empty_profit_forecast_frame()

    renamed = dataframe.rename(columns=PROFIT_FORECAST_BASE_COLUMN_MAP)
    for column in PROFIT_FORECAST_BASE_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    year_columns: dict[str, int] = {}
    for column in dataframe.columns:
        match = _FORECAST_YEAR_PATTERN.match(str(column))
        if match:
            year_columns[column] = int(match.group("year"))

    if not year_columns:
        logger.warning("No forecast EPS columns detected in profit forecast dataset.")
        return _empty_profit_forecast_frame()

    records = []
    for _, row in renamed.iterrows():
        base = {
            "symbol": row.get("symbol"),
            "stock_name": row.get("stock_name"),
            "report_count": row.get("report_count"),
            "rating_buy": row.get("rating_buy"),
            "rating_add": row.get("rating_add"),
            "rating_neutral": row.get("rating_neutral"),
            "rating_reduce": row.get("rating_reduce"),
            "rating_sell": row.get("rating_sell"),
            "row_number": row.get("row_number"),
        }
        for column, year in year_columns.items():
            value = row.get(column)
            numeric_value = None
            if value is not None and value != "":
                numeric_value = pd.to_numeric(value, errors="coerce")
                if pd.isna(numeric_value):
                    numeric_value = None
            record = base.copy()
            record["forecast_year"] = int(year)
            record["forecast_eps"] = float(numeric_value) if numeric_value is not None else None
            records.append(record)

    if not records:
        return _empty_profit_forecast_frame()

    normalized = pd.DataFrame.from_records(records, columns=list(_PROFIT_FORECAST_OUTPUT_COLUMNS))
    return normalized


def _empty_global_index_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(GLOBAL_INDEX_COLUMN_MAP.values()))


def fetch_global_indices() -> pd.DataFrame:
    """Fetch real-time global index snapshot."""
    try:
        dataframe = ak.index_global_spot_em()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch global index data via AkShare: %s", exc)
        return _empty_global_index_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no global index data.")
        return _empty_global_index_frame()

    renamed = dataframe.rename(columns=GLOBAL_INDEX_COLUMN_MAP)
    for column in GLOBAL_INDEX_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(GLOBAL_INDEX_COLUMN_MAP.values())]


def _empty_dollar_index_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(DOLLAR_INDEX_COLUMN_MAP.values()))


def fetch_dollar_index_history(symbol: str = "美元指数") -> pd.DataFrame:
    """Fetch historical quotes for a global index (default: Dollar Index)."""
    symbol_param = str(symbol).strip() or "美元指数"

    try:
        dataframe = ak.index_global_hist_em(symbol=symbol_param)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch dollar index history via AkShare: %s", exc)
        return _empty_dollar_index_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no dollar index history for %s", symbol_param)
        return _empty_dollar_index_frame()

    renamed = dataframe.rename(columns=DOLLAR_INDEX_COLUMN_MAP)
    for column in DOLLAR_INDEX_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(DOLLAR_INDEX_COLUMN_MAP.values())]


def _empty_rmb_midpoint_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(RMB_MIDPOINT_COLUMN_MAP.values()))


def fetch_rmb_midpoint_rates() -> pd.DataFrame:
    """Fetch historical RMB central parity rates from SAFE."""

    try:
        dataframe = ak.currency_boc_safe()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch RMB midpoint data via AkShare: %s", exc)
        return _empty_rmb_midpoint_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no RMB midpoint data.")
        return _empty_rmb_midpoint_frame()

    renamed = dataframe.rename(columns=RMB_MIDPOINT_COLUMN_MAP)
    for column in RMB_MIDPOINT_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(RMB_MIDPOINT_COLUMN_MAP.values())]


def _empty_futures_realtime_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(FUTURES_REALTIME_COLUMN_MAP.values()))


def fetch_futures_realtime(symbols: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Fetch realtime foreign commodity futures quotes for selected symbols."""

    if not symbols:
        symbols = list(FUTURES_TARGET_CODES.values())

    try:
        symbol_param = ",".join(symbols)
        dataframe = ak.futures_foreign_commodity_realtime(symbol=symbol_param)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch futures realtime data via AkShare: %s", exc)
        return _empty_futures_realtime_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no futures realtime data for %s", symbols)
        return _empty_futures_realtime_frame()

    renamed = dataframe.rename(columns=FUTURES_REALTIME_COLUMN_MAP)
    for column in FUTURES_REALTIME_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    filtered = renamed.loc[
        renamed["name"].isin(FUTURES_TARGET_CODES.keys())
    ]

    return filtered.loc[:, list(FUTURES_REALTIME_COLUMN_MAP.values())]


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


def _empty_individual_fund_flow_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(INDIVIDUAL_FUND_FLOW_COLUMN_MAP.values()))


def fetch_individual_fund_flow(symbol: str) -> pd.DataFrame:
    """Fetch individual stock fund flow snapshot for the specified ranking symbol."""
    if not symbol or not str(symbol).strip():
        raise ValueError("symbol is required for individual fund flow fetch.")

    try:
        dataframe = ak.stock_fund_flow_individual(symbol=str(symbol))
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch individual fund flow data via AkShare: %s", exc)
        return _empty_individual_fund_flow_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no individual fund flow data for %s", symbol)
        return _empty_individual_fund_flow_frame()

    renamed = dataframe.rename(columns=INDIVIDUAL_FUND_FLOW_COLUMN_MAP)
    for column in INDIVIDUAL_FUND_FLOW_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(INDIVIDUAL_FUND_FLOW_COLUMN_MAP.values())]


def _empty_big_deal_fund_flow_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(BIG_DEAL_FUND_FLOW_COLUMN_MAP.values()))


def fetch_big_deal_fund_flow() -> pd.DataFrame:
    """Fetch Tonghuashun big deal tracking snapshot."""
    try:
        dataframe = ak.stock_fund_flow_big_deal()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch big deal fund flow data via AkShare: %s", exc)
        return _empty_big_deal_fund_flow_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no big deal fund flow data.")
        return _empty_big_deal_fund_flow_frame()

    renamed = dataframe.rename(columns=BIG_DEAL_FUND_FLOW_COLUMN_MAP)
    for column in BIG_DEAL_FUND_FLOW_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(BIG_DEAL_FUND_FLOW_COLUMN_MAP.values())]

def _empty_stock_main_business_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(STOCK_MAIN_BUSINESS_COLUMN_MAP.values()))


def fetch_stock_main_business(symbol: str) -> pd.DataFrame:
    """Fetch Tonghuashun stock main business data for the specified symbol."""
    if not symbol or not str(symbol).strip():
        raise ValueError("symbol is required for stock main business fetch.")

    query_symbol = str(symbol).strip()
    try:
        dataframe = ak.stock_zyjs_ths(symbol=query_symbol)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch stock main business via AkShare: %s", exc)
        return _empty_stock_main_business_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no stock main business data for %s", query_symbol)
        return _empty_stock_main_business_frame()

    renamed = dataframe.rename(columns=STOCK_MAIN_BUSINESS_COLUMN_MAP)
    for column in STOCK_MAIN_BUSINESS_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(STOCK_MAIN_BUSINESS_COLUMN_MAP.values())]


def _empty_stock_main_composition_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(STOCK_MAIN_COMPOSITION_COLUMN_MAP.values()))


def fetch_stock_main_composition(symbol: str) -> pd.DataFrame:
    """Fetch Eastmoney stock main composition data for the specified symbol."""
    if not symbol or not str(symbol).strip():
        raise ValueError("symbol is required for stock main composition fetch.")

    query_symbol = str(symbol).strip().upper()
    try:
        dataframe = ak.stock_zygc_em(symbol=query_symbol)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch stock main composition via AkShare: %s", exc)
        return _empty_stock_main_composition_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no stock main composition data for %s", query_symbol)
        return _empty_stock_main_composition_frame()

    renamed = dataframe.rename(columns=STOCK_MAIN_COMPOSITION_COLUMN_MAP)
    for column in STOCK_MAIN_COMPOSITION_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(STOCK_MAIN_COMPOSITION_COLUMN_MAP.values())]


__all__ = [
    "FINANCE_BREAKFAST_COLUMNS",
    "PERFORMANCE_EXPRESS_COLUMN_MAP",
    "PERFORMANCE_FORECAST_COLUMN_MAP",
    "INDUSTRY_FUND_FLOW_COLUMN_MAP",
    "CONCEPT_FUND_FLOW_COLUMN_MAP",
    "INDIVIDUAL_FUND_FLOW_COLUMN_MAP",
    "STOCK_MAIN_BUSINESS_COLUMN_MAP",
    "STOCK_MAIN_COMPOSITION_COLUMN_MAP",
    "fetch_finance_breakfast",
    "fetch_performance_express_em",
    "fetch_performance_forecast_em",
    "fetch_industry_fund_flow",
    "fetch_concept_fund_flow",
    "fetch_individual_fund_flow",
    "fetch_big_deal_fund_flow",
    "fetch_stock_main_business",
    "fetch_stock_main_composition",
]
