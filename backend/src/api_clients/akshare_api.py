"""
Helpers for interacting with AkShare endpoints.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import re
import ssl
import time
from contextlib import suppress
from functools import lru_cache
from io import StringIO
from queue import Empty
from typing import Final, Optional, Tuple

import pandas as pd

import akshare as ak
import py_mini_racer
import requests
from akshare.datasets import get_ths_js
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

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

CONTINUOUS_VOLUME_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "rank",
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "涨跌幅": "price_change_percent",
    "最新价": "last_price",
    "成交量": "volume_text",
    "基准日成交量": "baseline_volume_text",
    "放量天数": "volume_days",
    "阶段涨跌幅": "stage_change_percent",
    "所属行业": "industry",
}

VOLUME_PRICE_RISE_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "rank",
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "最新价": "last_price",
    "量价齐升天数": "volume_days",
    "阶段涨幅": "stage_change_percent",
    "累计换手率": "turnover_percent",
    "所属行业": "industry",
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

UPWARD_BREAKOUT_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "rank",
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "最新价": "last_price",
    "成交额": "turnover_amount_text",
    "成交量": "volume_text",
    "涨跌幅": "price_change_percent",
    "换手率": "turnover_rate",
}

CONTINUOUS_RISE_COLUMN_MAP: Final[dict[str, str]] = {
    "序号": "rank",
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "收盘价": "last_price",
    "最高价": "high_price",
    "最低价": "low_price",
    "连涨天数": "volume_days",
    "连续涨跌幅": "stage_change_percent",
    "累计换手率": "turnover_percent",
    "所属行业": "industry",
}

HSGT_FUND_FLOW_COLUMN_MAP: Final[dict[str, str]] = {
    "日期": "trade_date",
    "当日成交净买额": "net_buy_amount",
    "买入成交额": "buy_amount",
    "卖出成交额": "sell_amount",
    "历史累计净买额": "net_buy_amount_cumulative",
    "当日资金流入": "fund_inflow",
    "当日余额": "balance",
    "持股市值": "market_value",
    "领涨股": "leading_stock",
    "领涨股-涨跌幅": "leading_stock_change_percent",
    "沪深300": "hs300_index",
    "沪深300-涨跌幅": "hs300_change_percent",
    "领涨股-代码": "leading_stock_code",
}

MARGIN_ACCOUNT_COLUMN_MAP: Final[dict[str, str]] = {
    "日期": "trade_date",
    "融资余额": "financing_balance",
    "融券余额": "securities_lending_balance",
    "融资买入额": "financing_purchase_amount",
    "融券卖出额": "securities_lending_sell_amount",
    "证券公司数量": "securities_company_count",
    "营业部数量": "business_department_count",
    "个人投资者数量": "individual_investor_count",
    "机构投资者数量": "institutional_investor_count",
    "参与交易的投资者数量": "participating_investor_count",
    "有融资融券负债的投资者数量": "liability_investor_count",
    "担保物总价值": "collateral_value",
    "平均维持担保比例": "average_collateral_ratio",
}

MARKET_FUND_FLOW_COLUMN_MAP: Final[dict[str, str]] = {
    "日期": "trade_date",
    "上证-收盘价": "shanghai_close",
    "上证-涨跌幅": "shanghai_change_percent",
    "深证-收盘价": "shenzhen_close",
    "深证-涨跌幅": "shenzhen_change_percent",
    "主力净流入-净额": "main_net_inflow_amount",
    "主力净流入-净占比": "main_net_inflow_ratio",
    "超大单净流入-净额": "huge_order_net_inflow_amount",
    "超大单净流入-净占比": "huge_order_net_inflow_ratio",
    "大单净流入-净额": "large_order_net_inflow_amount",
    "大单净流入-净占比": "large_order_net_inflow_ratio",
    "中单净流入-净额": "medium_order_net_inflow_amount",
    "中单净流入-净占比": "medium_order_net_inflow_ratio",
    "小单净流入-净额": "small_order_net_inflow_amount",
    "小单净流入-净占比": "small_order_net_inflow_ratio",
}
def fetch_market_activity_legu() -> pd.DataFrame:
    """Fetch market activity (赚钱效应) snapshot from LeGu."""
    try:
        dataframe = ak.stock_market_activity_legu()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch market activity via AkShare: %s", exc)
        return pd.DataFrame(columns=["metric", "value"])

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no market activity data.")
        return pd.DataFrame(columns=["metric", "value"])

    renamed = dataframe.rename(columns={"item": "metric", "value": "value"})
    for column in ("metric", "value"):
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, ["metric", "value"]]


def fetch_market_fund_flow() -> pd.DataFrame:
    """Fetch Eastmoney market fund flow history."""
    try:
        dataframe = ak.stock_market_fund_flow()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch market fund flow via AkShare: %s", exc)
        return pd.DataFrame(columns=list(MARKET_FUND_FLOW_COLUMN_MAP.values()))

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no market fund flow data.")
        return pd.DataFrame(columns=list(MARKET_FUND_FLOW_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=MARKET_FUND_FLOW_COLUMN_MAP)
    for column in MARKET_FUND_FLOW_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MARKET_FUND_FLOW_COLUMN_MAP.values())]


def fetch_stock_news(symbol: str) -> pd.DataFrame:
    """Fetch Eastmoney stock news via AkShare."""
    columns = list(STOCK_NEWS_COLUMN_MAP.values())
    try:
        dataframe = ak.stock_news_em(symbol=symbol)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch stock news via AkShare: %s", exc)
        return pd.DataFrame(columns=columns)

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no stock news for %s.", symbol)
        return pd.DataFrame(columns=columns)

    frame = dataframe.copy()
    canonical_map: dict[str, str] = {}
    for source, target in STOCK_NEWS_COLUMN_MAP.items():
        key = "".join(ch for ch in str(source).lower() if ch.isalnum())
        if key and key not in canonical_map:
            canonical_map[key] = target
    rename_map: dict[str, str] = {}
    for column in frame.columns:
        canonical = "".join(ch for ch in str(column).lower() if ch.isalnum())
        target = canonical_map.get(canonical)
        if target and target != column:
            rename_map[column] = target
    if rename_map:
        frame = frame.rename(columns=rename_map)

    for column in columns:
        if column not in frame.columns:
            frame[column] = None

    def _clean_text_local(value: object) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        lowered = text.lower()
        if lowered in {"nan", "none", "null"}:
            return None
        return text

    def _clean_optional_local(value: object) -> Optional[str]:
        text = _clean_text_local(value)
        if text is None:
            return None
        return text

    with pd.option_context("mode.chained_assignment", None):
        frame["title"] = frame["title"].apply(_clean_text_local)
        frame["content"] = frame["content"].apply(_clean_optional_local)
        frame["url"] = frame["url"].apply(_clean_text_local)
        frame["source"] = frame["source"].apply(_clean_optional_local)
        frame["keyword"] = frame["keyword"].apply(_clean_optional_local)
        frame["published_at"] = pd.to_datetime(frame["published_at"], errors="coerce")

    prepared = (
        frame.loc[:, columns]
        .dropna(subset=["title"])
        .drop_duplicates(subset=["url", "title"], keep="last")
        .reset_index(drop=True)
    )
    return prepared

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

STOCK_NEWS_COLUMN_MAP: Final[dict[str, str]] = {
    "关键词": "keyword",
    "keyword": "keyword",
    "新闻标题": "title",
    "title": "title",
    "新闻内容": "content",
    "content": "content",
    "发布时间": "published_at",
    "发布时间(北京时间)": "published_at",
    "publish_time": "published_at",
    "文章来源": "source",
    "source": "source",
    "新闻链接": "url",
    "url": "url",
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

MACRO_LEVERAGE_COLUMN_MAP: Final[dict[str, str]] = {
    "年份": "period_label",
    "居民部门": "household_ratio",
    "非金融企业部门": "non_financial_corporate_ratio",
    "政府部门": "government_ratio",
    "中央政府": "central_government_ratio",
    "地方政府": "local_government_ratio",
    "实体经济部门": "real_economy_ratio",
    "金融部门资产方": "financial_assets_ratio",
    "金融部门负债方": "financial_liabilities_ratio",
}

MACRO_SOCIAL_FINANCING_COLUMN_MAP: Final[dict[str, str]] = {
    "月份": "period_label",
    "社会融资规模增量": "total_financing",
    "其中-人民币贷款": "renminbi_loans",
    "其中-委托贷款外币贷款": "entrusted_and_fx_loans",
    "其中-委托贷款": "entrusted_loans",
    "其中-信托贷款": "trust_loans",
    "其中-未贴现银行承兑汇票": "undiscounted_bankers_acceptance",
    "其中-企业债券": "corporate_bonds",
    "其中-非金融企业境内股票融资": "domestic_equity_financing",
}

MACRO_CPI_COLUMN_MAP: Final[dict[str, str]] = {
    "商品": "category",
    "日期": "period_label",
    "今值": "actual_value",
    "预测值": "forecast_value",
    "前值": "previous_value",
}

MACRO_PMI_COLUMN_MAP: Final[dict[str, str]] = {
    "商品": "category",
    "日期": "period_label",
    "今值": "actual_value",
    "预测值": "forecast_value",
    "前值": "previous_value",
}

MACRO_PPI_COLUMN_MAP: Final[dict[str, str]] = {
    "月份": "period_label",
    "当月": "current_index",
    "当月同比增长": "yoy_change",
    "累计": "cumulative_index",
}

MACRO_PBC_RATE_COLUMN_MAP: Final[dict[str, str]] = {
    "商品": "category",
    "日期": "period_label",
    "今值": "actual_value",
    "预测值": "forecast_value",
    "前值": "previous_value",
}

GLOBAL_FLASH_COLUMN_MAP: Final[dict[str, str]] = {
    "标题": "title",
    "摘要": "summary",
    "发布时间": "published_at",
    "链接": "url",
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


def _empty_macro_leverage_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(MACRO_LEVERAGE_COLUMN_MAP.values()))


def fetch_macro_leverage_ratios() -> pd.DataFrame:
    """Fetch macro leverage ratios from National Finance and Development Lab."""

    try:
        dataframe = ak.macro_cnbs()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch macro leverage ratios via AkShare: %s", exc)
        return _empty_macro_leverage_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no macro leverage ratio data.")
        return _empty_macro_leverage_frame()

    renamed = dataframe.rename(columns=MACRO_LEVERAGE_COLUMN_MAP)
    for column in MACRO_LEVERAGE_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MACRO_LEVERAGE_COLUMN_MAP.values())]


def _empty_macro_social_financing_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(MACRO_SOCIAL_FINANCING_COLUMN_MAP.values()))


class _LegacyTLSAdapter(HTTPAdapter):
    """HTTP adapter that relaxes OpenSSL security level for legacy endpoints."""

    def __init__(self) -> None:
        super().__init__()

    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.set_ciphers("DEFAULT:@SECLEVEL=1")
        kwargs["ssl_context"] = context
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.set_ciphers("DEFAULT:@SECLEVEL=1")
        kwargs["ssl_context"] = context
        return super().proxy_manager_for(*args, **kwargs)


def _fetch_social_financing_with_legacy_tls() -> pd.DataFrame:
    """Fetch social financing data using a relaxed TLS security level."""

    session = requests.Session()
    session.mount("https://", _LegacyTLSAdapter())
    try:
        response = session.post(
            "https://data.mofcom.gov.cn/datamofcom/front/gnmy/shrzgmQuery",
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # pragma: no cover - fallback path
        logger.error("Legacy TLS social financing fetch failed: %s", exc)
        return _empty_macro_social_financing_frame()
    finally:
        session.close()

    if not payload:
        logger.warning("Social financing endpoint returned an empty payload.")
        return _empty_macro_social_financing_frame()

    frame = pd.DataFrame(payload)
    rename_map = {
        "date": "period_label",
        "tiosfs": "total_financing",
        "rmblaon": "renminbi_loans",
        "forcloan": "entrusted_and_fx_loans",
        "entrustloan": "entrusted_loans",
        "trustloan": "trust_loans",
        "ndbab": "undiscounted_bankers_acceptance",
        "bibae": "corporate_bonds",
        "sfinfe": "domestic_equity_financing",
    }
    for column in rename_map:
        if column not in frame.columns:
            frame[column] = None
    renamed = frame.rename(columns=rename_map)
    for column in MACRO_SOCIAL_FINANCING_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None
    result = renamed.loc[:, list(MACRO_SOCIAL_FINANCING_COLUMN_MAP.values())]
    numeric_columns = [col for col in result.columns if col != "period_label"]
    result[numeric_columns] = result[numeric_columns].apply(pd.to_numeric, errors="coerce")
    return result


def fetch_macro_social_financing() -> pd.DataFrame:
    """Fetch social financing incremental statistics from MOFCOM."""

    try:
        dataframe = ak.macro_china_shrzgm()
    except requests.exceptions.SSLError as exc:  # pragma: no cover - legacy TLS fallback
        logger.warning(
            "AkShare social financing fetch hit SSL error; attempting legacy TLS fallback: %s",
            exc,
        )
        dataframe = _fetch_social_financing_with_legacy_tls()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch social financing data via AkShare: %s", exc)
        return _empty_macro_social_financing_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no social financing data.")
        dataframe = _fetch_social_financing_with_legacy_tls()
        if dataframe.empty:
            return _empty_macro_social_financing_frame()

    renamed = dataframe.rename(columns=MACRO_SOCIAL_FINANCING_COLUMN_MAP)
    for column in MACRO_SOCIAL_FINANCING_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MACRO_SOCIAL_FINANCING_COLUMN_MAP.values())]


def _empty_macro_cpi_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(MACRO_CPI_COLUMN_MAP.values()))


def fetch_macro_cpi_monthly() -> pd.DataFrame:
    """Fetch monthly CPI report data from Jin10."""

    try:
        dataframe = ak.macro_china_cpi_monthly()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch CPI data via AkShare: %s", exc)
        return _empty_macro_cpi_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no CPI data.")
        return _empty_macro_cpi_frame()

    renamed = dataframe.rename(columns=MACRO_CPI_COLUMN_MAP)
    for column in MACRO_CPI_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MACRO_CPI_COLUMN_MAP.values())]


def fetch_macro_pmi_yearly() -> pd.DataFrame:
    """Fetch official manufacturing PMI data from Jin10."""

    try:
        dataframe = ak.macro_china_pmi_yearly()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch PMI data via AkShare: %s", exc)
        return pd.DataFrame(columns=list(MACRO_PMI_COLUMN_MAP.values()))

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no PMI data.")
        return pd.DataFrame(columns=list(MACRO_PMI_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=MACRO_PMI_COLUMN_MAP)
    for column in MACRO_PMI_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MACRO_PMI_COLUMN_MAP.values())]


def fetch_macro_non_man_pmi() -> pd.DataFrame:
    """Fetch official non-manufacturing PMI data from Jin10."""

    try:
        dataframe = ak.macro_china_non_man_pmi()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch non-manufacturing PMI data via AkShare: %s", exc)
        return pd.DataFrame(columns=list(MACRO_PMI_COLUMN_MAP.values()))

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no non-manufacturing PMI data.")
        return pd.DataFrame(columns=list(MACRO_PMI_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=MACRO_PMI_COLUMN_MAP)
    for column in MACRO_PMI_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MACRO_PMI_COLUMN_MAP.values())]


def fetch_macro_ppi_monthly() -> pd.DataFrame:
    """Fetch monthly PPI data from Jin10."""

    try:
        dataframe = ak.macro_china_ppi()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch PPI data via AkShare: %s", exc)
        return pd.DataFrame(columns=list(MACRO_PPI_COLUMN_MAP.values()))

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no PPI data.")
        return pd.DataFrame(columns=list(MACRO_PPI_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=MACRO_PPI_COLUMN_MAP)
    for column in MACRO_PPI_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MACRO_PPI_COLUMN_MAP.values())]


def fetch_macro_pbc_interest_rates() -> pd.DataFrame:
    """Fetch People's Bank of China interest rate decision history."""

    try:
        dataframe = ak.macro_bank_china_interest_rate()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch PBC interest rate data via AkShare: %s", exc)
        return pd.DataFrame(columns=list(MACRO_PBC_RATE_COLUMN_MAP.values()))

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no PBC interest rate data.")
        return pd.DataFrame(columns=list(MACRO_PBC_RATE_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=MACRO_PBC_RATE_COLUMN_MAP)
    for column in MACRO_PBC_RATE_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MACRO_PBC_RATE_COLUMN_MAP.values())]


def fetch_global_flash_news() -> pd.DataFrame:
    """Fetch global finance flash headlines from Eastmoney."""

    try:
        dataframe = ak.stock_info_global_em()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch global flash data via AkShare: %s", exc)
        return pd.DataFrame(columns=list(GLOBAL_FLASH_COLUMN_MAP.values()))

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no global flash data.")
        return pd.DataFrame(columns=list(GLOBAL_FLASH_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=GLOBAL_FLASH_COLUMN_MAP)
    for column in GLOBAL_FLASH_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(GLOBAL_FLASH_COLUMN_MAP.values())]


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
        dataframe = _fetch_ths_individual_fund_flow(symbol=str(symbol).strip())
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


THS_INDIVIDUAL_URLS: Final[dict[str, str]] = {
    "即时": "http://data.10jqka.com.cn/funds/ggzjl/field/zdf/order/desc/page/{page}/ajax/1/free/1/",
    "3日排行": "http://data.10jqka.com.cn/funds/ggzjl/board/3/field/zdf/order/desc/page/{page}/ajax/1/free/1/",
    "5日排行": "http://data.10jqka.com.cn/funds/ggzjl/board/5/field/zdf/order/desc/page/{page}/ajax/1/free/1/",
    "10日排行": "http://data.10jqka.com.cn/funds/ggzjl/board/10/field/zdf/order/desc/page/{page}/ajax/1/free/1/",
    "20日排行": "http://data.10jqka.com.cn/funds/ggzjl/board/20/field/zdf/order/desc/page/{page}/ajax/1/free/1/",
}
THS_REFERER = "http://data.10jqka.com.cn/funds/ggzjl/"
THS_MAX_PAGES = 150
THS_REQUEST_TIMEOUT = 12
THS_MAX_RETRIES = 3


def _fetch_ths_individual_fund_flow(symbol: str) -> pd.DataFrame:
    url_template = THS_INDIVIDUAL_URLS.get(symbol, THS_INDIVIDUAL_URLS["即时"])
    session = requests.Session()
    frames: list[pd.DataFrame] = []

    first_html = _request_ths_page(session, url_template, 1)
    if not first_html:
        return _empty_individual_fund_flow_frame()
    first_df = _parse_ths_individual_table(symbol, first_html)
    if first_df is None or first_df.empty:
        return _empty_individual_fund_flow_frame()
    frames.append(first_df)

    page_count = _extract_ths_page_count(first_html)
    max_pages = page_count or THS_MAX_PAGES
    for page in range(2, max_pages + 1):
        html = _request_ths_page(session, url_template, page)
        if not html:
            break
        df = _parse_ths_individual_table(symbol, html)
        if df is None or df.empty:
            break
        frames.append(df)
        if page_count is None and page >= THS_MAX_PAGES:
            break
        time.sleep(0.15)

    if not frames:
        return _empty_individual_fund_flow_frame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["股票代码"], keep="first")
    combined.insert(0, "序号", range(1, len(combined) + 1))
    return combined


def _extract_ths_page_count(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "lxml")
    span = soup.find("span", class_="page_info")
    if span and span.text and "/" in span.text:
        try:
            return int(span.text.split("/")[-1])
        except ValueError:
            pass
    match = re.search(r"page_info\">\\s*\\d+/(\\d+)", html)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _request_ths_page(session: requests.Session, url_template: str, page: int) -> Optional[str]:
    last_error: Exception | None = None
    for attempt in range(THS_MAX_RETRIES):
        headers = _build_ths_headers()
        try:
            response = session.get(
                url_template.format(page=page),
                headers=headers,
                timeout=THS_REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(0.3 * (attempt + 1))
            continue
        if response.status_code == 200 and "Nginx forbidden" not in response.text:
            return response.text
        last_error = RuntimeError(f"HTTP {response.status_code}")
        time.sleep(0.3 * (attempt + 1))
    if last_error:
        logger.warning("THS individual fund flow request failed for page %s: %s", page, last_error)
    return None


def _build_ths_headers() -> dict[str, str]:
    return {
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "hexin-v": _generate_ths_hexin(),
        "Host": "data.10jqka.com.cn",
        "Pragma": "no-cache",
        "Referer": THS_REFERER,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


@lru_cache(maxsize=1)
def _ths_js_content() -> str:
    with open(get_ths_js("ths.js"), encoding="utf-8") as handle:
        return handle.read()


def _generate_ths_hexin() -> str:
    js_code = py_mini_racer.MiniRacer()
    js_code.eval(_ths_js_content())
    return js_code.call("v")


def _parse_ths_individual_table(symbol: str, html: str) -> Optional[pd.DataFrame]:
    try:
        frames = pd.read_html(StringIO(html))
    except ValueError:
        return None
    if not frames:
        return None
    frame = frames[0]
    if frame.empty:
        return frame
    expected_columns = [
        "序号",
        "股票代码",
        "股票简称",
        "最新价",
    ]
    if symbol == "即时":
        expected_columns += ["涨跌幅", "换手率", "流入资金", "流出资金", "净额", "成交额"]
    else:
        expected_columns += ["阶段涨跌幅", "连续换手率", "资金流入净额"]
    missing = [col for col in expected_columns if col not in frame.columns]
    for col in missing:
        frame[col] = None
    frame = frame[expected_columns]
    frame = frame[expected_columns]
    frame["序号"] = pd.to_numeric(frame["序号"], errors="coerce").astype("Int64")
    return frame


def _empty_continuous_volume_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(CONTINUOUS_VOLUME_COLUMN_MAP.values()))


def fetch_stock_rank_cxfl_ths() -> pd.DataFrame:
    """Fetch Tonghuashun continuous volume ranking snapshot."""
    try:
        dataframe = ak.stock_rank_cxfl_ths()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch continuous volume ranking via AkShare: %s", exc)
        return _empty_continuous_volume_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no continuous volume ranking data.")
        return _empty_continuous_volume_frame()

    renamed = dataframe.rename(columns=CONTINUOUS_VOLUME_COLUMN_MAP)
    for column in CONTINUOUS_VOLUME_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    renamed["stock_code"] = renamed["stock_code"].astype(str).str.zfill(6)
    return renamed.loc[:, list(CONTINUOUS_VOLUME_COLUMN_MAP.values())]


def _empty_volume_price_rise_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(VOLUME_PRICE_RISE_COLUMN_MAP.values()))


def fetch_stock_rank_ljqs_ths() -> pd.DataFrame:
    """Fetch Tonghuashun volume-price rise ranking snapshot."""
    try:
        dataframe = ak.stock_rank_ljqs_ths()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch volume-price rise ranking via AkShare: %s", exc)
        return _empty_volume_price_rise_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no volume-price rise ranking data.")
        return _empty_volume_price_rise_frame()

    renamed = dataframe.rename(columns=VOLUME_PRICE_RISE_COLUMN_MAP)
    for column in VOLUME_PRICE_RISE_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    renamed["stock_code"] = renamed["stock_code"].astype(str).str.zfill(6)
    return renamed.loc[:, list(VOLUME_PRICE_RISE_COLUMN_MAP.values())]


def _empty_upward_breakout_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(UPWARD_BREAKOUT_COLUMN_MAP.values()))


def fetch_stock_rank_xstp_ths(symbol: str = "500日均线") -> pd.DataFrame:
    """Fetch Tonghuashun upward breakout ranking snapshot for the specified moving average window."""
    try:
        dataframe = ak.stock_rank_xstp_ths(symbol=symbol)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch upward breakout ranking via AkShare: %s", exc)
        return _empty_upward_breakout_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no upward breakout ranking data for symbol %s.", symbol)
        return _empty_upward_breakout_frame()

    renamed = dataframe.rename(columns=UPWARD_BREAKOUT_COLUMN_MAP)
    for column in UPWARD_BREAKOUT_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    renamed["stock_code"] = renamed["stock_code"].astype(str).str.zfill(6)
    return renamed.loc[:, list(UPWARD_BREAKOUT_COLUMN_MAP.values())]


def _empty_continuous_rise_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(CONTINUOUS_RISE_COLUMN_MAP.values()))


def fetch_stock_rank_lxsz_ths() -> pd.DataFrame:
    """Fetch Tonghuashun continuous rise ranking snapshot."""
    try:
        dataframe = ak.stock_rank_lxsz_ths()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch continuous rise ranking via AkShare: %s", exc)
        return _empty_continuous_rise_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no continuous rise ranking data.")
        return _empty_continuous_rise_frame()

    renamed = dataframe.rename(columns=CONTINUOUS_RISE_COLUMN_MAP)
    for column in CONTINUOUS_RISE_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    renamed["stock_code"] = renamed["stock_code"].astype(str).str.zfill(6)
    return renamed.loc[:, list(CONTINUOUS_RISE_COLUMN_MAP.values())]


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


def _empty_hsgt_fund_flow_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(HSGT_FUND_FLOW_COLUMN_MAP.values()))


def fetch_hsgt_fund_flow_history(symbol: str = "北向资金") -> pd.DataFrame:
    """Fetch Eastmoney HSGT historical fund flow data for the specified symbol."""
    try:
        dataframe = ak.stock_hsgt_hist_em(symbol=symbol)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch HSGT fund flow history via AkShare: %s", exc)
        return _empty_hsgt_fund_flow_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no HSGT fund flow history for %s", symbol)
        return _empty_hsgt_fund_flow_frame()

    renamed = dataframe.rename(columns=HSGT_FUND_FLOW_COLUMN_MAP)
    for column in HSGT_FUND_FLOW_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(HSGT_FUND_FLOW_COLUMN_MAP.values())]


def _empty_hsgt_fund_flow_summary_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "trade_date",
            "channel_type",
            "board_name",
            "funds_direction",
            "trading_status",
            "net_buy_amount",
            "fund_inflow",
            "balance",
            "rising_count",
            "flat_count",
            "falling_count",
            "index_name",
            "index_change_percent",
        ]
    )


def fetch_hsgt_fund_flow_summary() -> pd.DataFrame:
    """Fetch the latest HSGT fund flow summary snapshot."""
    try:
        dataframe = ak.stock_hsgt_fund_flow_summary_em()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch HSGT fund flow summary via AkShare: %s", exc)
        return _empty_hsgt_fund_flow_summary_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no HSGT fund flow summary data.")
        return _empty_hsgt_fund_flow_summary_frame()

    renamed = dataframe.rename(
        columns={
            "交易日": "trade_date",
            "类型": "channel_type",
            "板块": "board_name",
            "资金方向": "funds_direction",
            "交易状态": "trading_status",
            "成交净买额": "net_buy_amount",
            "资金净流入": "fund_inflow",
            "当日资金余额": "balance",
            "上涨数": "rising_count",
            "持平数": "flat_count",
            "下跌数": "falling_count",
            "相关指数": "index_name",
            "指数涨跌幅": "index_change_percent",
        }
    )

    renamed["trade_date"] = pd.to_datetime(renamed["trade_date"], errors="coerce").dt.date

    for column in [
        "net_buy_amount",
        "fund_inflow",
        "balance",
        "rising_count",
        "flat_count",
        "falling_count",
        "index_change_percent",
    ]:
        if column in renamed.columns:
            renamed[column] = pd.to_numeric(renamed[column], errors="coerce")

    return renamed.loc[
        :,
        [
            "trade_date",
            "channel_type",
            "board_name",
            "funds_direction",
            "trading_status",
            "net_buy_amount",
            "fund_inflow",
            "balance",
            "rising_count",
            "flat_count",
            "falling_count",
            "index_name",
            "index_change_percent",
        ],
    ]


def fetch_margin_account_info() -> pd.DataFrame:
    """Fetch Eastmoney margin (融资融券) account statistics."""
    try:
        dataframe = ak.stock_margin_account_info()
    except Exception as exc:  # pragma: no cover - network call
        logger.error("Failed to fetch margin account info via AkShare: %s", exc)
        return pd.DataFrame(columns=list(MARGIN_ACCOUNT_COLUMN_MAP.values()))

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no margin account info data.")
        return pd.DataFrame(columns=list(MARGIN_ACCOUNT_COLUMN_MAP.values()))

    renamed = dataframe.rename(columns=MARGIN_ACCOUNT_COLUMN_MAP)
    for column in MARGIN_ACCOUNT_COLUMN_MAP.values():
        if column not in renamed.columns:
            renamed[column] = None

    return renamed.loc[:, list(MARGIN_ACCOUNT_COLUMN_MAP.values())]

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
    "CONTINUOUS_VOLUME_COLUMN_MAP",
    "VOLUME_PRICE_RISE_COLUMN_MAP",
    "BIG_DEAL_FUND_FLOW_COLUMN_MAP",
    "HSGT_FUND_FLOW_COLUMN_MAP",
    "MARGIN_ACCOUNT_COLUMN_MAP",
    "STOCK_MAIN_BUSINESS_COLUMN_MAP",
    "STOCK_MAIN_COMPOSITION_COLUMN_MAP",
    "MACRO_LEVERAGE_COLUMN_MAP",
    "MACRO_SOCIAL_FINANCING_COLUMN_MAP",
    "MACRO_CPI_COLUMN_MAP",
    "MACRO_PMI_COLUMN_MAP",
    "MACRO_PPI_COLUMN_MAP",
    "MACRO_PBC_RATE_COLUMN_MAP",
    "GLOBAL_FLASH_COLUMN_MAP",
    "fetch_finance_breakfast",
    "fetch_performance_express_em",
    "fetch_performance_forecast_em",
    "fetch_industry_fund_flow",
    "fetch_concept_fund_flow",
    "fetch_individual_fund_flow",
    "fetch_stock_rank_cxfl_ths",
    "fetch_stock_rank_xstp_ths",
    "fetch_stock_rank_lxsz_ths",
    "fetch_stock_rank_ljqs_ths",
    "fetch_big_deal_fund_flow",
    "fetch_hsgt_fund_flow_history",
    "fetch_hsgt_fund_flow_summary",
    "fetch_margin_account_info",
    "fetch_stock_main_business",
    "fetch_stock_main_composition",
    "fetch_macro_leverage_ratios",
    "fetch_macro_social_financing",
    "fetch_macro_cpi_monthly",
    "fetch_macro_pmi_yearly",
    "fetch_macro_non_man_pmi",
    "fetch_macro_ppi_monthly",
    "fetch_macro_pbc_interest_rates",
    "fetch_global_flash_news",
    "fetch_market_activity_legu",
    "MARKET_FUND_FLOW_COLUMN_MAP",
    "fetch_market_fund_flow",
]
