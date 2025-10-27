"""
Service layer for synchronising Tushare stock basic data into PostgreSQL.
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime
from typing import Dict, Optional, Sequence

from ..api_clients import fetch_stock_basic
from ..config.runtime_config import load_runtime_config
from ..config.settings import AppSettings, load_settings
from ..dao import (
    DailyIndicatorDAO,
    DailyTradeDAO,
    DailyTradeMetricsDAO,
    FinancialIndicatorDAO,
    FundamentalMetricsDAO,
    IncomeStatementDAO,
    FavoriteStockDAO,
    StockBasicDAO,
)

logger = logging.getLogger(__name__)


def _resolve_token(token: str | None, settings: AppSettings) -> str:
    resolved = token or settings.tushare.token
    if not resolved:
        raise RuntimeError(
            "Tushare token is required. Update the configuration file or pass it explicitly."
        )
    return resolved


def sync_stock_basic(
    token: str | None = None,
    list_statuses: Sequence[str] = ("L", "D"),
    market: str | None = None,
    settings_path: str | None = None,
) -> int:
    """
    Fetch A-share stock basics from Tushare and upsert them into PostgreSQL.

    Args:
        token: Optional explicit API token override.
        list_statuses: Sequence of list statuses to request from Tushare.
        market: Optional Tushare ``market`` filter.
        settings_path: Optional path override for the JSON settings file.
    """
    settings = load_settings(settings_path)
    resolved_token = _resolve_token(token, settings)

    dataframe = fetch_stock_basic(
        token=resolved_token,
        list_statuses=list_statuses,
        market=market,
    )

    if dataframe.empty:
        logger.warning("No stock_basic data retrieved; nothing to store.")
        return 0

    dao = StockBasicDAO(settings.postgres)
    logger.info("Clearing existing stock_basic rows before insert")
    dao.clear_table()

    logger.info(
        "Inserting %s stock_basic rows into %s.%s",
        len(dataframe),
        settings.postgres.schema,
        settings.postgres.stock_table,
    )

    affected = dao.upsert(dataframe)
    logger.info("Insert completed, affected rows: %s", affected)
    return affected


def get_stock_overview(
    *,
    keyword: str | None = None,
    market: str | None = None,
    industry: str | None = None,
    exchange: str | None = None,
    limit: Optional[int] = 50,
    offset: int = 0,
    codes: Optional[Sequence[str]] = None,
    favorites_only: bool = False,
    favorite_group: Optional[str] = None,
    favorite_group_specified: bool = False,
    settings_path: str | None = None,
) -> dict[str, object]:
    """
    Retrieve stock fundamentals enriched with latest trading metrics.
    """
    settings = load_settings(settings_path)
    stock_dao = StockBasicDAO(settings.postgres)
    favorites_dao = FavoriteStockDAO(settings.postgres)
    runtime_config = load_runtime_config()
    favorite_entries = favorites_dao.list_entries()
    favorite_code_map: Dict[str, Optional[str]] = {
        entry["code"]: entry.get("group") for entry in favorite_entries
    }
    favorite_code_set = set(favorite_code_map.keys())
    favorite_codes_filter: Optional[Sequence[str]] = None

    if codes is None:
        if favorite_group_specified:
            if favorite_group is None:
                filtered_codes = [
                    code for code, group in favorite_code_map.items() if not group
                ]
            else:
                filtered_codes = [
                    code
                    for code, group in favorite_code_map.items()
                    if group == favorite_group
                ]
            if favorites_only or favorite_group_specified:
                if not filtered_codes:
                    return {"total": 0, "items": []}
                favorite_codes_filter = filtered_codes
        elif favorites_only:
            if not favorite_code_set:
                return {"total": 0, "items": []}
            favorite_codes_filter = list(favorite_code_set)

    result = stock_dao.query_fundamentals(
        keyword=keyword,
        market=market,
        industry=industry,
        exchange=exchange,
        include_st=runtime_config.include_st,
        include_delisted=runtime_config.include_delisted,
        limit=limit,
        offset=offset,
        codes=codes or favorite_codes_filter,
    )

    codes = [item["code"] for item in result["items"]]
    daily_dao = DailyTradeDAO(settings.postgres)
    metrics: Dict[str, dict] = daily_dao.fetch_latest_metrics(codes)
    indicator_dao = DailyIndicatorDAO(settings.postgres)
    indicators: Dict[str, dict] = indicator_dao.fetch_latest_indicators(codes)
    derived_metrics = DailyTradeMetricsDAO(settings.postgres).fetch_metrics(codes)
    fundamental_metrics = FundamentalMetricsDAO(settings.postgres).fetch_metrics(codes)
    income_dao = IncomeStatementDAO(settings.postgres)
    income_statements = income_dao.fetch_latest_statements(codes)
    financial_dao = FinancialIndicatorDAO(settings.postgres)
    financials = financial_dao.fetch_latest_indicators(codes)

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

    def _format_date(value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, date):
            return value.isoformat()
        text = str(value).strip()
        return text or None

    for item in result["items"]:
        metric = metrics.get(item["code"], {})
        item["last_price"] = _safe_float(metric.get("last_price"))
        item["pct_change"] = _safe_float(metric.get("pct_change"))
        item["volume"] = _safe_float(metric.get("volume"))
        item["trade_date"] = metric.get("trade_date")
        indicator = indicators.get(item["code"], {})
        item["market_cap"] = _safe_float(indicator.get("market_cap"))
        item["pe_ratio"] = _safe_float(indicator.get("pe"))
        item["turnover_rate"] = _safe_float(indicator.get("turnover_rate"))
        derived = derived_metrics.get(item["code"], {})
        item["pct_change_1y"] = _safe_float(derived.get("pct_change_1y"))
        item["pct_change_6m"] = _safe_float(derived.get("pct_change_6m"))
        item["pct_change_3m"] = _safe_float(derived.get("pct_change_3m"))
        item["pct_change_1m"] = _safe_float(derived.get("pct_change_1m"))
        item["pct_change_2w"] = _safe_float(derived.get("pct_change_2w"))
        item["pct_change_1w"] = _safe_float(derived.get("pct_change_1w"))
        item["ma_20"] = _safe_float(derived.get("ma_20"))
        item["ma_10"] = _safe_float(derived.get("ma_10"))
        item["ma_5"] = _safe_float(derived.get("ma_5"))
        spike_value = _safe_float(derived.get("volume_spike"))
        item["volume_spike"] = spike_value
        item["volumeSpike"] = spike_value
        income = income_statements.get(item["code"], {})
        ann_date = income.get("ann_date")
        end_date = income.get("end_date")
        item["ann_date"] = _format_date(ann_date)
        item["end_date"] = _format_date(end_date)
        item["basic_eps"] = _safe_float(income.get("basic_eps"))
        item["revenue"] = _safe_float(income.get("revenue"))
        item["operate_profit"] = _safe_float(income.get("operate_profit"))
        item["net_income"] = _safe_float(income.get("n_income"))
        financial = financials.get(item["code"], {})
        if item["ann_date"] is None:
            item["ann_date"] = _format_date(financial.get("ann_date"))
        if item["end_date"] is None:
            item["end_date"] = _format_date(financial.get("end_date"))
        item["gross_margin"] = _safe_float(financial.get("gross_margin"))
        item["roe"] = _safe_float(financial.get("roe"))
        fundamental = fundamental_metrics.get(item["code"], {})
        item["net_income_yoy_latest"] = _safe_float(fundamental.get("net_income_yoy_latest"))
        item["net_income_yoy_prev1"] = _safe_float(fundamental.get("net_income_yoy_prev1"))
        item["net_income_yoy_prev2"] = _safe_float(fundamental.get("net_income_yoy_prev2"))
        item["net_income_qoq_latest"] = _safe_float(fundamental.get("net_income_qoq_latest"))
        item["revenue_yoy_latest"] = _safe_float(fundamental.get("revenue_yoy_latest"))
        item["revenue_qoq_latest"] = _safe_float(fundamental.get("revenue_qoq_latest"))
        item["roe_yoy_latest"] = _safe_float(fundamental.get("roe_yoy_latest"))
        item["roe_qoq_latest"] = _safe_float(fundamental.get("roe_qoq_latest"))
        item["is_favorite"] = item["code"] in favorite_code_set
        item["favorite_group"] = favorite_code_map.get(item["code"])

    return result


def get_stock_detail(
    code: str,
    *,
    history_limit: int = 180,
    settings_path: str | None = None,
) -> Optional[dict[str, object]]:
    if not code:
        return None
    overview = get_stock_overview(
        codes=[code],
        limit=None,
        offset=0,
        settings_path=settings_path,
    )
    if not overview["items"]:
        return None
    item = overview["items"][0]

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

    settings = load_settings(settings_path)
    daily_trade_dao = DailyTradeDAO(settings.postgres)
    favorites_dao = FavoriteStockDAO(settings.postgres)
    favorite_entry = favorites_dao.get_entry(code)
    is_favorite = favorite_entry is not None
    favorite_group = favorite_entry.get("group") if favorite_entry else None
    history_rows = daily_trade_dao.fetch_price_history(code, limit=history_limit)
    candles: list[dict[str, object]] = []
    for row in history_rows:
        open_price = _safe_float(row.get("open"))
        high_price = _safe_float(row.get("high"))
        low_price = _safe_float(row.get("low"))
        close_price = _safe_float(row.get("close"))
        if None in (open_price, high_price, low_price, close_price):
            continue
        candles.append(
            {
                "time": row.get("trade_date"),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": _safe_float(row.get("volume")),
            }
        )

    trading_data = {
        "code": item["code"],
        "name": item.get("name"),
        "industry": item.get("industry"),
        "market": item.get("market"),
        "exchange": item.get("exchange"),
        "lastPrice": item.get("last_price"),
        "pctChange": item.get("pct_change"),
        "volume": item.get("volume"),
        "marketCap": item.get("market_cap"),
        "peRatio": item.get("pe_ratio"),
        "turnoverRate": item.get("turnover_rate"),
    }

    financial_data = {
        "code": item["code"],
        "name": item.get("name"),
        "annDate": item.get("ann_date"),
        "endDate": item.get("end_date"),
        "basicEps": item.get("basic_eps"),
        "revenue": item.get("revenue"),
        "operateProfit": item.get("operate_profit"),
        "netIncome": item.get("net_income"),
        "grossMargin": item.get("gross_margin"),
        "roe": item.get("roe"),
    }

    trading_stats = {
        "code": item["code"],
        "name": item.get("name"),
        "pctChange1Y": item.get("pct_change_1y"),
        "pctChange6M": item.get("pct_change_6m"),
        "pctChange3M": item.get("pct_change_3m"),
        "pctChange1M": item.get("pct_change_1m"),
        "pctChange2W": item.get("pct_change_2w"),
        "pctChange1W": item.get("pct_change_1w"),
        "volumeSpike": item.get("volume_spike"),
        "ma20": item.get("ma_20"),
        "ma10": item.get("ma_10"),
        "ma5": item.get("ma_5"),
    }

    financial_stats = {
        "code": item["code"],
        "name": item.get("name"),
        "reportingPeriod": item.get("end_date"),
        "netIncomeYoyLatest": item.get("net_income_yoy_latest"),
        "netIncomeYoyPrev1": item.get("net_income_yoy_prev1"),
        "netIncomeYoyPrev2": item.get("net_income_yoy_prev2"),
        "netIncomeQoqLatest": item.get("net_income_qoq_latest"),
        "revenueYoyLatest": item.get("revenue_yoy_latest"),
        "revenueQoqLatest": item.get("revenue_qoq_latest"),
        "roeYoyLatest": item.get("roe_yoy_latest"),
        "roeQoqLatest": item.get("roe_qoq_latest"),
    }

    profile = {
        "code": item["code"],
        "name": item.get("name"),
        "industry": item.get("industry"),
        "market": item.get("market"),
        "exchange": item.get("exchange"),
        "status": item.get("status"),
        "lastPrice": item.get("last_price"),
        "pctChange": item.get("pct_change"),
        "volume": item.get("volume"),
        "tradeDate": item.get("trade_date"),
        "marketCap": item.get("market_cap"),
        "peRatio": item.get("pe_ratio"),
        "turnoverRate": item.get("turnover_rate"),
        "volumeSpike": item.get("volume_spike"),
        "isFavorite": is_favorite,
        "favoriteGroup": favorite_group,
    }

    return {
        "profile": profile,
        "tradingData": trading_data,
        "financialData": financial_data,
        "tradingStats": trading_stats,
        "financialStats": financial_stats,
        "dailyTradeHistory": candles,
        "isFavorite": is_favorite,
        "favoriteGroup": favorite_group,
    }


__all__ = [
    "sync_stock_basic",
    "get_stock_overview",
    "get_stock_detail",
]




