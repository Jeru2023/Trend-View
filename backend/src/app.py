"""
FastAPI application exposing Trend View backend services and control panel APIs.
"""

from __future__ import annotations

import asyncio
import math
import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

from .config.runtime_config import RuntimeConfig, load_runtime_config, save_runtime_config
from .config.settings import load_settings
from .dao import (
    DailyIndicatorDAO,
    DailyTradeDAO,
    DailyTradeMetricsDAO,
    FinancialIndicatorDAO,
    FinanceBreakfastDAO,
    IncomeStatementDAO,
    FundamentalMetricsDAO,
    PerformanceExpressDAO,
    PerformanceForecastDAO,
    ProfitForecastDAO,
    GlobalIndexDAO,
    DollarIndexDAO,
    RmbMidpointDAO,
    FuturesRealtimeDAO,
    IndustryFundFlowDAO,
    ConceptFundFlowDAO,
    IndividualFundFlowDAO,
    BigDealFundFlowDAO,
    StockBasicDAO,
    StockMainBusinessDAO,
    StockMainCompositionDAO,
)
from .services import (
    get_stock_detail,
    get_stock_overview,
    get_favorite_status,
    list_favorite_entries,
    list_favorite_groups,
    list_finance_breakfast,
    list_fundamental_metrics,
    list_performance_express,
    list_performance_forecast,
    list_profit_forecast,
    list_global_indices,
    list_dollar_index,
    list_rmb_midpoint_rates,
    list_futures_realtime,
    list_industry_fund_flow,
    list_concept_fund_flow,
    list_individual_fund_flow,
    list_big_deal_fund_flow,
    set_favorite_state,
    sync_daily_indicator,
    sync_financial_indicators,
    sync_finance_breakfast,
    sync_income_statements,
    sync_daily_trade,
    sync_daily_trade_metrics,
    sync_fundamental_metrics,
    sync_performance_express,
    sync_performance_forecast,
    sync_profit_forecast,
    sync_global_indices,
    sync_dollar_index,
    sync_rmb_midpoint_rates,
    sync_futures_realtime,
    sync_industry_fund_flow,
    sync_concept_fund_flow,
    sync_individual_fund_flow,
    sync_big_deal_fund_flow,
    sync_stock_basic,
    sync_stock_main_business,
    sync_stock_main_composition,
    get_stock_main_composition,
)
from .state import monitor

scheduler = AsyncIOScheduler(timezone=ZoneInfo("Asia/Shanghai"))
logger = logging.getLogger(__name__)

FAVORITE_GROUP_NONE_SENTINEL = "__ungrouped__"
MAX_FAVORITE_GROUP_LENGTH = 64


def _validate_favorite_group_name(value: Optional[str]) -> Optional[str]:
    """Validate favorite group names from query params or payloads."""
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if len(normalized) > MAX_FAVORITE_GROUP_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Favorite group name must be at most {MAX_FAVORITE_GROUP_LENGTH} characters.",
        )
    if any(ord(char) < 32 for char in normalized):
        raise HTTPException(
            status_code=400,
            detail="Favorite group name contains invalid control characters.",
        )
    return normalized


def _parse_favorite_group_query(value: Optional[str]) -> tuple[Optional[str], bool]:
    """Normalize favorite group query parameter."""
    if value is None:
        return None, False
    if value == FAVORITE_GROUP_NONE_SENTINEL:
        return None, True
    normalized = value.strip()
    if not normalized:
        return None, True
    return _validate_favorite_group_name(normalized), True


class StockItem(BaseModel):
    code: str
    name: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    exchange: Optional[str] = None
    status: Optional[str] = None
    last_price: Optional[float] = Field(None, alias="lastPrice")
    pct_change: Optional[float] = Field(None, alias="pctChange")
    volume: Optional[float] = None
    trade_date: Optional[date] = Field(None, alias="tradeDate")
    market_cap: Optional[float] = Field(None, alias="marketCap")
    pe_ratio: Optional[float] = Field(None, alias="peRatio")
    turnover_rate: Optional[float] = Field(None, alias="turnoverRate")
    pct_change_1y: Optional[float] = Field(None, alias="pctChange1Y")
    pct_change_6m: Optional[float] = Field(None, alias="pctChange6M")
    pct_change_3m: Optional[float] = Field(None, alias="pctChange3M")
    pct_change_1m: Optional[float] = Field(None, alias="pctChange1M")
    pct_change_2w: Optional[float] = Field(None, alias="pctChange2W")
    pct_change_1w: Optional[float] = Field(None, alias="pctChange1W")
    ma_20: Optional[float] = Field(None, alias="ma20")
    ma_10: Optional[float] = Field(None, alias="ma10")
    ma_5: Optional[float] = Field(None, alias="ma5")
    volume_spike: Optional[float] = Field(None, alias="volumeSpike")
    ann_date: Optional[str] = Field(None, alias="annDate")
    end_date: Optional[str] = Field(None, alias="endDate")
    basic_eps: Optional[float] = Field(None, alias="basicEps")
    revenue: Optional[float] = None
    operate_profit: Optional[float] = Field(None, alias="operateProfit")
    net_income: Optional[float] = Field(None, alias="netIncome")
    gross_margin: Optional[float] = Field(None, alias="grossMargin")
    roe: Optional[float] = Field(None, alias="roe")
    net_income_yoy_latest: Optional[float] = Field(None, alias="netIncomeYoyLatest")
    net_income_yoy_prev1: Optional[float] = Field(None, alias="netIncomeYoyPrev1")
    net_income_yoy_prev2: Optional[float] = Field(None, alias="netIncomeYoyPrev2")
    net_income_qoq_latest: Optional[float] = Field(None, alias="netIncomeQoqLatest")
    revenue_yoy_latest: Optional[float] = Field(None, alias="revenueYoyLatest")
    revenue_qoq_latest: Optional[float] = Field(None, alias="revenueQoqLatest")
    roe_yoy_latest: Optional[float] = Field(None, alias="roeYoyLatest")
    roe_qoq_latest: Optional[float] = Field(None, alias="roeQoqLatest")

    class Config:
        allow_population_by_field_name = True


class StockTradingSnapshot(BaseModel):
    code: str
    name: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    exchange: Optional[str] = None
    last_price: Optional[float] = Field(None, alias="lastPrice")
    pct_change: Optional[float] = Field(None, alias="pctChange")
    volume: Optional[float] = None
    market_cap: Optional[float] = Field(None, alias="marketCap")
    pe_ratio: Optional[float] = Field(None, alias="peRatio")
    turnover_rate: Optional[float] = Field(None, alias="turnoverRate")

    class Config:
        allow_population_by_field_name = True


class StockFinancialSnapshot(BaseModel):
    code: str
    name: Optional[str] = None
    ann_date: Optional[str] = Field(None, alias="annDate")
    end_date: Optional[str] = Field(None, alias="endDate")
    basic_eps: Optional[float] = Field(None, alias="basicEps")
    revenue: Optional[float] = None
    operate_profit: Optional[float] = Field(None, alias="operateProfit")
    net_income: Optional[float] = Field(None, alias="netIncome")
    gross_margin: Optional[float] = Field(None, alias="grossMargin")
    roe: Optional[float] = Field(None, alias="roe")

    class Config:
        allow_population_by_field_name = True


class StockTradingStats(BaseModel):
    code: str
    name: Optional[str] = None
    pct_change_1y: Optional[float] = Field(None, alias="pctChange1Y")
    pct_change_6m: Optional[float] = Field(None, alias="pctChange6M")
    pct_change_3m: Optional[float] = Field(None, alias="pctChange3M")
    pct_change_1m: Optional[float] = Field(None, alias="pctChange1M")
    pct_change_2w: Optional[float] = Field(None, alias="pctChange2W")
    pct_change_1w: Optional[float] = Field(None, alias="pctChange1W")
    volume_spike: Optional[float] = Field(None, alias="volumeSpike")
    ma_20: Optional[float] = Field(None, alias="ma20")
    ma_10: Optional[float] = Field(None, alias="ma10")
    ma_5: Optional[float] = Field(None, alias="ma5")

    class Config:
        allow_population_by_field_name = True


class StockFinancialStats(BaseModel):
    code: str
    name: Optional[str] = None
    reporting_period: Optional[str] = Field(None, alias="reportingPeriod")
    net_income_yoy_latest: Optional[float] = Field(None, alias="netIncomeYoyLatest")
    net_income_yoy_prev1: Optional[float] = Field(None, alias="netIncomeYoyPrev1")
    net_income_yoy_prev2: Optional[float] = Field(None, alias="netIncomeYoyPrev2")
    net_income_qoq_latest: Optional[float] = Field(None, alias="netIncomeQoqLatest")
    revenue_yoy_latest: Optional[float] = Field(None, alias="revenueYoyLatest")
    revenue_qoq_latest: Optional[float] = Field(None, alias="revenueQoqLatest")
    roe_yoy_latest: Optional[float] = Field(None, alias="roeYoyLatest")
    roe_qoq_latest: Optional[float] = Field(None, alias="roeQoqLatest")
    is_favorite: bool = Field(False, alias="isFavorite")
    favorite_group: Optional[str] = Field(None, alias="favoriteGroup")

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True


class StockBusinessProfile(BaseModel):
    symbol: Optional[str] = None
    ts_code: Optional[str] = Field(None, alias="tsCode")
    main_business: Optional[str] = Field(None, alias="mainBusiness")
    product_type: Optional[str] = Field(None, alias="productType")
    product_name: Optional[str] = Field(None, alias="productName")
    business_scope: Optional[str] = Field(None, alias="businessScope")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True


class StockBusinessCompositionEntry(BaseModel):
    report_date: Optional[str] = Field(None, alias="reportDate")
    category_type: Optional[str] = Field(None, alias="categoryType")
    composition: Optional[str] = None
    revenue: Optional[float] = None
    revenue_ratio: Optional[float] = Field(None, alias="revenueRatio")
    cost: Optional[float] = None
    cost_ratio: Optional[float] = Field(None, alias="costRatio")
    profit: Optional[float] = None
    profit_ratio: Optional[float] = Field(None, alias="profitRatio")
    gross_margin: Optional[float] = Field(None, alias="grossMargin")

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True


class StockBusinessCompositionGroup(BaseModel):
    category_type: Optional[str] = Field(None, alias="categoryType")
    entries: List[StockBusinessCompositionEntry]

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True


class StockBusinessComposition(BaseModel):
    symbol: Optional[str] = None
    latest_report_date: Optional[str] = Field(None, alias="latestReportDate")
    groups: List[StockBusinessCompositionGroup]

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True


class DailyTradeBar(BaseModel):
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None


class StockDetailResponse(BaseModel):
    profile: StockItem
    trading_data: StockTradingSnapshot = Field(..., alias="tradingData")
    financial_data: StockFinancialSnapshot = Field(..., alias="financialData")
    trading_stats: StockTradingStats = Field(..., alias="tradingStats")
    financial_stats: StockFinancialStats = Field(..., alias="financialStats")
    business_profile: Optional[StockBusinessProfile] = Field(None, alias="businessProfile")
    business_composition: Optional[StockBusinessComposition] = Field(None, alias="businessComposition")
    daily_trade_history: List[DailyTradeBar] = Field(..., alias="dailyTradeHistory")
    is_favorite: bool = Field(False, alias="isFavorite")
    favorite_group: Optional[str] = Field(None, alias="favoriteGroup")

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True


class StockListResponse(BaseModel):
    total: int
    items: List[StockItem]
    industries: List[str] = Field(default_factory=list)

SORTABLE_STOCK_FIELDS: dict[str, str] = {
    "pctchange1y": "pct_change_1y",
    "pct_change_1y": "pct_change_1y",
    "pctchange6m": "pct_change_6m",
    "pct_change_6m": "pct_change_6m",
    "pctchange3m": "pct_change_3m",
    "pct_change_3m": "pct_change_3m",
    "pctchange1m": "pct_change_1m",
    "pct_change_1m": "pct_change_1m",
    "pctchange2w": "pct_change_2w",
    "pct_change_2w": "pct_change_2w",
    "pctchange1w": "pct_change_1w",
    "pct_change_1w": "pct_change_1w",
}


class FavoriteEntry(BaseModel):
    code: str
    group: Optional[str] = Field(None, alias="group")
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class FavoriteListResponse(BaseModel):
    total: int
    items: List[FavoriteEntry]


class FavoriteStatusResponse(BaseModel):
    code: str
    is_favorite: bool = Field(False, alias="isFavorite")
    group: Optional[str] = Field(None, alias="group")
    total: int = 0

    class Config:
        allow_population_by_field_name = True


class FavoriteGroupItem(BaseModel):
    name: Optional[str] = Field(None, alias="name")
    total: int = 0

    class Config:
        allow_population_by_field_name = True


class FavoriteGroupListResponse(BaseModel):
    items: List[FavoriteGroupItem]


class FavoriteUpsertRequest(BaseModel):
    group: Optional[str] = Field(None, alias="group")

    class Config:
        allow_population_by_field_name = True


class FundamentalMetricItem(BaseModel):
    code: str
    name: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    exchange: Optional[str] = None
    net_income_end_date_latest: Optional[str] = Field(None, alias="netIncomeEndDateLatest")
    net_income_end_date_prev1: Optional[str] = Field(None, alias="netIncomeEndDatePrev1")
    net_income_end_date_prev2: Optional[str] = Field(None, alias="netIncomeEndDatePrev2")
    revenue_end_date_latest: Optional[str] = Field(None, alias="revenueEndDateLatest")
    roe_end_date_latest: Optional[str] = Field(None, alias="roeEndDateLatest")
    net_income_yoy_latest: Optional[float] = Field(None, alias="netIncomeYoyLatest")
    net_income_yoy_prev1: Optional[float] = Field(None, alias="netIncomeYoyPrev1")
    net_income_yoy_prev2: Optional[float] = Field(None, alias="netIncomeYoyPrev2")
    net_income_qoq_latest: Optional[float] = Field(None, alias="netIncomeQoqLatest")
    revenue_yoy_latest: Optional[float] = Field(None, alias="revenueYoyLatest")
    revenue_qoq_latest: Optional[float] = Field(None, alias="revenueQoqLatest")
    roe_yoy_latest: Optional[float] = Field(None, alias="roeYoyLatest")
    roe_qoq_latest: Optional[float] = Field(None, alias="roeQoqLatest")
    is_favorite: bool = Field(False, alias="isFavorite")
    favorite_group: Optional[str] = Field(None, alias="favoriteGroup")

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True


class FundamentalMetricsListResponse(BaseModel):
    total: int
    items: List[FundamentalMetricItem]


class SyncDailyTradeRequest(BaseModel):
    batch_size: Optional[int] = Field(20, ge=1, le=200)
    window_days: Optional[int] = Field(None, ge=1, le=3650)
    start_date: Optional[str] = Field(None, regex=r"^\d{8}$")
    end_date: Optional[str] = Field(None, regex=r"^\d{8}$")
    codes: Optional[List[str]] = None
    batch_pause_seconds: Optional[float] = Field(0.6, ge=0, le=5)


class SyncDailyTradeResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")


class SyncDailyTradeMetricsRequest(BaseModel):
    history_window_days: Optional[int] = Field(
        None,
        alias="historyWindowDays",
        ge=180,
        le=3650,
        description="How many calendar days of history to load for derived metrics.",
    )

    class Config:
        allow_population_by_field_name = True


class SyncDailyTradeMetricsResponse(BaseModel):
    rows: int
    trade_date: Optional[str] = Field(None, alias="tradeDate")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncFundamentalMetricsRequest(BaseModel):
    per_code: Optional[int] = Field(8, ge=1, le=24, alias="perCode")

    class Config:
        allow_population_by_field_name = True


class SyncFundamentalMetricsResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncStockBasicRequest(BaseModel):
    list_statuses: Optional[List[str]] = Field(default_factory=lambda: ["L", "D"])
    market: Optional[str] = None


class SyncStockBasicResponse(BaseModel):
    rows: int


class SyncDailyIndicatorRequest(BaseModel):
    trade_date: Optional[str] = Field(None, regex=r"^\d{8}$")


class SyncDailyIndicatorResponse(BaseModel):
    rows: int
    trade_date: str = Field(..., alias="tradeDate")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")


class SyncIncomeStatementRequest(BaseModel):
    codes: Optional[List[str]] = Field(
        None, description="Optional list of ts_code identifiers to refresh."
    )
    initial_periods: int = Field(
        8,
        ge=1,
        le=16,
        alias="initialPeriods",
        description="Number of recent income statement rows to fetch per security.",
    )

    class Config:
        allow_population_by_field_name = True


class SyncIncomeStatementResponse(BaseModel):
    rows: int
    codes: List[str]
    code_count: int = Field(..., alias="codeCount")
    total_codes: int = Field(..., alias="totalCodes")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncFinancialIndicatorRequest(BaseModel):
    codes: Optional[List[str]] = Field(
        None, description="Optional list of ts_code identifiers to refresh."
    )
    limit: int = Field(
        8,
        ge=1,
        le=32,
        description="Number of recent financial indicator rows to fetch per security.",
    )

    class Config:
        allow_population_by_field_name = True


class SyncFinancialIndicatorResponse(BaseModel):
    rows: int
    codes: List[str]
    code_count: int = Field(..., alias="codeCount")
    total_codes: int = Field(..., alias="totalCodes")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncFinanceBreakfastRequest(BaseModel):
    """Placeholder request model for finance breakfast sync."""

    class Config:
        extra = "forbid"


class SyncPerformanceExpressRequest(BaseModel):
    codes: Optional[List[str]] = Field(
        None,
        description="Optional list of securities to refresh (accepts ts_code or symbol).",
    )
    report_period: Optional[str] = Field(
        None,
        alias="reportPeriod",
        description="Optional report period override (YYYYMMDD or YYYY-MM-DD).",
    )
    lookback_days: Optional[int] = Field(
        None,
        ge=1,
        le=3650,
        alias="lookbackDays",
        description="Legacy parameter retained for compatibility; ignored.",
    )

    class Config:
        allow_population_by_field_name = True


class SyncPerformanceExpressResponse(BaseModel):
    rows: int
    codes: List[str]
    code_count: int = Field(..., alias="codeCount")
    total_codes: int = Field(..., alias="totalCodes")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")
    report_period: Optional[str] = Field(None, alias="reportPeriod")

    class Config:
        allow_population_by_field_name = True


class SyncPerformanceForecastRequest(BaseModel):
    codes: Optional[List[str]] = Field(
        None,
        description="Optional list of securities to refresh (accepts ts_code or symbol).",
    )
    report_period: Optional[str] = Field(
        None,
        alias="reportPeriod",
        description="Optional report period override (YYYYMMDD or YYYY-MM-DD).",
    )
    lookback_days: Optional[int] = Field(
        None,
        ge=1,
        le=3650,
        alias="lookbackDays",
        description="Legacy parameter retained for compatibility; ignored.",
    )

    class Config:
        allow_population_by_field_name = True


class SyncPerformanceForecastResponse(BaseModel):
    rows: int
    codes: List[str]
    code_count: int = Field(..., alias="codeCount")
    total_codes: int = Field(..., alias="totalCodes")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")
    report_period: Optional[str] = Field(None, alias="reportPeriod")

    class Config:
        allow_population_by_field_name = True


class SyncProfitForecastRequest(BaseModel):
    symbol: Optional[str] = Field(
        None,
        description="Optional industry filter passed to the AkShare profit forecast API.",
    )

    class Config:
        allow_population_by_field_name = True


class SyncProfitForecastResponse(BaseModel):
    rows: int
    codes: List[str] = Field(default_factory=list)
    code_count: int = Field(..., alias="codeCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")
    years: List[int] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class SyncGlobalIndexRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncGlobalIndexResponse(BaseModel):
    rows: int
    codes: List[str] = Field(default_factory=list)
    code_count: int = Field(..., alias="codeCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncDollarIndexRequest(BaseModel):
    symbol: Optional[str] = Field(
        None,
        description="Optional AkShare symbol name for the index (default: 美元指数).",
    )

    class Config:
        extra = "forbid"
        allow_population_by_field_name = True


class SyncDollarIndexResponse(BaseModel):
    rows: int
    codes: List[str] = Field(default_factory=list)
    code_count: int = Field(..., alias="codeCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncRmbMidpointRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncRmbMidpointResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncFuturesRealtimeRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncFuturesRealtimeResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncIndustryFundFlowRequest(BaseModel):
    symbols: Optional[List[str]] = Field(
        None,
        description="Optional list of ranking symbols (e.g. 即时, 3日排行).",
    )

    class Config:
        allow_population_by_field_name = True


class SyncIndustryFundFlowResponse(BaseModel):
    rows: int
    symbols: List[str]
    symbol_count: int = Field(..., alias="symbolCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncConceptFundFlowRequest(BaseModel):
    symbols: Optional[List[str]] = Field(
        None,
        description="Optional list of ranking symbols (e.g. 即时, 3日排行).",
    )

    class Config:
        allow_population_by_field_name = True


class SyncConceptFundFlowResponse(BaseModel):
    rows: int
    symbols: List[str]
    symbol_count: int = Field(..., alias="symbolCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncIndividualFundFlowRequest(BaseModel):
    symbols: Optional[List[str]] = Field(
        None,
        description="Optional list of ranking symbols (e.g. 即时, 3日排行).",
    )

    class Config:
        allow_population_by_field_name = True


class SyncIndividualFundFlowResponse(BaseModel):
    rows: int
    symbols: List[str]
    symbol_count: int = Field(..., alias="symbolCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncBigDealFundFlowRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True


class SyncBigDealFundFlowResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncStockMainBusinessRequest(BaseModel):
    codes: Optional[List[str]] = Field(
        None,
        description="Optional list of stock codes (symbol or ts_code) to refresh.",
    )
    include_list_statuses: Optional[List[str]] = Field(
        None,
        alias="includeListStatuses",
        description="Optional list of list_status values used when codes are omitted.",
    )

    class Config:
        allow_population_by_field_name = True


class SyncStockMainBusinessResponse(BaseModel):
    rows: int
    codes: List[str]
    code_count: int = Field(..., alias="codeCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")
    skipped_count: int = Field(0, alias="skippedCount")

    class Config:
        allow_population_by_field_name = True


class SyncStockMainCompositionRequest(BaseModel):
    codes: Optional[List[str]] = Field(
        None,
        description="Optional list of stock codes (symbol or ts_code) to refresh.",
    )
    include_list_statuses: Optional[List[str]] = Field(
        None,
        alias="includeListStatuses",
        description="Optional list of list_status values used when codes are omitted.",
    )

    class Config:
        allow_population_by_field_name = True


class SyncStockMainCompositionResponse(BaseModel):
    rows: int
    codes: List[str]
    code_count: int = Field(..., alias="codeCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")
    skipped_symbols: int = Field(0, alias="skippedSymbols")

    class Config:
        allow_population_by_field_name = True


class PerformanceExpressRecord(BaseModel):
    symbol: str
    ts_code: Optional[str] = Field(None, alias="tsCode")
    name: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    ann_date: Optional[date] = Field(None, alias="annDate")
    end_date: Optional[date] = Field(None, alias="endDate")
    report_period: Optional[date] = Field(None, alias="reportPeriod")
    announcement_date: Optional[date] = Field(None, alias="announcementDate")
    eps: Optional[float] = None
    revenue: Optional[float] = None
    revenue_prev: Optional[float] = Field(None, alias="revenuePrev")
    revenue_yoy: Optional[float] = Field(None, alias="revenueYearlyGrowth")
    revenue_qoq: Optional[float] = Field(None, alias="revenueQuarterlyGrowth")
    net_profit: Optional[float] = Field(None, alias="netProfit")
    net_profit_prev: Optional[float] = Field(None, alias="netProfitPrev")
    net_profit_yoy: Optional[float] = Field(None, alias="netProfitYearlyGrowth")
    net_profit_qoq: Optional[float] = Field(None, alias="netProfitQuarterlyGrowth")
    net_assets_per_share: Optional[float] = Field(None, alias="netAssetsPerShare")
    return_on_equity: Optional[float] = Field(None, alias="returnOnEquity")
    row_number: Optional[int] = Field(None, alias="rowNumber")
    n_income: Optional[float] = Field(None, alias="netIncome")
    diluted_eps: Optional[float] = Field(None, alias="dilutedEps")
    diluted_roe: Optional[float] = Field(None, alias="dilutedRoe")
    yoy_net_profit: Optional[float] = Field(None, alias="yoyNetProfit")
    perf_summary: Optional[str] = Field(None, alias="perfSummary")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class PerformanceExpressListResponse(BaseModel):
    total: int
    items: List[PerformanceExpressRecord]


class PerformanceForecastRecord(BaseModel):
    symbol: str
    ts_code: Optional[str] = Field(None, alias="tsCode")
    name: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    ann_date: Optional[date] = Field(None, alias="annDate")
    end_date: Optional[date] = Field(None, alias="endDate")
    report_period: Optional[date] = Field(None, alias="reportPeriod")
    announcement_date: Optional[date] = Field(None, alias="announcementDate")
    forecast_metric: Optional[str] = Field(None, alias="forecastMetric")
    change_description: Optional[str] = Field(None, alias="changeDescription")
    forecast_value: Optional[float] = Field(None, alias="forecastValue")
    change_rate: Optional[float] = Field(None, alias="changeRate")
    change_reason: Optional[str] = Field(None, alias="changeReason")
    forecast_type: Optional[str] = Field(None, alias="forecastType")
    last_year_value: Optional[float] = Field(None, alias="lastYearValue")
    row_number: Optional[int] = Field(None, alias="rowNumber")
    type: Optional[str] = Field(None, alias="type")
    summary: Optional[str] = None
    p_change_min: Optional[float] = Field(None, alias="pctChangeMin")
    p_change_max: Optional[float] = Field(None, alias="pctChangeMax")
    net_profit_min: Optional[float] = Field(None, alias="netProfitMin")
    net_profit_max: Optional[float] = Field(None, alias="netProfitMax")
    last_parent_net: Optional[float] = Field(None, alias="lastParentNet")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class PerformanceForecastListResponse(BaseModel):
    total: int
    items: List[PerformanceForecastRecord]


class ProfitForecastRating(BaseModel):
    buy: Optional[float] = None
    add: Optional[float] = None
    neutral: Optional[float] = None
    reduce: Optional[float] = None
    sell: Optional[float] = None


class ProfitForecastPoint(BaseModel):
    year: int
    eps: Optional[float] = None


class ProfitForecastItem(BaseModel):
    code: str
    symbol: str
    ts_code: Optional[str] = Field(None, alias="tsCode")
    name: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    report_count: Optional[int] = Field(None, alias="reportCount")
    ratings: ProfitForecastRating
    forecasts: List[ProfitForecastPoint] = Field(default_factory=list)
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class ProfitForecastListResponse(BaseModel):
    total: int
    items: List[ProfitForecastItem]
    industries: List[str] = Field(default_factory=list)
    years: List[int] = Field(default_factory=list)


class GlobalIndexRecord(BaseModel):
    code: str
    seq: Optional[int] = None
    name: Optional[str] = None
    latest_price: Optional[float] = Field(None, alias="latestPrice")
    change_amount: Optional[float] = Field(None, alias="changeAmount")
    change_percent: Optional[float] = Field(None, alias="changePercent")
    open_price: Optional[float] = Field(None, alias="openPrice")
    high_price: Optional[float] = Field(None, alias="highPrice")
    low_price: Optional[float] = Field(None, alias="lowPrice")
    prev_close: Optional[float] = Field(None, alias="prevClose")
    amplitude: Optional[float] = None
    last_quote_time: Optional[datetime] = Field(None, alias="lastQuoteTime")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class GlobalIndexListResponse(BaseModel):
    total: int
    items: List[GlobalIndexRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class DollarIndexRecord(BaseModel):
    trade_date: date = Field(..., alias="tradeDate")
    code: str
    name: Optional[str] = None
    open_price: Optional[float] = Field(None, alias="openPrice")
    close_price: Optional[float] = Field(None, alias="closePrice")
    high_price: Optional[float] = Field(None, alias="highPrice")
    low_price: Optional[float] = Field(None, alias="lowPrice")
    amplitude: Optional[float] = None
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class DollarIndexListResponse(BaseModel):
    total: int
    items: List[DollarIndexRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class RmbMidpointRecord(BaseModel):
    trade_date: date = Field(..., alias="tradeDate")
    usd: Optional[float] = None
    eur: Optional[float] = None
    jpy: Optional[float] = None
    hkd: Optional[float] = None
    gbp: Optional[float] = None
    aud: Optional[float] = None
    cad: Optional[float] = None
    nzd: Optional[float] = None
    sgd: Optional[float] = None
    chf: Optional[float] = None
    myr: Optional[float] = None
    rub: Optional[float] = None
    zar: Optional[float] = None
    krw: Optional[float] = None
    aed: Optional[float] = None
    sar: Optional[float] = None
    huf: Optional[float] = None
    pln: Optional[float] = None
    dkk: Optional[float] = None
    sek: Optional[float] = None
    nok: Optional[float] = None
    try_value: Optional[float] = Field(None, alias="try")
    mxn: Optional[float] = None
    thb: Optional[float] = None
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class RmbMidpointListResponse(BaseModel):
    total: int
    items: List[RmbMidpointRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class FuturesRealtimeRecord(BaseModel):
    name: str
    code: Optional[str] = None
    last_price: Optional[float] = Field(None, alias="lastPrice")
    price_cny: Optional[float] = Field(None, alias="priceCny")
    change_amount: Optional[float] = Field(None, alias="changeAmount")
    change_percent: Optional[float] = Field(None, alias="changePercent")
    open_price: Optional[float] = Field(None, alias="openPrice")
    high_price: Optional[float] = Field(None, alias="highPrice")
    low_price: Optional[float] = Field(None, alias="lowPrice")
    prev_settlement: Optional[float] = Field(None, alias="prevSettlement")
    open_interest: Optional[float] = Field(None, alias="openInterest")
    bid_price: Optional[float] = Field(None, alias="bidPrice")
    ask_price: Optional[float] = Field(None, alias="askPrice")
    quote_time: Optional[str] = Field(None, alias="quoteTime")
    trade_date: Optional[date] = Field(None, alias="tradeDate")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class FuturesRealtimeListResponse(BaseModel):
    total: int
    items: List[FuturesRealtimeRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class IndustryFundFlowRecord(BaseModel):
    symbol: str
    industry: str
    rank: Optional[int] = None
    industry_index: Optional[float] = Field(None, alias="industryIndex")
    price_change_percent: Optional[float] = Field(None, alias="priceChangePercent")
    stage_change_percent: Optional[float] = Field(None, alias="stageChangePercent")
    inflow: Optional[float] = None
    outflow: Optional[float] = None
    net_amount: Optional[float] = Field(None, alias="netAmount")
    company_count: Optional[int] = Field(None, alias="companyCount")
    leading_stock: Optional[str] = Field(None, alias="leadingStock")
    leading_stock_change_percent: Optional[float] = Field(None, alias="leadingStockChangePercent")
    current_price: Optional[float] = Field(None, alias="currentPrice")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class IndustryFundFlowListResponse(BaseModel):
    total: int
    items: List[IndustryFundFlowRecord]


class ConceptFundFlowRecord(BaseModel):
    symbol: str
    concept: str
    rank: Optional[int] = None
    concept_index: Optional[float] = Field(None, alias="conceptIndex")
    price_change_percent: Optional[float] = Field(None, alias="priceChangePercent")
    stage_change_percent: Optional[float] = Field(None, alias="stageChangePercent")
    inflow: Optional[float] = None
    outflow: Optional[float] = None
    net_amount: Optional[float] = Field(None, alias="netAmount")
    company_count: Optional[int] = Field(None, alias="companyCount")
    leading_stock: Optional[str] = Field(None, alias="leadingStock")
    leading_stock_change_percent: Optional[float] = Field(None, alias="leadingStockChangePercent")
    current_price: Optional[float] = Field(None, alias="currentPrice")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class ConceptFundFlowListResponse(BaseModel):
    total: int
    items: List[ConceptFundFlowRecord]


class IndividualFundFlowRecord(BaseModel):
    symbol: str
    stock_code: str = Field(..., alias="stockCode")
    stock_name: Optional[str] = Field(None, alias="stockName")
    rank: Optional[int] = None
    latest_price: Optional[float] = Field(None, alias="latestPrice")
    price_change_percent: Optional[float] = Field(None, alias="priceChangePercent")
    stage_change_percent: Optional[float] = Field(None, alias="stageChangePercent")
    turnover_rate: Optional[float] = Field(None, alias="turnoverRate")
    continuous_turnover_rate: Optional[float] = Field(None, alias="continuousTurnoverRate")
    inflow: Optional[float] = None
    outflow: Optional[float] = None
    net_amount: Optional[float] = Field(None, alias="netAmount")
    net_inflow: Optional[float] = Field(None, alias="netInflow")
    turnover_amount: Optional[float] = Field(None, alias="turnoverAmount")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class IndividualFundFlowListResponse(BaseModel):
    total: int
    items: List[IndividualFundFlowRecord]


class BigDealFundFlowRecord(BaseModel):
    trade_time: Optional[datetime] = Field(None, alias="tradeTime")
    stock_code: str = Field(..., alias="stockCode")
    stock_name: Optional[str] = Field(None, alias="stockName")
    trade_price: Optional[float] = Field(None, alias="tradePrice")
    trade_volume: Optional[int] = Field(None, alias="tradeVolume")
    trade_amount: Optional[float] = Field(None, alias="tradeAmount")
    trade_side: Optional[str] = Field(None, alias="tradeSide")
    price_change_percent: Optional[float] = Field(None, alias="priceChangePercent")
    price_change: Optional[float] = Field(None, alias="priceChange")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class BigDealFundFlowListResponse(BaseModel):
    total: int
    items: List[BigDealFundFlowRecord]


class SyncFinanceBreakfastResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

class FinanceBreakfastItem(BaseModel):
    title: str
    summary: Optional[str] = None
    content: Optional[str] = None
    ai_extract: Optional[Union[str, Dict[str, Any]]] = Field(None, alias="aiExtract")
    ai_extract_summary: Optional[Union[str, Dict[str, Any]]] = Field(None, alias="aiExtractSummary")
    ai_extract_detail: Optional[Union[str, List[Any], Dict[str, Any]]] = Field(None, alias="aiExtractDetail")
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    url: Optional[str] = None

    class Config:
        allow_population_by_field_name = True


class RuntimeConfigPayload(BaseModel):
    include_st: bool = Field(..., alias="includeST")
    include_delisted: bool = Field(..., alias="includeDelisted")
    daily_trade_window_days: int = Field(..., alias="dailyTradeWindowDays", ge=1, le=3650)

    class Config:
        allow_population_by_field_name = True


class JobStatusPayload(BaseModel):
    status: str
    started_at: Optional[str]
    finished_at: Optional[str]
    progress: float
    message: Optional[str]
    total_rows: Optional[int]
    last_duration: Optional[float]
    last_market: Optional[str]
    error: Optional[str]

    class Config:
        allow_population_by_field_name = True
        fields = {
            "started_at": "startedAt",
            "finished_at": "finishedAt",
            "total_rows": "totalRows",
            "last_duration": "lastDuration",
            "last_market": "lastMarket",
        }


class ControlStatusResponse(BaseModel):
    jobs: Dict[str, JobStatusPayload]
    config: RuntimeConfigPayload


app = FastAPI(title="Trend View API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _runtime_config_to_payload(config: RuntimeConfig) -> RuntimeConfigPayload:
    return RuntimeConfigPayload(
        include_st=config.include_st,
        include_delisted=config.include_delisted,
        daily_trade_window_days=config.daily_trade_window_days,
    )


async def _run_stock_basic_job(list_statuses: Optional[List[str]], market: Optional[str]) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        context_parts: List[str] = []
        if market:
            context_parts.append(f"MARKET:{market}")
        if list_statuses:
            context_parts.append(f"STATUS:{','.join(list_statuses)}")
        if context_parts:
            monitor.update("stock_basic", last_market=" ".join(context_parts))
        else:
            monitor.update("stock_basic", last_market="AUTO")
        try:
            rows = sync_stock_basic(
                list_statuses=tuple(list_statuses or ["L", "D", "P"]),
                market=market,
            )
            stats: Dict[str, object] = {}
            try:
                stats = StockBasicDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh stock_basic stats: %s", stats_exc)
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = rows
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            monitor.finish(
                "stock_basic",
                success=True,
                total_rows=total_rows,
                message="Stock basic sync completed",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "stock_basic",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_daily_trade_job(request: SyncDailyTradeRequest) -> None:
    loop = asyncio.get_running_loop()

    runtime_config = load_runtime_config()
    window_days = request.window_days or runtime_config.daily_trade_window_days

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "daily_trade",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        try:
            result = sync_daily_trade(
                batch_size=request.batch_size or 20,
                window_days=window_days,
                start_date=request.start_date,
                end_date=request.end_date,
                codes=request.codes,
                batch_pause_seconds=request.batch_pause_seconds or 0.6,
                progress_callback=progress_callback,
            )
            stats: Dict[str, object] = {}
            try:
                stats = DailyTradeDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh daily_trade stats: %s", stats_exc)
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result["rows"]
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            monitor.finish(
                "daily_trade",
                success=True,
                total_rows=total_rows,
                message="Daily trade sync completed",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "daily_trade",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    monitor.update("daily_trade", last_market=f"WINDOW:{window_days}")
    await loop.run_in_executor(None, job)


async def _run_daily_trade_metrics_job(request: SyncDailyTradeMetricsRequest) -> None:
    loop = asyncio.get_running_loop()
    history_window_days = request.history_window_days

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "daily_trade_metrics",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        try:
            kwargs: Dict[str, object] = {"progress_callback": progress_callback}
            if history_window_days is not None:
                kwargs["history_window_days"] = history_window_days
            result = sync_daily_trade_metrics(**kwargs)
            stats: Dict[str, object] = {}
            try:
                stats = DailyTradeMetricsDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh daily_trade_metrics stats: %s", stats_exc)
            elapsed = float(result.get("elapsed_seconds", time.perf_counter() - started))
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            trade_date_value = stats.get("latest_trade_date") if isinstance(stats, dict) else None
            if not trade_date_value:
                trade_date_value = result.get("trade_date")
                if isinstance(trade_date_value, str):
                    try:
                        trade_date_value = datetime.strptime(trade_date_value, "%Y%m%d").date()
                    except (ValueError, TypeError):
                        pass
            trade_date_str: Optional[str] = None
            if trade_date_value:
                if hasattr(trade_date_value, "strftime"):
                    trade_date_str = trade_date_value.strftime("%Y-%m-%d")
                else:
                    trade_date_str = str(trade_date_value)
            if trade_date_str:
                monitor.update("daily_trade_metrics", last_market=trade_date_str)

            message = "Daily trade metrics sync completed"
            if trade_date_str:
                message = f"Metrics ready for {trade_date_str}"
            if history_window_days:
                message = f"{message} (history {history_window_days}d)"
            monitor.finish(
                "daily_trade_metrics",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message,
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "daily_trade_metrics",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    label = history_window_days if history_window_days is not None else "AUTO"
    monitor.update("daily_trade_metrics", last_market=f"HISTORY:{label}")
    await loop.run_in_executor(None, job)



async def _run_fundamental_metrics_job(request: SyncFundamentalMetricsRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "fundamental_metrics",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update(
            "fundamental_metrics",
            last_market=f"PER:{request.per_code}" if request.per_code is not None else "PER:AUTO",
        )
        try:
            kwargs: dict[str, object] = {"progress_callback": progress_callback}
            if request.per_code is not None:
                kwargs["per_code"] = request.per_code
            result = sync_fundamental_metrics(**kwargs)
            stats: Dict[str, object] = {}
            try:
                stats = FundamentalMetricsDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh fundamental_metrics stats: %s", stats_exc)
            elapsed = float(result.get("elapsed_seconds", time.perf_counter() - started))
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            monitor.finish(
                "fundamental_metrics",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message="Fundamental metrics sync completed",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "fundamental_metrics",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)

async def _run_daily_indicator_job(request: SyncDailyIndicatorRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "daily_indicator",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        try:
            result = sync_daily_indicator(
                trade_date=request.trade_date,
                progress_callback=progress_callback,
            )
            stats: Dict[str, object] = {}
            try:
                stats = DailyIndicatorDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh daily_indicator stats: %s", stats_exc)
            elapsed = float(result.get("elapsed_seconds", time.perf_counter() - started))
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            monitor.finish(
                "daily_indicator",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message="Daily indicator sync completed",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "daily_indicator",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_income_statement_job(request: SyncIncomeStatementRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "income_statement",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        try:
            result = sync_income_statements(
                codes=request.codes,
                initial_periods=request.initial_periods,
                progress_callback=progress_callback,
            )
            stats: Dict[str, object] = {}
            try:
                stats = IncomeStatementDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh income_statement stats: %s", stats_exc)
            elapsed = float(result.get("elapsed_seconds", time.perf_counter() - started))
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            codes = result.get("codes") or []
            code_count = int(result.get("code_count", len(codes)))
            total_codes = int(result.get("total_codes", code_count))
            base_message = f"{code_count}/{total_codes} codes"
            if codes:
                preview = ", ".join(codes[:3])
                suffix = "" if code_count <= 3 else " ??"
                monitor.update(
                    "income_statement",
                    last_market=f"{base_message} ({preview}{suffix})",
                )
            else:
                monitor.update(
                    "income_statement",
                    last_market=base_message,
                )
            monitor.finish(
                "income_statement",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message="Income statement sync completed",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "income_statement",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Income statement sync failed: %s", error_message)

    await loop.run_in_executor(None, job)


async def _run_financial_indicator_job(request: SyncFinancialIndicatorRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "financial_indicator",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        try:
            result = sync_financial_indicators(
                codes=request.codes,
                limit=request.limit,
                progress_callback=progress_callback,
            )
            stats: Dict[str, object] = {}
            try:
                stats = FinancialIndicatorDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh financial_indicator stats: %s", stats_exc)
            elapsed = float(result.get("elapsed_seconds", time.perf_counter() - started))
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            codes = result.get("codes") or []
            code_count = int(result.get("code_count", len(codes)))
            total_codes = int(result.get("total_codes", code_count))
            base_message = f"{code_count}/{total_codes} codes"
            if codes:
                preview = ", ".join(codes[:3])
                suffix = "" if code_count <= 3 else " ??"
                monitor.update(
                    "financial_indicator",
                    last_market=f"{base_message} ({preview}{suffix})",
                )
            else:
                monitor.update(
                    "financial_indicator",
                    last_market=base_message,
                )
            monitor.finish(
                "financial_indicator",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message="Financial indicator sync completed",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "financial_indicator",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Financial indicator sync failed: %s", error_message)

    await loop.run_in_executor(None, job)


async def _run_finance_breakfast_job(request: SyncFinanceBreakfastRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "finance_breakfast",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        try:
            result = sync_finance_breakfast(
                progress_callback=progress_callback,
            )
            stats: Dict[str, object] = {}
            try:
                stats = FinanceBreakfastDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh finance_breakfast stats: %s", stats_exc)
            elapsed = float(result.get("elapsed_seconds", time.perf_counter() - started))
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            monitor.finish(
                "finance_breakfast",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message="Finance breakfast sync completed",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "finance_breakfast",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Finance breakfast sync failed: %s", error_message)

    await loop.run_in_executor(None, job)



async def _run_performance_express_job(request: SyncPerformanceExpressRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "performance_express",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("performance_express", message="Collecting performance express data")
        try:
            result = sync_performance_express(
                codes=request.codes,
                lookback_days=request.lookback_days,
                report_period=request.report_period,
                progress_callback=progress_callback,
            )
            stats = PerformanceExpressDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            message_text = f"Synced {result.get('rows', 0)} performance express rows"
            monitor.finish(
                "performance_express",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "performance_express",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_performance_forecast_job(request: SyncPerformanceForecastRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "performance_forecast",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("performance_forecast", message="Collecting performance forecast data")
        try:
            result = sync_performance_forecast(
                codes=request.codes,
                lookback_days=request.lookback_days,
                report_period=request.report_period,
                progress_callback=progress_callback,
            )
            stats = PerformanceForecastDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            message_text = f"Synced {result.get('rows', 0)} performance forecast rows"
            monitor.finish(
                "performance_forecast",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "performance_forecast",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_profit_forecast_job(request: SyncProfitForecastRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("profit_forecast", message="Collecting profit forecast data", progress=0.0)
        try:
            result = sync_profit_forecast(symbol=request.symbol)
            stats = ProfitForecastDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "profit_forecast",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} profit forecast rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "profit_forecast",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_global_index_job(request: SyncGlobalIndexRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("global_index", message="Syncing global index snapshot", progress=0.0)
        try:
            result = sync_global_indices()
            stats = GlobalIndexDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "global_index",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} global index rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "global_index",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_dollar_index_job(request: SyncDollarIndexRequest) -> None:
    loop = asyncio.get_running_loop()
    symbol = request.symbol or "美元指数"

    def job() -> None:
        started = time.perf_counter()
        monitor.update("dollar_index", message=f"Syncing {symbol} history", progress=0.0)
        try:
            result = sync_dollar_index(symbol=symbol)
            stats = DollarIndexDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "dollar_index",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} dollar index rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "dollar_index",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_rmb_midpoint_job(request: SyncRmbMidpointRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("rmb_midpoint", message="Syncing RMB midpoint rates", progress=0.0)
        try:
            result = sync_rmb_midpoint_rates()
            stats = RmbMidpointDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "rmb_midpoint",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} midpoint rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "rmb_midpoint",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_futures_realtime_job(request: SyncFuturesRealtimeRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("futures_realtime", message="Syncing futures realtime data", progress=0.0)
        try:
            result = sync_futures_realtime()
            stats = FuturesRealtimeDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "futures_realtime",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} futures rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "futures_realtime",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_industry_fund_flow_job(request: SyncIndustryFundFlowRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "industry_fund_flow",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("industry_fund_flow", message="Collecting industry fund flow data")
        try:
            result = sync_industry_fund_flow(
                symbols=request.symbols,
                progress_callback=progress_callback,
            )
            stats = IndustryFundFlowDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            message_text = f"Synced {result.get('rows', 0)} industry fund flow rows"
            monitor.finish(
                "industry_fund_flow",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "industry_fund_flow",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_concept_fund_flow_job(request: SyncConceptFundFlowRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "concept_fund_flow",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("concept_fund_flow", message="Collecting concept fund flow data")
        try:
            result = sync_concept_fund_flow(
                symbols=request.symbols,
                progress_callback=progress_callback,
            )
            stats = ConceptFundFlowDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            message_text = f"Synced {result.get('rows', 0)} concept fund flow rows"
            monitor.finish(
                "concept_fund_flow",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "concept_fund_flow",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_individual_fund_flow_job(request: SyncIndividualFundFlowRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "individual_fund_flow",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("individual_fund_flow", message="Collecting individual fund flow data")
        try:
            result = sync_individual_fund_flow(
                symbols=request.symbols,
                progress_callback=progress_callback,
            )
            stats = IndividualFundFlowDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            message_text = f"Synced {result.get('rows', 0)} individual fund flow rows"
            monitor.finish(
                "individual_fund_flow",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "individual_fund_flow",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_big_deal_fund_flow_job(request: SyncBigDealFundFlowRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "big_deal_fund_flow",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("big_deal_fund_flow", message="Collecting big deal fund flow data")
        try:
            result = sync_big_deal_fund_flow(progress_callback=progress_callback)
            stats = BigDealFundFlowDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            message_text = f"Synced {result.get('rows', 0)} big deal rows"
            monitor.finish(
                "big_deal_fund_flow",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "big_deal_fund_flow",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_stock_main_business_job(request: SyncStockMainBusinessRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "stock_main_business",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("stock_main_business", message="Collecting stock main business data")
        try:
            result = sync_stock_main_business(
                codes=request.codes,
                include_list_statuses=request.include_list_statuses,
                progress_callback=progress_callback,
            )
            stats = StockMainBusinessDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            rows_synced = int(result.get("rows", 0) or 0)
            new_codes = int(result.get("codeCount", 0) or 0)
            skipped_codes = int(result.get("skippedCount", 0) or 0)
            message_text = (
                f"Synced {rows_synced} main business rows "
                f"(new {new_codes}, skipped {skipped_codes})"
            )
            monitor.finish(
                "stock_main_business",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "stock_main_business",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_stock_main_composition_job(request: SyncStockMainCompositionRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "stock_main_composition",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("stock_main_composition", message="Collecting stock main composition data")
        try:
            result = sync_stock_main_composition(
                codes=request.codes,
                include_list_statuses=request.include_list_statuses,
                progress_callback=progress_callback,
            )
            stats = StockMainCompositionDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            rows_synced = int(result.get("rows", 0) or 0)
            new_codes = int(result.get("codeCount", 0) or 0)
            skipped_symbols = int(result.get("skippedSymbols", 0) or 0)
            message_text = (
                f"Synced {rows_synced} main composition rows "
                f"(new {new_codes}, skipped {skipped_symbols})"
            )
            monitor.finish(
                "stock_main_composition",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "stock_main_composition",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)



def _job_running(job: str) -> bool:
    snapshot = monitor.snapshot()
    job_state = snapshot.get(job)
    if not job_state:
        return False
    return job_state.get("status") == "running"


async def start_stock_basic_job(payload: SyncStockBasicRequest) -> None:
    if _job_running("stock_basic"):
        raise HTTPException(status_code=409, detail="Stock basic sync already running")
    monitor.start("stock_basic", message="Syncing stock basics")
    monitor.update("stock_basic", progress=0.0)
    asyncio.create_task(_run_stock_basic_job(payload.list_statuses, payload.market))


async def start_daily_trade_job(payload: SyncDailyTradeRequest) -> None:
    if _job_running("daily_trade"):
        raise HTTPException(status_code=409, detail="Daily trade sync already running")
    monitor.start("daily_trade", message="Syncing daily trade data")
    monitor.update("daily_trade", progress=0.0)
    asyncio.create_task(_run_daily_trade_job(payload))


async def start_daily_trade_metrics_job(payload: SyncDailyTradeMetricsRequest) -> None:
    if _job_running("daily_trade_metrics"):
        raise HTTPException(status_code=409, detail="Daily trade metrics sync already running")
    monitor.start("daily_trade_metrics", message="Generating daily trade derived metrics")
    monitor.update("daily_trade_metrics", progress=0.0)
    asyncio.create_task(_run_daily_trade_metrics_job(payload))


async def start_daily_indicator_job(payload: SyncDailyIndicatorRequest) -> None:
    if _job_running("daily_indicator"):
        raise HTTPException(status_code=409, detail="Daily indicator sync already running")
    monitor.start("daily_indicator", message="Syncing daily indicator data")
    monitor.update("daily_indicator", progress=0.0)
    asyncio.create_task(_run_daily_indicator_job(payload))


async def start_income_statement_job(payload: SyncIncomeStatementRequest) -> None:
    if _job_running("income_statement"):
        raise HTTPException(status_code=409, detail="Income statement sync already running")
    monitor.start("income_statement", message="Syncing income statements")
    monitor.update("income_statement", progress=0.0)
    asyncio.create_task(_run_income_statement_job(payload))


async def start_financial_indicator_job(payload: SyncFinancialIndicatorRequest) -> None:
    if _job_running("financial_indicator"):
        raise HTTPException(status_code=409, detail="Financial indicator sync already running")
    monitor.start("financial_indicator", message="Syncing financial indicators")
    monitor.update("financial_indicator", progress=0.0)
    asyncio.create_task(_run_financial_indicator_job(payload))


async def start_fundamental_metrics_job(payload: SyncFundamentalMetricsRequest) -> None:
    if _job_running("fundamental_metrics"):
        raise HTTPException(status_code=409, detail="Fundamental metrics sync already running")
    monitor.start("fundamental_metrics", message="Syncing fundamental metrics")
    monitor.update("fundamental_metrics", progress=0.0)
    asyncio.create_task(_run_fundamental_metrics_job(payload))




async def start_performance_express_job(payload: SyncPerformanceExpressRequest) -> None:
    if _job_running("performance_express"):
        raise HTTPException(status_code=409, detail="Performance express sync already running")
    monitor.start("performance_express", message="Syncing performance express data")
    monitor.update("performance_express", progress=0.0)
    asyncio.create_task(_run_performance_express_job(payload))


async def start_performance_forecast_job(payload: SyncPerformanceForecastRequest) -> None:
    if _job_running("performance_forecast"):
        raise HTTPException(status_code=409, detail="Performance forecast sync already running")
    monitor.start("performance_forecast", message="Syncing performance forecast data")
    monitor.update("performance_forecast", progress=0.0)
    asyncio.create_task(_run_performance_forecast_job(payload))


async def start_profit_forecast_job(payload: SyncProfitForecastRequest) -> None:
    if _job_running("profit_forecast"):
        raise HTTPException(status_code=409, detail="Profit forecast sync already running")
    monitor.start("profit_forecast", message="Syncing profit forecast data")
    monitor.update("profit_forecast", progress=0.0)
    asyncio.create_task(_run_profit_forecast_job(payload))


async def start_global_index_job(payload: SyncGlobalIndexRequest) -> None:  # noqa: ARG001
    if _job_running("global_index"):
        raise HTTPException(status_code=409, detail="Global index sync already running")
    monitor.start("global_index", message="Syncing global index snapshot")
    monitor.update("global_index", progress=0.0)
    asyncio.create_task(_run_global_index_job(payload))


async def start_dollar_index_job(payload: SyncDollarIndexRequest) -> None:
    if _job_running("dollar_index"):
        raise HTTPException(status_code=409, detail="Dollar index sync already running")
    monitor.start("dollar_index", message="Syncing dollar index history")
    monitor.update("dollar_index", progress=0.0)
    asyncio.create_task(_run_dollar_index_job(payload))


async def start_rmb_midpoint_job(payload: SyncRmbMidpointRequest) -> None:
    if _job_running("rmb_midpoint"):
        raise HTTPException(status_code=409, detail="RMB midpoint sync already running")
    monitor.start("rmb_midpoint", message="Syncing RMB midpoint rates")
    monitor.update("rmb_midpoint", progress=0.0)
    asyncio.create_task(_run_rmb_midpoint_job(payload))


async def start_futures_realtime_job(payload: SyncFuturesRealtimeRequest) -> None:
    if _job_running("futures_realtime"):
        raise HTTPException(status_code=409, detail="Futures realtime sync already running")
    monitor.start("futures_realtime", message="Syncing futures realtime data")
    monitor.update("futures_realtime", progress=0.0)
    asyncio.create_task(_run_futures_realtime_job(payload))


async def start_industry_fund_flow_job(payload: SyncIndustryFundFlowRequest) -> None:
    if _job_running("industry_fund_flow"):
        raise HTTPException(status_code=409, detail="Industry fund flow sync already running")
    monitor.start("industry_fund_flow", message="Syncing industry fund flow data")
    monitor.update("industry_fund_flow", progress=0.0)
    asyncio.create_task(_run_industry_fund_flow_job(payload))


async def start_concept_fund_flow_job(payload: SyncConceptFundFlowRequest) -> None:
    if _job_running("concept_fund_flow"):
        raise HTTPException(status_code=409, detail="Concept fund flow sync already running")
    monitor.start("concept_fund_flow", message="Syncing concept fund flow data")
    monitor.update("concept_fund_flow", progress=0.0)
    asyncio.create_task(_run_concept_fund_flow_job(payload))


async def start_individual_fund_flow_job(payload: SyncIndividualFundFlowRequest) -> None:
    if _job_running("individual_fund_flow"):
        raise HTTPException(status_code=409, detail="Individual fund flow sync already running")
    monitor.start("individual_fund_flow", message="Syncing individual fund flow data")
    monitor.update("individual_fund_flow", progress=0.0)
    asyncio.create_task(_run_individual_fund_flow_job(payload))


async def start_big_deal_fund_flow_job(payload: SyncBigDealFundFlowRequest) -> None:
    if _job_running("big_deal_fund_flow"):
        raise HTTPException(status_code=409, detail="Big deal fund flow sync already running")
    monitor.start("big_deal_fund_flow", message="Syncing big deal fund flow data")
    monitor.update("big_deal_fund_flow", progress=0.0)
    asyncio.create_task(_run_big_deal_fund_flow_job(payload))


async def start_stock_main_business_job(payload: SyncStockMainBusinessRequest) -> None:
    if _job_running("stock_main_business"):
        raise HTTPException(status_code=409, detail="Stock main business sync already running")
    monitor.start("stock_main_business", message="Syncing stock main business data")
    monitor.update("stock_main_business", progress=0.0)
    asyncio.create_task(_run_stock_main_business_job(payload))


async def start_stock_main_composition_job(payload: SyncStockMainCompositionRequest) -> None:
    if _job_running("stock_main_composition"):
        raise HTTPException(status_code=409, detail="Stock main composition sync already running")
    monitor.start("stock_main_composition", message="Syncing stock main composition data")
    monitor.update("stock_main_composition", progress=0.0)
    asyncio.create_task(_run_stock_main_composition_job(payload))


async def start_finance_breakfast_job(payload: SyncFinanceBreakfastRequest) -> None:
    if _job_running("finance_breakfast"):
        raise HTTPException(status_code=409, detail="Finance breakfast sync already running")
    monitor.start("finance_breakfast", message="Syncing finance breakfast summaries")
    monitor.update("finance_breakfast", progress=0.0)
    asyncio.create_task(_run_finance_breakfast_job(payload))


async def safe_start_stock_basic_job(payload: SyncStockBasicRequest) -> None:
    try:
        await start_stock_basic_job(payload)
    except HTTPException as exc:
        logger.info("Stock basic sync skipped: %s", exc.detail)


async def safe_start_daily_trade_job(payload: SyncDailyTradeRequest) -> None:
    try:
        await start_daily_trade_job(payload)
    except HTTPException as exc:
        logger.info("Daily trade sync skipped: %s", exc.detail)


async def safe_start_daily_trade_metrics_job(payload: SyncDailyTradeMetricsRequest) -> None:
    try:
        await start_daily_trade_metrics_job(payload)
    except HTTPException as exc:
        logger.info("Daily trade metrics sync skipped: %s", exc.detail)


async def safe_start_fundamental_metrics_job(payload: SyncFundamentalMetricsRequest) -> None:
    try:
        await start_fundamental_metrics_job(payload)
    except HTTPException as exc:
        logger.info("Fundamental metrics sync skipped: %s", exc.detail)

async def safe_start_daily_indicator_job(payload: SyncDailyIndicatorRequest) -> None:
    try:
        await start_daily_indicator_job(payload)
    except HTTPException as exc:
        logger.info("Daily indicator sync skipped: %s", exc.detail)


async def safe_start_income_statement_job(payload: SyncIncomeStatementRequest) -> None:
    try:
        await start_income_statement_job(payload)
    except HTTPException as exc:
        logger.info("Income statement sync skipped: %s", exc.detail)


async def safe_start_financial_indicator_job(payload: SyncFinancialIndicatorRequest) -> None:
    try:
        await start_financial_indicator_job(payload)
    except HTTPException as exc:
        logger.info("Financial indicator sync skipped: %s", exc.detail)


async def safe_start_finance_breakfast_job(payload: SyncFinanceBreakfastRequest) -> None:
    try:
        await start_finance_breakfast_job(payload)
    except HTTPException as exc:
        logger.info("Finance breakfast sync skipped: %s", exc.detail)



async def safe_start_performance_express_job(payload: SyncPerformanceExpressRequest) -> None:
    try:
        await start_performance_express_job(payload)
    except HTTPException as exc:
        logger.info("Performance express sync skipped: %s", exc.detail)


async def safe_start_performance_forecast_job(payload: SyncPerformanceForecastRequest) -> None:
    try:
        await start_performance_forecast_job(payload)
    except HTTPException as exc:
        logger.info("Performance forecast sync skipped: %s", exc.detail)


async def safe_start_profit_forecast_job(payload: SyncProfitForecastRequest) -> None:
    try:
        await start_profit_forecast_job(payload)
    except HTTPException as exc:
        logger.info("Profit forecast sync skipped: %s", exc.detail)


async def safe_start_global_index_job(payload: SyncGlobalIndexRequest) -> None:
    try:
        await start_global_index_job(payload)
    except HTTPException as exc:
        logger.info("Global index sync skipped: %s", exc.detail)


async def safe_start_dollar_index_job(payload: SyncDollarIndexRequest) -> None:
    try:
        await start_dollar_index_job(payload)
    except HTTPException as exc:
        logger.info("Dollar index sync skipped: %s", exc.detail)


async def safe_start_rmb_midpoint_job(payload: SyncRmbMidpointRequest) -> None:
    try:
        await start_rmb_midpoint_job(payload)
    except HTTPException as exc:
        logger.info("RMB midpoint sync skipped: %s", exc.detail)


async def safe_start_futures_realtime_job(payload: SyncFuturesRealtimeRequest) -> None:
    try:
        await start_futures_realtime_job(payload)
    except HTTPException as exc:
        logger.info("Futures realtime sync skipped: %s", exc.detail)


async def safe_start_industry_fund_flow_job(payload: SyncIndustryFundFlowRequest) -> None:
    try:
        await start_industry_fund_flow_job(payload)
    except HTTPException as exc:
        logger.info("Industry fund flow sync skipped: %s", exc.detail)


async def safe_start_concept_fund_flow_job(payload: SyncConceptFundFlowRequest) -> None:
    try:
        await start_concept_fund_flow_job(payload)
    except HTTPException as exc:
        logger.info("Concept fund flow sync skipped: %s", exc.detail)


async def safe_start_individual_fund_flow_job(payload: SyncIndividualFundFlowRequest) -> None:
    try:
        await start_individual_fund_flow_job(payload)
    except HTTPException as exc:
        logger.info("Individual fund flow sync skipped: %s", exc.detail)


async def safe_start_big_deal_fund_flow_job(payload: SyncBigDealFundFlowRequest) -> None:
    try:
        await start_big_deal_fund_flow_job(payload)
    except HTTPException as exc:
        logger.info("Big deal fund flow sync skipped: %s", exc.detail)


async def safe_start_stock_main_business_job(payload: SyncStockMainBusinessRequest) -> None:
    try:
        await start_stock_main_business_job(payload)
    except HTTPException as exc:
        logger.info("Stock main business sync skipped: %s", exc.detail)


async def safe_start_stock_main_composition_job(payload: SyncStockMainCompositionRequest) -> None:
    try:
        await start_stock_main_composition_job(payload)
    except HTTPException as exc:
        logger.info("Stock main composition sync skipped: %s", exc.detail)


@app.on_event("startup")
async def startup_event() -> None:
    if not scheduler.running:
        scheduler.start()
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_stock_basic_job(SyncStockBasicRequest(list_statuses=["L", "D"], market=None))
            ),
            CronTrigger(day=1, hour=0, minute=0),
            id="stock_basic_monthly",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_daily_trade_job(SyncDailyTradeRequest())
            ),
            CronTrigger(hour=17, minute=0),
            id="daily_trade_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_daily_indicator_job(SyncDailyIndicatorRequest())
            ),
            CronTrigger(hour=17, minute=5),
            id="daily_indicator_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_daily_trade_metrics_job(SyncDailyTradeMetricsRequest())
            ),
            CronTrigger(hour=19, minute=0),
            id="daily_trade_metrics_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_fundamental_metrics_job(SyncFundamentalMetricsRequest())
            ),
            CronTrigger(hour=19, minute=10),
            id="fundamental_metrics_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_finance_breakfast_job(SyncFinanceBreakfastRequest())
            ),
            CronTrigger(hour=7, minute=0),
            id="finance_breakfast_daily",
            replace_existing=True,
        )


        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_industry_fund_flow_job(SyncIndustryFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=25),
            id="industry_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_concept_fund_flow_job(SyncConceptFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=30),
            id="concept_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_individual_fund_flow_job(SyncIndividualFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=35),
            id="individual_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_big_deal_fund_flow_job(SyncBigDealFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=37),
            id="big_deal_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_performance_express_job(SyncPerformanceExpressRequest())
            ),
            CronTrigger(hour=19, minute=20),
            id="performance_express_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_performance_forecast_job(SyncPerformanceForecastRequest())
            ),
            CronTrigger(hour=19, minute=40),
            id="performance_forecast_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_global_index_job(SyncGlobalIndexRequest())
            ),
            CronTrigger(hour="7,9,11,13,15,17", minute=0),
            id="global_index_intraday",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_dollar_index_job(SyncDollarIndexRequest())
            ),
            CronTrigger(hour="7,17", minute=10),
            id="dollar_index_twice_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_rmb_midpoint_job(SyncRmbMidpointRequest())
            ),
            CronTrigger(hour=9, minute=20),
            id="rmb_midpoint_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_futures_realtime_job(SyncFuturesRealtimeRequest())
            ),
            CronTrigger(hour="7,10,13,16,19", minute=5),
            id="futures_realtime_intraday",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_profit_forecast_job(SyncProfitForecastRequest())
            ),
            CronTrigger(hour=19, minute=45),
            id="profit_forecast_daily",
            replace_existing=True,
        )


@app.on_event("shutdown")
async def shutdown_event() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)


@app.get("/health")
def health_check() -> dict[str, str]:
    """Simple health probe."""
    return {"status": "ok"}


@app.get("/stocks", response_model=StockListResponse)
def list_stocks(
    keyword: Optional[str] = Query(None, description="Keyword to search code/name/industry"),
    industry: Optional[str] = Query(None, description="Filter by industry"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    search_only: bool = Query(
        False,
        alias="searchOnly",
        description="When true, bypass filter metrics and return keyword matches directly.",
    ),
    pct_change_min: Optional[float] = Query(
        None,
        alias="pctChangeMin",
        description="Minimum daily percentage change filter.",
    ),
    pct_change_max: Optional[float] = Query(
        None,
        alias="pctChangeMax",
        description="Maximum daily percentage change filter.",
    ),
    volume_spike_min: Optional[float] = Query(
        None,
        alias="volumeSpikeMin",
        ge=0.0,
        description="Minimum volume spike ratio (latest volume / 10-day average).",
    ),
    market_cap_min: Optional[float] = Query(
        None,
        alias="marketCapMin",
        ge=0.0,
        description="Minimum market capitalization filter (absolute currency units).",
    ),
    market_cap_max: Optional[float] = Query(
        None,
        alias="marketCapMax",
        ge=0.0,
        description="Maximum market capitalization filter (absolute currency units).",
    ),
    pe_min: Optional[float] = Query(
        None,
        alias="peMin",
        description="Minimum PE ratio filter.",
    ),
    pe_max: Optional[float] = Query(
        None,
        alias="peMax",
        description="Maximum PE ratio filter.",
    ),
    roe_min: Optional[float] = Query(
        None,
        alias="roeMin",
        description="Minimum ROE filter.",
    ),
    net_income_qoq_min: Optional[float] = Query(
        None,
        alias="netIncomeQoqMin",
        description="Minimum net income QoQ ratio filter (allow negatives for declines).",
    ),
    net_income_yoy_min: Optional[float] = Query(
        None,
        alias="netIncomeYoyMin",
        description="Minimum net income YoY ratio filter (allow negatives for declines).",
    ),
    favorites_only: bool = Query(
        False,
        alias="favoritesOnly",
        description="When true, limit the response to favorite stocks only.",
    ),
    favorite_group: Optional[str] = Query(
        None,
        alias="favoriteGroup",
        description=(
            "Optional favorite group filter. "
            f"Use '{FAVORITE_GROUP_NONE_SENTINEL}' to show ungrouped favorites."
        ),
    ),
    sort_by: Optional[str] = Query(
        None,
        alias="sortBy",
        description="Optional sort field (pctChange1Y, pctChange6M, pctChange3M, pctChange1M, pctChange2W, pctChange1W).",
    ),
    sort_order: Optional[str] = Query(
        "desc",
        alias="sortOrder",
        description="Sort order: asc or desc (default desc).",
    ),
) -> StockListResponse:
    """Return paginated stock fundamentals enriched with latest trading data."""
    if keyword is not None:
        stripped_keyword = keyword.strip()
        keyword = stripped_keyword or None

    normalized_group, group_specified = _parse_favorite_group_query(favorite_group)
    effective_favorites_only = favorites_only or group_specified
    result = get_stock_overview(
        keyword=keyword,
        industry=industry,
        exchange=exchange,
        limit=None,
        offset=0,
        favorites_only=effective_favorites_only,
        favorite_group=normalized_group,
        favorite_group_specified=group_specified,
    )

    available_industries = sorted(
        {
            item.get("industry")
            for item in result["items"]
            if isinstance(item.get("industry"), str) and item.get("industry")
        }
    )

    def _passes_filters(payload: dict[str, object]) -> bool:
        def _extract_numeric(key: str) -> Optional[float]:
            value = payload.get(key)
            if value is None:
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(numeric):
                return None
            return numeric

        def _gt(key: str, threshold: float) -> bool:
            if threshold is None:
                return True
            numeric = _extract_numeric(key)
            if numeric is None:
                return False
            return numeric > threshold

        def _range(
            key: str, *, minimum: Optional[float] = None, maximum: Optional[float] = None
        ) -> bool:
            if minimum is None and maximum is None:
                return True
            numeric = _extract_numeric(key)
            if numeric is None:
                return False
            if minimum is not None and numeric < minimum:
                return False
            if maximum is not None and numeric > maximum:
                return False
            return True

        return all(
            (
                _range("pct_change", minimum=pct_change_min, maximum=pct_change_max),
                _gt("volume_spike", volume_spike_min),
                _range("pe_ratio", minimum=pe_min, maximum=pe_max),
                _range("market_cap", minimum=market_cap_min, maximum=market_cap_max),
                _gt("roe", roe_min),
                _gt("net_income_qoq_latest", net_income_qoq_min),
                _gt("net_income_yoy_latest", net_income_yoy_min),
            )
        )

    keyword_only_search = bool(keyword and keyword.strip())
    keyword_bypass = bool(search_only and keyword_only_search)
    filters_at_defaults = (
        pct_change_min is None
        and pct_change_max is None
        and volume_spike_min is None
        and market_cap_min is None
        and market_cap_max is None
        and pe_min is None
        and pe_max is None
        and roe_min is None
        and net_income_qoq_min is None
        and net_income_yoy_min is None
        and (industry is None or industry.lower() == "all")
        and (exchange is None or exchange.lower() == "all")
    )

    sort_field = None
    if sort_by:
        normalized_sort = sort_by.replace("_", "").lower()
        sort_field = SORTABLE_STOCK_FIELDS.get(normalized_sort)
    sort_direction = (sort_order or "desc").lower()
    if sort_direction not in {"asc", "desc"}:
        sort_direction = "desc"

    if effective_favorites_only or keyword_bypass or (keyword_only_search and filters_at_defaults):
        filtered_items = list(result["items"])
    else:
        filtered_items = [item for item in result["items"] if _passes_filters(item)]

    if sort_field:
        reverse = sort_direction != "asc"

        def _sort_value(payload: dict[str, object]) -> float:
            value = payload.get(sort_field)
            if value is None:
                return float("-inf") if reverse else float("inf")
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return float("-inf") if reverse else float("inf")
            if not math.isfinite(numeric):
                return float("-inf") if reverse else float("inf")
            return numeric

        filtered_items = sorted(filtered_items, key=_sort_value, reverse=reverse)

    start_index = offset if offset >= 0 else 0
    end_index = start_index + limit if limit is not None else None
    paged_items = filtered_items[start_index:end_index]

    items = [
        StockItem(
            code=item["code"],
            name=item.get("name"),
            industry=item.get("industry"),
            market=item.get("market"),
            exchange=item.get("exchange"),
            status=item.get("status"),
            lastPrice=item.get("last_price"),
            pctChange=item.get("pct_change"),
            volume=item.get("volume"),
            tradeDate=item.get("trade_date"),
            marketCap=item.get("market_cap"),
            peRatio=item.get("pe_ratio"),
            turnoverRate=item.get("turnover_rate"),
            pctChange1Y=item.get("pct_change_1y"),
            pctChange6M=item.get("pct_change_6m"),
            pctChange3M=item.get("pct_change_3m"),
            pctChange1M=item.get("pct_change_1m"),
            pctChange2W=item.get("pct_change_2w"),
            pctChange1W=item.get("pct_change_1w"),
            ma20=item.get("ma_20"),
            ma10=item.get("ma_10"),
            ma5=item.get("ma_5"),
            volumeSpike=item.get("volume_spike"),
            annDate=item.get("ann_date"),
            endDate=item.get("end_date"),
            basicEps=item.get("basic_eps"),
            revenue=item.get("revenue"),
            operateProfit=item.get("operate_profit"),
            netIncome=item.get("net_income"),
            grossMargin=item.get("gross_margin"),
            roe=item.get("roe"),
            netIncomeYoyLatest=item.get("net_income_yoy_latest"),
            netIncomeYoyPrev1=item.get("net_income_yoy_prev1"),
            netIncomeYoyPrev2=item.get("net_income_yoy_prev2"),
            netIncomeQoqLatest=item.get("net_income_qoq_latest"),
            revenueYoyLatest=item.get("revenue_yoy_latest"),
            revenueQoqLatest=item.get("revenue_qoq_latest"),
            roeYoyLatest=item.get("roe_yoy_latest"),
            roeQoqLatest=item.get("roe_qoq_latest"),
            favoriteGroup=item.get("favorite_group"),
            isFavorite=bool(item.get("is_favorite")),
        )
        for item in paged_items
    ]
    return StockListResponse(total=len(filtered_items), items=items, industries=available_industries)


@app.get("/stocks/search", response_model=StockListResponse)
def search_stocks_api(
    keyword: str = Query(..., min_length=1, description="Keyword to match code/name/industry."),
    limit: int = Query(20, ge=1, le=200),
) -> StockListResponse:
    stripped_keyword = keyword.strip()
    if not stripped_keyword:
        return StockListResponse(total=0, items=[], industries=[])

    settings = load_settings()
    runtime = load_runtime_config()
    dao = StockBasicDAO(settings.postgres)
    result = dao.query_fundamentals(
        keyword=stripped_keyword,
        limit=limit,
        offset=0,
        include_st=runtime.include_st,
        include_delisted=runtime.include_delisted,
    )

    items = [
        StockItem(
            code=item["code"],
            name=item.get("name"),
            industry=item.get("industry"),
            market=item.get("market"),
            exchange=item.get("exchange"),
            status=item.get("status"),
        )
        for item in result["items"]
    ]

    industries = sorted({item.industry for item in items if isinstance(item.industry, str) and item.industry})
    return StockListResponse(total=result["total"], items=items, industries=industries)


@app.get("/stocks/{code}", response_model=StockDetailResponse)
def get_stock_detail_api(
    code: str,
    history_limit: int = Query(
        180,
        ge=30,
        le=500,
        description="Number of most recent trading days to include in the candlestick series.",
    ),
) -> StockDetailResponse:
    detail = get_stock_detail(code, history_limit=history_limit)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Stock '{code}' not found")
    return StockDetailResponse(**detail)


@app.get("/favorites", response_model=FavoriteListResponse)
def list_favorites_api(
    group: Optional[str] = Query(
        None,
        alias="group",
        description=(
            "Optional favorite group filter. "
            f"Use '{FAVORITE_GROUP_NONE_SENTINEL}' to show ungrouped favorites."
        ),
    )
) -> FavoriteListResponse:
    """Return the persisted favorites list."""
    normalized_group, group_specified = _parse_favorite_group_query(group)
    if group_specified:
        if group == FAVORITE_GROUP_NONE_SENTINEL or (
            group is not None and not group.strip()
        ):
            entries = list_favorite_entries(group=FAVORITE_GROUP_NONE_SENTINEL)
        else:
            entries = list_favorite_entries(group=normalized_group)
    else:
        entries = list_favorite_entries()
    items = [
        FavoriteEntry(
            code=entry["code"],
            group=entry.get("group"),
            created_at=entry.get("created_at"),
            updated_at=entry.get("updated_at"),
        )
        for entry in entries
    ]
    return FavoriteListResponse(total=len(items), items=items)


@app.get("/favorites/groups", response_model=FavoriteGroupListResponse)
def list_favorite_groups_api() -> FavoriteGroupListResponse:
    """Return all available favorite groups."""
    groups = list_favorite_groups()
    items = [
        FavoriteGroupItem(name=entry.get("name"), total=int(entry.get("total") or 0))
        for entry in groups
    ]
    return FavoriteGroupListResponse(items=items)


@app.get("/favorites/{code}", response_model=FavoriteStatusResponse)
def get_favorite_status_api(code: str) -> FavoriteStatusResponse:
    """Return favorite status for a specific stock code."""
    try:
        result = get_favorite_status(code)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FavoriteStatusResponse(**result)


@app.put("/favorites/{code}", response_model=FavoriteStatusResponse)
def add_favorite_api(
    code: str,
    payload: FavoriteUpsertRequest | None = Body(default=None),
) -> FavoriteStatusResponse:
    """Mark a stock as favorite."""
    group_value = payload.group if payload else None
    normalized_group, _ = _parse_favorite_group_query(group_value)
    try:
        result = set_favorite_state(code, favorite=True, group=normalized_group)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FavoriteStatusResponse(**result)


@app.delete("/favorites/{code}", response_model=FavoriteStatusResponse)
def remove_favorite_api(code: str) -> FavoriteStatusResponse:
    """Remove a stock from favorites."""
    try:
        result = set_favorite_state(code, favorite=False)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FavoriteStatusResponse(**result)

@app.get("/fundamental-metrics", response_model=FundamentalMetricsListResponse)
def list_fundamental_metrics_api(
    keyword: Optional[str] = Query(None, description="Keyword to search code/name/industry"),
    market: Optional[str] = Query(None, description="Filter by market"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> FundamentalMetricsListResponse:
    """Return paginated derived fundamental metrics."""
    result = list_fundamental_metrics(
        keyword=keyword,
        market=market,
        exchange=exchange,
        limit=limit,
        offset=offset,
    )
    items = [
        FundamentalMetricItem(
            code=item["code"],
            name=item.get("name"),
            industry=item.get("industry"),
            market=item.get("market"),
            exchange=item.get("exchange"),
            netIncomeEndDateLatest=item.get("net_income_end_date_latest"),
            netIncomeEndDatePrev1=item.get("net_income_end_date_prev1"),
            netIncomeEndDatePrev2=item.get("net_income_end_date_prev2"),
            revenueEndDateLatest=item.get("revenue_end_date_latest"),
            roeEndDateLatest=item.get("roe_end_date_latest"),
            netIncomeYoyLatest=item.get("net_income_yoy_latest"),
            netIncomeYoyPrev1=item.get("net_income_yoy_prev1"),
            netIncomeYoyPrev2=item.get("net_income_yoy_prev2"),
            netIncomeQoqLatest=item.get("net_income_qoq_latest"),
            revenueYoyLatest=item.get("revenue_yoy_latest"),
            revenueQoqLatest=item.get("revenue_qoq_latest"),
            roeYoyLatest=item.get("roe_yoy_latest"),
            roeQoqLatest=item.get("roe_qoq_latest"),
        )
        for item in result["items"]
    ]
    return FundamentalMetricsListResponse(total=result["total"], items=items)


@app.get("/performance/express", response_model=PerformanceExpressListResponse)
def list_performance_express_entries(
    limit: int = Query(100, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
    start_date: Optional[str] = Query(None, alias="startDate"),
    end_date: Optional[str] = Query(None, alias="endDate"),
    keyword: Optional[str] = Query(None, description="Optional keyword filtering ts_code or stock name."),
) -> PerformanceExpressListResponse:
    normalized_keyword = keyword.strip() if keyword else None
    result = list_performance_express(
        limit=limit,
        offset=offset,
        start_date=start_date,
        end_date=end_date,
        keyword=normalized_keyword,
    )
    items = [
        PerformanceExpressRecord(
            symbol=entry.get("symbol"),
            ts_code=entry.get("ts_code"),
            name=entry.get("name"),
            industry=entry.get("industry"),
            market=entry.get("market"),
            ann_date=entry.get("announcement_date"),
            end_date=entry.get("report_period"),
            report_period=entry.get("report_period"),
            announcement_date=entry.get("announcement_date"),
            eps=entry.get("eps"),
            revenue=entry.get("revenue"),
            revenue_prev=entry.get("revenue_prev"),
            revenue_yoy=entry.get("revenue_yoy"),
            revenue_qoq=entry.get("revenue_qoq"),
            net_profit=entry.get("net_profit"),
            net_profit_prev=entry.get("net_profit_prev"),
            net_profit_yoy=entry.get("net_profit_yoy"),
            net_profit_qoq=entry.get("net_profit_qoq"),
            net_assets_per_share=entry.get("net_assets_per_share"),
            return_on_equity=entry.get("return_on_equity"),
            row_number=entry.get("row_number"),
            n_income=entry.get("net_profit"),
            diluted_eps=entry.get("eps"),
            diluted_roe=entry.get("return_on_equity"),
            yoy_net_profit=entry.get("net_profit_yoy"),
            updated_at=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return PerformanceExpressListResponse(total=int(result.get("total", 0)), items=items)


@app.get("/performance/forecast", response_model=PerformanceForecastListResponse)
def list_performance_forecast_entries(
    limit: int = Query(100, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
    start_date: Optional[str] = Query(None, alias="startDate"),
    end_date: Optional[str] = Query(None, alias="endDate"),
    keyword: Optional[str] = Query(None, description="Optional keyword filtering ts_code or stock name."),
) -> PerformanceForecastListResponse:
    normalized_keyword = keyword.strip() if keyword else None
    result = list_performance_forecast(
        limit=limit,
        offset=offset,
        start_date=start_date,
        end_date=end_date,
        keyword=normalized_keyword,
    )
    items = [
        PerformanceForecastRecord(
            symbol=entry.get("symbol"),
            ts_code=entry.get("ts_code"),
            name=entry.get("name"),
            industry=entry.get("industry"),
            market=entry.get("market"),
            ann_date=entry.get("announcement_date"),
            end_date=entry.get("report_period"),
            report_period=entry.get("report_period"),
            announcement_date=entry.get("announcement_date"),
            forecast_metric=entry.get("forecast_metric"),
            change_description=entry.get("change_description"),
            forecast_value=entry.get("forecast_value"),
            change_rate=entry.get("change_rate"),
            change_reason=entry.get("change_reason"),
            forecast_type=entry.get("forecast_type"),
            last_year_value=entry.get("last_year_value"),
            row_number=entry.get("row_number"),
            type=entry.get("forecast_type"),
            p_change_min=entry.get("change_rate"),
            p_change_max=entry.get("change_rate"),
            net_profit_min=entry.get("forecast_value"),
            net_profit_max=entry.get("forecast_value"),
            updated_at=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return PerformanceForecastListResponse(total=int(result.get("total", 0)), items=items)


@app.get("/profit-forecast", response_model=ProfitForecastListResponse)
def list_profit_forecast_entries(
    limit: int = Query(100, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
    keyword: Optional[str] = Query(None, description="Optional keyword filtering code or name."),
    industry: Optional[str] = Query(None, description="Optional industry filter."),
    year: Optional[int] = Query(None, description="Optional forecast year filter."),
) -> ProfitForecastListResponse:
    normalized_keyword = keyword.strip() if keyword else None
    normalized_industry = industry.strip() if industry else None
    result = list_profit_forecast(
        limit=limit,
        offset=offset,
        keyword=normalized_keyword,
        industry=normalized_industry,
        forecast_year=year,
    )
    items = [
        ProfitForecastItem(
            code=item.get("code"),
            symbol=item.get("symbol"),
            tsCode=item.get("tsCode"),
            name=item.get("name"),
            industry=item.get("industry"),
            market=item.get("market"),
            reportCount=item.get("reportCount"),
            ratings=ProfitForecastRating(**(item.get("ratings") or {})),
            forecasts=[ProfitForecastPoint(**point) for point in item.get("forecasts", [])],
            updatedAt=item.get("updatedAt"),
        )
        for item in result.get("items", [])
    ]
    return ProfitForecastListResponse(
        total=int(result.get("total", 0)),
        items=items,
        industries=list(result.get("industries", [])),
        years=list(result.get("years", [])),
    )


@app.get("/macro/global-indices", response_model=GlobalIndexListResponse)
def list_global_indices_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> GlobalIndexListResponse:
    result = list_global_indices(limit=limit, offset=offset)
    items = [
        GlobalIndexRecord(
            code=entry.get("code"),
            seq=entry.get("seq"),
            name=entry.get("name"),
            latestPrice=entry.get("latest_price"),
            changeAmount=entry.get("change_amount"),
            changePercent=entry.get("change_percent"),
            openPrice=entry.get("open_price"),
            highPrice=entry.get("high_price"),
            lowPrice=entry.get("low_price"),
            prevClose=entry.get("prev_close"),
            amplitude=entry.get("amplitude"),
            lastQuoteTime=entry.get("last_quote_time"),
            updatedAt=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return GlobalIndexListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("last_synced_at") or result.get("updated_at"),
    )


@app.get("/macro/dollar-index", response_model=DollarIndexListResponse)
def list_dollar_index_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
    start_date: Optional[date] = Query(None, alias="startDate", description="Filter results on or after this date."),
    end_date: Optional[date] = Query(None, alias="endDate", description="Filter results on or before this date."),
) -> DollarIndexListResponse:
    result = list_dollar_index(limit=limit, offset=offset, start_date=start_date, end_date=end_date)
    items = [
        DollarIndexRecord(
            tradeDate=entry.get("trade_date"),
            code=entry.get("code"),
            name=entry.get("name"),
            openPrice=entry.get("open_price"),
            closePrice=entry.get("close_price"),
            highPrice=entry.get("high_price"),
            lowPrice=entry.get("low_price"),
            amplitude=entry.get("amplitude"),
            updatedAt=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return DollarIndexListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("last_synced_at") or result.get("updated_at"),
    )


@app.get("/macro/rmb-midpoint", response_model=RmbMidpointListResponse)
def list_rmb_midpoint_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
    start_date: Optional[date] = Query(None, alias="startDate", description="Filter results on or after this date."),
    end_date: Optional[date] = Query(None, alias="endDate", description="Filter results on or before this date."),
) -> RmbMidpointListResponse:
    result = list_rmb_midpoint_rates(limit=limit, offset=offset, start_date=start_date, end_date=end_date)
    items: List[RmbMidpointRecord] = []
    for entry in result.get("items", []):
        payload = {
            "tradeDate": entry.get("trade_date"),
            "usd": entry.get("usd"),
            "eur": entry.get("eur"),
            "jpy": entry.get("jpy"),
            "hkd": entry.get("hkd"),
            "gbp": entry.get("gbp"),
            "aud": entry.get("aud"),
            "cad": entry.get("cad"),
            "nzd": entry.get("nzd"),
            "sgd": entry.get("sgd"),
            "chf": entry.get("chf"),
            "myr": entry.get("myr"),
            "rub": entry.get("rub"),
            "zar": entry.get("zar"),
            "krw": entry.get("krw"),
            "aed": entry.get("aed"),
            "sar": entry.get("sar"),
            "huf": entry.get("huf"),
            "pln": entry.get("pln"),
            "dkk": entry.get("dkk"),
            "sek": entry.get("sek"),
            "nok": entry.get("nok"),
            "try": entry.get("try"),
            "mxn": entry.get("mxn"),
            "thb": entry.get("thb"),
            "updatedAt": entry.get("updated_at"),
        }
        items.append(RmbMidpointRecord(**payload))

    return RmbMidpointListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("last_synced_at") or result.get("updated_at"),
    )


@app.get("/macro/futures-realtime", response_model=FuturesRealtimeListResponse)
def list_futures_realtime_api(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> FuturesRealtimeListResponse:
    result = list_futures_realtime(limit=limit, offset=offset)
    items = [
        FuturesRealtimeRecord(
            name=entry.get("name"),
            code=entry.get("code"),
            lastPrice=entry.get("last_price"),
            priceCny=entry.get("price_cny"),
            changeAmount=entry.get("change_amount"),
            changePercent=entry.get("change_percent"),
            openPrice=entry.get("open_price"),
            highPrice=entry.get("high_price"),
            lowPrice=entry.get("low_price"),
            prevSettlement=entry.get("prev_settlement"),
            openInterest=entry.get("open_interest"),
            bidPrice=entry.get("bid_price"),
            askPrice=entry.get("ask_price"),
            quoteTime=entry.get("quote_time"),
            tradeDate=entry.get("trade_date"),
            updatedAt=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return FuturesRealtimeListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("last_synced_at") or result.get("updated_at"),
    )


@app.get("/fund-flow/industry", response_model=IndustryFundFlowListResponse)
def list_industry_fund_flow_entries(
    symbol: Optional[str] = Query(
        None,
        description="Optional ranking symbol filter (例如: 即时, 3日排行).",
    ),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> IndustryFundFlowListResponse:
    result = list_industry_fund_flow(symbol=symbol, limit=limit, offset=offset)
    items = [
        IndustryFundFlowRecord(
            symbol=entry.get("symbol"),
            industry=entry.get("industry"),
            rank=entry.get("rank"),
            industry_index=entry.get("industry_index"),
            price_change_percent=entry.get("price_change_percent"),
            stage_change_percent=entry.get("stage_change_percent"),
            inflow=entry.get("inflow"),
            outflow=entry.get("outflow"),
            net_amount=entry.get("net_amount"),
            company_count=entry.get("company_count"),
            leading_stock=entry.get("leading_stock"),
            leading_stock_change_percent=entry.get("leading_stock_change_percent"),
            current_price=entry.get("current_price"),
            updated_at=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return IndustryFundFlowListResponse(total=int(result.get("total", 0)), items=items)


@app.get("/fund-flow/concept", response_model=ConceptFundFlowListResponse)
def list_concept_fund_flow_entries(
    symbol: Optional[str] = Query(
        None,
        description="Optional ranking symbol filter (例如: 即时, 3日排行).",
    ),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> ConceptFundFlowListResponse:
    result = list_concept_fund_flow(symbol=symbol, limit=limit, offset=offset)
    items = [
        ConceptFundFlowRecord(
            symbol=entry.get("symbol"),
            concept=entry.get("concept"),
            rank=entry.get("rank"),
            concept_index=entry.get("concept_index"),
            price_change_percent=entry.get("price_change_percent"),
            stage_change_percent=entry.get("stage_change_percent"),
            inflow=entry.get("inflow"),
            outflow=entry.get("outflow"),
            net_amount=entry.get("net_amount"),
            company_count=entry.get("company_count"),
            leading_stock=entry.get("leading_stock"),
            leading_stock_change_percent=entry.get("leading_stock_change_percent"),
            current_price=entry.get("current_price"),
            updated_at=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return ConceptFundFlowListResponse(total=int(result.get("total", 0)), items=items)


@app.get("/fund-flow/individual", response_model=IndividualFundFlowListResponse)
def list_individual_fund_flow_entries(
    symbol: Optional[str] = Query(
        None,
        description="Optional ranking symbol filter (例如: 即时, 3日排行).",
    ),
    code: Optional[str] = Query(
        None,
        description="Optional stock code filter (例如: 000063.SZ).",
    ),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> IndividualFundFlowListResponse:
    result = list_individual_fund_flow(symbol=symbol, stock_code=code, limit=limit, offset=offset)
    items = [
        IndividualFundFlowRecord(
            symbol=entry.get("symbol"),
            stock_code=entry.get("stock_code"),
            stock_name=entry.get("stock_name"),
            rank=entry.get("rank"),
            latest_price=entry.get("latest_price"),
            price_change_percent=entry.get("price_change_percent"),
            stage_change_percent=entry.get("stage_change_percent"),
            turnover_rate=entry.get("turnover_rate"),
            continuous_turnover_rate=entry.get("continuous_turnover_rate"),
            inflow=entry.get("inflow"),
            outflow=entry.get("outflow"),
            net_amount=entry.get("net_amount"),
            net_inflow=entry.get("net_inflow"),
            turnover_amount=entry.get("turnover_amount"),
            updated_at=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return IndividualFundFlowListResponse(total=int(result.get("total", 0)), items=items)


@app.get("/fund-flow/big-deal", response_model=BigDealFundFlowListResponse)
def list_big_deal_fund_flow_entries(
    limit: int = Query(100, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
    side: Optional[str] = Query(
        None,
        description="Optional filter by trade side (e.g. 买盘 / 卖盘).",
    ),
    code: Optional[str] = Query(
        None,
        description="Optional stock code filter (例如: 000063.SZ).",
    ),
) -> BigDealFundFlowListResponse:
    result = list_big_deal_fund_flow(limit=limit, offset=offset, side=side, stock_code=code)
    items = [
        BigDealFundFlowRecord(
            trade_time=entry.get("trade_time"),
            stock_code=entry.get("stock_code"),
            stock_name=entry.get("stock_name"),
            trade_price=entry.get("trade_price"),
            trade_volume=entry.get("trade_volume"),
            trade_amount=entry.get("trade_amount"),
            trade_side=entry.get("trade_side"),
            price_change_percent=entry.get("price_change_percent"),
            price_change=entry.get("price_change"),
            updated_at=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return BigDealFundFlowListResponse(total=int(result.get("total", 0)), items=items)


@app.get("/finance-breakfast", response_model=List[FinanceBreakfastItem])
async def list_finance_breakfast_entries(
    limit: int = Query(50, ge=1, le=200, description="Maximum number of entries to return."),
) -> List[FinanceBreakfastItem]:
    try:
        entries = list_finance_breakfast(limit=limit)
    except Exception as exc:
        logger.warning("Finance breakfast query failed: %s", exc)
        entries = []
    if not entries:
        try:
            asyncio.get_running_loop().create_task(
                safe_start_finance_breakfast_job(SyncFinanceBreakfastRequest())
            )
        except RuntimeError:
            logger.debug("Finance breakfast sync could not be scheduled (no running loop).")
    return [
        FinanceBreakfastItem(
            title=entry.get("title", ""),
            summary=entry.get("summary"),
            content=entry.get("content"),
            ai_extract=entry.get("ai_extract"),
            ai_extract_summary=entry.get("ai_extract_summary"),
            ai_extract_detail=entry.get("ai_extract_detail"),
            published_at=entry.get("published_at"),
            url=entry.get("url"),
        )
        for entry in entries
    ]


@app.get("/control/status", response_model=ControlStatusResponse)
def get_control_status() -> ControlStatusResponse:
    config = load_runtime_config()
    settings = load_settings()

    stats_map: Dict[str, dict] = {}
    try:
        stats_map["stock_basic"] = StockBasicDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect stock_basic stats: %s", exc)
        stats_map["stock_basic"] = {}
    try:
        stats_map["daily_trade"] = DailyTradeDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect daily_trade stats: %s", exc)
        stats_map["daily_trade"] = {}
    try:
        stats_map["daily_indicator"] = DailyIndicatorDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect daily_indicator stats: %s", exc)
        stats_map["daily_indicator"] = {}
    try:
        stats_map["daily_trade_metrics"] = DailyTradeMetricsDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect daily_trade_metrics stats: %s", exc)
        stats_map["daily_trade_metrics"] = {}
    try:
        stats_map["fundamental_metrics"] = FundamentalMetricsDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect fundamental_metrics stats: %s", exc)
        stats_map["fundamental_metrics"] = {}
    try:
        stats_map["income_statement"] = IncomeStatementDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect income_statement stats: %s", exc)
        stats_map["income_statement"] = {}
    try:
        stats_map["financial_indicator"] = FinancialIndicatorDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect financial_indicator stats: %s", exc)
        stats_map["financial_indicator"] = {}
    try:
        stats_map["performance_express"] = PerformanceExpressDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect performance_express stats: %s", exc)
        stats_map["performance_express"] = {}
    try:
        stats_map["performance_forecast"] = PerformanceForecastDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect performance_forecast stats: %s", exc)
        stats_map["performance_forecast"] = {}
    try:
        stats_map["profit_forecast"] = ProfitForecastDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect profit_forecast stats: %s", exc)
        stats_map["profit_forecast"] = {}
    try:
        stats_map["global_index"] = GlobalIndexDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect global_index stats: %s", exc)
        stats_map["global_index"] = {}
    try:
        stats_map["dollar_index"] = DollarIndexDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect dollar_index stats: %s", exc)
        stats_map["dollar_index"] = {}
    try:
        stats_map["rmb_midpoint"] = RmbMidpointDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect rmb_midpoint stats: %s", exc)
        stats_map["rmb_midpoint"] = {}
    try:
        stats_map["futures_realtime"] = FuturesRealtimeDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect futures_realtime stats: %s", exc)
        stats_map["futures_realtime"] = {}
    try:
        stats_map["industry_fund_flow"] = IndustryFundFlowDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect industry_fund_flow stats: %s", exc)
        stats_map["industry_fund_flow"] = {}
    try:
        stats_map["concept_fund_flow"] = ConceptFundFlowDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect concept_fund_flow stats: %s", exc)
        stats_map["concept_fund_flow"] = {}
    try:
        stats_map["individual_fund_flow"] = IndividualFundFlowDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect individual_fund_flow stats: %s", exc)
        stats_map["individual_fund_flow"] = {}
    try:
        stats_map["big_deal_fund_flow"] = BigDealFundFlowDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect big_deal_fund_flow stats: %s", exc)
        stats_map["big_deal_fund_flow"] = {}
    try:
        stats_map["stock_main_business"] = StockMainBusinessDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect stock_main_business stats: %s", exc)
        stats_map["stock_main_business"] = {}
    try:
        stats_map["stock_main_composition"] = StockMainCompositionDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect stock_main_composition stats: %s", exc)
        stats_map["stock_main_composition"] = {}
    try:
        stats_map["finance_breakfast"] = FinanceBreakfastDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect finance_breakfast stats: %s", exc)
        stats_map["finance_breakfast"] = {}

    jobs: Dict[str, JobStatusPayload] = {}
    for name, info in monitor.snapshot().items():
        stats = stats_map.get(name, {})
        finished_at = info.get("finishedAt") or stats.get("updated_at")
        if finished_at is not None and hasattr(finished_at, "isoformat"):
            finished_at = finished_at.isoformat()
        total_rows = info.get("totalRows")
        if total_rows is None:
            total_rows = stats.get("count")
        if total_rows is not None:
            try:
                total_rows = int(total_rows)
            except (TypeError, ValueError):
                pass
        logger.debug(
            "control status job=%s snapshot=%s stats=%s -> finished=%s total=%s",
            name,
            info,
            stats,
            finished_at,
            total_rows,
        )
        jobs[name] = JobStatusPayload(
            status=info.get("status"),
            started_at=info.get("startedAt"),
            finished_at=finished_at,
            progress=info.get("progress", 0.0),
            message=info.get("message"),
            total_rows=total_rows,
            last_duration=info.get("lastDuration"),
            last_market=info.get("lastMarket"),
            error=info.get("error"),
        )

    for name, stats in stats_map.items():
        if name in jobs:
            continue
        finished_at = stats.get("updated_at")
        if finished_at is not None and hasattr(finished_at, "isoformat"):
            finished_at = finished_at.isoformat()
        total_rows = stats.get("count")
        if total_rows is not None:
            try:
                total_rows = int(total_rows)
            except (TypeError, ValueError):
                pass
        jobs[name] = JobStatusPayload(
            status="idle",
            started_at=None,
            finished_at=finished_at,
            progress=0.0,
            message=None,
            total_rows=total_rows,
            last_duration=None,
            last_market=None,
            error=None,
        )

    return ControlStatusResponse(jobs=jobs, config=_runtime_config_to_payload(config))


@app.put("/control/config", response_model=RuntimeConfigPayload)
def update_runtime_config(payload: RuntimeConfigPayload) -> RuntimeConfigPayload:
    config = RuntimeConfig(
        include_st=payload.include_st,
        include_delisted=payload.include_delisted,
        daily_trade_window_days=payload.daily_trade_window_days,
    )
    save_runtime_config(config)
    return _runtime_config_to_payload(config)


@app.post("/control/sync/stock-basic")
async def control_sync_stock_basic(payload: SyncStockBasicRequest) -> dict[str, str]:
    await start_stock_basic_job(payload)
    return {"status": "started"}


@app.post("/control/sync/daily-trade")
async def control_sync_daily_trade(payload: SyncDailyTradeRequest) -> dict[str, str]:
    await start_daily_trade_job(payload)
    return {"status": "started"}


@app.post("/control/sync/daily-trade-metrics")
async def control_sync_daily_trade_metrics(payload: SyncDailyTradeMetricsRequest) -> dict[str, str]:
    await start_daily_trade_metrics_job(payload)
    return {"status": "started"}


@app.post("/control/sync/fundamental-metrics")
async def control_sync_fundamental_metrics(payload: SyncFundamentalMetricsRequest) -> dict[str, str]:
    await start_fundamental_metrics_job(payload)
    return {"status": "started"}


@app.post("/control/sync/daily-indicators")
async def control_sync_daily_indicators(payload: SyncDailyIndicatorRequest) -> dict[str, str]:
    await start_daily_indicator_job(payload)
    return {"status": "started"}


@app.post("/control/sync/income-statements")
async def control_sync_income_statements(payload: SyncIncomeStatementRequest) -> dict[str, str]:
    await start_income_statement_job(payload)
    return {"status": "started"}


@app.post("/control/sync/financial-indicators")
async def control_sync_financial_indicators(payload: SyncFinancialIndicatorRequest) -> dict[str, str]:
    await start_financial_indicator_job(payload)
    return {"status": "started"}


@app.post("/control/sync/performance-express")
async def control_sync_performance_express(payload: SyncPerformanceExpressRequest) -> dict[str, str]:
    await start_performance_express_job(payload)
    return {"status": "started"}


@app.post("/control/sync/performance-forecast")
async def control_sync_performance_forecast(payload: SyncPerformanceForecastRequest) -> dict[str, str]:
    await start_performance_forecast_job(payload)
    return {"status": "started"}


@app.post("/control/sync/profit-forecast")
async def control_sync_profit_forecast(payload: SyncProfitForecastRequest) -> dict[str, str]:
    await start_profit_forecast_job(payload)
    return {"status": "started"}


@app.post("/control/sync/global-indices")
async def control_sync_global_indices(payload: SyncGlobalIndexRequest) -> dict[str, str]:
    await start_global_index_job(payload)
    return {"status": "started"}


@app.post("/control/sync/dollar-index")
async def control_sync_dollar_index(payload: SyncDollarIndexRequest) -> dict[str, str]:
    await start_dollar_index_job(payload)
    return {"status": "started"}


@app.post("/control/sync/rmb-midpoint")
async def control_sync_rmb_midpoint(payload: SyncRmbMidpointRequest) -> dict[str, str]:
    await start_rmb_midpoint_job(payload)
    return {"status": "started"}


@app.post("/control/sync/futures-realtime")
async def control_sync_futures_realtime(payload: SyncFuturesRealtimeRequest) -> dict[str, str]:
    await start_futures_realtime_job(payload)
    return {"status": "started"}


@app.post("/control/sync/industry-fund-flow")
async def control_sync_industry_fund_flow(payload: SyncIndustryFundFlowRequest) -> dict[str, str]:
    await start_industry_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/concept-fund-flow")
async def control_sync_concept_fund_flow(payload: SyncConceptFundFlowRequest) -> dict[str, str]:
    await start_concept_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/individual-fund-flow")
async def control_sync_individual_fund_flow(payload: SyncIndividualFundFlowRequest) -> dict[str, str]:
    await start_individual_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/big-deal-fund-flow")
async def control_sync_big_deal_fund_flow(payload: SyncBigDealFundFlowRequest) -> dict[str, str]:
    await start_big_deal_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/stock-main-business")
async def control_sync_stock_main_business(payload: SyncStockMainBusinessRequest) -> dict[str, str]:
    await start_stock_main_business_job(payload)
    return {"status": "started"}


@app.post("/control/sync/stock-main-composition")
async def control_sync_stock_main_composition(payload: SyncStockMainCompositionRequest) -> dict[str, str]:
    await start_stock_main_composition_job(payload)
    return {"status": "started"}


@app.post("/control/sync/finance-breakfast")
async def control_sync_finance_breakfast(payload: SyncFinanceBreakfastRequest) -> dict[str, str]:
    await start_finance_breakfast_job(payload)
    return {"status": "started"}


@app.get("/control/debug/stats", include_in_schema=False)
def control_debug_stats() -> dict[str, object]:
    settings = load_settings()
    return {
        "stats": {
            "stock_basic": StockBasicDAO(settings.postgres).stats(),
            "daily_trade": DailyTradeDAO(settings.postgres).stats(),
            "daily_indicator": DailyIndicatorDAO(settings.postgres).stats(),
            "daily_trade_metrics": DailyTradeMetricsDAO(settings.postgres).stats(),
            "income_statement": IncomeStatementDAO(settings.postgres).stats(),
            "financial_indicator": FinancialIndicatorDAO(settings.postgres).stats(),
            "finance_breakfast": FinanceBreakfastDAO(settings.postgres).stats(),
            "fundamental_metrics": FundamentalMetricsDAO(settings.postgres).stats(),
            "stock_main_business": StockMainBusinessDAO(settings.postgres).stats(),
            "stock_main_composition": StockMainCompositionDAO(settings.postgres).stats(),
        },
        "monitor": monitor.snapshot(),
    }


# Backwards compatible endpoints

@app.post("/sync/stock-basic", response_model=SyncStockBasicResponse)
def trigger_stock_basic_sync(payload: SyncStockBasicRequest) -> SyncStockBasicResponse:
    rows = sync_stock_basic(
        list_statuses=tuple(payload.list_statuses or ["L", "D", "P"]),
        market=payload.market,
    )
    return SyncStockBasicResponse(rows=rows)


@app.post("/sync/daily-trade", response_model=SyncDailyTradeResponse)
def trigger_daily_trade_sync(payload: SyncDailyTradeRequest) -> SyncDailyTradeResponse:
    result = sync_daily_trade(
        batch_size=payload.batch_size or 20,
        window_days=payload.window_days,
        start_date=payload.start_date,
        end_date=payload.end_date,
        codes=payload.codes,
        batch_pause_seconds=payload.batch_pause_seconds or 0.6,
    )
    return SyncDailyTradeResponse(
        rows=result["rows"],
        elapsedSeconds=result["elapsed_seconds"],
    )


@app.post("/sync/daily-trade-metrics", response_model=SyncDailyTradeMetricsResponse)
def trigger_daily_trade_metrics_sync(payload: SyncDailyTradeMetricsRequest) -> SyncDailyTradeMetricsResponse:
    kwargs: Dict[str, object] = {}
    if payload.history_window_days is not None:
        kwargs["history_window_days"] = payload.history_window_days
    result = sync_daily_trade_metrics(**kwargs)
    return SyncDailyTradeMetricsResponse(
        rows=result["rows"],
        tradeDate=result.get("trade_date"),
        elapsedSeconds=result["elapsed_seconds"],
    )


@app.post("/sync/fundamental-metrics", response_model=SyncFundamentalMetricsResponse)
def trigger_fundamental_metrics_sync(payload: SyncFundamentalMetricsRequest) -> SyncFundamentalMetricsResponse:
    kwargs: Dict[str, object] = {}
    if payload.per_code is not None:
        kwargs["per_code"] = payload.per_code
    result = sync_fundamental_metrics(**kwargs)
    return SyncFundamentalMetricsResponse(
        rows=int(result["rows"]),
        elapsedSeconds=float(result["elapsed_seconds"]),
    )


@app.post("/sync/daily-indicators", response_model=SyncDailyIndicatorResponse)
def trigger_daily_indicator_sync(payload: SyncDailyIndicatorRequest) -> SyncDailyIndicatorResponse:
    result = sync_daily_indicator(
        trade_date=payload.trade_date,
    )
    return SyncDailyIndicatorResponse(
        rows=int(result["rows"]),
        tradeDate=str(result["trade_date"]),
        elapsedSeconds=float(result["elapsed_seconds"]),
    )


@app.post("/sync/income-statements", response_model=SyncIncomeStatementResponse)
def trigger_income_statement_sync(payload: SyncIncomeStatementRequest) -> SyncIncomeStatementResponse:
    result = sync_income_statements(
        codes=payload.codes,
        initial_periods=payload.initial_periods,
    )
    return SyncIncomeStatementResponse(
        rows=int(result["rows"]),
        codes=[str(code) for code in result.get("codes", [])],
        code_count=int(result.get("code_count", len(result.get("codes", [])))),
        total_codes=int(result.get("total_codes", result.get("code_count", 0))),
        elapsed_seconds=float(result.get("elapsed_seconds", 0.0)),
    )


@app.post("/sync/financial-indicators", response_model=SyncFinancialIndicatorResponse)
def trigger_financial_indicator_sync(payload: SyncFinancialIndicatorRequest) -> SyncFinancialIndicatorResponse:
    result = sync_financial_indicators(
        codes=payload.codes,
        limit=payload.limit,
    )
    return SyncFinancialIndicatorResponse(
        rows=int(result["rows"]),
        codes=[str(code) for code in result.get("codes", [])],
        code_count=int(result.get("code_count", len(result.get("codes", [])))),
        total_codes=int(result.get("total_codes", result.get("code_count", 0))),
        elapsed_seconds=float(result.get("elapsed_seconds", 0.0)),
    )


@app.post("/sync/finance-breakfast", response_model=SyncFinanceBreakfastResponse)
def trigger_finance_breakfast_sync(payload: SyncFinanceBreakfastRequest) -> SyncFinanceBreakfastResponse:
    del payload
    result = sync_finance_breakfast()
    return SyncFinanceBreakfastResponse(
        rows=int(result["rows"]),
        elapsedSeconds=float(result.get("elapsed_seconds", 0.0)),
    )


__all__ = ["app"]
