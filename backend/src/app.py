"""
FastAPI application exposing Trend View backend services and control panel APIs.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
import json
import math
import logging
import time
import re
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple, Union, Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Body, FastAPI, HTTPException, Query, Path
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

from .config.runtime_config import (
    RuntimeConfig,
    VolumeSurgeConfig,
    load_runtime_config,
    save_runtime_config,
    normalize_concept_alias_map,
)
from .config.settings import load_settings
from .dao import (
    DailyIndicatorDAO,
    DailyTradeDAO,
    DailyTradeMetricsDAO,
    FinancialIndicatorDAO,
    NewsArticleDAO,
    NewsMarketInsightDAO,
    NewsSectorInsightDAO,
    IndexHistoryDAO,
    TradeCalendarDAO,
    IncomeStatementDAO,
    FundamentalMetricsDAO,
    PerformanceExpressDAO,
    PerformanceForecastDAO,
    ProfitForecastDAO,
    GlobalIndexDAO,
    RealtimeIndexDAO,
    DollarIndexDAO,
    RmbMidpointDAO,
    FuturesRealtimeDAO,
    FedStatementDAO,
    PeripheralInsightDAO,
    MacroLeverageDAO,
    MacroSocialFinancingDAO,
    MacroCpiDAO,
    MacroPmiDAO,
    MacroM2DAO,
    MacroPpiDAO,
    MacroLprDAO,
    MacroShiborDAO,
    MacroInsightDAO,
    IndustryFundFlowDAO,
    ConceptFundFlowDAO,
    ConceptIndexHistoryDAO,
    ConceptConstituentDAO,
    ConceptDirectoryDAO,
    ConceptInsightDAO,
    IndustryInsightDAO,
    IndividualFundFlowDAO,
    BigDealFundFlowDAO,
    HSGTFundFlowDAO,
    MarginAccountDAO,
    MarketActivityDAO,
    MarketFundFlowDAO,
    StockBasicDAO,
    StockMainBusinessDAO,
    StockMainCompositionDAO,
    MarketOverviewInsightDAO,
)
from .services import (
    get_stock_detail,
    get_stock_overview,
    get_favorite_status,
    list_favorite_entries,
    list_favorite_groups,
    list_news_articles,
    list_fundamental_metrics,
    list_performance_express,
    list_performance_forecast,
    list_profit_forecast,
    list_global_indices,
    list_realtime_indices,
    list_dollar_index,
    list_rmb_midpoint_rates,
    list_futures_realtime,
    list_fed_statements,
    get_latest_peripheral_insight,
    get_latest_macro_insight,
    list_macro_leverage_ratios,
    sync_macro_leverage_ratios,
    list_social_financing_ratios,
    sync_social_financing_ratios,
    list_macro_cpi,
    sync_macro_cpi,
    list_macro_pmi,
    sync_macro_pmi,
    list_macro_m2,
    sync_macro_m2,
    list_macro_ppi,
    sync_macro_ppi,
    list_macro_lpr,
    sync_macro_lpr,
    list_macro_shibor,
    sync_macro_shibor,
    list_industry_fund_flow,
    list_concept_fund_flow,
    list_concept_index_history,
    list_concept_insights,
    build_industry_snapshot,
    generate_industry_insight_summary,
    get_latest_industry_insight,
    list_industry_insights,
    list_industry_news,
    list_individual_fund_flow,
    list_big_deal_fund_flow,
    list_hsgt_fund_flow,
    list_stock_news,
    add_stock_note,
    list_stock_notes,
    list_recent_stock_notes,
    list_margin_account_info,
    list_market_activity,
    list_market_fund_flow,
    build_sector_fund_flow_snapshot,
    set_favorite_state,
    sync_daily_indicator,
    sync_financial_indicators,
    sync_finance_breakfast,
    sync_global_flash,
    classify_relevance_batch,
    classify_impact_batch,
    sync_trade_calendar,
    sync_income_statements,
    sync_daily_trade,
    sync_daily_trade_metrics,
    sync_fundamental_metrics,
    sync_performance_express,
    sync_performance_forecast,
    sync_profit_forecast,
    sync_global_indices,
    sync_realtime_indices,
    sync_dollar_index,
    sync_rmb_midpoint_rates,
    sync_futures_realtime,
    sync_fed_statements,
    generate_peripheral_insight,
    generate_market_insight_summary,
    collect_recent_market_headlines,
    get_latest_market_insight,
    list_market_insights,
    collect_recent_sector_headlines,
    build_sector_group_snapshot,
    generate_sector_insight_summary,
    get_latest_sector_insight,
    list_sector_insights,
    list_index_history,
    sync_index_history,
    sync_industry_fund_flow,
    sync_concept_fund_flow,
    sync_concept_index_history,
    generate_concept_insight_summary,
    get_latest_concept_insight,
    build_concept_snapshot,
    list_concept_news,
    search_concepts,
    search_industries,
    list_all_concepts,
    list_all_industries,
    list_concept_watchlist,
    list_industry_watchlist,
    get_concept_status,
    get_industry_status,
    delete_concept_watch_entry,
    delete_industry_watch_entry,
    set_concept_watch_state,
    set_industry_watch_state,
    refresh_concept_history,
    refresh_industry_history,
    generate_concept_volume_price_reasoning,
    get_latest_volume_price_reasoning,
    list_volume_price_history,
    generate_industry_volume_price_reasoning,
    get_latest_industry_volume_price_reasoning,
    list_industry_volume_price_history,
    generate_stock_volume_price_reasoning,
    get_latest_stock_volume_price_reasoning,
    list_stock_volume_price_history,
    generate_stock_integrated_analysis,
    get_latest_stock_integrated_analysis,
    list_stock_integrated_analysis_history,
    sync_indicator_continuous_volume,
    sync_indicator_screening,
    sync_all_indicator_screenings,
    list_indicator_screenings,
    run_indicator_realtime_refresh,
    list_industry_index_history,
    list_concept_constituents,
    sync_concept_directory,
    sync_individual_fund_flow,
    sync_big_deal_fund_flow,
    sync_hsgt_fund_flow,
    sync_margin_account_info,
    sync_market_activity,
    sync_market_fund_flow,
    get_latest_macro_insight,
    list_macro_insight_history,
    generate_macro_insight,
    build_market_overview_payload,
    generate_market_overview_reasoning,
    list_investment_journal_entries,
    get_investment_journal_entry,
    upsert_investment_journal_entry,
    sync_stock_basic,
    sync_stock_main_business,
    sync_stock_main_composition,
    sync_stock_news,
    get_stock_main_composition,
    is_trading_day,
    INDEX_CONFIG,
)
from .state import monitor

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
INTEGRATED_NEWS_DAYS_DEFAULT = 10
INTEGRATED_TRADE_DAYS_DEFAULT = 10

scheduler = AsyncIOScheduler(timezone=LOCAL_TZ)
scheduler_loop: Optional[asyncio.AbstractEventLoop] = None


def _parse_time_string(value: str) -> Tuple[int, int]:
    if not isinstance(value, str) or ":" not in value:
        raise HTTPException(status_code=400, detail="Invalid time format. Expected HH:MM.")
    hour_part, minute_part = value.split(":", 1)
    try:
        hour = int(hour_part)
        minute = int(minute_part)
    except ValueError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail="Invalid time format. Expected HH:MM.") from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise HTTPException(status_code=400, detail="Time must be between 00:00 and 23:59.")
    return hour, minute


def _normalize_time_string(value: str) -> str:
    hour, minute = _parse_time_string(value)
    return f"{hour:02d}:{minute:02d}"


def _stringify_value(value: Any) -> str:
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(value)
    if isinstance(value, (list, tuple)):
        return "；".join(str(item) for item in value)
    return str(value)


_SANITIZE_SCRIPT_STYLE_RE = re.compile(r"<\s*(script|style)[^>]*>.*?<\s*/\s*\1\s*>", re.IGNORECASE | re.DOTALL)
_SANITIZE_EVENT_RE = re.compile(r"\son[a-z]+\s*=\s*(\"[^\"]*\"|'[^']*')", re.IGNORECASE)
_SANITIZE_JS_URL_RE = re.compile(r"javascript\s*:", re.IGNORECASE)


def _sanitize_rich_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    sanitized = _SANITIZE_SCRIPT_STYLE_RE.sub("", value)
    sanitized = _SANITIZE_EVENT_RE.sub("", sanitized)
    sanitized = _SANITIZE_JS_URL_RE.sub("", sanitized)
    sanitized = sanitized.strip()
    return sanitized or None


def _format_volume_summary_lines(summary_payload: Any, raw_text: Optional[str]) -> List[str]:
    parsed_summary: Optional[Dict[str, Any]] = None
    if isinstance(summary_payload, dict):
        parsed_summary = summary_payload
    elif isinstance(summary_payload, str) and summary_payload.strip():
        try:
            parsed_summary = json.loads(summary_payload)
        except (TypeError, json.JSONDecodeError):
            parsed_summary = None
    elif raw_text and raw_text.strip():
        try:
            parsed_summary = json.loads(raw_text)
        except (TypeError, json.JSONDecodeError):
            parsed_summary = None

    lines: List[str] = []
    if parsed_summary:
        phase = parsed_summary.get("wyckoffPhase")
        confidence = parsed_summary.get("confidence")
        if phase or confidence is not None:
            header = f"【阶段判定】{phase or '--'}"
            if confidence is not None:
                try:
                    confidence_value = float(confidence)
                    header += f" · 置信度 {confidence_value * 100:.0f}%"
                except (TypeError, ValueError):
                    header += f" · 置信度 {confidence}"
            lines.append(header)
            lines.append("")

        summary_text = parsed_summary.get("stageSummary")
        if summary_text:
            lines.append("【量价结论】")
            if isinstance(summary_text, (list, tuple)):
                lines.extend(str(item) for item in summary_text if item is not None)
            else:
                lines.extend(str(summary_text).splitlines())
            lines.append("")

        intent = parsed_summary.get("compositeIntent")
        if intent:
            lines.append(f"【主力意图】{intent}")
            lines.append("")

        def _append_section(label: str, items: Any) -> None:
            if not items:
                return
            if isinstance(items, (list, tuple)):
                formatted = [str(item) if not isinstance(item, dict) else _stringify_value(item) for item in items]
            else:
                formatted = [str(items)]
            lines.append(f"【{label}】")
            for idx, item in enumerate(formatted, start=1):
                lines.append(f"{idx}. {item}")
            lines.append("")

        _append_section("量能信号", parsed_summary.get("volumeSignals"))
        _append_section("价格/结构信号", parsed_summary.get("priceSignals"))
        _append_section("策略建议", parsed_summary.get("strategy"))
        _append_section("风险提示", parsed_summary.get("risks"))
        _append_section("后续观察", parsed_summary.get("checklist"))

    if not lines:
        fallback = raw_text or (summary_payload if isinstance(summary_payload, str) else "")
        fallback_text = str(fallback or "").strip()
        if fallback_text:
            lines = fallback_text.splitlines()
        else:
            lines = ["暂无推理输出。"]

    return lines
logger = logging.getLogger(__name__)


def _submit_scheduler_task(coro: Awaitable[object]) -> bool:
    global scheduler_loop
    loop = scheduler_loop
    if loop is None or loop.is_closed():
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.error("Scheduler job triggered without available asyncio loop.")
            return False
        else:
            scheduler_loop = loop
    loop.call_soon_threadsafe(asyncio.create_task, coro)
    return True

FAVORITE_GROUP_NONE_SENTINEL = "__ungrouped__"
MAX_FAVORITE_GROUP_LENGTH = 64
DEFAULT_MARKET_INSIGHT_LOOKBACK_HOURS = 24
MARKET_INSIGHT_STALE_GRACE_HOURS = 2


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



def _local_now() -> datetime:
    return datetime.now(LOCAL_TZ)


def _localize_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=LOCAL_TZ)
        return value.astimezone(LOCAL_TZ)
    return None


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _localize_datetime(value)
    try:
        parsed = datetime.fromisoformat(value)
    except Exception:
        return None
    return _localize_datetime(parsed)


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
    net_income_qoq_latest: Optional[float] = Field(None, alias="netIncomeQoqLatest")
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
    pct_change: Optional[float] = Field(None, alias="pctChange")


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


class StockNoteItem(BaseModel):
    id: int
    stock_code: str = Field(..., alias="stockCode")
    content: str
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")

    class Config:
        allow_population_by_field_name = True
        allow_population_by_alias = True


class StockNoteListResponse(BaseModel):
    total: int
    items: List[StockNoteItem]


class StockNoteCreateRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=1000)


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


class SyncGlobalFlashRequest(BaseModel):
    """Placeholder request model for global flash sync."""

    class Config:
        extra = "forbid"


class SyncGlobalFlashResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncConceptDirectoryRequest(BaseModel):
    refresh: bool = Field(True, alias="refresh")

    class Config:
        allow_population_by_field_name = True


class SyncTradeCalendarRequest(BaseModel):
    start_date: Optional[str] = Field(None, alias="startDate")
    end_date: Optional[str] = Field(None, alias="endDate")
    exchange: Optional[str] = "SSE"

    class Config:
        allow_population_by_field_name = True


class SyncTradeCalendarResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")
    start_date: Optional[str] = Field(None, alias="startDate")
    end_date: Optional[str] = Field(None, alias="endDate")

    class Config:
        allow_population_by_field_name = True


class SyncGlobalFlashClassifyRequest(BaseModel):
    batch_size: int = Field(10, ge=1, le=100, alias="batchSize")

    class Config:
        allow_population_by_field_name = True


class SyncGlobalFlashClassifyResponse(BaseModel):
    rows: int
    relevance_rows: int = Field(..., alias="relevanceRows")
    relevance_requested: int = Field(..., alias="relevanceRequested")
    impact_rows: int = Field(..., alias="impactRows")
    impact_requested: int = Field(..., alias="impactRequested")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class NewsRelevancePayload(BaseModel):
    is_relevant: Optional[bool] = Field(None, alias="isRelevant")
    confidence: Optional[float] = None
    reason: Optional[str] = None
    checked_at: Optional[datetime] = Field(None, alias="checkedAt")

    class Config:
        allow_population_by_field_name = True


class NewsImpactPayload(BaseModel):
    summary: Optional[str] = None
    analysis: Optional[str] = None
    confidence: Optional[float] = None
    checked_at: Optional[datetime] = Field(None, alias="checkedAt")
    levels: List[str] = Field(default_factory=list)
    markets: List[str] = Field(default_factory=list)
    industries: List[str] = Field(default_factory=list)
    sectors: List[str] = Field(default_factory=list)
    themes: List[str] = Field(default_factory=list)
    stocks: List[str] = Field(default_factory=list)
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        allow_population_by_field_name = True


class NewsArticleItem(BaseModel):
    article_id: str = Field(..., alias="articleId")
    source: str
    title: str
    summary: Optional[str] = None
    content: Optional[str] = None
    content_type: Optional[str] = Field(None, alias="contentType")
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    url: Optional[str] = None
    language: Optional[str] = None
    processing_status: Optional[str] = Field(None, alias="processingStatus")
    content_fetched: Optional[bool] = Field(None, alias="contentFetched")
    content_fetched_at: Optional[datetime] = Field(None, alias="contentFetchedAt")
    relevance_attempts: Optional[int] = Field(None, alias="relevanceAttempts")
    impact_attempts: Optional[int] = Field(None, alias="impactAttempts")
    last_error: Optional[str] = Field(None, alias="lastError")
    relevance: NewsRelevancePayload
    impact: NewsImpactPayload

    class Config:
        allow_population_by_field_name = True


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


class SyncMarketInsightRequest(BaseModel):
    lookback_hours: int = Field(24, ge=1, le=72, alias="lookbackHours")
    article_limit: int = Field(40, ge=5, le=50, alias="articleLimit")

    class Config:
        allow_population_by_field_name = True


class SyncSectorInsightRequest(BaseModel):
    lookback_hours: int = Field(24, ge=1, le=72, alias="lookbackHours")
    article_limit: int = Field(40, ge=5, le=60, alias="articleLimit")

    class Config:
        allow_population_by_field_name = True


class SyncMarketOverviewRequest(BaseModel):
    run_llm: bool = Field(True, alias="runLLM")

    class Config:
        allow_population_by_field_name = True


class SyncIndexHistoryRequest(BaseModel):
    index_codes: Optional[List[str]] = Field(
        None,
        alias="indexCodes",
        description="Optional subset of index codes to refresh (defaults to all core indices).",
    )

    class Config:
        allow_population_by_field_name = True
        extra = "forbid"


class MarketInsightArticleItem(BaseModel):
    article_id: Optional[str] = Field(None, alias="articleId")
    source: Optional[str] = None
    title: Optional[str] = None
    impact_summary: Optional[str] = Field(None, alias="impactSummary")
    impact_analysis: Optional[str] = Field(None, alias="impactAnalysis")
    impact_confidence: Optional[float] = Field(None, alias="impactConfidence")
    markets: List[str] = Field(default_factory=list)
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    url: Optional[str] = None

    class Config:
        allow_population_by_field_name = True


class MarketInsightSummaryPayload(BaseModel):
    summary_id: str = Field(..., alias="summaryId")
    generated_at: datetime = Field(..., alias="generatedAt")
    window_start: datetime = Field(..., alias="windowStart")
    window_end: datetime = Field(..., alias="windowEnd")
    headline_count: int = Field(..., alias="headlineCount")
    summary: Optional[Dict[str, Any]] = None
    raw_response: Optional[str] = Field(None, alias="rawResponse")
    prompt_tokens: Optional[int] = Field(None, alias="promptTokens")
    completion_tokens: Optional[int] = Field(None, alias="completionTokens")
    total_tokens: Optional[int] = Field(None, alias="totalTokens")
    elapsed_seconds: Optional[float] = Field(None, alias="elapsedSeconds")
    model_used: Optional[str] = Field(None, alias="modelUsed")

    class Config:
        allow_population_by_field_name = True


class MarketInsightResponse(BaseModel):
    summary: Optional[MarketInsightSummaryPayload]
    articles: List[MarketInsightArticleItem]

    class Config:
        allow_population_by_field_name = True


class SectorInsightGroupArticle(BaseModel):
    article_id: Optional[str] = Field(None, alias="articleId")
    title: Optional[str] = None
    impact_summary: Optional[str] = Field(None, alias="impactSummary")
    impact_analysis: Optional[str] = Field(None, alias="impactAnalysis")
    confidence: Optional[float] = None
    severity: Optional[str] = None
    severity_score: Optional[float] = Field(None, alias="severityScore")
    event_type: Optional[str] = Field(None, alias="eventType")
    time_sensitivity: List[str] = Field(default_factory=list, alias="timeSensitivity")
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    source: Optional[str] = None
    url: Optional[str] = None
    impact_levels: List[str] = Field(default_factory=list, alias="impactLevels")

    class Config:
        allow_population_by_field_name = True


class SectorInsightArticleAssignment(BaseModel):
    article_id: Optional[str] = Field(None, alias="articleId")
    title: Optional[str] = None
    impact_summary: Optional[str] = Field(None, alias="impactSummary")
    impact_analysis: Optional[str] = Field(None, alias="impactAnalysis")
    confidence: Optional[float] = None
    severity: Optional[str] = None
    severity_score: Optional[float] = Field(None, alias="severityScore")
    event_type: Optional[str] = Field(None, alias="eventType")
    time_sensitivity: List[str] = Field(default_factory=list, alias="timeSensitivity")
    focus_topics: List[str] = Field(default_factory=list, alias="focusTopics")
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    source: Optional[str] = None
    url: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    tag_types: List[str] = Field(default_factory=list, alias="tagTypes")
    impact_levels: List[str] = Field(default_factory=list, alias="impactLevels")

    class Config:
        allow_population_by_field_name = True


class SectorInsightGroup(BaseModel):
    name: str
    tag_type: str = Field(..., alias="tagType")
    article_count: int = Field(..., alias="articleCount")
    average_confidence: Optional[float] = Field(None, alias="averageConfidence")
    average_severity_score: Optional[float] = Field(None, alias="averageSeverityScore")
    max_severity: Optional[str] = Field(None, alias="maxSeverity")
    max_severity_score: Optional[float] = Field(None, alias="maxSeverityScore")
    latest_published_at: Optional[datetime] = Field(None, alias="latestPublishedAt")
    event_types: List[str] = Field(default_factory=list, alias="eventTypes")
    time_sensitivity: List[str] = Field(default_factory=list, alias="timeSensitivity")
    focus_topics: List[str] = Field(default_factory=list, alias="focusTopics")
    impact_levels: List[str] = Field(default_factory=list, alias="impactLevels")
    sources: List[str] = Field(default_factory=list)
    score: Optional[float] = None
    sample_articles: List[SectorInsightGroupArticle] = Field(default_factory=list, alias="sampleArticles")

    class Config:
        allow_population_by_field_name = True


class SectorInsightSnapshot(BaseModel):
    generated_at: datetime = Field(..., alias="generatedAt")
    lookback_hours: int = Field(..., alias="lookbackHours")
    headline_count: int = Field(..., alias="headlineCount")
    group_count: int = Field(..., alias="groupCount")
    groups: List[SectorInsightGroup]
    article_assignments: List[SectorInsightArticleAssignment] = Field(default_factory=list, alias="articleAssignments")
    excluded_count: Optional[int] = Field(None, alias="excludedCount")
    excluded_article_ids: Optional[List[str]] = Field(None, alias="excludedArticleIds")

    class Config:
        allow_population_by_field_name = True


class SectorInsightSummaryPayload(BaseModel):
    summary_id: str = Field(..., alias="summaryId")
    generated_at: datetime = Field(..., alias="generatedAt")
    window_start: datetime = Field(..., alias="windowStart")
    window_end: datetime = Field(..., alias="windowEnd")
    headline_count: int = Field(..., alias="headlineCount")
    group_count: int = Field(..., alias="groupCount")
    summary: Optional[Dict[str, Any]] = None
    group_snapshot: Optional[SectorInsightSnapshot] = Field(None, alias="groupSnapshot")
    raw_response: Optional[str] = Field(None, alias="rawResponse")
    prompt_tokens: Optional[int] = Field(None, alias="promptTokens")
    completion_tokens: Optional[int] = Field(None, alias="completionTokens")
    total_tokens: Optional[int] = Field(None, alias="totalTokens")
    elapsed_seconds: Optional[float] = Field(None, alias="elapsedSeconds")
    model_used: Optional[str] = Field(None, alias="modelUsed")
    referenced_articles: List[SectorInsightArticleAssignment] = Field(default_factory=list, alias="referencedArticles")

    class Config:
        allow_population_by_field_name = True


class SectorInsightResponse(BaseModel):
    summary: Optional[SectorInsightSummaryPayload]
    snapshot: Optional[SectorInsightSnapshot]

    class Config:
        allow_population_by_field_name = True


class IndexHistoryRecord(BaseModel):
    index_code: str = Field(..., alias="indexCode")
    index_name: Optional[str] = Field(None, alias="indexName")
    trade_date: date = Field(..., alias="tradeDate")
    open: Optional[float] = None
    close: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None
    amplitude: Optional[float] = None
    pct_change: Optional[float] = Field(None, alias="pctChange")
    change_amount: Optional[float] = Field(None, alias="changeAmount")
    turnover: Optional[float] = None

    class Config:
        allow_population_by_field_name = True


class IndexOption(BaseModel):
    code: str
    name: str
    symbol: str


class IndexHistoryListResponse(BaseModel):
    index_code: str = Field(..., alias="indexCode")
    index_name: Optional[str] = Field(None, alias="indexName")
    items: List[IndexHistoryRecord]
    available_indices: List[IndexOption] = Field(..., alias="availableIndices")

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


class SyncRealtimeIndexRequest(BaseModel):
    class Config:
        extra = "forbid"


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


class SyncMacroLeverageRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncSocialFinancingRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncRmbMidpointResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncSocialFinancingResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncMacroCpiRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncMacroCpiResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncMacroPmiRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncMacroPmiResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncMacroM2Request(BaseModel):
    class Config:
        extra = "forbid"


class SyncMacroM2Response(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncMacroPpiRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncMacroPpiResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncMacroLprRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncMacroLprResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncMacroShiborRequest(BaseModel):
    class Config:
        extra = "forbid"


class SyncMacroShiborResponse(BaseModel):
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


class SyncFedStatementRequest(BaseModel):
    limit: Optional[int] = Field(
        None,
        ge=1,
        le=50,
        description="Optional override for number of statements to fetch (default: 5).",
    )

    class Config:
        extra = "forbid"
        allow_population_by_field_name = True


class SyncFedStatementResponse(BaseModel):
    rows: int
    urls: List[str] = Field(default_factory=list)
    url_count: int = Field(..., alias="urlCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")
    pruned: int = 0

    class Config:
        allow_population_by_field_name = True


class SyncPeripheralInsightRequest(BaseModel):
    run_llm: Optional[bool] = Field(True, alias="runLLM")

    class Config:
        allow_population_by_field_name = True
        extra = "forbid"


class SyncPeripheralAggregateRequest(BaseModel):
    fed_limit: Optional[int] = Field(
        None,
        alias="fedLimit",
        ge=1,
        le=50,
        description="Optional override for number of Fed statements to fetch during aggregate sync.",
    )
    run_llm: Optional[bool] = Field(
        None,
        alias="runLLM",
        description="Optional override for generating LLM insight during aggregate sync.",
    )

    class Config:
        allow_population_by_field_name = True
        extra = "forbid"


class SyncMacroAggregateRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True
        extra = "forbid"


class SyncFundFlowAggregateRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True
        extra = "forbid"


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


class ConceptIndexSyncItem(BaseModel):
    concept: str
    ts_code: Optional[str] = Field(None, alias="tsCode")
    rows: int

    class Config:
        allow_population_by_field_name = True


class SyncConceptIndexHistoryRequest(BaseModel):
    concepts: List[str] = Field(..., alias="concepts", description="List of concept names to sync.")
    start_date: Optional[str] = Field(None, alias="startDate", description="Optional start date (YYYYMMDD).")
    end_date: Optional[str] = Field(None, alias="endDate", description="Optional end date (YYYYMMDD).")

    class Config:
        allow_population_by_field_name = True


class SyncConceptIndexHistoryResponse(BaseModel):
    concepts: List[ConceptIndexSyncItem]
    errors: List[Dict[str, Any]]
    start_date: str = Field(..., alias="startDate")
    end_date: str = Field(..., alias="endDate")
    total_rows: int = Field(..., alias="totalRows")

    class Config:
        allow_population_by_field_name = True


class ConceptIndexHistoryRecord(BaseModel):
    ts_code: str = Field(..., alias="tsCode")
    concept_name: Optional[str] = Field(None, alias="conceptName")
    trade_date: date = Field(..., alias="tradeDate")
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    pre_close: Optional[float] = Field(None, alias="preClose")
    change: Optional[float] = None
    pct_chg: Optional[float] = Field(None, alias="pctChg")
    vol: Optional[float] = None
    amount: Optional[float] = None
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class ConceptIndexHistoryResponse(BaseModel):
    total: int
    items: List[ConceptIndexHistoryRecord]

    class Config:
        allow_population_by_field_name = True


class ConceptFundFlowBreakdown(BaseModel):
    score: Optional[float] = None
    best_rank: Optional[int] = Field(None, alias="bestRank")
    best_symbol: Optional[str] = Field(None, alias="bestSymbol")
    total_net_amount: Optional[float] = Field(None, alias="totalNetAmount")
    total_inflow: Optional[float] = Field(None, alias="totalInflow")
    total_outflow: Optional[float] = Field(None, alias="totalOutflow")
    stages: List[FundFlowStageSnapshot] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class ConceptIndexMetrics(BaseModel):
    latest_close: Optional[float] = Field(None, alias="latestClose")
    change1d: Optional[float] = None
    change5d: Optional[float] = None
    change20d: Optional[float] = None
    avg_volume5d: Optional[float] = Field(None, alias="avgVolume5d")

    class Config:
        allow_population_by_field_name = True


class ConceptNewsArticle(BaseModel):
    article_id: Optional[str] = Field(None, alias="articleId")
    source: Optional[str] = None
    title: Optional[str] = None
    summary: Optional[str] = None
    published_at: Optional[str] = Field(None, alias="publishedAt")
    url: Optional[str] = None
    impact_summary: Optional[str] = Field(None, alias="impactSummary")
    impact_analysis: Optional[str] = Field(None, alias="impactAnalysis")
    impact_confidence: Optional[float] = Field(None, alias="impactConfidence")
    relevance_confidence: Optional[float] = Field(None, alias="relevanceConfidence")
    relevance_reason: Optional[str] = Field(None, alias="relevanceReason")
    impact_themes: Optional[Any] = Field(None, alias="impactThemes")
    impact_industries: Optional[Any] = Field(None, alias="impactIndustries")
    impact_sectors: Optional[Any] = Field(None, alias="impactSectors")
    impact_stocks: Optional[Any] = Field(None, alias="impactStocks")
    extra_metadata: Optional[Dict[str, Any]] = Field(None, alias="extraMetadata")

    class Config:
        allow_population_by_field_name = True


class ConceptSnapshotEntry(BaseModel):
    name: str
    ts_code: Optional[str] = Field(None, alias="tsCode")
    latest_trade_date: Optional[str] = Field(None, alias="latestTradeDate")
    fund_flow: ConceptFundFlowBreakdown = Field(..., alias="fundFlow")
    index_metrics: Optional[ConceptIndexMetrics] = Field(None, alias="indexMetrics")
    news: List[ConceptNewsArticle] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class ConceptSnapshot(BaseModel):
    generated_at: Optional[str] = Field(None, alias="generatedAt")
    lookback_hours: Optional[int] = Field(None, alias="lookbackHours")
    concept_count: Optional[int] = Field(None, alias="conceptCount")
    concepts: List[ConceptSnapshotEntry] = Field(default_factory=list)
    fund_snapshot: Optional[FundFlowSectorHotlistResponse] = Field(None, alias="fundSnapshot")

    class Config:
        allow_population_by_field_name = True


class SyncConceptInsightRequest(BaseModel):
    lookback_hours: int = Field(48, ge=1, le=168, alias="lookbackHours")
    concept_limit: int = Field(10, ge=1, le=15, alias="conceptLimit")
    run_llm: bool = Field(True, alias="runLLM")
    refresh_index_history: bool = Field(True, alias="refreshIndexHistory")

    class Config:
        allow_population_by_field_name = True


class ConceptInsightSummary(BaseModel):
    summary_id: str = Field(..., alias="summaryId")
    generated_at: Optional[str] = Field(None, alias="generatedAt")
    window_start: Optional[str] = Field(None, alias="windowStart")
    window_end: Optional[str] = Field(None, alias="windowEnd")
    concept_count: Optional[int] = Field(None, alias="conceptCount")
    summary_snapshot: Optional[Dict[str, Any]] = Field(None, alias="summarySnapshot")
    summary_json: Optional[Dict[str, Any]] = Field(None, alias="summaryJson")
    raw_response: Optional[str] = Field(None, alias="rawResponse")
    referenced_concepts: Optional[Sequence[str]] = Field(None, alias="referencedConcepts")
    referenced_articles: Optional[Sequence[Dict[str, Any]]] = Field(None, alias="referencedArticles")
    prompt_tokens: Optional[int] = Field(None, alias="promptTokens")
    completion_tokens: Optional[int] = Field(None, alias="completionTokens")
    total_tokens: Optional[int] = Field(None, alias="totalTokens")
    elapsed_ms: Optional[int] = Field(None, alias="elapsedMs")
    model_used: Optional[str] = Field(None, alias="modelUsed")

    class Config:
        allow_population_by_field_name = True


class ConceptInsightResponse(BaseModel):
    insight: Optional[ConceptInsightSummary]
    snapshot: Optional[ConceptSnapshot]

    class Config:
        allow_population_by_field_name = True


class ConceptInsightHistoryResponse(BaseModel):
    items: List[ConceptInsightSummary] = Field(default_factory=list)

ConceptInsightSummary.update_forward_refs(ConceptSnapshot=ConceptSnapshot, Sequence=Sequence)
ConceptInsightResponse.update_forward_refs(ConceptInsightSummary=ConceptInsightSummary, ConceptSnapshot=ConceptSnapshot)
ConceptInsightHistoryResponse.update_forward_refs(ConceptInsightSummary=ConceptInsightSummary, Sequence=Sequence)

class IndustryNewsArticle(BaseModel):
    article_id: Optional[str] = Field(None, alias="articleId")
    source: Optional[str] = None
    title: Optional[str] = None
    summary: Optional[str] = None
    published_at: Optional[str] = Field(None, alias="publishedAt")
    url: Optional[str] = None
    impact_summary: Optional[str] = Field(None, alias="impactSummary")
    impact_analysis: Optional[str] = Field(None, alias="impactAnalysis")
    impact_confidence: Optional[float] = Field(None, alias="impactConfidence")
    relevance_confidence: Optional[float] = Field(None, alias="relevanceConfidence")
    relevance_reason: Optional[str] = Field(None, alias="relevanceReason")
    impact_themes: Optional[Any] = Field(None, alias="impactThemes")
    impact_industries: Optional[Any] = Field(None, alias="impactIndustries")
    impact_sectors: Optional[Any] = Field(None, alias="impactSectors")
    impact_stocks: Optional[Any] = Field(None, alias="impactStocks")
    extra_metadata: Optional[Dict[str, Any]] = Field(None, alias="extraMetadata")

    class Config:
        allow_population_by_field_name = True


class IndustrySnapshotEntry(BaseModel):
    name: str
    fund_flow: ConceptFundFlowBreakdown = Field(..., alias="fundFlow")
    stage_metrics: Dict[str, Optional[float]] = Field(default_factory=dict, alias="stageMetrics")
    news: List[IndustryNewsArticle] = Field(default_factory=list)
    latest_updated_at: Optional[str] = Field(None, alias="latestUpdatedAt")

    class Config:
        allow_population_by_field_name = True


class IndustrySnapshot(BaseModel):
    generated_at: Optional[str] = Field(None, alias="generatedAt")
    lookback_hours: Optional[int] = Field(None, alias="lookbackHours")
    industry_count: Optional[int] = Field(None, alias="industryCount")
    industries: List[IndustrySnapshotEntry] = Field(default_factory=list)
    fund_snapshot: Optional[Dict[str, Any]] = Field(None, alias="fundSnapshot")

    class Config:
        allow_population_by_field_name = True


class SyncIndustryInsightRequest(BaseModel):
    lookback_hours: int = Field(48, ge=1, le=168, alias="lookbackHours")
    industry_limit: int = Field(5, ge=1, le=10, alias="industryLimit")
    run_llm: bool = Field(True, alias="runLLM")

    class Config:
        allow_population_by_field_name = True


class IndustryInsightSummary(BaseModel):
    summary_id: str = Field(..., alias="summaryId")
    generated_at: Optional[str] = Field(None, alias="generatedAt")
    window_start: Optional[str] = Field(None, alias="windowStart")
    window_end: Optional[str] = Field(None, alias="windowEnd")
    industry_count: Optional[int] = Field(None, alias="industryCount")
    summary_snapshot: Optional[Dict[str, Any]] = Field(None, alias="summarySnapshot")
    summary_json: Optional[Dict[str, Any]] = Field(None, alias="summaryJson")
    raw_response: Optional[str] = Field(None, alias="rawResponse")
    referenced_industries: Optional[Sequence[str]] = Field(None, alias="referencedIndustries")
    referenced_articles: Optional[Sequence[Dict[str, Any]]] = Field(None, alias="referencedArticles")
    prompt_tokens: Optional[int] = Field(None, alias="promptTokens")
    completion_tokens: Optional[int] = Field(None, alias="completionTokens")
    total_tokens: Optional[int] = Field(None, alias="totalTokens")
    elapsed_ms: Optional[int] = Field(None, alias="elapsedMs")
    model_used: Optional[str] = Field(None, alias="modelUsed")

    class Config:
        allow_population_by_field_name = True


class IndustryInsightResponse(BaseModel):
    insight: Optional[IndustryInsightSummary]
    snapshot: Optional[IndustrySnapshot]

    class Config:
        allow_population_by_field_name = True


class IndustryInsightHistoryResponse(BaseModel):
    items: List[IndustryInsightSummary] = Field(default_factory=list)

IndustryInsightSummary.update_forward_refs(IndustrySnapshot=IndustrySnapshot, Sequence=Sequence)
IndustryInsightResponse.update_forward_refs(IndustryInsightSummary=IndustryInsightSummary, IndustrySnapshot=IndustrySnapshot)
IndustryInsightHistoryResponse.update_forward_refs(IndustryInsightSummary=IndustryInsightSummary, Sequence=Sequence)

def _build_concept_insight_payload(
    summary: Dict[str, Any],
    *,
    include_snapshot: bool = True,
) -> Dict[str, Any]:
    snapshot_obj = summary.get("summary_snapshot") if include_snapshot else None
    generated_dt = _parse_datetime(summary.get("generated_at"))
    window_start_dt = _parse_datetime(summary.get("window_start"))
    window_end_dt = _parse_datetime(summary.get("window_end"))
    summary_payload = ConceptInsightSummary(
        summaryId=str(summary.get("summary_id")),
        generatedAt=generated_dt.isoformat() if generated_dt else None,
        windowStart=window_start_dt.isoformat() if window_start_dt else None,
        windowEnd=window_end_dt.isoformat() if window_end_dt else None,
        conceptCount=summary.get("concept_count"),
        summarySnapshot=snapshot_obj if isinstance(snapshot_obj, dict) else (snapshot_obj if include_snapshot else None),
        summaryJson=summary.get("summary_json"),
        rawResponse=summary.get("raw_response"),
        referencedConcepts=summary.get("referenced_concepts"),
        referencedArticles=summary.get("referenced_articles"),
        promptTokens=summary.get("prompt_tokens"),
        completionTokens=summary.get("completion_tokens"),
        totalTokens=summary.get("total_tokens"),
        elapsedMs=summary.get("elapsed_ms"),
        modelUsed=summary.get("model_used"),
    )

    snapshot_payload: Optional[ConceptSnapshot] = None
    if include_snapshot and isinstance(snapshot_obj, dict):
        try:
            snapshot_payload = ConceptSnapshot(**snapshot_obj)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to parse stored concept snapshot for %s: %s", summary.get("summary_id"), exc)
            snapshot_payload = None

    return {"summary": summary_payload, "snapshot": snapshot_payload}


def _build_concept_insight_response(
    *,
    summary: Optional[Dict[str, Any]],
    snapshot: Optional[Dict[str, Any]],
) -> ConceptInsightResponse:
    summary_payload: Optional[ConceptInsightSummary] = None
    snapshot_payload: Optional[ConceptSnapshot] = None

    if summary:
        payload = _build_concept_insight_payload(summary, include_snapshot=True)
        summary_payload = payload.get("summary")  # type: ignore[assignment]
        snapshot_payload = payload.get("snapshot")  # type: ignore[assignment]

    if snapshot and not snapshot_payload:
        try:
            snapshot_payload = ConceptSnapshot(**snapshot)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to parse on-demand concept snapshot: %s", exc)
            snapshot_payload = None

    return ConceptInsightResponse(insight=summary_payload, snapshot=snapshot_payload)


def _build_industry_insight_response_wrapper(
    *,
    summary: Optional[Dict[str, Any]],
    snapshot: Optional[Dict[str, Any]],
) -> IndustryInsightResponse:
    return _build_industry_insight_response(summary=summary, snapshot=snapshot)


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


class SyncHsgtFundFlowRequest(BaseModel):
    symbol: Optional[str] = None

    class Config:
        allow_population_by_field_name = True


class SyncHsgtFundFlowResponse(BaseModel):
    rows: int
    trade_dates: List[str] = Field(default_factory=list, alias="tradeDates")
    trade_date_count: int = Field(..., alias="tradeDateCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")
    symbol: Optional[str] = None

    class Config:
        allow_population_by_field_name = True


class SyncMarginAccountRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True


class SyncMarginAccountResponse(BaseModel):
    rows: int
    trade_dates: List[str] = Field(default_factory=list, alias="tradeDates")
    trade_date_count: int = Field(..., alias="tradeDateCount")
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

    class Config:
        allow_population_by_field_name = True


class SyncMarketActivityRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True


class SyncMarketActivityResponse(BaseModel):
    rows: int
    dataset_timestamp: Optional[datetime] = Field(None, alias="datasetTimestamp")

    class Config:
        allow_population_by_field_name = True


class SyncMarketFundFlowRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True


class SyncMarketFundFlowResponse(BaseModel):
    rows: int

    class Config:
        allow_population_by_field_name = True


class SyncMacroInsightRequest(BaseModel):
    run_llm: bool = Field(True, alias="runLLM")

    class Config:
        allow_population_by_field_name = True


class SyncMacroInsightResponse(BaseModel):
    snapshot_date: date = Field(..., alias="snapshotDate")
    generated_at: datetime = Field(..., alias="generatedAt")

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


class RealtimeIndexRecord(BaseModel):
    code: str
    name: Optional[str] = None
    latest_price: Optional[float] = Field(None, alias="latestPrice")
    change_amount: Optional[float] = Field(None, alias="changeAmount")
    change_percent: Optional[float] = Field(None, alias="changePercent")
    prev_close: Optional[float] = Field(None, alias="prevClose")
    open_price: Optional[float] = Field(None, alias="openPrice")
    high_price: Optional[float] = Field(None, alias="highPrice")
    low_price: Optional[float] = Field(None, alias="lowPrice")
    volume: Optional[float] = None
    turnover: Optional[float] = None
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class RealtimeIndexListResponse(BaseModel):
    total: int
    items: List[RealtimeIndexRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")

    class Config:
        allow_population_by_field_name = True


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


class MacroLeverageRecord(BaseModel):
    period_date: date = Field(..., alias="periodDate")
    period_label: Optional[str] = Field(None, alias="periodLabel")
    household_ratio: Optional[float] = Field(None, alias="householdRatio")
    non_financial_corporate_ratio: Optional[float] = Field(None, alias="nonFinancialCorporateRatio")
    government_ratio: Optional[float] = Field(None, alias="governmentRatio")
    central_government_ratio: Optional[float] = Field(None, alias="centralGovernmentRatio")
    local_government_ratio: Optional[float] = Field(None, alias="localGovernmentRatio")
    real_economy_ratio: Optional[float] = Field(None, alias="realEconomyRatio")
    financial_assets_ratio: Optional[float] = Field(None, alias="financialAssetsRatio")
    financial_liabilities_ratio: Optional[float] = Field(None, alias="financialLiabilitiesRatio")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class MacroLeverageListResponse(BaseModel):
    total: int
    items: List[MacroLeverageRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class SocialFinancingRecord(BaseModel):
    period_date: date = Field(..., alias="periodDate")
    period_label: Optional[str] = Field(None, alias="periodLabel")
    total_financing: Optional[float] = Field(None, alias="totalFinancing")
    renminbi_loans: Optional[float] = Field(None, alias="renminbiLoans")
    entrusted_and_fx_loans: Optional[float] = Field(None, alias="entrustedAndFxLoans")
    entrusted_loans: Optional[float] = Field(None, alias="entrustedLoans")
    trust_loans: Optional[float] = Field(None, alias="trustLoans")
    undiscounted_bankers_acceptance: Optional[float] = Field(None, alias="undiscountedBankersAcceptance")
    corporate_bonds: Optional[float] = Field(None, alias="corporateBonds")
    domestic_equity_financing: Optional[float] = Field(None, alias="domesticEquityFinancing")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class SocialFinancingListResponse(BaseModel):
    total: int
    items: List[SocialFinancingRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class CpiRecord(BaseModel):
    period_date: date = Field(..., alias="periodDate")
    period_label: Optional[str] = Field(None, alias="periodLabel")
    actual_value: Optional[float] = Field(None, alias="actualValue")
    forecast_value: Optional[float] = Field(None, alias="forecastValue")
    previous_value: Optional[float] = Field(None, alias="previousValue")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class CpiListResponse(BaseModel):
    total: int
    items: List[CpiRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class PmiRecord(BaseModel):
    series: str = Field(..., alias="series")
    period_date: date = Field(..., alias="periodDate")
    period_label: Optional[str] = Field(None, alias="periodLabel")
    actual_value: Optional[float] = Field(None, alias="actualValue")
    forecast_value: Optional[float] = Field(None, alias="forecastValue")
    previous_value: Optional[float] = Field(None, alias="previousValue")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class PmiListResponse(BaseModel):
    total: int
    items: List[PmiRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class M2Record(BaseModel):
    period_date: date = Field(..., alias="periodDate")
    period_label: Optional[str] = Field(None, alias="periodLabel")
    m0: Optional[float]
    m0_yoy: Optional[float]
    m0_mom: Optional[float]
    m1: Optional[float]
    m1_yoy: Optional[float]
    m1_mom: Optional[float]
    m2: Optional[float]
    m2_yoy: Optional[float]
    m2_mom: Optional[float]
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class M2ListResponse(BaseModel):
    total: int
    items: List[M2Record]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class PpiRecord(BaseModel):
    period_date: date = Field(..., alias="periodDate")
    period_label: Optional[str] = Field(None, alias="periodLabel")
    current_index: Optional[float] = Field(None, alias="currentIndex")
    yoy_change: Optional[float] = Field(None, alias="yoyChange")
    cumulative_index: Optional[float] = Field(None, alias="cumulativeIndex")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class PpiListResponse(BaseModel):
    total: int
    items: List[PpiRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class LprRecord(BaseModel):
    period_date: date = Field(..., alias="periodDate")
    period_label: Optional[str] = Field(None, alias="periodLabel")
    rate_1y: Optional[float] = Field(None, alias="rate1Y")
    rate_5y: Optional[float] = Field(None, alias="rate5Y")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class LprListResponse(BaseModel):
    total: int
    items: List[LprRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class ShiborRecord(BaseModel):
    period_date: date = Field(..., alias="periodDate")
    period_label: Optional[str] = Field(None, alias="periodLabel")
    on_rate: Optional[float] = Field(None, alias="onRate")
    rate_1w: Optional[float] = Field(None, alias="rate1W")
    rate_2w: Optional[float] = Field(None, alias="rate2W")
    rate_1m: Optional[float] = Field(None, alias="rate1M")
    rate_3m: Optional[float] = Field(None, alias="rate3M")
    rate_6m: Optional[float] = Field(None, alias="rate6M")
    rate_9m: Optional[float] = Field(None, alias="rate9M")
    rate_1y: Optional[float] = Field(None, alias="rate1Y")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class ShiborListResponse(BaseModel):
    total: int
    items: List[ShiborRecord]
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


class FedStatementRecord(BaseModel):
    title: str
    url: str
    statement_date: Optional[date] = Field(None, alias="statementDate")
    content: Optional[str] = None
    raw_text: Optional[str] = Field(None, alias="rawText")
    position: Optional[int] = None
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class FedStatementListResponse(BaseModel):
    total: int
    items: List[FedStatementRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")


class PeripheralInsightRecord(BaseModel):
    snapshot_date: date = Field(..., alias="snapshotDate")
    generated_at: datetime = Field(..., alias="generatedAt")
    metrics: Dict[str, Any]
    summary: Optional[str] = None
    raw_response: Optional[str] = Field(None, alias="rawResponse")
    model: Optional[str] = None
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class PeripheralInsightResponse(BaseModel):
    insight: Optional[PeripheralInsightRecord]


class MacroInsightDatasetField(BaseModel):
    key: str
    label_key: str = Field(..., alias="labelKey")
    format: Optional[str] = None

    class Config:
        allow_population_by_field_name = True


class MacroInsightDataset(BaseModel):
    key: str
    title_key: str = Field(..., alias="titleKey")
    fields: List[MacroInsightDatasetField]
    series: List[Dict[str, Any]]
    latest: Optional[Dict[str, Any]]
    updated_at: Optional[str] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class MacroInsightResponse(BaseModel):
    snapshot_date: date = Field(..., alias="snapshotDate")
    generated_at: datetime = Field(..., alias="generatedAt")
    summary: Optional[Dict[str, Any]]
    raw_response: Optional[str] = Field(None, alias="rawResponse")
    model: Optional[str]
    datasets: List[MacroInsightDataset]
    warnings: List[str] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class MacroInsightHistoryItem(BaseModel):
    snapshot_date: date = Field(..., alias="snapshotDate")
    generated_at: Optional[datetime] = Field(None, alias="generatedAt")
    summary_json: Optional[Dict[str, Any]] = Field(None, alias="summaryJson")
    raw_response: Optional[str] = Field(None, alias="rawResponse")
    model: Optional[str] = None

    class Config:
        allow_population_by_field_name = True


class MacroInsightHistoryResponse(BaseModel):
    items: List[MacroInsightHistoryItem]


class MarketInsightHistoryItem(BaseModel):
    summary_id: str = Field(..., alias="summaryId")
    generated_at: Optional[datetime] = Field(None, alias="generatedAt")
    window_start: Optional[datetime] = Field(None, alias="windowStart")
    window_end: Optional[datetime] = Field(None, alias="windowEnd")
    summary_json: Optional[Dict[str, Any]] = Field(None, alias="summaryJson")
    model_used: Optional[str] = Field(None, alias="modelUsed")

    class Config:
        allow_population_by_field_name = True


class MarketInsightHistoryResponse(BaseModel):
    items: List[MarketInsightHistoryItem]


class MarketOverviewReasoningSnapshot(BaseModel):
    summary: Optional[Dict[str, Any]] = None
    raw_text: Optional[str] = Field(None, alias="rawText")
    model: Optional[str] = None
    generated_at: Optional[datetime] = Field(None, alias="generatedAt")

    class Config:
        allow_population_by_field_name = True


class MarketOverviewResponse(BaseModel):
    generated_at: datetime = Field(..., alias="generatedAt")
    realtime_indices: List[Dict[str, Any]] = Field(default_factory=list, alias="realtimeIndices")
    index_history: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict, alias="indexHistory")
    market_insight: Optional[Dict[str, Any]] = Field(None, alias="marketInsight")
    macro_insight: Optional[Dict[str, Any]] = Field(None, alias="macroInsight")
    market_fund_flow: List[Dict[str, Any]] = Field(default_factory=list, alias="marketFundFlow")
    hsgt_fund_flow: List[Dict[str, Any]] = Field(default_factory=list, alias="hsgtFundFlow")
    margin_account: List[Dict[str, Any]] = Field(default_factory=list, alias="marginAccount")
    peripheral_insight: Optional[Dict[str, Any]] = Field(None, alias="peripheralInsight")
    market_activity: List[Dict[str, Any]] = Field(default_factory=list, alias="marketActivity")
    latest_reasoning: Optional[MarketOverviewReasoningSnapshot] = Field(None, alias="latestReasoning")

    class Config:
        allow_population_by_field_name = True


class MarketOverviewReasonRequest(BaseModel):
    run_llm: bool = Field(True, alias="runLLM")

    class Config:
        allow_population_by_field_name = True


class InvestmentJournalEntryPayload(BaseModel):
    review_html: Optional[str] = Field(None, alias="reviewHtml")
    plan_html: Optional[str] = Field(None, alias="planHtml")

    class Config:
        allow_population_by_field_name = True


class InvestmentJournalEntryResponse(BaseModel):
    entry_date: date = Field(..., alias="entryDate")
    review_html: Optional[str] = Field(None, alias="reviewHtml")
    plan_html: Optional[str] = Field(None, alias="planHtml")
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


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


class ConceptSearchItem(BaseModel):
    name: str
    code: str


class ConceptSearchResponse(BaseModel):
    items: List[ConceptSearchItem]


class ConceptWatchEntry(BaseModel):
    concept: str
    concept_code: str = Field(..., alias="conceptCode")
    is_watched: bool = Field(True, alias="isWatched")
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")
    latest_trade_date: Optional[str] = Field(None, alias="latestTradeDate")

    class Config:
        allow_population_by_field_name = True


class ConceptWatchlistResponse(BaseModel):
    items: List[ConceptWatchEntry]


class ConceptWatchRequest(BaseModel):
    concept: str


class ConceptStatusResponse(BaseModel):
    concept: str
    concept_code: str = Field(..., alias="conceptCode")
    is_watched: bool = Field(..., alias="isWatched")
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    latest_trade_date: Optional[str] = Field(None, alias="latestTradeDate")

    class Config:
        allow_population_by_field_name = True


class ConceptRefreshRequest(BaseModel):
    concept: str
    lookback_days: Optional[int] = Field(180, alias="lookbackDays", ge=1, le=1095)

    class Config:
        allow_population_by_field_name = True


class ConceptVolumePriceRequest(BaseModel):
    concept: str
    lookback_days: int = Field(90, alias="lookbackDays", ge=30, le=240)
    run_llm: bool = Field(True, alias="runLlm")

    class Config:
        allow_population_by_field_name = True


class ConceptRefreshResponse(BaseModel):
    concept: str
    concept_code: str = Field(..., alias="conceptCode")
    start_date: Optional[str] = Field(None, alias="startDate")
    end_date: Optional[str] = Field(None, alias="endDate")
    total_rows: Optional[int] = Field(None, alias="totalRows")
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    latest_trade_date: Optional[str] = Field(None, alias="latestTradeDate")
    is_watched: bool = Field(False, alias="isWatched")
    errors: List[Dict[str, Any]] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class ConceptConstituentItem(BaseModel):
    rank: Optional[int] = None
    symbol: Optional[str] = None
    name: Optional[str] = None
    last_price: Optional[float] = Field(None, alias="lastPrice")
    change_percent: Optional[float] = Field(None, alias="changePercent")
    change_amount: Optional[float] = Field(None, alias="changeAmount")
    speed_percent: Optional[float] = Field(None, alias="speedPercent")
    turnover_rate: Optional[float] = Field(None, alias="turnoverRate")
    volume_ratio: Optional[float] = Field(None, alias="volumeRatio")
    amplitude_percent: Optional[float] = Field(None, alias="amplitudePercent")
    turnover_amount: Optional[float] = Field(None, alias="turnoverAmount")

    class Config:
        allow_population_by_field_name = True


class ConceptConstituentResponse(BaseModel):
    concept: str
    concept_code: str = Field(..., alias="conceptCode")
    total_pages: int = Field(..., alias="totalPages")
    pages_fetched: int = Field(..., alias="pagesFetched")
    blocked: bool
    items: List[ConceptConstituentItem]

    class Config:
        allow_population_by_field_name = True


class IndustrySearchItem(BaseModel):
    name: str
    code: str


class IndustrySearchResponse(BaseModel):
    items: List[IndustrySearchItem]


class IndustryWatchEntry(BaseModel):
    industry: str
    industry_code: str = Field(..., alias="industryCode")
    is_watched: bool = Field(True, alias="isWatched")
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")
    latest_trade_date: Optional[str] = Field(None, alias="latestTradeDate")

    class Config:
        allow_population_by_field_name = True


class IndustryWatchlistResponse(BaseModel):
    items: List[IndustryWatchEntry]


class IndustryWatchRequest(BaseModel):
    industry: str


class IndustryStatusResponse(BaseModel):
    industry: str
    industry_code: str = Field(..., alias="industryCode")
    is_watched: bool = Field(..., alias="isWatched")
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    latest_trade_date: Optional[str] = Field(None, alias="latestTradeDate")

    class Config:
        allow_population_by_field_name = True


class IndustryRefreshRequest(BaseModel):
    industry: str
    lookback_days: Optional[int] = Field(180, alias="lookbackDays", ge=1, le=1095)

    class Config:
        allow_population_by_field_name = True


class IndustryRefreshResponse(BaseModel):
    industry: str
    industry_code: str = Field(..., alias="industryCode")
    start_date: Optional[str] = Field(None, alias="startDate")
    end_date: Optional[str] = Field(None, alias="endDate")
    total_rows: Optional[int] = Field(None, alias="totalRows")
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    latest_trade_date: Optional[str] = Field(None, alias="latestTradeDate")
    is_watched: bool = Field(False, alias="isWatched")
    errors: List[str] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class IndustryIndexBar(BaseModel):
    trade_date: date = Field(..., alias="tradeDate")
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    pct_chg: Optional[float] = Field(None, alias="pctChg")
    vol: Optional[float] = None
    amount: Optional[float] = None

    class Config:
        allow_population_by_field_name = True


class IndustryIndexHistoryResponse(BaseModel):
    industry: str
    total: int
    rows: List[IndustryIndexBar]


class IndustryNewsListResponse(BaseModel):
    items: List[IndustryNewsArticle] = Field(default_factory=list)


class ConceptNewsListResponse(BaseModel):
    items: List[IndustryNewsArticle] = Field(default_factory=list)


class ConceptVolumePriceRecord(BaseModel):
    id: int
    concept: str
    concept_code: str = Field(..., alias="conceptCode")
    lookback_days: int = Field(..., alias="lookbackDays")
    summary: Dict[str, Any]
    raw_text: str = Field(..., alias="rawText")
    model: Optional[str]
    generated_at: datetime = Field(..., alias="generatedAt")

    class Config:
        allow_population_by_field_name = True


class ConceptVolumePriceHistoryResponse(BaseModel):
    total: int
    items: List[ConceptVolumePriceRecord]


class IndustryVolumePriceRequest(BaseModel):
    industry: str
    lookback_days: int = Field(90, alias="lookbackDays", ge=30, le=240)
    run_llm: bool = Field(True, alias="runLlm")

    class Config:
        allow_population_by_field_name = True


class IndustryVolumePriceRecord(BaseModel):
    id: int
    industry: str
    industry_code: str = Field(..., alias="industryCode")
    lookback_days: int = Field(..., alias="lookbackDays")
    summary: Dict[str, Any]
    raw_text: str = Field(..., alias="rawText")
    model: Optional[str]
    generated_at: datetime = Field(..., alias="generatedAt")

    class Config:
        allow_population_by_field_name = True


class IndustryVolumePriceHistoryResponse(BaseModel):
    total: int
    items: List[IndustryVolumePriceRecord]


class StockVolumePriceRequest(BaseModel):
    code: str
    lookback_days: int = Field(90, alias="lookbackDays", ge=30, le=240)
    run_llm: bool = Field(True, alias="runLlm")

    class Config:
        allow_population_by_field_name = True


class StockVolumePriceRecord(BaseModel):
    id: int
    code: str
    name: Optional[str]
    lookback_days: int = Field(..., alias="lookbackDays")
    summary: Dict[str, Any]
    raw_text: str = Field(..., alias="rawText")
    model: Optional[str]
    generated_at: datetime = Field(..., alias="generatedAt")

    class Config:
        allow_population_by_field_name = True


class StockVolumePriceHistoryResponse(BaseModel):
    total: int
    items: List[StockVolumePriceRecord]


class StockIntegratedAnalysisRequest(BaseModel):
    code: str
    news_days: int = Field(INTEGRATED_NEWS_DAYS_DEFAULT, alias="newsDays", ge=1, le=30)
    trade_days: int = Field(INTEGRATED_TRADE_DAYS_DEFAULT, alias="tradeDays", ge=5, le=30)
    run_llm: bool = Field(True, alias="runLlm")
    force: bool = False

    class Config:
        allow_population_by_field_name = True


class StockIntegratedAnalysisRecord(BaseModel):
    id: int
    code: str
    name: Optional[str]
    news_days: int = Field(..., alias="newsDays")
    trade_days: int = Field(..., alias="tradeDays")
    summary: Dict[str, Any]
    raw_text: str = Field(..., alias="rawText")
    model: Optional[str]
    generated_at: datetime = Field(..., alias="generatedAt")
    context: Optional[Dict[str, Any]] = None

    class Config:
        allow_population_by_field_name = True


class StockIntegratedAnalysisHistoryResponse(BaseModel):
    total: int
    items: List[StockIntegratedAnalysisRecord]


class StockNewsSyncRequest(BaseModel):
    code: str


class StockNewsSyncResponse(BaseModel):
    fetched: int
    inserted: int


class StockNewsItem(BaseModel):
    id: int
    stock_code: str = Field(..., alias="stockCode")
    keyword: Optional[str] = None
    title: str
    content: Optional[str] = None
    source: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class StockNewsListResponse(BaseModel):
    total: int
    items: List[StockNewsItem]


class ConceptIndexBar(BaseModel):
    trade_date: date = Field(..., alias="tradeDate")
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    pre_close: Optional[float] = Field(None, alias="preClose")
    change: Optional[float] = None
    pct_chg: Optional[float] = Field(None, alias="pctChg")
    vol: Optional[float] = None
    amount: Optional[float] = None

    class Config:
        allow_population_by_field_name = True


class ConceptIndexHistoryResponse(BaseModel):
    concept: str
    total: int
    rows: List[ConceptIndexBar]


class FundFlowHotlistSymbol(BaseModel):
    symbol: str
    weight: float

    class Config:
        allow_population_by_field_name = True


class FundFlowStageSnapshot(BaseModel):
    symbol: str
    weight: float
    rank: Optional[int] = None
    net_amount: Optional[float] = Field(None, alias="netAmount")
    inflow: Optional[float] = None
    outflow: Optional[float] = None
    price_change_percent: Optional[float] = Field(None, alias="priceChangePercent")
    stage_change_percent: Optional[float] = Field(None, alias="stageChangePercent")
    index_value: Optional[float] = Field(None, alias="indexValue")
    current_price: Optional[float] = Field(None, alias="currentPrice")
    company_count: Optional[int] = Field(None, alias="companyCount")
    leading_stock: Optional[str] = Field(None, alias="leadingStock")
    leading_stock_change_percent: Optional[float] = Field(None, alias="leadingStockChangePercent")
    updated_at: Optional[str] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


ConceptFundFlowBreakdown.update_forward_refs(FundFlowStageSnapshot=FundFlowStageSnapshot)


class FundFlowHotlistEntry(BaseModel):
    name: str
    score: float
    best_rank: Optional[int] = Field(None, alias="bestRank")
    best_symbol: Optional[str] = Field(None, alias="bestSymbol")
    total_net_amount: Optional[float] = Field(None, alias="totalNetAmount")
    total_inflow: Optional[float] = Field(None, alias="totalInflow")
    total_outflow: Optional[float] = Field(None, alias="totalOutflow")
    stages: List[FundFlowStageSnapshot] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class FundFlowSectorHotlistResponse(BaseModel):
    generated_at: Optional[str] = Field(None, alias="generatedAt")
    symbols: List[FundFlowHotlistSymbol] = Field(default_factory=list)
    industries: List[FundFlowHotlistEntry] = Field(default_factory=list)
    concepts: List[FundFlowHotlistEntry] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


ConceptSnapshot.update_forward_refs(FundFlowSectorHotlistResponse=FundFlowSectorHotlistResponse)


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


class HsgtFundFlowRecord(BaseModel):
    trade_date: Optional[date] = Field(None, alias="tradeDate")
    symbol: Optional[str] = None
    net_buy_amount: Optional[float] = Field(None, alias="netBuyAmount")
    buy_amount: Optional[float] = Field(None, alias="buyAmount")
    sell_amount: Optional[float] = Field(None, alias="sellAmount")
    net_buy_amount_cumulative: Optional[float] = Field(None, alias="netBuyAmountCumulative")
    fund_inflow: Optional[float] = Field(None, alias="fundInflow")
    balance: Optional[float] = None
    market_value: Optional[float] = Field(None, alias="marketValue")
    leading_stock: Optional[str] = Field(None, alias="leadingStock")
    leading_stock_change_percent: Optional[float] = Field(None, alias="leadingStockChangePercent")
    leading_stock_code: Optional[str] = Field(None, alias="leadingStockCode")
    hs300_index: Optional[float] = Field(None, alias="hs300Index")
    hs300_change_percent: Optional[float] = Field(None, alias="hs300ChangePercent")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class HsgtFundFlowListResponse(BaseModel):
    total: int
    items: List[HsgtFundFlowRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    available_years: List[int] = Field(default_factory=list, alias="availableYears")

    class Config:
        allow_population_by_field_name = True


class MarginAccountRecord(BaseModel):
    trade_date: Optional[date] = Field(None, alias="tradeDate")
    financing_balance: Optional[float] = Field(None, alias="financingBalance")
    securities_lending_balance: Optional[float] = Field(None, alias="securitiesLendingBalance")
    financing_purchase_amount: Optional[float] = Field(None, alias="financingPurchaseAmount")
    securities_lending_sell_amount: Optional[float] = Field(None, alias="securitiesLendingSellAmount")
    securities_company_count: Optional[float] = Field(None, alias="securitiesCompanyCount")
    business_department_count: Optional[float] = Field(None, alias="businessDepartmentCount")
    individual_investor_count: Optional[float] = Field(None, alias="individualInvestorCount")
    institutional_investor_count: Optional[float] = Field(None, alias="institutionalInvestorCount")
    participating_investor_count: Optional[float] = Field(None, alias="participatingInvestorCount")
    liability_investor_count: Optional[float] = Field(None, alias="liabilityInvestorCount")
    collateral_value: Optional[float] = Field(None, alias="collateralValue")
    average_collateral_ratio: Optional[float] = Field(None, alias="averageCollateralRatio")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class MarginAccountListResponse(BaseModel):
    total: int
    items: List[MarginAccountRecord]
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    available_years: List[int] = Field(default_factory=list, alias="availableYears")

    class Config:
        allow_population_by_field_name = True


class MarketActivityRecord(BaseModel):
    metric: str
    display_order: int = Field(..., alias="displayOrder")
    value_text: Optional[str] = Field(None, alias="valueText")
    value_number: Optional[float] = Field(None, alias="valueNumber")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class MarketActivityListResponse(BaseModel):
    items: List[MarketActivityRecord]
    dataset_timestamp: Optional[datetime] = Field(None, alias="datasetTimestamp")

    class Config:
        allow_population_by_field_name = True


class IndicatorScreeningRecord(BaseModel):
    indicator_code: str = Field(..., alias="indicatorCode")
    indicator_name: Optional[str] = Field(None, alias="indicatorName")
    captured_at: Optional[datetime] = Field(None, alias="capturedAt")
    rank: Optional[int]
    stock_code: Optional[str] = Field(None, alias="stockCode")
    stock_code_full: Optional[str] = Field(None, alias="stockCodeFull")
    stock_name: Optional[str] = Field(None, alias="stockName")
    price_change_percent: Optional[float] = Field(None, alias="priceChangePercent")
    stage_change_percent: Optional[float] = Field(None, alias="stageChangePercent")
    last_price: Optional[float] = Field(None, alias="lastPrice")
    volume_shares: Optional[float] = Field(None, alias="volumeShares")
    volume_text: Optional[str] = Field(None, alias="volumeText")
    baseline_volume_shares: Optional[float] = Field(None, alias="baselineVolumeShares")
    baseline_volume_text: Optional[str] = Field(None, alias="baselineVolumeText")
    volume_days: Optional[int] = Field(None, alias="volumeDays")
    industry: Optional[str]
    turnover_percent: Optional[float] = Field(None, alias="turnoverPercent")
    turnover_rate: Optional[float] = Field(None, alias="turnoverRate")
    turnover_amount: Optional[float] = Field(None, alias="turnoverAmount")
    turnover_amount_text: Optional[str] = Field(None, alias="turnoverAmountText")
    high_price: Optional[float] = Field(None, alias="highPrice")
    low_price: Optional[float] = Field(None, alias="lowPrice")
    net_income_yoy_latest: Optional[float] = Field(None, alias="netIncomeYoyLatest")
    pe_ratio: Optional[float] = Field(None, alias="peRatio")
    matched_indicators: List[str] = Field(default_factory=list, alias="matchedIndicators")
    has_big_deal_inflow: Optional[bool] = Field(None, alias="hasBigDealInflow")
    indicator_details: Dict[str, Dict[str, Any]] = Field(default_factory=dict, alias="indicatorDetails")

    class Config:
        allow_population_by_field_name = True


class IndicatorScreeningListResponse(BaseModel):
    indicator_code: str = Field(..., alias="indicatorCode")
    indicator_codes: List[str] = Field(default_factory=list, alias="indicatorCodes")
    indicator_name: Optional[str] = Field(None, alias="indicatorName")
    captured_at: Optional[datetime] = Field(None, alias="capturedAt")
    total: int
    items: List[IndicatorScreeningRecord]

    class Config:
        allow_population_by_field_name = True


class IndicatorSyncResponse(BaseModel):
    indicator_code: str = Field(..., alias="indicatorCode")
    indicator_name: Optional[str] = Field(None, alias="indicatorName")
    rows: int
    captured_at: Optional[datetime] = Field(None, alias="capturedAt")
    skipped: bool = False
    reason: Optional[str] = None

    class Config:
        allow_population_by_field_name = True


class IndicatorSyncBatchResponse(BaseModel):
    results: List[IndicatorSyncResponse]


class IndicatorRealtimeRequest(BaseModel):
    codes: Optional[List[str]] = Field(None, description="Optional subset of ts_codes to refresh.")
    syncAll: bool = Field(False, description="Refresh all stocks when true.")


class IndicatorRealtimeResponse(BaseModel):
    processed: int
    metricsUpdated: int
    codes: List[str]
    updatedAt: datetime


class MarketFundFlowRecord(BaseModel):
    trade_date: date = Field(..., alias="tradeDate")
    shanghai_close: Optional[float] = Field(None, alias="shanghaiClose")
    shanghai_change_percent: Optional[float] = Field(None, alias="shanghaiChangePercent")
    shenzhen_close: Optional[float] = Field(None, alias="shenzhenClose")
    shenzhen_change_percent: Optional[float] = Field(None, alias="shenzhenChangePercent")
    main_net_inflow_amount: Optional[float] = Field(None, alias="mainNetInflowAmount")
    main_net_inflow_ratio: Optional[float] = Field(None, alias="mainNetInflowRatio")
    huge_order_net_inflow_amount: Optional[float] = Field(None, alias="hugeOrderNetInflowAmount")
    huge_order_net_inflow_ratio: Optional[float] = Field(None, alias="hugeOrderNetInflowRatio")
    large_order_net_inflow_amount: Optional[float] = Field(None, alias="largeOrderNetInflowAmount")
    large_order_net_inflow_ratio: Optional[float] = Field(None, alias="largeOrderNetInflowRatio")
    medium_order_net_inflow_amount: Optional[float] = Field(None, alias="mediumOrderNetInflowAmount")
    medium_order_net_inflow_ratio: Optional[float] = Field(None, alias="mediumOrderNetInflowRatio")
    small_order_net_inflow_amount: Optional[float] = Field(None, alias="smallOrderNetInflowAmount")
    small_order_net_inflow_ratio: Optional[float] = Field(None, alias="smallOrderNetInflowRatio")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class MarketFundFlowListResponse(BaseModel):
    total: int
    items: List[MarketFundFlowRecord]
    latest_trade_date: Optional[date] = Field(None, alias="latestTradeDate")
    last_synced_at: Optional[datetime] = Field(None, alias="lastSyncedAt")
    available_years: List[int] = Field(default_factory=list, alias="availableYears")

    class Config:
        allow_population_by_field_name = True


class SyncFinanceBreakfastResponse(BaseModel):
    rows: int
    elapsed_seconds: float = Field(..., alias="elapsedSeconds")

class VolumeSurgeConfigPayload(BaseModel):
    min_volume_ratio: float = Field(3.0, alias="minVolumeRatio", ge=0.5, le=1000)
    breakout_threshold_percent: float = Field(3.0, alias="breakoutPercent", ge=0.0, le=100.0)
    daily_change_threshold_percent: float = Field(7.0, alias="dailyChangePercent", ge=0.0, le=200.0)
    max_range_percent: float = Field(25.0, alias="maxRangePercent", ge=1.0, le=200.0)

    class Config:
        allow_population_by_field_name = True


class RuntimeConfigPayload(BaseModel):
    include_st: bool = Field(..., alias="includeST")
    include_delisted: bool = Field(..., alias="includeDelisted")
    daily_trade_window_days: int = Field(..., alias="dailyTradeWindowDays", ge=1, le=3650)
    peripheral_aggregate_time: Optional[str] = Field(
        None,
        alias="peripheralAggregateTime",
        description="Daily run time (HH:MM, 24h) for peripheral aggregate scheduler.",
    )
    global_flash_frequency_minutes: int = Field(
        180,
        alias="globalFlashFrequencyMinutes",
        ge=10,
        le=1440,
        description="Interval in minutes between global flash data refresh runs.",
    )
    concept_alias_map: Dict[str, List[str]] = Field(
        default_factory=dict,
        alias="conceptAliasMap",
        description="Mapping between concept name and whitespace-separated alias keywords.",
    )
    volume_surge_config: VolumeSurgeConfigPayload = Field(
        default_factory=VolumeSurgeConfigPayload,
        alias="volumeSurgeConfig",
        description="Threshold controls for the volume surge breakout indicator.",
    )

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
        peripheral_aggregate_time=config.peripheral_aggregate_time,
        global_flash_frequency_minutes=config.global_flash_frequency_minutes,
        concept_alias_map=config.concept_alias_map,
        volume_surge_config=VolumeSurgeConfigPayload(
            min_volume_ratio=config.volume_surge_config.min_volume_ratio,
            breakout_threshold_percent=config.volume_surge_config.breakout_threshold_percent,
            daily_change_threshold_percent=config.volume_surge_config.daily_change_threshold_percent,
            max_range_percent=config.volume_surge_config.max_range_percent,
        ),
    )


def _sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_for_json(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_for_json(item) for item in value)
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, (int, float)):
        numeric = float(value)
        if math.isnan(numeric) or math.isinf(numeric):
            return None
        return numeric
    return value


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


async def _run_concept_directory_job() -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        try:
            result = sync_concept_directory(settings_path=None)
            stats: Dict[str, object] = {}
            try:
                stats = ConceptDirectoryDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh concept_directory stats: %s", stats_exc)
            elapsed = time.perf_counter() - started
            total_rows = None
            finished_at = None
            if isinstance(stats, dict):
                total_rows = stats.get("count")
                finished_at = stats.get("updated_at")
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "concept_directory",
                success=True,
                total_rows=total_rows,
                message="Concept directory synced",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "concept_directory",
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
            elapsed = float(result.get("elapsedSeconds", result.get("elapsed_seconds", time.perf_counter() - started)))
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
            elapsed = float(result.get("elapsedSeconds", result.get("elapsed_seconds", time.perf_counter() - started)))
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
            elapsed = float(result.get("elapsedSeconds", result.get("elapsed_seconds", time.perf_counter() - started)))
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
            elapsed = float(result.get("elapsedSeconds", result.get("elapsed_seconds", time.perf_counter() - started)))
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
            elapsed = float(result.get("elapsedSeconds", result.get("elapsed_seconds", time.perf_counter() - started)))
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

    def job() -> None:
        monitor.update("finance_breakfast", progress=0.0, message="Fetching finance breakfast feed")
        started = time.perf_counter()
        try:
            result = sync_finance_breakfast()
            stats: Dict[str, object] = {}
            try:
                stats = NewsArticleDAO(load_settings().postgres).stats(source="finance_breakfast")
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh finance_breakfast stats: %s", stats_exc)
            elapsed = float(result.get("elapsedSeconds", result.get("elapsed_seconds", time.perf_counter() - started)))
            total_rows = stats.get("total") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            monitor.update("finance_breakfast", progress=1.0, message="Finance breakfast sync completed")
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
async def _run_market_overview_job(request: SyncMarketOverviewRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update(
            "market_overview",
            progress=0.2,
            message="Building market overview snapshot",
        )
        try:
            result = generate_market_overview_reasoning(run_llm=request.run_llm)
            elapsed = time.perf_counter() - started
            generated_at = result.get("generatedAt")
            finished_at = _parse_datetime(generated_at) or _local_now()
            llm_flag = " with LLM" if request.run_llm else ""
            monitor.update(
                "market_overview",
                progress=1.0,
                message=f"Market overview refreshed{llm_flag}",
            )
            monitor.finish(
                "market_overview",
                success=True,
                total_rows=1,
                message=f"Market overview refreshed{llm_flag}",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "market_overview",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Market overview generation failed: %s", error_message)

    await loop.run_in_executor(None, job)


async def _run_market_insight_job(request: SyncMarketInsightRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update(
            "market_insight",
            progress=0.1,
            message="Collecting market-impact headlines",
        )
        try:
            result = generate_market_insight_summary(
                lookback_hours=request.lookback_hours,
                limit=request.article_limit,
            )
            elapsed = time.perf_counter() - started
            headline_count = int(result.get("headline_count", 0) or 0)
            generated_at = result.get("generated_at")
            monitor.update(
                "market_insight",
                progress=1.0,
                message=f"Generated summary from {headline_count} headlines",
            )
            monitor.finish(
                "market_insight",
                success=True,
                total_rows=headline_count,
                message=f"Generated summary from {headline_count} headlines",
                finished_at=generated_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "market_insight",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Market insight generation failed: %s", error_message)

    await loop.run_in_executor(None, job)


async def _run_sector_insight_job(request: SyncSectorInsightRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update(
            "sector_insight",
            progress=0.1,
            message="Collecting sector-impact headlines",
        )
        try:
            result = generate_sector_insight_summary(
                lookback_hours=request.lookback_hours,
                limit=request.article_limit,
            )
            elapsed = time.perf_counter() - started
            headline_count = int(result.get("headline_count", 0) or 0)
            snapshot = result.get("group_snapshot") if isinstance(result, dict) else None
            group_count = int(result.get("group_count") or 0)
            if isinstance(snapshot, dict):
                try:
                    group_count = int(snapshot.get("groupCount", group_count) or group_count)
                except (TypeError, ValueError):
                    group_count = int(result.get("group_count") or 0)
            generated_at = result.get("generated_at")
            message = f"Generated sector insight from {headline_count} headlines across {group_count} groups"
            monitor.update("sector_insight", progress=1.0, message=message)
            monitor.finish(
                "sector_insight",
                success=True,
                total_rows=headline_count,
                message=message,
                finished_at=generated_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "sector_insight",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Sector insight generation failed: %s", error_message)

    await loop.run_in_executor(None, job)


async def _run_global_flash_job(request: SyncGlobalFlashRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        try:
            result = sync_global_flash()
            stats: Dict[str, object] = {}
            try:
                stats = NewsArticleDAO(load_settings().postgres).stats(source="global_flash")
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh global_flash stats: %s", stats_exc)
            elapsed = float(result.get("elapsedSeconds", result.get("elapsed_seconds", time.perf_counter() - started)))
            total_rows = None
            if isinstance(stats, dict):
                total_rows = stats.get("count")
                finished_at = stats.get("updated_at")
            else:
                finished_at = None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "global_flash",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message="Global flash sync completed",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "global_flash",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Global flash sync failed: %s", error_message)

    await loop.run_in_executor(None, job)


async def _run_trade_calendar_job(request: SyncTradeCalendarRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("trade_calendar", message="Syncing A-share trading calendar", progress=0.0)
        try:
            result = sync_trade_calendar(
                start_date=request.start_date,
                end_date=request.end_date,
                exchange=request.exchange or "SSE",
            )
            stats: Dict[str, object] = {}
            try:
                stats = TradeCalendarDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to refresh trade_calendar stats: %s", stats_exc)
            elapsed = float(result.get("elapsedSeconds", time.perf_counter() - started))
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            finished_at = stats.get("updated_at") if isinstance(stats, dict) else None
            monitor.finish(
                "trade_calendar",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Trade calendar synced ({result.get('rows', 0)} rows)",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "trade_calendar",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Trade calendar sync failed: %s", error_message)

    await loop.run_in_executor(None, job)


async def _run_global_flash_classification_job(request: SyncGlobalFlashClassifyRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("global_flash_classification", message="Classifying global flash entries", progress=0.0)
        try:
            relevance_result = classify_relevance_batch(batch_size=request.batch_size)
            impact_result = classify_impact_batch(batch_size=request.batch_size)

            relevance_rows = int(relevance_result.get("rows", 0) or 0)
            impact_rows = int(impact_result.get("rows", 0) or 0)
            relevance_requested = int(relevance_result.get("requested", relevance_rows) or relevance_rows)
            impact_requested = int(impact_result.get("requested", impact_rows) or impact_rows)
            skipped = bool(relevance_result.get("skipped")) and bool(impact_result.get("skipped"))

            elapsed_relevance = float(relevance_result.get("elapsedSeconds", 0.0) or 0.0)
            elapsed_impact = float(impact_result.get("elapsedSeconds", 0.0) or 0.0)
            elapsed = elapsed_relevance + elapsed_impact
            if elapsed <= 0:
                elapsed = time.perf_counter() - started

            rows = relevance_rows + impact_rows
            message = (
                "DeepSeek configuration missing; classification skipped"
                if skipped
                else (
                    f"Relevance {relevance_rows}/{relevance_requested}; "
                    f"Impact {impact_rows}/{impact_requested}"
                )
            )
            monitor.finish(
                "global_flash_classification",
                success=True,
                total_rows=rows,
                message=message,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            error_message = str(exc)
            monitor.finish(
                "global_flash_classification",
                success=False,
                message=error_message,
                error=error_message,
                last_duration=elapsed,
            )
            logger.error("Global flash classification failed: %s", error_message)

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


async def _run_realtime_index_job(request: SyncRealtimeIndexRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("realtime_index", message="Syncing realtime China indices", progress=0.0)
        try:
            result = sync_realtime_indices()
            stats = RealtimeIndexDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "realtime_index",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} realtime index rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "realtime_index",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_index_history_job(request: SyncIndexHistoryRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        normalized_codes: Optional[List[str]] = None
        if request.index_codes:
            filtered = []
            for code in request.index_codes:
                if not code:
                    continue
                normalized = str(code).strip().upper()
                if normalized:
                    filtered.append(normalized)
            if filtered:
                normalized_codes = filtered

        display_codes = ", ".join(normalized_codes) if normalized_codes else "core indices"
        monitor.update(
            "index_history",
            message=f"Syncing index history ({display_codes})",
            progress=0.0,
        )

        started = time.perf_counter()
        try:
            result = sync_index_history(index_codes=normalized_codes)
            stats: Dict[str, object] = {}
            try:
                stats = IndexHistoryDAO(load_settings().postgres).stats()
            except Exception as stats_exc:  # pragma: no cover - defensive
                logger.warning("Failed to collect index_history stats: %s", stats_exc)
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            finished_at = None
            latest_date = stats.get("latest") if isinstance(stats, dict) else None
            if isinstance(latest_date, date):
                finished_at = datetime.combine(latest_date, datetime.min.time())
            rows_synced = int(result.get("rows", 0) or 0)
            monitor.finish(
                "index_history",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {rows_synced} index history rows",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "index_history",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            logger.error("Index history sync failed: %s", exc)
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


async def _run_macro_leverage_job(request: SyncMacroLeverageRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("leverage_ratio", message="Syncing macro leverage ratios", progress=0.0)
        try:
            result = sync_macro_leverage_ratios()
            stats = MacroLeverageDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "leverage_ratio",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} macro leverage rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "leverage_ratio",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_social_financing_job(request: SyncSocialFinancingRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("social_financing", message="Syncing social financing data", progress=0.0)
        try:
            result = sync_social_financing_ratios()
            stats = MacroSocialFinancingDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "social_financing",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} social financing rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "social_financing",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_macro_cpi_job(request: SyncMacroCpiRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("cpi_monthly", message="Syncing CPI data", progress=0.0)
        try:
            result = sync_macro_cpi()
            stats = MacroCpiDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "cpi_monthly",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} CPI rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "cpi_monthly",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_macro_pmi_job(request: SyncMacroPmiRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("pmi_monthly", message="Syncing PMI data", progress=0.0)
        try:
            result = sync_macro_pmi()
            stats = MacroPmiDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "pmi_monthly",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} PMI rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "pmi_monthly",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_macro_m2_job(request: SyncMacroM2Request) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("m2_monthly", message="Syncing M2 money supply", progress=0.0)
        try:
            result = sync_macro_m2()
            stats = MacroM2DAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "m2_monthly",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} M2 rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "m2_monthly",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_macro_ppi_job(request: SyncMacroPpiRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("ppi_monthly", message="Syncing PPI data", progress=0.0)
        try:
            result = sync_macro_ppi()
            stats = MacroPpiDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "ppi_monthly",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} PPI rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "ppi_monthly",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_macro_lpr_job(request: SyncMacroLprRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("lpr_rate", message="Syncing LPR data", progress=0.0)
        try:
            result = sync_macro_lpr()
            stats = MacroLprDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "lpr_rate",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} LPR rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "lpr_rate",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_macro_shibor_job(request: SyncMacroShiborRequest) -> None:  # noqa: ARG001
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("shibor_rate", message="Syncing SHIBOR data", progress=0.0)
        try:
            result = sync_macro_shibor()
            stats = MacroShiborDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "shibor_rate",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} SHIBOR rows",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "shibor_rate",
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


async def _run_fed_statement_job(request: SyncFedStatementRequest) -> None:
    loop = asyncio.get_running_loop()
    limit = request.limit or 5

    def job() -> None:
        started = time.perf_counter()
        monitor.update("fed_statements", message="Syncing Federal Reserve statements", progress=0.0)
        try:
            result = sync_fed_statements(limit=limit)
            stats = FedStatementDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "fed_statements",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} Fed statements",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "fed_statements",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_peripheral_insight_job(request: SyncPeripheralInsightRequest) -> None:
    loop = asyncio.get_running_loop()
    run_llm = True if request.run_llm is None else bool(request.run_llm)

    def job() -> None:
        started = time.perf_counter()
        monitor.update("peripheral_insight", message="Generating peripheral market insight", progress=0.0)
        try:
            result = generate_peripheral_insight(run_llm=run_llm)
            stats = PeripheralInsightDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = 1
            monitor.finish(
                "peripheral_insight",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message="Peripheral insight snapshot generated",
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "peripheral_insight",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_macro_aggregate_job(request: SyncMacroAggregateRequest) -> None:
    started = time.perf_counter()

    steps: List[Tuple[str, str, Callable[[BaseModel], Awaitable[None]], BaseModel]] = [
        (
            "leverage_ratio",
            "Syncing macro leverage ratios",
            _run_macro_leverage_job,
            SyncMacroLeverageRequest(),
        ),
        (
            "social_financing",
            "Syncing social financing data",
            _run_social_financing_job,
            SyncSocialFinancingRequest(),
        ),
        (
            "cpi_monthly",
            "Syncing CPI data",
            _run_macro_cpi_job,
            SyncMacroCpiRequest(),
        ),
        (
            "ppi_monthly",
            "Syncing PPI data",
            _run_macro_ppi_job,
            SyncMacroPpiRequest(),
        ),
        (
            "pmi_monthly",
            "Syncing PMI data",
            _run_macro_pmi_job,
            SyncMacroPmiRequest(),
        ),
        (
            "m2_monthly",
            "Syncing M2 money supply",
            _run_macro_m2_job,
            SyncMacroM2Request(),
        ),
        (
            "lpr_rate",
            "Syncing LPR data",
            _run_macro_lpr_job,
            SyncMacroLprRequest(),
        ),
        (
            "shibor_rate",
            "Syncing SHIBOR data",
            _run_macro_shibor_job,
            SyncMacroShiborRequest(),
        ),
        (
            "macro_insight",
            "Generating macro insight summary",
            _run_macro_insight_job,
            SyncMacroInsightRequest(),
        ),
    ]

    total_steps = len(steps)

    try:
        for idx, (job_key, status_message, runner, runner_request) in enumerate(steps, start=1):
            monitor.update(
                "macro_aggregate",
                progress=(idx - 1) / total_steps,
                message=status_message,
            )
            monitor.start(job_key, message=status_message)
            monitor.update(job_key, progress=0.0)
            await runner(runner_request)
            monitor.update(
                "macro_aggregate",
                progress=idx / total_steps,
                message=status_message,
            )

        elapsed = time.perf_counter() - started
        monitor.finish(
            "macro_aggregate",
            success=True,
            total_rows=total_steps,
            message="Macro aggregate sync completed",
            last_duration=elapsed,
        )
    except Exception as exc:  # pragma: no cover - defensive
        elapsed = time.perf_counter() - started
        completed_steps = 0
        if "idx" in locals():
            try:
                completed_steps = max(0, int(idx) - 1)
            except Exception:  # pragma: no cover - defensive
                completed_steps = 0
        monitor.finish(
            "macro_aggregate",
            success=False,
            total_rows=completed_steps or None,
            error=str(exc),
            last_duration=elapsed,
        )
        raise


async def _run_peripheral_aggregate_job(request: SyncPeripheralAggregateRequest) -> None:
    started = time.perf_counter()
    run_llm = request.run_llm
    fed_limit = request.fed_limit

    steps: List[Tuple[str, str, Callable[[BaseModel], Awaitable[None]], BaseModel]] = [
        (
            "global_index",
            "Syncing global index snapshot",
            _run_global_index_job,
            SyncGlobalIndexRequest(),
        ),
        (
            "dollar_index",
            "Syncing dollar index history",
            _run_dollar_index_job,
            SyncDollarIndexRequest(),
        ),
        (
            "rmb_midpoint",
            "Syncing RMB midpoint rates",
            _run_rmb_midpoint_job,
            SyncRmbMidpointRequest(),
        ),
        (
            "futures_realtime",
            "Syncing futures realtime data",
            _run_futures_realtime_job,
            SyncFuturesRealtimeRequest(),
        ),
        (
            "fed_statements",
            "Syncing Federal Reserve statements",
            _run_fed_statement_job,
            SyncFedStatementRequest(limit=fed_limit),
        ),
        (
            "peripheral_insight",
            "Generating peripheral market insight",
            _run_peripheral_insight_job,
            SyncPeripheralInsightRequest(run_llm=run_llm),
        ),
    ]

    total_steps = len(steps)

    try:
        for idx, (job_key, status_message, runner, runner_request) in enumerate(steps, start=1):
            monitor.update(
                "peripheral_aggregate",
                progress=(idx - 1) / total_steps,
                message=status_message,
            )
            monitor.start(job_key, message=status_message)
            monitor.update(job_key, progress=0.0)
            await runner(runner_request)
            monitor.update(
                "peripheral_aggregate",
                progress=idx / total_steps,
                message=status_message,
            )

        elapsed = time.perf_counter() - started
        monitor.finish(
            "peripheral_aggregate",
            success=True,
            total_rows=total_steps,
            message="Peripheral aggregate sync completed",
            last_duration=elapsed,
        )
    except Exception as exc:  # pragma: no cover - defensive
        elapsed = time.perf_counter() - started
        completed_steps = 0
        if 'idx' in locals():
            try:
                completed_steps = max(0, int(idx) - 1)
            except Exception:  # pragma: no cover - defensive
                completed_steps = 0
        monitor.finish(
            "peripheral_aggregate",
            success=False,
            total_rows=completed_steps or None,
            error=str(exc),
            last_duration=elapsed,
        )
        raise


async def _run_fund_flow_aggregate_job(request: SyncFundFlowAggregateRequest) -> None:
    started = time.perf_counter()

    steps: List[Tuple[str, str, Callable[[BaseModel], Awaitable[None]], BaseModel]] = [
        (
            "industry_fund_flow",
            "Syncing industry fund flow data",
            _run_industry_fund_flow_job,
            SyncIndustryFundFlowRequest(),
        ),
        (
            "concept_fund_flow",
            "Syncing concept fund flow data",
            _run_concept_fund_flow_job,
            SyncConceptFundFlowRequest(),
        ),
        (
            "individual_fund_flow",
            "Syncing individual fund flow data",
            _run_individual_fund_flow_job,
            SyncIndividualFundFlowRequest(),
        ),
        (
            "hsgt_fund_flow",
            "Syncing HSGT fund flow summary",
            _run_hsgt_fund_flow_job,
            SyncHsgtFundFlowRequest(),
        ),
        (
            "margin_account",
            "Syncing margin account statistics",
            _run_margin_account_job,
            SyncMarginAccountRequest(),
        ),
        (
            "market_fund_flow",
            "Syncing market fund flow history",
            _run_market_fund_flow_job,
            SyncMarketFundFlowRequest(),
        ),
        (
            "big_deal_fund_flow",
            "Syncing big deal fund flow data",
            _run_big_deal_fund_flow_job,
            SyncBigDealFundFlowRequest(),
        ),
    ]

    total_steps = len(steps)

    try:
        for idx, (job_key, status_message, runner, runner_request) in enumerate(steps, start=1):
            monitor.update(
                "fund_flow_aggregate",
                progress=(idx - 1) / total_steps,
                message=status_message,
            )
            monitor.start(job_key, message=status_message)
            monitor.update(job_key, progress=0.0)
            await runner(runner_request)
            monitor.update(
                "fund_flow_aggregate",
                progress=idx / total_steps,
                message=status_message,
            )

        elapsed = time.perf_counter() - started
        monitor.finish(
            "fund_flow_aggregate",
            success=True,
            total_rows=total_steps,
            message="Fund flow aggregate sync completed",
            last_duration=elapsed,
        )
    except Exception as exc:  # pragma: no cover - defensive
        elapsed = time.perf_counter() - started
        completed_steps = 0
        if "idx" in locals():
            try:
                completed_steps = max(0, int(idx) - 1)
            except Exception:  # pragma: no cover - defensive
                completed_steps = 0
        monitor.finish(
            "fund_flow_aggregate",
            success=False,
            total_rows=completed_steps or None,
            error=str(exc),
            last_duration=elapsed,
        )
        raise


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


async def _run_concept_index_history_job(request: SyncConceptIndexHistoryRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update(
            "concept_index_history",
            message="Syncing concept index history",
            progress=0.0,
        )
        try:
            result = sync_concept_index_history(
                request.concepts,
                start_date=request.start_date,
                end_date=request.end_date,
            )
            stats = ConceptIndexHistoryDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("totalRows")
            message_text = (
                f"Synced {result.get('totalRows', 0)} concept index rows across {len(result.get('concepts', []))} concepts"
            )
            monitor.finish(
                "concept_index_history",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "concept_index_history",
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


async def _run_hsgt_fund_flow_job(request: SyncHsgtFundFlowRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "hsgt_fund_flow",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        default_symbol = "北向资金"
        symbol = (request.symbol or default_symbol).strip() if hasattr(request, "symbol") else default_symbol
        monitor.update("hsgt_fund_flow", message=f"Collecting HSGT fund flow data ({symbol})")
        try:
            result = sync_hsgt_fund_flow(symbol=symbol, progress_callback=progress_callback)
            stats = HSGTFundFlowDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            message_text = f"Synced {result.get('rows', 0)} HSGT fund flow rows ({symbol})"
            monitor.finish(
                "hsgt_fund_flow",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "hsgt_fund_flow",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_margin_account_job(request: SyncMarginAccountRequest) -> None:
    loop = asyncio.get_running_loop()

    def progress_callback(progress: float, message: Optional[str], total_rows: Optional[int]) -> None:
        monitor.update(
            "margin_account",
            progress=progress,
            message=message,
            total_rows=total_rows,
        )

    def job() -> None:
        started = time.perf_counter()
        monitor.update("margin_account", message="Collecting margin account statistics")
        try:
            result = sync_margin_account_info(progress_callback=progress_callback)
            stats = MarginAccountDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            total_rows = stats.get("count") if isinstance(stats, dict) else None
            if total_rows is None:
                total_rows = result.get("rows")
            message_text = f"Synced {result.get('rows', 0)} margin account rows"
            monitor.finish(
                "margin_account",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=message_text,
                finished_at=stats.get("updated_at") if isinstance(stats, dict) else None,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "margin_account",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_market_activity_job(request: SyncMarketActivityRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("market_activity", message="Collecting market activity snapshot")
        try:
            result = sync_market_activity()
            dao_result = MarketActivityDAO(load_settings().postgres).list_entries()
            elapsed = time.perf_counter() - started
            items = dao_result.get("items", [])
            message_text = f"Synced {result.get('rows', 0)} market activity rows"
            monitor.finish(
                "market_activity",
                success=True,
                total_rows=len(items),
                message=message_text,
                finished_at=dao_result.get("dataset_timestamp"),
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "market_activity",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_market_fund_flow_job(request: SyncMarketFundFlowRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("market_fund_flow", message="Collecting market fund flow history")
        try:
            result = sync_market_fund_flow()
            dao = MarketFundFlowDAO(load_settings().postgres)
            stats = dao.stats()
            elapsed = time.perf_counter() - started
            total_rows = None
            finished_at = None
            if isinstance(stats, dict):
                total_rows = stats.get("count")
                finished_at = stats.get("updated_at")
            if total_rows is None:
                total_rows = result.get("rows")
            monitor.finish(
                "market_fund_flow",
                success=True,
                total_rows=int(total_rows) if total_rows is not None else None,
                message=f"Synced {result.get('rows', 0)} market fund flow rows",
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "market_fund_flow",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_macro_insight_job(request: SyncMacroInsightRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update("macro_insight", message="Generating macro insight summary")
        try:
            result = generate_macro_insight(run_llm=request.run_llm)
            stats = MacroInsightDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            finished_at = result.get("generated_at")
            message_text = "Macro insight generated"
            monitor.finish(
                "macro_insight",
                success=True,
                total_rows=1,
                message=message_text,
                finished_at=finished_at,
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "macro_insight",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_concept_insight_job(request: SyncConceptInsightRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update(
            "concept_insight",
            message="Generating concept insight summary",
            progress=0.0,
        )
        try:
            result = generate_concept_insight_summary(
                lookback_hours=request.lookback_hours,
                concept_limit=request.concept_limit,
                run_llm=request.run_llm,
                refresh_index_history=request.refresh_index_history,
            )
            stats = ConceptInsightDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            message_text = "Concept insight generated"
            monitor.finish(
                "concept_insight",
                success=True,
                total_rows=1,
                message=message_text,
                finished_at=result.get("generated_at"),
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "concept_insight",
                success=False,
                error=str(exc),
                last_duration=elapsed,
            )
            raise

    await loop.run_in_executor(None, job)


async def _run_industry_insight_job(request: SyncIndustryInsightRequest) -> None:
    loop = asyncio.get_running_loop()

    def job() -> None:
        started = time.perf_counter()
        monitor.update(
            "industry_insight",
            message="Generating industry insight summary",
            progress=0.0,
        )
        try:
            result = generate_industry_insight_summary(
                lookback_hours=request.lookback_hours,
                industry_limit=request.industry_limit,
                run_llm=request.run_llm,
            )
            stats = IndustryInsightDAO(load_settings().postgres).stats()
            elapsed = time.perf_counter() - started
            message_text = "Industry insight generated"
            monitor.finish(
                "industry_insight",
                success=True,
                total_rows=1,
                message=message_text,
                finished_at=result.get("generated_at"),
                last_duration=elapsed,
            )
        except Exception as exc:  # pragma: no cover - defensive
            elapsed = time.perf_counter() - started
            monitor.finish(
                "industry_insight",
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
    if job_state.get("status") != "running":
        return False

    started_at_raw = job_state.get("startedAt")
    started_at: Optional[datetime]
    if started_at_raw:
        try:
            started_at = datetime.fromisoformat(str(started_at_raw))
        except ValueError:
            started_at = None
    else:
        started_at = None

    if started_at:
        elapsed = (datetime.now(LOCAL_TZ).replace(tzinfo=None) - started_at).total_seconds()
        if elapsed > 300:  # auto-reset after 5 minutes
            logger.warning("Job %s marked stale after %.0f seconds; resetting status", job, elapsed)
            monitor.finish(
                job,
                success=False,
                message="Job reset due to inactivity",
                error="stale job auto-reset",
                last_duration=elapsed,
            )
            return False
    else:
        logger.warning("Job %s marked stale (missing start timestamp); resetting status", job)
        monitor.finish(
            job,
            success=False,
            message="Job reset due to invalid state",
            error="stale job auto-reset",
            last_duration=0.0,
        )
        return False

    return True


async def start_concept_directory_job() -> None:
    if _job_running("concept_directory"):
        raise HTTPException(status_code=409, detail="Concept directory sync already running")
    monitor.start("concept_directory", message="Syncing concept directory")
    monitor.update("concept_directory", progress=0.0)
    asyncio.create_task(_run_concept_directory_job())


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


async def start_market_overview_job(payload: SyncMarketOverviewRequest) -> None:
    if _job_running("market_overview"):
        raise HTTPException(status_code=409, detail="Market overview job already running")
    monitor.start("market_overview", message="Generating market overview insight")
    monitor.update("market_overview", progress=0.0)
    asyncio.create_task(_run_market_overview_job(payload))


async def start_market_insight_job(payload: SyncMarketInsightRequest) -> None:
    if _job_running("market_insight"):
        raise HTTPException(status_code=409, detail="Market insight job already running")
    monitor.start("market_insight", message="Generating market insight summary")
    monitor.update("market_insight", progress=0.0)
    asyncio.create_task(_run_market_insight_job(payload))


async def start_sector_insight_job(payload: SyncSectorInsightRequest) -> None:
    if _job_running("sector_insight"):
        raise HTTPException(status_code=409, detail="Sector insight job already running")
    monitor.start("sector_insight", message="Generating sector insight summary")
    monitor.update("sector_insight", progress=0.0)
    asyncio.create_task(_run_sector_insight_job(payload))


async def start_global_index_job(payload: SyncGlobalIndexRequest) -> None:  # noqa: ARG001
    if _job_running("global_index"):
        raise HTTPException(status_code=409, detail="Global index sync already running")
    monitor.start("global_index", message="Syncing global index snapshot")
    monitor.update("global_index", progress=0.0)
    asyncio.create_task(_run_global_index_job(payload))


async def start_realtime_index_job(payload: SyncRealtimeIndexRequest) -> None:  # noqa: ARG001
    if _job_running("realtime_index"):
        raise HTTPException(status_code=409, detail="Realtime index sync already running")
    monitor.start("realtime_index", message="Syncing realtime China indices")
    monitor.update("realtime_index", progress=0.0)
    asyncio.create_task(_run_realtime_index_job(payload))


async def start_index_history_job(payload: SyncIndexHistoryRequest) -> None:
    if _job_running("index_history"):
        raise HTTPException(status_code=409, detail="Index history sync already running")

    normalized_codes: Optional[List[str]] = None
    if payload.index_codes:
        filtered = []
        for code in payload.index_codes:
            if not code:
                continue
            normalized = str(code).strip().upper()
            if normalized:
                filtered.append(normalized)
        if filtered:
            normalized_codes = filtered

    request = payload
    if normalized_codes is not None:
        request = payload.copy(update={"index_codes": normalized_codes})

    if normalized_codes:
        monitor.start(
            "index_history",
            message=f"Syncing index history ({', '.join(normalized_codes)})",
        )
    else:
        monitor.start("index_history", message="Syncing index history (core indices)")
    monitor.update("index_history", progress=0.0)
    asyncio.create_task(_run_index_history_job(request))


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


async def start_macro_leverage_job(payload: SyncMacroLeverageRequest) -> None:
    if _job_running("leverage_ratio"):
        raise HTTPException(status_code=409, detail="Macro leverage sync already running")
    monitor.start("leverage_ratio", message="Syncing macro leverage ratios")
    monitor.update("leverage_ratio", progress=0.0)
    asyncio.create_task(_run_macro_leverage_job(payload))


async def start_social_financing_job(payload: SyncSocialFinancingRequest) -> None:
    if _job_running("social_financing"):
        raise HTTPException(status_code=409, detail="Social financing sync already running")
    monitor.start("social_financing", message="Syncing social financing data")
    monitor.update("social_financing", progress=0.0)
    asyncio.create_task(_run_social_financing_job(payload))


async def start_macro_cpi_job(payload: SyncMacroCpiRequest) -> None:
    if _job_running("cpi_monthly"):
        raise HTTPException(status_code=409, detail="CPI sync already running")
    monitor.start("cpi_monthly", message="Syncing CPI data")
    monitor.update("cpi_monthly", progress=0.0)
    asyncio.create_task(_run_macro_cpi_job(payload))


async def start_macro_pmi_job(payload: SyncMacroPmiRequest) -> None:
    if _job_running("pmi_monthly"):
        raise HTTPException(status_code=409, detail="PMI sync already running")
    monitor.start("pmi_monthly", message="Syncing PMI data")
    monitor.update("pmi_monthly", progress=0.0)
    asyncio.create_task(_run_macro_pmi_job(payload))


async def start_macro_m2_job(payload: SyncMacroM2Request) -> None:
    if _job_running("m2_monthly"):
        raise HTTPException(status_code=409, detail="M2 sync already running")
    monitor.start("m2_monthly", message="Syncing M2 money supply")
    monitor.update("m2_monthly", progress=0.0)
    asyncio.create_task(_run_macro_m2_job(payload))


async def start_macro_ppi_job(payload: SyncMacroPpiRequest) -> None:
    if _job_running("ppi_monthly"):
        raise HTTPException(status_code=409, detail="PPI sync already running")
    monitor.start("ppi_monthly", message="Syncing PPI data")
    monitor.update("ppi_monthly", progress=0.0)
    asyncio.create_task(_run_macro_ppi_job(payload))


async def start_macro_lpr_job(payload: SyncMacroLprRequest) -> None:
    if _job_running("lpr_rate"):
        raise HTTPException(status_code=409, detail="LPR sync already running")
    monitor.start("lpr_rate", message="Syncing LPR data")
    monitor.update("lpr_rate", progress=0.0)
    asyncio.create_task(_run_macro_lpr_job(payload))


async def start_macro_shibor_job(payload: SyncMacroShiborRequest) -> None:
    if _job_running("shibor_rate"):
        raise HTTPException(status_code=409, detail="SHIBOR sync already running")
    monitor.start("shibor_rate", message="Syncing SHIBOR data")
    monitor.update("shibor_rate", progress=0.0)
    asyncio.create_task(_run_macro_shibor_job(payload))


async def start_futures_realtime_job(payload: SyncFuturesRealtimeRequest) -> None:
    if _job_running("futures_realtime"):
        raise HTTPException(status_code=409, detail="Futures realtime sync already running")
    monitor.start("futures_realtime", message="Syncing futures realtime data")
    monitor.update("futures_realtime", progress=0.0)
    asyncio.create_task(_run_futures_realtime_job(payload))


async def start_fed_statement_job(payload: SyncFedStatementRequest) -> None:
    if _job_running("fed_statements"):
        raise HTTPException(status_code=409, detail="Fed statements sync already running")
    monitor.start("fed_statements", message="Syncing Federal Reserve statements")
    monitor.update("fed_statements", progress=0.0)
    asyncio.create_task(_run_fed_statement_job(payload))


async def start_peripheral_insight_job(payload: SyncPeripheralInsightRequest) -> None:
    if _job_running("peripheral_insight"):
        raise HTTPException(status_code=409, detail="Peripheral insight job already running")
    monitor.start("peripheral_insight", message="Generating peripheral market insight")
    monitor.update("peripheral_insight", progress=0.0)
    asyncio.create_task(_run_peripheral_insight_job(payload))


async def start_macro_aggregate_job(payload: SyncMacroAggregateRequest) -> None:
    if _job_running("macro_aggregate"):
        raise HTTPException(status_code=409, detail="Macro aggregate sync already running")

    dependent_jobs = [
        ("leverage_ratio", "Macro leverage sync already running"),
        ("social_financing", "Social financing sync already running"),
        ("cpi_monthly", "CPI sync already running"),
        ("ppi_monthly", "PPI sync already running"),
        ("pmi_monthly", "PMI sync already running"),
        ("m2_monthly", "M2 sync already running"),
        ("lpr_rate", "LPR sync already running"),
        ("shibor_rate", "SHIBOR sync already running"),
    ]

    for job_key, detail in dependent_jobs:
        if _job_running(job_key):
            raise HTTPException(status_code=409, detail=detail)

    monitor.start("macro_aggregate", message="Syncing macro data bundle")
    monitor.update("macro_aggregate", progress=0.0)
    asyncio.create_task(_run_macro_aggregate_job(payload))


async def start_peripheral_aggregate_job(payload: SyncPeripheralAggregateRequest) -> None:
    if _job_running("peripheral_aggregate"):
        raise HTTPException(status_code=409, detail="Peripheral aggregate sync already running")

    dependent_jobs = [
        ("global_index", "Global indices sync already running"),
        ("dollar_index", "Dollar index sync already running"),
        ("rmb_midpoint", "RMB midpoint sync already running"),
        ("futures_realtime", "Futures realtime sync already running"),
        ("fed_statements", "Fed statements sync already running"),
        ("peripheral_insight", "Peripheral insight sync already running"),
    ]

    for job_key, error_message in dependent_jobs:
        if _job_running(job_key):
            raise HTTPException(status_code=409, detail=error_message)

    monitor.start("peripheral_aggregate", message="Syncing peripheral data bundle")
    monitor.update("peripheral_aggregate", progress=0.0)
    asyncio.create_task(_run_peripheral_aggregate_job(payload))


async def start_fund_flow_aggregate_job(payload: SyncFundFlowAggregateRequest) -> None:
    if _job_running("fund_flow_aggregate"):
        raise HTTPException(status_code=409, detail="Fund flow aggregate sync already running")

    dependent_jobs = [
        ("industry_fund_flow", "Industry fund flow sync already running"),
        ("concept_fund_flow", "Concept fund flow sync already running"),
        ("individual_fund_flow", "Individual fund flow sync already running"),
        ("hsgt_fund_flow", "HSGT fund flow sync already running"),
        ("margin_account", "Margin account sync already running"),
        ("big_deal_fund_flow", "Big deal fund flow sync already running"),
    ]

    for job_key, error_message in dependent_jobs:
        if _job_running(job_key):
            raise HTTPException(status_code=409, detail=error_message)

    monitor.start("fund_flow_aggregate", message="Syncing fund flow bundle")
    monitor.update("fund_flow_aggregate", progress=0.0)
    asyncio.create_task(_run_fund_flow_aggregate_job(payload))


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


async def start_concept_index_history_job(payload: SyncConceptIndexHistoryRequest) -> None:
    if _job_running("concept_index_history"):
        raise HTTPException(status_code=409, detail="Concept index history sync already running")
    monitor.start("concept_index_history", message="Syncing concept index history")
    monitor.update("concept_index_history", progress=0.0)
    asyncio.create_task(_run_concept_index_history_job(payload))


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


async def start_hsgt_fund_flow_job(payload: SyncHsgtFundFlowRequest) -> None:
    if _job_running("hsgt_fund_flow"):
        raise HTTPException(status_code=409, detail="HSGT fund flow sync already running")
    symbol = (payload.symbol or "北向资金").strip() if getattr(payload, "symbol", None) else "北向资金"
    monitor.start("hsgt_fund_flow", message=f"Syncing HSGT fund flow data ({symbol})")
    monitor.update("hsgt_fund_flow", progress=0.0)
    asyncio.create_task(_run_hsgt_fund_flow_job(payload))


async def start_margin_account_job(payload: SyncMarginAccountRequest) -> None:
    if _job_running("margin_account"):
        raise HTTPException(status_code=409, detail="Margin account sync already running")
    monitor.start("margin_account", message="Syncing margin account statistics")
    monitor.update("margin_account", progress=0.0)
    asyncio.create_task(_run_margin_account_job(payload))


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


async def start_global_flash_job(payload: SyncGlobalFlashRequest) -> None:
    if _job_running("global_flash"):
        raise HTTPException(status_code=409, detail="Global flash sync already running")
    monitor.start("global_flash", message="Syncing global finance flash data")
    monitor.update("global_flash", progress=0.0)
    asyncio.create_task(_run_global_flash_job(payload))


async def start_trade_calendar_job(payload: SyncTradeCalendarRequest) -> None:
    if _job_running("trade_calendar"):
        raise HTTPException(status_code=409, detail="Trade calendar sync already running")
    monitor.start("trade_calendar", message="Syncing A-share trading calendar")
    monitor.update("trade_calendar", progress=0.0)
    asyncio.create_task(_run_trade_calendar_job(payload))


async def start_global_flash_classification_job(payload: SyncGlobalFlashClassifyRequest) -> None:
    if _job_running("global_flash_classification"):
        raise HTTPException(status_code=409, detail="Global flash classification already running")
    monitor.start("global_flash_classification", message="Classifying global flash entries")
    monitor.update("global_flash_classification", progress=0.0)
    asyncio.create_task(_run_global_flash_classification_job(payload))


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


async def safe_start_trade_calendar_job(payload: SyncTradeCalendarRequest) -> None:
    try:
        await start_trade_calendar_job(payload)
    except HTTPException as exc:
        logger.info("Trade calendar sync skipped: %s", exc.detail)


async def safe_start_global_flash_classification_job(payload: SyncGlobalFlashClassifyRequest) -> None:
    try:
        await start_global_flash_classification_job(payload)
    except HTTPException as exc:
        logger.info("Global flash classification skipped: %s", exc.detail)



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


async def safe_start_realtime_index_job(payload: SyncRealtimeIndexRequest) -> None:
    try:
        await start_realtime_index_job(payload)
    except HTTPException as exc:
        logger.info("Realtime index sync skipped: %s", exc.detail)


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


async def safe_start_macro_leverage_job(payload: SyncMacroLeverageRequest) -> None:
    try:
        await start_macro_leverage_job(payload)
    except HTTPException as exc:
        logger.info("Macro leverage sync skipped: %s", exc.detail)


async def safe_start_social_financing_job(payload: SyncSocialFinancingRequest) -> None:
    try:
        await start_social_financing_job(payload)
    except HTTPException as exc:
        logger.info("Social financing sync skipped: %s", exc.detail)


async def safe_start_macro_cpi_job(payload: SyncMacroCpiRequest) -> None:
    try:
        await start_macro_cpi_job(payload)
    except HTTPException as exc:
        logger.info("CPI sync skipped: %s", exc.detail)


async def safe_start_macro_pmi_job(payload: SyncMacroPmiRequest) -> None:
    try:
        await start_macro_pmi_job(payload)
    except HTTPException as exc:
        logger.info("PMI sync skipped: %s", exc.detail)


async def safe_start_macro_m2_job(payload: SyncMacroM2Request) -> None:
    try:
        await start_macro_m2_job(payload)
    except HTTPException as exc:
        logger.info("M2 sync skipped: %s", exc.detail)


async def safe_start_macro_ppi_job(payload: SyncMacroPpiRequest) -> None:
    try:
        await start_macro_ppi_job(payload)
    except HTTPException as exc:
        logger.info("PPI sync skipped: %s", exc.detail)


async def safe_start_futures_realtime_job(payload: SyncFuturesRealtimeRequest) -> None:
    try:
        await start_futures_realtime_job(payload)
    except HTTPException as exc:
        logger.info("Futures realtime sync skipped: %s", exc.detail)


async def safe_start_fed_statement_job(payload: SyncFedStatementRequest) -> None:
    try:
        await start_fed_statement_job(payload)
    except HTTPException as exc:
        logger.info("Fed statements sync skipped: %s", exc.detail)


async def safe_start_peripheral_insight_job(payload: SyncPeripheralInsightRequest) -> None:
    try:
        await start_peripheral_insight_job(payload)
    except HTTPException as exc:
        logger.info("Peripheral insight generation skipped: %s", exc.detail)


async def safe_start_macro_aggregate_job(payload: SyncMacroAggregateRequest) -> None:
    try:
        await start_macro_aggregate_job(payload)
    except HTTPException as exc:
        logger.info("Macro aggregate sync skipped: %s", exc.detail)


async def safe_start_peripheral_aggregate_job(payload: SyncPeripheralAggregateRequest) -> None:
    try:
        await start_peripheral_aggregate_job(payload)
    except HTTPException as exc:
        logger.info("Peripheral aggregate sync skipped: %s", exc.detail)


async def safe_start_fund_flow_aggregate_job(payload: SyncFundFlowAggregateRequest) -> None:
    try:
        await start_fund_flow_aggregate_job(payload)
    except HTTPException as exc:
        logger.info("Fund flow aggregate sync skipped: %s", exc.detail)


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


async def safe_start_concept_index_history_job(payload: SyncConceptIndexHistoryRequest) -> None:
    try:
        await start_concept_index_history_job(payload)
    except HTTPException as exc:
        logger.info("Concept index history sync skipped: %s", exc.detail)


def schedule_peripheral_aggregate_job(config: RuntimeConfig) -> None:
    job_id = "peripheral_aggregate_daily"
    try:
        trigger_hour, trigger_minute = _parse_time_string(config.peripheral_aggregate_time)
    except HTTPException:
        trigger_hour, trigger_minute = 6, 0
    try:
        scheduler.remove_job(job_id)
    except Exception:  # pragma: no cover - defensive
        pass

    scheduler.add_job(
        lambda: _submit_scheduler_task(
            safe_start_peripheral_aggregate_job(SyncPeripheralAggregateRequest())
        ),
        CronTrigger(hour=trigger_hour, minute=trigger_minute),
        id=job_id,
        replace_existing=True,
    )


def _maybe_queue_global_flash_sync(trigger: str) -> None:
    local_now = datetime.now(tz=scheduler.timezone)
    try:
        trading_status = is_trading_day(local_now)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to determine trading day status: %s", exc)
        trading_status = None

    is_trading = bool(trading_status)
    if trading_status is None:
        logger.debug("Trading day status unknown for %s; defaulting to non-trading logic.", local_now.date())

    hour = local_now.hour
    minute = local_now.minute

    if trigger == "intraday":
        if not is_trading or not (8 <= hour < 20):
            logger.debug(
                "Skipping global flash intraday run at %s (trading=%s, hour=%s)",
                local_now.isoformat(),
                is_trading,
                hour,
            )
            return
    elif trigger == "intraday_close":
        if not is_trading or not (hour == 20 and minute == 0):
            logger.debug(
                "Skipping global flash 20:00 run at %s (trading=%s)",
                local_now.isoformat(),
                is_trading,
            )
            return
    else:  # hourly
        if is_trading and 8 <= hour <= 20:
            logger.debug(
                "Skipping hourly global flash run during trading session (%s)",
                local_now.isoformat(),
            )
            return

    if not _submit_scheduler_task(safe_start_global_flash_job(SyncGlobalFlashRequest())):
        logger.debug("Global flash job queueing skipped; scheduler loop unavailable.")


def schedule_global_flash_job(config: RuntimeConfig) -> None:
    for job_id in [
        "global_flash_intraday",
        "global_flash_intraday_close",
        "global_flash_hourly",
    ]:
        try:
            scheduler.remove_job(job_id)
        except Exception:  # pragma: no cover - defensive
            pass

    scheduler.add_job(
        lambda: _maybe_queue_global_flash_sync("intraday"),
        CronTrigger(day_of_week="mon-fri", hour="8-19", minute="*/10"),
        id="global_flash_intraday",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: _maybe_queue_global_flash_sync("intraday_close"),
        CronTrigger(day_of_week="mon-fri", hour="20", minute="0"),
        id="global_flash_intraday_close",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: _maybe_queue_global_flash_sync("hourly"),
        CronTrigger(minute=0),
        id="global_flash_hourly",
        replace_existing=True,
    )


def schedule_trade_calendar_job() -> None:
    job_id = "trade_calendar_daily"
    try:
        scheduler.remove_job(job_id)
    except Exception:  # pragma: no cover - defensive
        pass

    scheduler.add_job(
        lambda: _submit_scheduler_task(
            safe_start_trade_calendar_job(SyncTradeCalendarRequest())
        ),
        CronTrigger(hour=2, minute=30),
        id=job_id,
        replace_existing=True,
    )


def schedule_global_flash_classification_job(default_batch_size: int = 10) -> None:
    job_id = "global_flash_classification"
    try:
        scheduler.remove_job(job_id)
    except Exception:  # pragma: no cover - defensive
        pass

    scheduler.add_job(
        lambda: _submit_scheduler_task(
            safe_start_global_flash_classification_job(
                SyncGlobalFlashClassifyRequest(batch_size=default_batch_size)
            )
        ),
        CronTrigger(minute="*/10"),
        id=job_id,
        replace_existing=True,
    )


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


async def safe_start_hsgt_fund_flow_job(payload: SyncHsgtFundFlowRequest) -> None:
    try:
        await start_hsgt_fund_flow_job(payload)
    except HTTPException as exc:
        logger.info("HSGT fund flow sync skipped: %s", exc.detail)


async def safe_start_margin_account_job(payload: SyncMarginAccountRequest) -> None:
    try:
        await start_margin_account_job(payload)
    except HTTPException as exc:
        logger.info("Margin account sync skipped: %s", exc.detail)


async def start_market_activity_job(payload: SyncMarketActivityRequest) -> None:
    if _job_running("market_activity"):
        raise HTTPException(status_code=409, detail="Market activity sync already running")
    monitor.start("market_activity", message="Syncing market activity snapshot")
    monitor.update("market_activity", progress=0.0)
    asyncio.create_task(_run_market_activity_job(payload))


async def safe_start_market_activity_job(payload: SyncMarketActivityRequest) -> None:
    try:
        await start_market_activity_job(payload)
    except HTTPException as exc:
        logger.info("Market activity sync skipped: %s", exc.detail)


async def start_market_fund_flow_job(payload: SyncMarketFundFlowRequest) -> None:
    if _job_running("market_fund_flow"):
        raise HTTPException(status_code=409, detail="Market fund flow sync already running")
    monitor.start("market_fund_flow", message="Syncing market fund flow data")
    monitor.update("market_fund_flow", progress=0.0)
    asyncio.create_task(_run_market_fund_flow_job(payload))


async def safe_start_market_fund_flow_job(payload: SyncMarketFundFlowRequest) -> None:
    try:
        await start_market_fund_flow_job(payload)
    except HTTPException as exc:
        logger.info("Market fund flow sync skipped: %s", exc.detail)


async def start_macro_insight_job(payload: SyncMacroInsightRequest) -> None:
    if _job_running("macro_insight"):
        raise HTTPException(status_code=409, detail="Macro insight generation is already running")
    monitor.start("macro_insight", message="Generating macro insight summary")
    monitor.update("macro_insight", progress=0.0)
    asyncio.create_task(_run_macro_insight_job(payload))


async def safe_start_macro_insight_job(payload: SyncMacroInsightRequest) -> None:
    try:
        await start_macro_insight_job(payload)
    except HTTPException as exc:
        logger.info("Macro insight generation skipped: %s", exc.detail)


async def start_concept_insight_job(payload: SyncConceptInsightRequest) -> None:
    if _job_running("concept_insight"):
        raise HTTPException(status_code=409, detail="Concept insight generation is already running")
    monitor.start("concept_insight", message="Generating concept insight summary")
    monitor.update("concept_insight", progress=0.0)
    asyncio.create_task(_run_concept_insight_job(payload))


async def safe_start_concept_insight_job(payload: SyncConceptInsightRequest) -> None:
    try:
        await start_concept_insight_job(payload)
    except HTTPException as exc:
        logger.info("Concept insight generation skipped: %s", exc.detail)


async def start_industry_insight_job(payload: SyncIndustryInsightRequest) -> None:
    if _job_running("industry_insight"):
        raise HTTPException(status_code=409, detail="Industry insight generation is already running")
    monitor.start("industry_insight", message="Generating industry insight summary")
    monitor.update("industry_insight", progress=0.0)
    asyncio.create_task(_run_industry_insight_job(payload))


async def safe_start_industry_insight_job(payload: SyncIndustryInsightRequest) -> None:
    try:
        await start_industry_insight_job(payload)
    except HTTPException as exc:
        logger.info("Industry insight generation skipped: %s", exc.detail)


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


async def safe_start_global_flash_job(payload: SyncGlobalFlashRequest) -> None:
    try:
        await start_global_flash_job(payload)
    except HTTPException as exc:
        logger.info("Global flash sync skipped: %s", exc.detail)


async def safe_start_market_overview_job(payload: SyncMarketOverviewRequest) -> None:
    try:
        await start_market_overview_job(payload)
    except HTTPException as exc:
        logger.info("Market overview job skipped: %s", exc.detail)


async def safe_start_market_insight_job(payload: SyncMarketInsightRequest) -> None:
    try:
        await start_market_insight_job(payload)
    except HTTPException as exc:
        logger.info("Market insight job skipped: %s", exc.detail)


async def safe_start_sector_insight_job(payload: SyncSectorInsightRequest) -> None:
    try:
        await start_sector_insight_job(payload)
    except HTTPException as exc:
        logger.info("Sector insight job skipped: %s", exc.detail)


async def safe_start_index_history_job(payload: SyncIndexHistoryRequest) -> None:
    try:
        await start_index_history_job(payload)
    except HTTPException as exc:
        logger.info("Index history sync skipped: %s", exc.detail)


@app.on_event("startup")
async def startup_event() -> None:
    global scheduler_loop
    scheduler_loop = asyncio.get_running_loop()
    if not scheduler.running:
        scheduler.start()
        config = load_runtime_config()
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_stock_basic_job(SyncStockBasicRequest(list_statuses=["L", "D"], market=None))
            ),
            CronTrigger(day=1, hour=0, minute=0),
            id="stock_basic_monthly",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_daily_trade_job(SyncDailyTradeRequest())
            ),
            CronTrigger(hour=17, minute=0),
            id="daily_trade_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_daily_indicator_job(SyncDailyIndicatorRequest())
            ),
            CronTrigger(hour=17, minute=5),
            id="daily_indicator_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_index_history_job(SyncIndexHistoryRequest())
            ),
            CronTrigger(hour=17, minute=0),
            id="index_history_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_daily_trade_metrics_job(SyncDailyTradeMetricsRequest())
            ),
            CronTrigger(hour=19, minute=0),
            id="daily_trade_metrics_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_fundamental_metrics_job(SyncFundamentalMetricsRequest())
            ),
            CronTrigger(hour=19, minute=10),
            id="fundamental_metrics_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_finance_breakfast_job(SyncFinanceBreakfastRequest())
            ),
            CronTrigger(hour=7, minute=0),
            id="finance_breakfast_daily",
            replace_existing=True,
        )


        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_industry_fund_flow_job(SyncIndustryFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=25),
            id="industry_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_concept_fund_flow_job(SyncConceptFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=30),
            id="concept_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_individual_fund_flow_job(SyncIndividualFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=35),
            id="individual_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_hsgt_fund_flow_job(SyncHsgtFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=36),
            id="hsgt_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_margin_account_job(SyncMarginAccountRequest())
            ),
            CronTrigger(hour=19, minute=37),
            id="margin_account_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_market_fund_flow_job(SyncMarketFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=38),
            id="market_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_big_deal_fund_flow_job(SyncBigDealFundFlowRequest())
            ),
            CronTrigger(hour=19, minute=39),
            id="big_deal_fund_flow_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_performance_express_job(SyncPerformanceExpressRequest())
            ),
            CronTrigger(hour=19, minute=20),
            id="performance_express_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_performance_forecast_job(SyncPerformanceForecastRequest())
            ),
            CronTrigger(hour=19, minute=40),
            id="performance_forecast_daily",
            replace_existing=True,
        )
        schedule_peripheral_aggregate_job(config)
        schedule_global_flash_job(config)
        schedule_trade_calendar_job()
        schedule_global_flash_classification_job()
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_profit_forecast_job(SyncProfitForecastRequest())
            ),
            CronTrigger(hour=19, minute=45),
            id="profit_forecast_daily",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_cpi_job(SyncMacroCpiRequest())
            ),
            CronTrigger(day=9, hour=22, minute=0),
            id="macro_cpi_monthly_day9",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_cpi_job(SyncMacroCpiRequest())
            ),
            CronTrigger(day=10, hour=22, minute=0),
            id="macro_cpi_monthly_day10",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_pmi_job(SyncMacroPmiRequest())
            ),
            CronTrigger(day=9, hour=22, minute=0),
            id="macro_pmi_monthly_day9",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_pmi_job(SyncMacroPmiRequest())
            ),
            CronTrigger(day=10, hour=22, minute=0),
            id="macro_pmi_monthly_day10",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_m2_job(SyncMacroM2Request())
            ),
            CronTrigger(day=10, hour=17, minute=1),
            id="macro_m2_monthly_day10",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_m2_job(SyncMacroM2Request())
            ),
            CronTrigger(day=11, hour=17, minute=1),
            id="macro_m2_monthly_day11",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_m2_job(SyncMacroM2Request())
            ),
            CronTrigger(day=12, hour=17, minute=1),
            id="macro_m2_monthly_day12",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_ppi_job(SyncMacroPpiRequest())
            ),
            CronTrigger(day=9, hour=10, minute=0),
            id="macro_ppi_monthly_day9",
            replace_existing=True,
        )
        scheduler.add_job(
            lambda: _submit_scheduler_task(
                safe_start_macro_ppi_job(SyncMacroPpiRequest())
            ),
            CronTrigger(day=10, hour=10, minute=0),
            id="macro_ppi_monthly_day10",
            replace_existing=True,
        )
        _submit_scheduler_task(safe_start_trade_calendar_job(SyncTradeCalendarRequest()))
        _submit_scheduler_task(
            safe_start_global_flash_classification_job(SyncGlobalFlashClassifyRequest())
        )
        _maybe_queue_global_flash_sync("hourly")


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
    concept: Optional[str] = Query(None, description="Filter by concept name"),
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

    def _normalize_symbol(value: Optional[object]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip().upper()
        if not text:
            return None
        if "." in text:
            text = text.split(".", 1)[0]
        if len(text) > 2 and text[:2] in {"SH", "SZ", "BJ"}:
            text = text[2:]
        return text or None

    concept_filter = concept.strip() if concept else None
    concept_symbol_filter: Optional[Set[str]] = None
    if concept_filter:
        settings = load_settings()
        concept_dao = ConceptConstituentDAO(settings.postgres)
        entries = concept_dao.list_entries(concept_filter)
        concept_symbol_filter = set()
        for entry in entries:
            symbol = entry.get("symbol")
            normalized_symbol = _normalize_symbol(symbol)
            if normalized_symbol:
                concept_symbol_filter.add(normalized_symbol)

    def _matches_concept(payload: dict[str, object]) -> bool:
        if concept_symbol_filter is None:
            return True
        if not concept_symbol_filter:
            return False
        code = payload.get("code") or payload.get("symbol")
        normalized_code = _normalize_symbol(code)
        if not normalized_code:
            return False
        return normalized_code in concept_symbol_filter

    source_items = [item for item in result["items"] if _matches_concept(item)]

    available_industries = sorted(
        {
            item.get("industry")
            for item in source_items
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
        and not concept_filter
    )

    sort_field = None
    if sort_by:
        normalized_sort = sort_by.replace("_", "").lower()
        sort_field = SORTABLE_STOCK_FIELDS.get(normalized_sort)
    sort_direction = (sort_order or "desc").lower()
    if sort_direction not in {"asc", "desc"}:
        sort_direction = "desc"

    if effective_favorites_only or keyword_bypass or (keyword_only_search and filters_at_defaults):
        filtered_items = list(source_items)
    else:
        filtered_items = [item for item in source_items if _passes_filters(item)]

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


@app.get("/stocks/news", response_model=StockNewsListResponse)
def list_stock_news_api(
    code: str = Query(..., min_length=1),
    limit: int = Query(100, ge=1, le=200),
) -> StockNewsListResponse:
    records = list_stock_news(code, limit=limit)
    items = [StockNewsItem(**entry) for entry in records]
    return StockNewsListResponse(total=len(items), items=items)


@app.post("/stocks/news/sync", response_model=StockNewsSyncResponse)
def sync_stock_news_api(payload: StockNewsSyncRequest = Body(...)) -> StockNewsSyncResponse:
    result = sync_stock_news(payload.code)
    return StockNewsSyncResponse(**result)


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


@app.get("/stocks/{code}/notes", response_model=StockNoteListResponse)
def list_stock_notes_api(
    code: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> StockNoteListResponse:
    try:
        result = list_stock_notes(code, limit=limit, offset=offset)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    items = [StockNoteItem(**entry) for entry in result.get("items", [])]
    total = int(result.get("total", len(items)))
    return StockNoteListResponse(total=total, items=items)


@app.post("/stocks/{code}/notes", response_model=StockNoteItem)
def create_stock_note_api(code: str, payload: StockNoteCreateRequest = Body(...)) -> StockNoteItem:
    try:
        record = add_stock_note(code, payload.content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StockNoteItem(**record)


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
    limit: int = Query(500, ge=1, le=2000, description="Maximum number of entries to return."),
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


@app.get("/markets/realtime-indices", response_model=RealtimeIndexListResponse)
def list_realtime_indices_api(
    limit: int = Query(500, ge=1, le=1000, description="Maximum number of realtime indices to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> RealtimeIndexListResponse:
    result = list_realtime_indices(limit=limit, offset=offset)
    items = [
        RealtimeIndexRecord(
            code=entry.get("code"),
            name=entry.get("name"),
            latestPrice=entry.get("latest_price"),
            changeAmount=entry.get("change_amount"),
            changePercent=entry.get("change_percent"),
            prevClose=entry.get("prev_close"),
            openPrice=entry.get("open_price"),
            highPrice=entry.get("high_price"),
            lowPrice=entry.get("low_price"),
            volume=entry.get("volume"),
            turnover=entry.get("turnover"),
            updatedAt=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return RealtimeIndexListResponse(
        total=int(result.get("total", 0)),
        items=items,
        last_synced_at=result.get("lastSyncedAt") or result.get("last_synced_at") or result.get("updated_at"),
    )


@app.get("/markets/index-history", response_model=IndexHistoryListResponse)
def get_index_history_api(
    index_code: Optional[str] = Query(
        None,
        alias="indexCode",
        description="Index code with exchange suffix (e.g. 000001.SH). Defaults to the first configured index.",
    ),
    limit: int = Query(
        500,
        ge=50,
        le=2000,
        description="Maximum number of records to return (oldest to newest).",
    ),
    start_date: Optional[date] = Query(
        None,
        alias="startDate",
        description="Optional filter to include records on or after this date.",
    ),
    end_date: Optional[date] = Query(
        None,
        alias="endDate",
        description="Optional filter to include records on or before this date.",
    ),
) -> IndexHistoryListResponse:
    default_code = next(iter(INDEX_CONFIG.keys()))
    selected_code = (index_code or default_code).strip().upper()

    def _safe_numeric(value: Optional[object]) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            numeric = float(value)
        else:
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
        if math.isnan(numeric) or math.isinf(numeric):
            return None
        return numeric

    history_rows = list_index_history(
        index_code=selected_code,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
    )

    inferred_name: Optional[str] = None
    if history_rows:
        inferred_name = history_rows[-1].get("index_name") or history_rows[0].get("index_name")
    index_meta = INDEX_CONFIG.get(selected_code)
    if not inferred_name and index_meta:
        inferred_name = index_meta.get("name")

    items_payload: List[Dict[str, object]] = []
    for row in history_rows:
        item = {
            "indexCode": row.get("index_code") or selected_code,
            "indexName": row.get("index_name") or inferred_name,
            "tradeDate": row.get("trade_date"),
            "open": _safe_numeric(row.get("open")),
            "close": _safe_numeric(row.get("close")),
            "high": _safe_numeric(row.get("high")),
            "low": _safe_numeric(row.get("low")),
            "volume": _safe_numeric(row.get("volume")),
            "amount": _safe_numeric(row.get("amount")),
            "amplitude": _safe_numeric(row.get("amplitude")),
            "pctChange": _safe_numeric(row.get("pct_change")),
            "changeAmount": _safe_numeric(row.get("change_amount")),
            "turnover": _safe_numeric(row.get("turnover")),
        }
        items_payload.append(item)

    available_indices = [
        {
            "code": code,
            "name": meta.get("name", code),
            "symbol": meta.get("symbol", code),
        }
        for code, meta in INDEX_CONFIG.items()
    ]

    return IndexHistoryListResponse(
        indexCode=selected_code,
        indexName=inferred_name,
        items=[IndexHistoryRecord(**item) for item in items_payload],
        availableIndices=[IndexOption(**option) for option in available_indices],
    )


@app.get("/macro/dollar-index", response_model=DollarIndexListResponse)
def list_dollar_index_api(
    limit: int = Query(500, ge=1, le=2000, description="Maximum number of entries to return."),
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
    limit: int = Query(500, ge=1, le=2000, description="Maximum number of entries to return."),
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


@app.get("/macro/fed-statements", response_model=FedStatementListResponse)
def list_fed_statements_api(
    limit: int = Query(20, ge=1, le=100, description="Maximum number of statements to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> FedStatementListResponse:
    result = list_fed_statements(limit=limit, offset=offset)
    items = [
        FedStatementRecord(
            title=entry.get("title", ""),
            url=entry.get("url", ""),
            statement_date=entry.get("statement_date"),
            content=entry.get("content"),
            raw_text=entry.get("raw_text"),
            position=entry.get("position"),
            updated_at=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return FedStatementListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/leverage-ratio", response_model=MacroLeverageListResponse)
def list_macro_leverage_ratios_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of leverage entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> MacroLeverageListResponse:
    result = list_macro_leverage_ratios(limit=limit, offset=offset)
    items = []
    for entry in result.get("items", []):
        items.append(
            MacroLeverageRecord(
                period_date=entry.get("period_date"),
                period_label=entry.get("period_label"),
                household_ratio=entry.get("household_ratio"),
                non_financial_corporate_ratio=entry.get("non_financial_corporate_ratio"),
                government_ratio=entry.get("government_ratio"),
                central_government_ratio=entry.get("central_government_ratio"),
                local_government_ratio=entry.get("local_government_ratio"),
                real_economy_ratio=entry.get("real_economy_ratio"),
                financial_assets_ratio=entry.get("financial_assets_ratio"),
                financial_liabilities_ratio=entry.get("financial_liabilities_ratio"),
                updated_at=entry.get("updated_at"),
            )
        )
    return MacroLeverageListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/social-financing", response_model=SocialFinancingListResponse)
def list_social_financing_ratios_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of social financing rows to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> SocialFinancingListResponse:
    result = list_social_financing_ratios(limit=limit, offset=offset)
    items: List[SocialFinancingRecord] = []
    for entry in result.get("items", []):
        items.append(
            SocialFinancingRecord(
                periodDate=entry.get("period_date"),
                periodLabel=entry.get("period_label"),
                totalFinancing=entry.get("total_financing"),
                renminbiLoans=entry.get("renminbi_loans"),
                entrustedAndFxLoans=entry.get("entrusted_and_fx_loans"),
                entrustedLoans=entry.get("entrusted_loans"),
                trustLoans=entry.get("trust_loans"),
                undiscountedBankersAcceptance=entry.get("undiscounted_bankers_acceptance"),
                corporateBonds=entry.get("corporate_bonds"),
                domesticEquityFinancing=entry.get("domestic_equity_financing"),
                updatedAt=entry.get("updated_at"),
            )
        )
    return SocialFinancingListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/cpi", response_model=CpiListResponse)
def list_macro_cpi_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of CPI rows to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> CpiListResponse:
    result = list_macro_cpi(limit=limit, offset=offset)
    items: List[CpiRecord] = []
    for entry in result.get("items", []):
        items.append(
            CpiRecord(
                periodDate=entry.get("period_date"),
                periodLabel=entry.get("period_label"),
                actualValue=entry.get("actual_value"),
                forecastValue=entry.get("forecast_value"),
                previousValue=entry.get("previous_value"),
                updatedAt=entry.get("updated_at"),
            )
        )
    return CpiListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/pmi", response_model=PmiListResponse)
def list_macro_pmi_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of PMI rows to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> PmiListResponse:
    result = list_macro_pmi(limit=limit, offset=offset)
    items: List[PmiRecord] = []
    for entry in result.get("items", []):
        items.append(
            PmiRecord(
                series=entry.get("series") or "manufacturing",
                periodDate=entry.get("period_date"),
                periodLabel=entry.get("period_label"),
                actualValue=entry.get("actual_value"),
                forecastValue=entry.get("forecast_value"),
                previousValue=entry.get("previous_value"),
                updatedAt=entry.get("updated_at"),
            )
        )
    return PmiListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/m2", response_model=M2ListResponse)
def list_macro_m2_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of M2 rows to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> M2ListResponse:
    result = list_macro_m2(limit=limit, offset=offset)
    items: List[M2Record] = []
    for entry in result.get("items", []):
        items.append(
            M2Record(
                period_date=entry.get("period_date"),
                period_label=entry.get("period_label"),
                m0=entry.get("m0"),
                m0_yoy=entry.get("m0_yoy"),
                m0_mom=entry.get("m0_mom"),
                m1=entry.get("m1"),
                m1_yoy=entry.get("m1_yoy"),
                m1_mom=entry.get("m1_mom"),
                m2=entry.get("m2"),
                m2_yoy=entry.get("m2_yoy"),
                m2_mom=entry.get("m2_mom"),
                updated_at=entry.get("updated_at"),
            )
        )
    return M2ListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/ppi", response_model=PpiListResponse)
def list_macro_ppi_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of PPI rows to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> PpiListResponse:
    result = list_macro_ppi(limit=limit, offset=offset)
    items: List[PpiRecord] = []
    for entry in result.get("items", []):
        items.append(
            PpiRecord(
                periodDate=entry.get("period_date"),
                periodLabel=entry.get("period_label"),
                currentIndex=entry.get("current_index"),
                yoyChange=entry.get("yoy_change"),
                cumulativeIndex=entry.get("cumulative_index"),
                updatedAt=entry.get("updated_at"),
            )
        )
    return PpiListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/lpr", response_model=LprListResponse)
def list_macro_lpr_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of LPR rows to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> LprListResponse:
    result = list_macro_lpr(limit=limit, offset=offset)
    items: List[LprRecord] = []
    for entry in result.get("items", []):
        items.append(
            LprRecord(
                periodDate=entry.get("period_date"),
                periodLabel=entry.get("period_label"),
                rate1Y=entry.get("rate_1y"),
                rate5Y=entry.get("rate_5y"),
                updatedAt=entry.get("updated_at"),
            )
        )
    return LprListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/pbc-rate", response_model=LprListResponse)
def legacy_macro_pbc_rate_api(
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> LprListResponse:
    return list_macro_lpr_api(limit=limit, offset=offset)


@app.get("/macro/shibor", response_model=ShiborListResponse)
def list_macro_shibor_api(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of SHIBOR rows to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> ShiborListResponse:
    result = list_macro_shibor(limit=limit, offset=offset)
    items: List[ShiborRecord] = []
    for entry in result.get("items", []):
        items.append(
            ShiborRecord(
                periodDate=entry.get("period_date"),
                periodLabel=entry.get("period_label"),
                onRate=entry.get("on_rate"),
                rate1W=entry.get("rate_1w"),
                rate2W=entry.get("rate_2w"),
                rate1M=entry.get("rate_1m"),
                rate3M=entry.get("rate_3m"),
                rate6M=entry.get("rate_6m"),
                rate9M=entry.get("rate_9m"),
                rate1Y=entry.get("rate_1y"),
                updatedAt=entry.get("updated_at"),
            )
        )
    return ShiborListResponse(
        total=int(result.get("total", 0)),
        items=items,
        lastSyncedAt=result.get("lastSyncedAt") or result.get("updated_at"),
    )


@app.get("/macro/insight", response_model=MacroInsightResponse)
def get_macro_insight_snapshot() -> MacroInsightResponse:
    result = get_latest_macro_insight()
    if not result:
        raise HTTPException(status_code=404, detail="Macro insight has not been generated yet")

    datasets_payload = []
    for dataset in result.get("datasets", []) or []:
        fields = [MacroInsightDatasetField(**field) for field in dataset.get("fields", [])]
        datasets_payload.append(
            MacroInsightDataset(
                key=dataset.get("key"),
                titleKey=dataset.get("titleKey"),
                fields=fields,
                series=dataset.get("series") or [],
                latest=dataset.get("latest"),
                updatedAt=dataset.get("updatedAt"),
            )
        )

    return MacroInsightResponse(
        snapshotDate=result.get("snapshot_date"),
        generatedAt=result.get("generated_at"),
        summary=result.get("summary"),
        rawResponse=result.get("raw_response"),
        model=result.get("model"),
        datasets=datasets_payload,
        warnings=list(result.get("warnings", [])),
    )


@app.get("/macro/insight/history", response_model=MacroInsightHistoryResponse)
def list_macro_insight_history_api(
    limit: int = Query(6, ge=1, le=20, description="Number of historical macro insight snapshots to return."),
) -> MacroInsightHistoryResponse:
    records = list_macro_insight_history(limit=limit)
    items: List[MacroInsightHistoryItem] = []
    for record in records:
        items.append(
            MacroInsightHistoryItem(
                snapshotDate=record.get("snapshot_date"),
                generatedAt=record.get("generated_at"),
                summaryJson=record.get("summary_json"),
                rawResponse=record.get("raw_response"),
                model=record.get("model"),
            )
        )
    return MacroInsightHistoryResponse(items=items)


@app.get("/market/overview", response_model=MarketOverviewResponse)
def get_market_overview() -> MarketOverviewResponse:
    payload = _sanitize_for_json(build_market_overview_payload())
    return MarketOverviewResponse(**payload)


@app.post("/market/overview/reason")
def stream_market_overview_reasoning(payload: MarketOverviewReasonRequest) -> StreamingResponse:
    bias_map = {
        "bullish": "偏多",
        "neutral": "中性",
        "bearish": "偏空",
    }

    def format_summary(summary_payload: Any, raw_text: Optional[str]) -> List[str]:
        parsed_summary: Optional[Dict[str, Any]] = None
        if isinstance(summary_payload, dict):
            parsed_summary = summary_payload
        elif isinstance(summary_payload, str) and summary_payload.strip():
            try:
                parsed_summary = json.loads(summary_payload)
            except (TypeError, json.JSONDecodeError):
                parsed_summary = None
        elif raw_text and raw_text.strip():
            try:
                parsed_summary = json.loads(raw_text)
            except (TypeError, json.JSONDecodeError):
                parsed_summary = None

        lines: List[str] = []

        def _stringify_point(value: Any) -> str:
            if isinstance(value, dict):
                title = value.get("title") or value.get("name")
                detail = value.get("detail") or value.get("description") or value.get("value")
                if title and detail:
                    return f"{title} - {detail}"
                if detail:
                    return str(detail)
                if title:
                    return str(title)
                try:
                    return json.dumps(value, ensure_ascii=False)
                except TypeError:
                    return str(value)
            if isinstance(value, (list, tuple)):
                joined = "；".join(str(item) for item in value if item is not None)
                return joined or "--"
            return str(value)

        if parsed_summary:
            bias = parsed_summary.get("bias")
            confidence = parsed_summary.get("confidence")
            if bias or confidence is not None:
                confidence_display = ""
                if confidence is not None:
                    try:
                        confidence_value = float(confidence)
                        if 0 <= confidence_value <= 1.2:
                            confidence_display = f"{confidence_value * 100:.0f}%"
                        else:
                            confidence_display = f"{confidence_value:.0f}%"
                    except (TypeError, ValueError):
                        confidence_display = str(confidence)
                bias_label = bias_map.get(bias, str(bias or "--"))
                if confidence_display:
                    lines.append(f"【倾向】{bias_label} · 置信度 {confidence_display}")
                else:
                    lines.append(f"【倾向】{bias_label}")
                lines.append("")

            overview = parsed_summary.get("summary")
            if overview:
                lines.append("【总结】")
                if isinstance(overview, (list, tuple)):
                    for item in overview:
                        lines.append(str(item))
                else:
                    lines.extend(str(overview).splitlines())
                lines.append("")

            signals = parsed_summary.get("key_signals") or []
            if signals:
                lines.append("【关键信号】")
                for idx, item in enumerate(signals, start=1):
                    if isinstance(item, dict):
                        title = item.get("title") or item.get("name") or f"信号{idx}"
                        detail = item.get("detail") or item.get("description") or item.get("value") or ""
                        detail_text = detail.strip() if isinstance(detail, str) else str(detail)
                        if detail_text:
                            lines.append(f"{idx}. {title} - {detail_text}")
                        else:
                            lines.append(f"{idx}. {title}")
                    else:
                        lines.append(f"{idx}. {_stringify_point(item)}")
                lines.append("")

            suggestion = parsed_summary.get("position_suggestion")
            if suggestion:
                lines.append("【仓位建议】")
                if isinstance(suggestion, (list, tuple)):
                    for item in suggestion:
                        lines.append(str(item))
                else:
                    lines.extend(str(suggestion).splitlines())
                lines.append("")

            risks = parsed_summary.get("risks") or []
            if risks:
                lines.append("【风险提示】")
                for risk in risks:
                    lines.append(f"- {_stringify_point(risk)}")
                lines.append("")

        if not lines:
            fallback = raw_text or (summary_payload if isinstance(summary_payload, str) else "")
            fallback_text = str(fallback or "").strip()
            if fallback_text:
                lines = fallback_text.splitlines()
            else:
                lines = ["暂无推理输出。"]

        return lines

    def stream_generator():
        result = generate_market_overview_reasoning(run_llm=payload.run_llm)
        model_name = result.get("model")
        summary_payload = result.get("summary")
        raw_text = result.get("rawText")
        generated_at = result.get("generatedAt")

        header_lines: List[str] = []
        if model_name:
            header_lines.append(f"模型: {model_name}")
        if generated_at:
            header_lines.append(f"推理时间: {generated_at}")
        if header_lines:
            yield " · ".join(header_lines) + "\n\n"

        for line in format_summary(summary_payload, raw_text):
            yield line + "\n"

    return StreamingResponse(stream_generator(), media_type="text/plain; charset=utf-8")


@app.get("/journal/entries", response_model=List[InvestmentJournalEntryResponse])
def list_investment_journal_entries_api(
    start_date: Optional[date] = Query(
        None,
        alias="startDate",
        description="Optional start date (inclusive, YYYY-MM-DD). Defaults to 30 days ago.",
    ),
    end_date: Optional[date] = Query(
        None,
        alias="endDate",
        description="Optional end date (inclusive, YYYY-MM-DD). Defaults to today.",
    ),
) -> List[InvestmentJournalEntryResponse]:
    entries = list_investment_journal_entries(start_date=start_date, end_date=end_date)
    payload = [_journal_entry_to_payload(entry) for entry in entries if entry]
    return [entry for entry in payload if entry]


@app.get("/journal/entries/{entry_date}", response_model=InvestmentJournalEntryResponse)
def get_investment_journal_entry_api(entry_date: date = Path(..., description="Entry date in YYYY-MM-DD format.")) -> InvestmentJournalEntryResponse:
    entry = get_investment_journal_entry(entry_date)
    payload = _journal_entry_to_payload(entry)
    if payload is None:
        raise HTTPException(status_code=404, detail="Journal entry not found.")
    return payload


@app.put("/journal/entries/{entry_date}", response_model=InvestmentJournalEntryResponse)
def upsert_investment_journal_entry_api(
    entry_date: date = Path(..., description="Entry date in YYYY-MM-DD format."),
    payload: InvestmentJournalEntryPayload = Body(...),
) -> InvestmentJournalEntryResponse:
    review_html = _sanitize_rich_text(payload.review_html)
    plan_html = _sanitize_rich_text(payload.plan_html)
    entry = upsert_investment_journal_entry(
        entry_date,
        review_html=review_html,
        plan_html=plan_html,
    )
    return _journal_entry_to_payload(entry)


@app.get("/journal/stock-notes", response_model=StockNoteListResponse)
def list_recent_stock_notes_api(
    start_date: Optional[date] = Query(
        None,
        alias="startDate",
        description="Start date inclusive (YYYY-MM-DD). Defaults to 90 days ago.",
    ),
    end_date: Optional[date] = Query(
        None,
        alias="endDate",
        description="End date inclusive (YYYY-MM-DD). Defaults to today.",
    ),
    limit: int = Query(200, ge=1, le=500, description="Maximum number of notes to return."),
) -> StockNoteListResponse:
    records = list_recent_stock_notes(start_date=start_date, end_date=end_date, limit=limit)
    items = [StockNoteItem(**entry) for entry in records.get("items", [])]
    total = records.get("total", len(items)) or len(items)
    return StockNoteListResponse(total=int(total), items=items)


@app.get("/peripheral/insights/latest", response_model=PeripheralInsightResponse)
def get_latest_peripheral_insight_api() -> PeripheralInsightResponse:
    record = get_latest_peripheral_insight()
    if not record:
        return PeripheralInsightResponse(insight=None)

    payload = PeripheralInsightRecord(
        snapshotDate=record.get("snapshot_date"),
        generatedAt=record.get("generated_at"),
        metrics=record.get("metrics") or {},
        summary=record.get("summary"),
        rawResponse=record.get("raw_response"),
        model=record.get("model"),
        createdAt=record.get("created_at"),
        updatedAt=record.get("updated_at"),
    )
    return PeripheralInsightResponse(insight=payload)


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

def _refresh_concept_history_on_demand(concept: str, limit: int) -> bool:
    """Fetch concept index history from THS when cache misses."""
    concept_name = (concept or "").strip()
    if not concept_name:
        return False

    lookback_days = max(120, min(int(limit) * 2, 365))
    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)
        sync_concept_index_history(
            [concept_name],
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
        )
        return True
    except Exception as exc:  # pragma: no cover - best effort safeguard
        logger.warning("On-demand concept index sync failed for %s: %s", concept_name, exc)
        return False


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


@app.get("/market/concept-index-history", response_model=ConceptIndexHistoryResponse)
def get_concept_index_history_api(
    concept: str = Query(..., min_length=1, description="Concept name to fetch index history for."),
    limit: int = Query(90, ge=10, le=300, description="Number of trading days to return."),
) -> ConceptIndexHistoryResponse:
    try:
        result = list_concept_index_history(concept_name=concept, limit=limit)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to load concept index history for %s: %s", concept, exc)
        raise HTTPException(status_code=500, detail="Failed to load concept index history.") from exc

    rows = result.get("items", [])
    if not rows:
        refreshed = _refresh_concept_history_on_demand(concept, limit)
        if refreshed:
            try:
                result = list_concept_index_history(concept_name=concept, limit=limit)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to load concept index history after refresh for %s: %s", concept, exc)
            else:
                rows = result.get("items", [])

    bars = [ConceptIndexBar(**item) for item in rows if isinstance(item, dict)]
    total = int(result.get("total") or 0)
    return ConceptIndexHistoryResponse(concept=concept, total=total, rows=bars)


@app.get("/fund-flow/sector-hotlist", response_model=FundFlowSectorHotlistResponse)
def get_fund_flow_sector_hotlist() -> FundFlowSectorHotlistResponse:
    snapshot = build_sector_fund_flow_snapshot()
    symbols = [FundFlowHotlistSymbol(**item) for item in snapshot.get("symbols", [])]
    industries = [FundFlowHotlistEntry(**item) for item in snapshot.get("industries", [])]
    concepts = [FundFlowHotlistEntry(**item) for item in snapshot.get("concepts", [])]
    return FundFlowSectorHotlistResponse(
        generatedAt=snapshot.get("generatedAt"),
        symbols=symbols,
        industries=industries,
        concepts=concepts,
    )


@app.get("/industries/search", response_model=IndustrySearchResponse)
def search_industries_api(
    q: Optional[str] = Query(None, description="Optional keyword to filter industries."),
    limit: int = Query(20, ge=1, le=100),
) -> IndustrySearchResponse:
    items = [IndustrySearchItem(**entry) for entry in search_industries(q, limit=limit)]
    return IndustrySearchResponse(items=items)


@app.get("/industries/watchlist", response_model=IndustryWatchlistResponse)
def list_industry_watchlist_api() -> IndustryWatchlistResponse:
    items = [IndustryWatchEntry(**entry) for entry in list_industry_watchlist()]
    return IndustryWatchlistResponse(items=items)


@app.post("/industries/watchlist", response_model=IndustryWatchEntry)
def add_industry_watch(payload: IndustryWatchRequest = Body(...)) -> IndustryWatchEntry:
    result = set_industry_watch_state(payload.industry, watch=True)
    return IndustryWatchEntry(**result)


@app.delete("/industries/watchlist/{industry}", response_model=IndustryWatchEntry)
def remove_industry_watch(
    industry: str = Path(..., min_length=1),
    permanent: bool = Query(False, description="When true, delete the entry instead of toggling watch state."),
) -> IndustryWatchEntry:
    if permanent:
        result = delete_industry_watch_entry(industry)
    else:
        result = set_industry_watch_state(industry, watch=False)
    return IndustryWatchEntry(**result)


@app.get("/industries/status", response_model=IndustryStatusResponse)
def get_industry_status_api(industry: str = Query(..., min_length=1)) -> IndustryStatusResponse:
    data = get_industry_status(industry)
    return IndustryStatusResponse(**data)


@app.post("/industries/refresh-history", response_model=IndustryRefreshResponse)
def refresh_industry_history_api(payload: IndustryRefreshRequest = Body(...)) -> IndustryRefreshResponse:
    result = refresh_industry_history(
        payload.industry,
        lookback_days=payload.lookback_days or 180,
    )
    return IndustryRefreshResponse(**result)


@app.get("/market/industry-index-history", response_model=IndustryIndexHistoryResponse)
def get_industry_index_history_api(
    industry: str = Query(..., min_length=1),
    limit: int = Query(240, ge=30, le=600),
) -> IndustryIndexHistoryResponse:
    result = list_industry_index_history(industry, limit=limit)
    rows = [IndustryIndexBar(**row) for row in result.get("rows", [])]
    return IndustryIndexHistoryResponse(industry=industry, total=int(result.get("total", 0)), rows=rows)


@app.get("/industries/news", response_model=IndustryNewsListResponse)
def list_industry_news_api(
    industry: str = Query(..., min_length=1),
    lookback_hours: int = Query(48, ge=1, le=240, alias="lookbackHours"),
    limit: int = Query(30, ge=1, le=100),
) -> IndustryNewsListResponse:
    try:
        records = list_industry_news(industry, lookback_hours=lookback_hours, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    items = [IndustryNewsArticle(**entry) for entry in records]
    return IndustryNewsListResponse(items=items)


@app.get("/concepts/search", response_model=ConceptSearchResponse)
def search_concepts_api(
    q: Optional[str] = Query(None, description="Optional keyword to filter concepts."),
    limit: int = Query(20, ge=1, le=100),
) -> ConceptSearchResponse:
    items = [ConceptSearchItem(**entry) for entry in search_concepts(q, limit=limit)]
    return ConceptSearchResponse(items=items)


@app.get("/concepts/watchlist", response_model=ConceptWatchlistResponse)
def list_concept_watchlist_api() -> ConceptWatchlistResponse:
    items = [ConceptWatchEntry(**entry) for entry in list_concept_watchlist()]
    return ConceptWatchlistResponse(items=items)


@app.post("/concepts/watchlist", response_model=ConceptWatchEntry)
def add_concept_watch(payload: ConceptWatchRequest = Body(...)) -> ConceptWatchEntry:
    result = set_concept_watch_state(payload.concept, watch=True)
    return ConceptWatchEntry(**result)


@app.delete("/concepts/watchlist/{concept}", response_model=ConceptWatchEntry)
def remove_concept_watch(
    concept: str = Path(..., min_length=1),
    permanent: bool = Query(False, description="When true, delete the entry instead of toggling watch state."),
) -> ConceptWatchEntry:
    if permanent:
        result = delete_concept_watch_entry(concept)
    else:
        result = set_concept_watch_state(concept, watch=False)
    return ConceptWatchEntry(**result)


@app.get("/concepts/status", response_model=ConceptStatusResponse)
def get_concept_status_api(concept: str = Query(..., min_length=1)) -> ConceptStatusResponse:
    data = get_concept_status(concept)
    return ConceptStatusResponse(**data)


@app.post("/concepts/refresh-history", response_model=ConceptRefreshResponse)
def refresh_concept_history_api(payload: ConceptRefreshRequest = Body(...)) -> ConceptRefreshResponse:
    result = refresh_concept_history(
        payload.concept,
        lookback_days=payload.lookback_days or 180,
    )
    return ConceptRefreshResponse(**result)


@app.get("/concepts/constituents", response_model=ConceptConstituentResponse)
def get_concept_constituents_api(
    concept: str = Query(..., min_length=1),
    max_pages: Optional[int] = Query(None, ge=1, le=10, alias="maxPages"),
    refresh: bool = Query(False, description="Set true to fetch new THS data."),
) -> ConceptConstituentResponse:
    result = list_concept_constituents(concept, max_pages=max_pages, refresh=refresh)
    return ConceptConstituentResponse(**result)


@app.get("/concepts/volume-price-analysis/latest", response_model=ConceptVolumePriceRecord)
def get_concept_volume_price_latest(concept: str = Query(..., min_length=1)) -> ConceptVolumePriceRecord:
    record = get_latest_volume_price_reasoning(concept)
    if not record:
        raise HTTPException(status_code=404, detail="Volume/price reasoning not found for concept.")
    return ConceptVolumePriceRecord(**record)


@app.get("/concepts/volume-price-analysis/history", response_model=ConceptVolumePriceHistoryResponse)
def list_concept_volume_price_history(
    concept: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> ConceptVolumePriceHistoryResponse:
    result = list_volume_price_history(concept, limit=limit, offset=offset)
    items = [ConceptVolumePriceRecord(**entry) for entry in result.get("items", []) if entry]
    return ConceptVolumePriceHistoryResponse(total=int(result.get("total", 0)), items=items)


@app.get("/concepts/news", response_model=ConceptNewsListResponse)
def list_concept_news_api(
    concept: str = Query(..., min_length=1),
    lookback_hours: int = Query(48, ge=1, le=240, alias="lookbackHours"),
    limit: int = Query(40, ge=1, le=100),
) -> ConceptNewsListResponse:
    try:
        records = list_concept_news(concept, lookback_hours=lookback_hours, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    items = [IndustryNewsArticle(**entry) for entry in records]
    return ConceptNewsListResponse(items=items)


@app.post("/concepts/volume-price-analysis")
def stream_concept_volume_price_analysis(payload: ConceptVolumePriceRequest = Body(...)) -> StreamingResponse:
    def stream_generator():
        result = generate_concept_volume_price_reasoning(
            payload.concept,
            lookback_days=payload.lookback_days,
            run_llm=payload.run_llm,
        )

        header_parts = [
            f"概念: {result.get('concept')} ({result.get('conceptCode')})",
            f"样本: 最近{result.get('lookbackDays')}个交易日",
        ]
        stats = result.get("statistics") or {}
        change_percent = stats.get("changePercent")
        if change_percent is not None:
            header_parts.append(f"区间涨跌幅 {change_percent}%")
        avg_volume = stats.get("avgVolume")
        if avg_volume is not None:
            header_parts.append(f"平均成交量 {avg_volume}")
        model_name = result.get("model")
        if model_name:
            header_parts.append(f"模型 {model_name}")
        generated_at = result.get("generatedAt")
        if generated_at:
            header_parts.append(f"时间 {generated_at}")
        yield " · ".join(str(part) for part in header_parts if part) + "\n\n"

        for line in _format_volume_summary_lines(result.get("summary"), result.get("rawText")):
            yield line + "\n"

    return StreamingResponse(stream_generator(), media_type="text/plain; charset=utf-8")


@app.get("/stocks/volume-price-analysis/latest", response_model=StockVolumePriceRecord)
def get_stock_volume_price_latest(code: str = Query(..., min_length=1)) -> StockVolumePriceRecord:
    record = get_latest_stock_volume_price_reasoning(code)
    if not record:
        fallback_summary = {
            "wyckoffPhase": "未生成",
            "stageSummary": "暂未生成量价推理，可点击“生成推理”获取最新分析。",
            "volumeSignals": [],
            "priceSignals": [],
            "compositeIntent": "未知",
            "strategy": [],
            "risks": [],
            "checklist": [],
            "confidence": 0,
        }
        now = datetime.now(LOCAL_TZ)
        return StockVolumePriceRecord(
            id=0,
            code=code,
            name=None,
            lookback_days=90,
            summary=fallback_summary,
            raw_text=json.dumps(fallback_summary, ensure_ascii=False),
            model=None,
            generated_at=now,
        )
    return StockVolumePriceRecord(**record)


@app.get("/stocks/volume-price-analysis/history", response_model=StockVolumePriceHistoryResponse)
def list_stock_volume_price_history_api(
    code: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> StockVolumePriceHistoryResponse:
    result = list_stock_volume_price_history(code, limit=limit, offset=offset)
    items = [StockVolumePriceRecord(**entry) for entry in result.get("items", []) if entry]
    return StockVolumePriceHistoryResponse(total=int(result.get("total", 0)), items=items)


@app.post("/stocks/volume-price-analysis", response_model=StockVolumePriceRecord)
def run_stock_volume_price_analysis(payload: StockVolumePriceRequest = Body(...)) -> StockVolumePriceRecord:
    record = generate_stock_volume_price_reasoning(
        payload.code,
        lookback_days=payload.lookback_days,
        run_llm=payload.run_llm,
    )
    return StockVolumePriceRecord(**record)


@app.get("/stocks/integrated-analysis/latest", response_model=StockIntegratedAnalysisRecord)
def get_stock_integrated_analysis_latest(code: str = Query(..., min_length=1)) -> StockIntegratedAnalysisRecord:
    record = get_latest_stock_integrated_analysis(code)
    if not record:
        fallback_summary = {
            "overview": "尚未生成综合分析，可点击“生成分析”获取最新结论。",
            "keyFindings": [],
            "bullBearFactors": {"bull": [], "bear": []},
            "strategy": {"timeframe": "", "actions": []},
            "risks": [],
            "confidence": 0,
        }
        now = datetime.now(LOCAL_TZ)
        return StockIntegratedAnalysisRecord(
            id=0,
            code=code,
            name=None,
            news_days=INTEGRATED_NEWS_DAYS_DEFAULT,
            trade_days=INTEGRATED_TRADE_DAYS_DEFAULT,
            summary=fallback_summary,
            raw_text=json.dumps(fallback_summary, ensure_ascii=False),
            model=None,
            generated_at=now,
            context=None,
        )
    return StockIntegratedAnalysisRecord(**record)


@app.get("/stocks/integrated-analysis/history", response_model=StockIntegratedAnalysisHistoryResponse)
def list_stock_integrated_analysis_history_api(
    code: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> StockIntegratedAnalysisHistoryResponse:
    result = list_stock_integrated_analysis_history(code, limit=limit, offset=offset)
    items = [StockIntegratedAnalysisRecord(**entry) for entry in result.get("items", []) if entry]
    return StockIntegratedAnalysisHistoryResponse(total=int(result.get("total", 0)), items=items)


@app.post("/stocks/integrated-analysis", response_model=StockIntegratedAnalysisRecord)
def run_stock_integrated_analysis(payload: StockIntegratedAnalysisRequest = Body(...)) -> StockIntegratedAnalysisRecord:
    try:
        record = generate_stock_integrated_analysis(
            payload.code,
            news_days=payload.news_days,
            trade_days=payload.trade_days,
            run_llm=payload.run_llm,
            force=payload.force,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    return StockIntegratedAnalysisRecord(**record)

@app.get("/industries/volume-price-analysis/latest", response_model=IndustryVolumePriceRecord)
def get_industry_volume_price_latest(industry: str = Query(..., min_length=1)) -> IndustryVolumePriceRecord:
    record = get_latest_industry_volume_price_reasoning(industry)
    if not record:
        raise HTTPException(status_code=404, detail="Volume/price reasoning not found for industry.")
    return IndustryVolumePriceRecord(**record)


@app.get("/industries/volume-price-analysis/history", response_model=IndustryVolumePriceHistoryResponse)
def list_industry_volume_price_history_api(
    industry: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> IndustryVolumePriceHistoryResponse:
    result = list_industry_volume_price_history(industry, limit=limit, offset=offset)
    items = [IndustryVolumePriceRecord(**entry) for entry in result.get("items", []) if entry]
    return IndustryVolumePriceHistoryResponse(total=int(result.get("total", 0)), items=items)


@app.post("/industries/volume-price-analysis")
def stream_industry_volume_price_analysis(payload: IndustryVolumePriceRequest = Body(...)) -> StreamingResponse:
    def stream_generator():
        result = generate_industry_volume_price_reasoning(
            payload.industry,
            lookback_days=payload.lookback_days,
            run_llm=payload.run_llm,
        )

        header_parts = [
            f"行业: {result.get('industry')} ({result.get('industryCode')})",
            f"样本: 最近{result.get('lookbackDays')}个交易日",
        ]
        stats = result.get("statistics") or {}
        change_percent = stats.get("changePercent")
        if change_percent is not None:
            header_parts.append(f"区间涨跌幅 {change_percent}%")
        avg_volume = stats.get("avgVolume")
        if avg_volume is not None:
            header_parts.append(f"平均成交量 {avg_volume}")
        model_name = result.get("model")
        if model_name:
            header_parts.append(f"模型 {model_name}")
        generated_at = result.get("generatedAt")
        if generated_at:
            header_parts.append(f"时间 {generated_at}")
        yield " · ".join(str(part) for part in header_parts if part) + "\n\n"

        for line in _format_volume_summary_lines(result.get("summary"), result.get("rawText")):
            yield line + "\n"

    return StreamingResponse(stream_generator(), media_type="text/plain; charset=utf-8")


@app.get("/market/industry-insight", response_model=IndustryInsightResponse)
def get_industry_insight_api(
    lookback_hours: int = Query(
        48,
        ge=1,
        le=168,
        alias="lookbackHours",
        description="Hours to look back when building ad-hoc snapshot.",
    ),
    industry_limit: int = Query(
        5,
        ge=1,
        le=15,
        alias="industryLimit",
        description="Maximum number of industries to include in snapshot.",
    ),
) -> IndustryInsightResponse:
    summary_record: Optional[Dict[str, Any]] = None
    snapshot_dict: Optional[Dict[str, Any]] = None

    try:
        summary = get_latest_industry_insight()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to load latest industry insight: %s", exc)
        summary = None

    if summary:
        summary_record = summary
        existing_snapshot = summary.get("summary_snapshot")
        if isinstance(existing_snapshot, dict):
            snapshot_dict = existing_snapshot

    if snapshot_dict is None:
        try:
            snapshot_dict = build_industry_snapshot(
                lookback_hours=lookback_hours,
                industry_limit=industry_limit,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to build industry snapshot: %s", exc)
            snapshot_dict = None

    return _build_industry_insight_response(summary=summary_record, snapshot=snapshot_dict)


@app.get("/market/industry-insight/history", response_model=IndustryInsightHistoryResponse)
def get_industry_insight_history(
    limit: int = Query(5, ge=1, le=20, description="Number of historical industry insights to return."),
) -> IndustryInsightHistoryResponse:
    try:
        records = list_industry_insights(limit=limit)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to list industry insight history: %s", exc)
        records = []

    items: List[IndustryInsightSummary] = []
    for record in records:
        payload = _build_industry_insight_payload(record, include_snapshot=False)
        summary_item = payload.get("summary")
        if isinstance(summary_item, IndustryInsightSummary):
            items.append(summary_item)
    return IndustryInsightHistoryResponse(items=items)


@app.get("/market/concept-insight", response_model=ConceptInsightResponse)
def get_concept_insight_api(
    lookback_hours: int = Query(
        48,
        ge=1,
        le=168,
        alias="lookbackHours",
        description="Hours to look back when building ad-hoc snapshot.",
    ),
    concept_limit: int = Query(
        10,
        ge=1,
        le=15,
        alias="conceptLimit",
        description="Maximum number of concepts to include in snapshot.",
    ),
    refresh_index_history: bool = Query(
        False,
        alias="refreshIndexHistory",
        description="Whether to trigger index history refresh when building snapshot on the fly.",
    ),
) -> ConceptInsightResponse:
    summary_record: Optional[Dict[str, Any]] = None
    snapshot_dict: Optional[Dict[str, Any]] = None

    try:
        summary = get_latest_concept_insight()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to load latest concept insight: %s", exc)
        summary = None

    if summary:
        summary_record = summary
        existing_snapshot = summary.get("summary_snapshot")
        if isinstance(existing_snapshot, dict):
            snapshot_dict = existing_snapshot

    if snapshot_dict is None:
        try:
            snapshot_dict = build_concept_snapshot(
                lookback_hours=lookback_hours,
                concept_limit=concept_limit,
                refresh_index_history=refresh_index_history,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to build concept snapshot: %s", exc)
            snapshot_dict = None

    return _build_concept_insight_response(summary=summary_record, snapshot=snapshot_dict)


@app.get("/market/concept-insight/history", response_model=ConceptInsightHistoryResponse)
def get_concept_insight_history(
    limit: int = Query(5, ge=1, le=20, description="Number of historical concept insights to return."),
) -> ConceptInsightHistoryResponse:
    try:
        records = list_concept_insights(limit=limit)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to list concept insight history: %s", exc)
        records = []

    items: List[ConceptInsightSummary] = []
    for record in records:
        payload = _build_concept_insight_payload(record, include_snapshot=False)
        summary_item = payload.get("summary")
        if isinstance(summary_item, ConceptInsightSummary):
            items.append(summary_item)
    return ConceptInsightHistoryResponse(items=items)


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


@app.get("/fund-flow/hsgt", response_model=HsgtFundFlowListResponse)
def list_hsgt_fund_flow_entries(
    symbol: Optional[str] = Query(
        None,
        description="Optional symbol selector (default 北向资金).",
    ),
    start_date: Optional[str] = Query(
        None,
        alias="startDate",
        description="Optional start date filter (YYYY-MM-DD).",
    ),
    end_date: Optional[str] = Query(
        None,
        alias="endDate",
        description="Optional end date filter (YYYY-MM-DD).",
    ),
    limit: int = Query(200, ge=1, le=2000, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> HsgtFundFlowListResponse:
    result = list_hsgt_fund_flow(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )
    items = [
        HsgtFundFlowRecord(
            tradeDate=entry.get("trade_date"),
            symbol=entry.get("symbol"),
            netBuyAmount=entry.get("net_buy_amount"),
            buyAmount=entry.get("buy_amount"),
            sellAmount=entry.get("sell_amount"),
            netBuyAmountCumulative=entry.get("net_buy_amount_cumulative"),
            fundInflow=entry.get("fund_inflow"),
            balance=entry.get("balance"),
            marketValue=entry.get("market_value"),
            leadingStock=entry.get("leading_stock"),
            leadingStockChangePercent=entry.get("leading_stock_change_percent"),
            leadingStockCode=entry.get("leading_stock_code"),
            hs300Index=entry.get("hs300_index"),
            hs300ChangePercent=entry.get("hs300_change_percent"),
            updatedAt=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return HsgtFundFlowListResponse(
        total=int(result.get("total", 0)),
        items=items,
        last_synced_at=result.get("lastSyncedAt") or result.get("last_synced_at") or result.get("updated_at"),
        availableYears=[int(year) for year in result.get("available_years", [])],
    )


@app.get("/margin/account", response_model=MarginAccountListResponse)
def list_margin_account_entries(
    start_date: Optional[str] = Query(
        None,
        alias="startDate",
        description="Optional start date filter (YYYY-MM-DD).",
    ),
    end_date: Optional[str] = Query(
        None,
        alias="endDate",
        description="Optional end date filter (YYYY-MM-DD).",
    ),
    limit: int = Query(200, ge=1, le=2000, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> MarginAccountListResponse:
    result = list_margin_account_info(
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )
    items = [
        MarginAccountRecord(
            tradeDate=entry.get("trade_date"),
            financingBalance=entry.get("financing_balance"),
            securitiesLendingBalance=entry.get("securities_lending_balance"),
            financingPurchaseAmount=entry.get("financing_purchase_amount"),
            securitiesLendingSellAmount=entry.get("securities_lending_sell_amount"),
            securitiesCompanyCount=entry.get("securities_company_count"),
            businessDepartmentCount=entry.get("business_department_count"),
            individualInvestorCount=entry.get("individual_investor_count"),
            institutionalInvestorCount=entry.get("institutional_investor_count"),
            participatingInvestorCount=entry.get("participating_investor_count"),
            liabilityInvestorCount=entry.get("liability_investor_count"),
            collateralValue=entry.get("collateral_value"),
            averageCollateralRatio=entry.get("average_collateral_ratio"),
            updatedAt=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    return MarginAccountListResponse(
        total=int(result.get("total", 0)),
        items=items,
        last_synced_at=result.get("lastSyncedAt") or result.get("last_synced_at") or result.get("updated_at"),
        availableYears=[int(year) for year in result.get("available_years", []) if year is not None],
    )


@app.get("/market/activity", response_model=MarketActivityListResponse)
def get_market_activity_snapshot() -> MarketActivityListResponse:
    result = list_market_activity()
    items = [
        MarketActivityRecord(
            metric=entry.get("metric"),
            displayOrder=int(entry.get("display_order", idx)),
            valueText=entry.get("value_text"),
            valueNumber=entry.get("value_number"),
            updatedAt=entry.get("updated_at"),
        )
        for idx, entry in enumerate(result.get("items", []))
    ]
    dataset_timestamp = result.get("datasetTimestamp") or result.get("dataset_timestamp")
    return MarketActivityListResponse(items=items, datasetTimestamp=dataset_timestamp)


@app.get("/indicator-screenings", response_model=IndicatorScreeningListResponse)
def list_indicator_screenings_endpoint(
    indicators: Optional[List[str]] = Query(None, alias="indicators", description="Indicator code filters."),
    net_income_yoy_min: Optional[float] = Query(None, alias="netIncomeYoyMin", description="Minimum net income YoY."),
    net_income_qoq_min: Optional[float] = Query(None, alias="netIncomeQoqMin", description="Minimum net income QoQ."),
    pe_min: Optional[float] = Query(None, alias="peMin", description="Minimum PE ratio."),
    pe_max: Optional[float] = Query(None, alias="peMax", description="Maximum PE ratio."),
    has_big_deal_inflow: Optional[bool] = Query(
        None,
        alias="hasBigDealInflow",
        description="Require same-day big-deal inflow when true.",
    ),
    limit: int = Query(200, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> IndicatorScreeningListResponse:
    result = list_indicator_screenings(
        indicator_codes=indicators,
        limit=limit,
        offset=offset,
        net_income_yoy_min=net_income_yoy_min,
        net_income_qoq_min=net_income_qoq_min,
        pe_min=pe_min,
        pe_max=pe_max,
        has_big_deal_inflow=has_big_deal_inflow,
    )
    items = [
        IndicatorScreeningRecord(
            indicatorCode=entry.get("indicatorCode", "continuous_volume"),
            indicatorName=entry.get("indicatorName"),
            capturedAt=entry.get("capturedAt"),
            rank=entry.get("rank"),
            stockCode=entry.get("stockCode"),
            stockCodeFull=entry.get("stockCodeFull"),
            stockName=entry.get("stockName"),
            priceChangePercent=entry.get("priceChangePercent"),
            stageChangePercent=entry.get("stageChangePercent"),
            lastPrice=entry.get("lastPrice"),
            volumeShares=entry.get("volumeShares"),
            volumeText=entry.get("volumeText"),
            baselineVolumeShares=entry.get("baselineVolumeShares"),
            baselineVolumeText=entry.get("baselineVolumeText"),
            volumeDays=entry.get("volumeDays"),
            turnoverRate=entry.get("turnoverRate"),
            turnoverAmount=entry.get("turnoverAmount"),
            turnoverAmountText=entry.get("turnoverAmountText"),
            highPrice=entry.get("highPrice"),
            lowPrice=entry.get("lowPrice"),
            netIncomeYoyLatest=entry.get("netIncomeYoyLatest"),
            netIncomeQoqLatest=entry.get("netIncomeQoqLatest"),
            peRatio=entry.get("peRatio"),
            turnoverPercent=entry.get("turnoverPercent"),
            industry=entry.get("industry"),
            matchedIndicators=entry.get("matchedIndicators") or [],
            hasBigDealInflow=entry.get("hasBigDealInflow"),
            indicatorDetails=entry.get("indicatorDetails") or {},
        )
        for entry in result.get("items", [])
    ]
    return IndicatorScreeningListResponse(
        indicatorCode=result.get("indicatorCode", "continuous_volume"),
        indicatorCodes=result.get("indicatorCodes") or [result.get("indicatorCode", "continuous_volume")],
        indicatorName=result.get("indicatorName"),
        capturedAt=result.get("capturedAt"),
        total=int(result.get("total", 0)),
        items=items,
    )


@app.get("/indicator-screenings/continuous-volume", response_model=IndicatorScreeningListResponse)
def list_indicator_continuous_volume(
    limit: int = Query(200, ge=1, le=500, description="Maximum number of entries to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> IndicatorScreeningListResponse:
    return list_indicator_screenings_endpoint(indicators=[CONTINUOUS_VOLUME_CODE], limit=limit, offset=offset)


@app.post("/indicator-screenings/continuous-volume/sync", response_model=IndicatorSyncResponse)
def sync_indicator_continuous_volume_endpoint() -> IndicatorSyncResponse:
    result = sync_indicator_continuous_volume()
    return IndicatorSyncResponse(**result)


@app.post("/indicator-screenings/sync", response_model=IndicatorSyncBatchResponse)
def sync_indicator_screening_endpoint(
    indicator: Optional[str] = Query(None, description="Indicator code to refresh (default: all)."),
) -> IndicatorSyncBatchResponse:
    if indicator:
        result = sync_indicator_screening(indicator_code=indicator)
        payload = [IndicatorSyncResponse(**result)]
    else:
        batch = sync_all_indicator_screenings()
        payload = [IndicatorSyncResponse(**item) for item in batch]
    return IndicatorSyncBatchResponse(results=payload)


@app.post("/indicator-screenings/realtime-refresh", response_model=IndicatorRealtimeResponse)
def indicator_realtime_refresh_endpoint(payload: IndicatorRealtimeRequest) -> IndicatorRealtimeResponse:
    result = run_indicator_realtime_refresh(payload.codes, sync_all=payload.syncAll)
    return IndicatorRealtimeResponse(**result)


@app.get("/fund-flow/market", response_model=MarketFundFlowListResponse)
def list_market_fund_flow_entries(
    start_date: Optional[str] = Query(None, alias="startDate", description="Filter start date (YYYY-MM-DD)."),
    end_date: Optional[str] = Query(None, alias="endDate", description="Filter end date (YYYY-MM-DD)."),
    limit: int = Query(100, ge=1, le=500, description="Max number of records to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
) -> MarketFundFlowListResponse:
    result = list_market_fund_flow(
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )
    items = [
        MarketFundFlowRecord(
            tradeDate=entry.get("trade_date"),
            shanghaiClose=entry.get("shanghai_close"),
            shanghaiChangePercent=entry.get("shanghai_change_percent"),
            shenzhenClose=entry.get("shenzhen_close"),
            shenzhenChangePercent=entry.get("shenzhen_change_percent"),
            mainNetInflowAmount=entry.get("main_net_inflow_amount"),
            mainNetInflowRatio=entry.get("main_net_inflow_ratio"),
            hugeOrderNetInflowAmount=entry.get("huge_order_net_inflow_amount"),
            hugeOrderNetInflowRatio=entry.get("huge_order_net_inflow_ratio"),
            largeOrderNetInflowAmount=entry.get("large_order_net_inflow_amount"),
            largeOrderNetInflowRatio=entry.get("large_order_net_inflow_ratio"),
            mediumOrderNetInflowAmount=entry.get("medium_order_net_inflow_amount"),
            mediumOrderNetInflowRatio=entry.get("medium_order_net_inflow_ratio"),
            smallOrderNetInflowAmount=entry.get("small_order_net_inflow_amount"),
            smallOrderNetInflowRatio=entry.get("small_order_net_inflow_ratio"),
            updatedAt=entry.get("updated_at"),
        )
        for entry in result.get("items", [])
    ]
    total = int(result.get("total", result.get("count", len(items))) or 0)
    latest_trade_date = result.get("latestTradeDate") or result.get("latest_trade_date")
    last_synced_at = result.get("lastSyncedAt") or result.get("updated_at")
    available_years = [
        int(year)
        for year in result.get("availableYears", result.get("available_years", []))
        if year is not None
    ]
    return MarketFundFlowListResponse(
        total=total,
        items=items,
        latestTradeDate=latest_trade_date,
        lastSyncedAt=last_synced_at,
        availableYears=available_years,
    )


@app.get("/finance-breakfast", response_model=List[NewsArticleItem])
async def list_finance_breakfast_entries(
    limit: int = Query(50, ge=1, le=200, description="Maximum number of entries to return."),
) -> List[NewsArticleItem]:
    try:
        entries = list_news_articles(source="finance_breakfast", limit=limit)
    except Exception as exc:
        logger.warning("Finance breakfast query failed: %s", exc)
        entries = []
    if not entries:
        if not _submit_scheduler_task(safe_start_finance_breakfast_job(SyncFinanceBreakfastRequest())):
            logger.debug("Finance breakfast sync could not be scheduled (no running loop).")
    return [NewsArticleItem(**entry) for entry in entries]


@app.get("/news/articles", response_model=List[NewsArticleItem])
def list_news_articles_endpoint(
    source: Optional[str] = Query(None, description="Optional source filter (e.g. global_flash, finance_breakfast)."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of articles to return."),
    only_relevant: bool = Query(
        False,
        alias="onlyRelevant",
        description="Return only articles marked as relevant by the classifier.",
    ),
    stock: Optional[str] = Query(
        None,
        description="Comma-separated stock identifiers used to match tagged articles (code or name).",
    ),
    lookback_hours: Optional[int] = Query(
        None,
        alias="lookbackHours",
        ge=1,
        le=240,
        description="Limit the publishing window to the most recent N hours.",
    ),
) -> List[NewsArticleItem]:
    entries = list_news_articles(
        source=source,
        limit=limit,
        only_relevant=only_relevant,
        stock=stock,
        lookback_hours=lookback_hours,
    )
    return [NewsArticleItem(**entry) for entry in entries]


@app.get("/news/global-flash", response_model=List[NewsArticleItem])
async def list_global_flash_entries(
    limit: int = Query(100, ge=1, le=500, description="Maximum number of headlines to return."),
) -> List[NewsArticleItem]:
    try:
        entries = list_news_articles(source="global_flash", limit=limit)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Global flash query failed: %s", exc)
        entries = []

    if not entries:
        if not _submit_scheduler_task(safe_start_global_flash_job(SyncGlobalFlashRequest())):
            logger.debug("Global flash sync could not be scheduled (no running loop).")

    return [NewsArticleItem(**entry) for entry in entries]


@app.get("/news/market-insight", response_model=MarketInsightResponse)
def get_market_insight(
    lookback_hours: int = Query(24, ge=1, le=72, alias="lookbackHours", description="Hours to look back when no summary exists."),
    article_limit: int = Query(40, ge=5, le=50, alias="articleLimit", description="Maximum referenced headlines when no summary exists."),
) -> MarketInsightResponse:
    summary_payload: Optional[MarketInsightSummaryPayload] = None
    articles: List[MarketInsightArticleItem] = []
    summary_age_hours: Optional[float] = None
    should_trigger_refresh = False

    try:
        summary = get_latest_market_insight()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to load latest market insight: %s", exc)
        summary = None

    if summary:
        reference_dt = _parse_datetime(summary.get("window_end")) or _parse_datetime(summary.get("generated_at"))
        if reference_dt:
            delta = _local_now() - reference_dt
            summary_age_hours = max(delta.total_seconds() / 3600.0, 0.0)
        freshness_cutoff = max(lookback_hours, DEFAULT_MARKET_INSIGHT_LOOKBACK_HOURS) + MARKET_INSIGHT_STALE_GRACE_HOURS
        summary_is_fresh = summary_age_hours is None or summary_age_hours <= freshness_cutoff
        if summary_is_fresh:
            payload = _build_market_insight_payload(summary)
            summary_payload = payload["summary"]
            articles = payload["articles"]
        else:
            logger.info(
                "Latest market insight summary is stale (age %.1fh, cutoff %.1fh); returning live headlines",
                summary_age_hours or -1.0,
                freshness_cutoff,
            )
            summary = None

    if summary is None:
        should_trigger_refresh = True
        try:
            recent_articles = collect_recent_market_headlines(
                lookback_hours=lookback_hours,
                limit=article_limit,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to collect recent market headlines: %s", exc)
            recent_articles = []

        for entry in recent_articles:
            published_at = entry.get("published_at")
            articles.append(
                MarketInsightArticleItem(
                    articleId=entry.get("article_id"),
                    source=entry.get("source"),
                    title=entry.get("title"),
                    impactSummary=entry.get("impact_summary"),
                    impactAnalysis=entry.get("impact_analysis"),
                    impactConfidence=entry.get("impact_confidence"),
                    markets=list(entry.get("impact_markets") or []),
                    publishedAt=_localize_datetime(published_at) if isinstance(published_at, datetime) else None,
                    url=entry.get("url"),
                )
            )

    if should_trigger_refresh:
        try:
            payload = SyncMarketInsightRequest(lookback_hours=lookback_hours, article_limit=article_limit)
            if not _submit_scheduler_task(safe_start_market_insight_job(payload)):
                logger.debug("Market insight refresh could not be scheduled (no available event loop).")
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Unable to schedule market insight refresh: %s", exc)

    return MarketInsightResponse(summary=summary_payload, articles=articles)


@app.get("/news/market-insight/history", response_model=MarketInsightHistoryResponse)
def get_market_insight_history(
    limit: int = Query(6, ge=1, le=20, description="Number of historical market insight records to return."),
) -> MarketInsightHistoryResponse:
    try:
        records = list_market_insights(limit=limit)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to list market insight history: %s", exc)
        records = []

    items: List[MarketInsightHistoryItem] = []
    for record in records:
        summary_json = record.get("summary_json")
        if isinstance(summary_json, str):
            try:
                summary_json = json.loads(summary_json)
            except json.JSONDecodeError:
                summary_json = None
        items.append(
            MarketInsightHistoryItem(
                summaryId=record.get("summary_id"),
                generatedAt=_localize_datetime(record.get("generated_at")),
                windowStart=_localize_datetime(record.get("window_start")),
                windowEnd=_localize_datetime(record.get("window_end")),
                summaryJson=summary_json,
                modelUsed=record.get("model_used"),
            )
        )
    return MarketInsightHistoryResponse(items=items)


@app.get("/news/sector-insight", response_model=SectorInsightResponse)
def get_sector_insight(
    lookback_hours: int = Query(24, ge=1, le=72, alias="lookbackHours", description="Hours to look back when no cached summary exists."),
    article_limit: int = Query(40, ge=5, le=80, alias="articleLimit", description="Maximum articles to aggregate when rebuilding snapshot."),
) -> SectorInsightResponse:
    summary_record: Optional[Dict[str, Any]] = None
    snapshot_dict: Optional[Dict[str, Any]] = None

    try:
        summary = get_latest_sector_insight()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to load latest sector insight: %s", exc)
        summary = None

    if summary:
        summary_record = summary
        group_snapshot = summary.get("group_snapshot")
        if isinstance(group_snapshot, dict):
            snapshot_dict = group_snapshot
    else:
        try:
            headlines = collect_recent_sector_headlines(
                lookback_hours=lookback_hours,
                limit=article_limit,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to collect sector-impact headlines: %s", exc)
            headlines = []

        if headlines:
            snapshot_dict = build_sector_group_snapshot(
                headlines,
                lookback_hours=lookback_hours,
                reference_time=_local_now(),
            )

    return _build_sector_insight_response(summary=summary_record, snapshot=snapshot_dict)


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
        stats_map["realtime_index"] = RealtimeIndexDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect realtime_index stats: %s", exc)
        stats_map["realtime_index"] = {}
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
        stats_map["index_history"] = IndexHistoryDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect index_history stats: %s", exc)
        stats_map["index_history"] = {}
    try:
        stats_map["futures_realtime"] = FuturesRealtimeDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect futures_realtime stats: %s", exc)
        stats_map["futures_realtime"] = {}
    try:
        stats_map["fed_statements"] = FedStatementDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect fed_statements stats: %s", exc)
        stats_map["fed_statements"] = {}
    try:
        stats_map["peripheral_insight"] = PeripheralInsightDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect peripheral_insight stats: %s", exc)
        stats_map["peripheral_insight"] = {}
    try:
        stats_map["macro_insight"] = MacroInsightDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect macro_insight stats: %s", exc)
        stats_map["macro_insight"] = {}
    try:
        stats_map["leverage_ratio"] = MacroLeverageDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect leverage_ratio stats: %s", exc)
        stats_map["leverage_ratio"] = {}
    try:
        stats_map["social_financing"] = MacroSocialFinancingDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect social_financing stats: %s", exc)
        stats_map["social_financing"] = {}
    try:
        stats_map["cpi_monthly"] = MacroCpiDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect cpi_monthly stats: %s", exc)
        stats_map["cpi_monthly"] = {}
    try:
        stats_map["pmi_monthly"] = MacroPmiDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect pmi_monthly stats: %s", exc)
        stats_map["pmi_monthly"] = {}
    try:
        stats_map["m2_monthly"] = MacroM2DAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect m2_monthly stats: %s", exc)
        stats_map["m2_monthly"] = {}
    try:
        stats_map["ppi_monthly"] = MacroPpiDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect ppi_monthly stats: %s", exc)
        stats_map["ppi_monthly"] = {}
    try:
        stats_map["lpr_rate"] = MacroLprDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect lpr_rate stats: %s", exc)
        stats_map["lpr_rate"] = {}
    try:
        stats_map["shibor_rate"] = MacroShiborDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect shibor_rate stats: %s", exc)
        stats_map["shibor_rate"] = {}
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
        stats_map["concept_index_history"] = ConceptIndexHistoryDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect concept_index_history stats: %s", exc)
        stats_map["concept_index_history"] = {}
    try:
        stats_map["concept_directory"] = ConceptDirectoryDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect concept_directory stats: %s", exc)
        stats_map["concept_directory"] = {}
    try:
        stats_map["concept_insight"] = ConceptInsightDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect concept_insight stats: %s", exc)
        stats_map["concept_insight"] = {}
    else:
        stats = stats_map["concept_insight"]
        if isinstance(stats, dict) and "latest" in stats and "updated_at" not in stats:
            stats["updated_at"] = stats.get("latest")
    try:
        stats_map["industry_insight"] = IndustryInsightDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect industry_insight stats: %s", exc)
        stats_map["industry_insight"] = {}
    else:
        stats = stats_map["industry_insight"]
        if isinstance(stats, dict) and "latest" in stats and "updated_at" not in stats:
            stats["updated_at"] = stats.get("latest")
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
        stats_map["hsgt_fund_flow"] = HSGTFundFlowDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect hsgt_fund_flow stats: %s", exc)
        stats_map["hsgt_fund_flow"] = {}
    try:
        stats_map["margin_account"] = MarginAccountDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect margin_account stats: %s", exc)
        stats_map["margin_account"] = {}
    try:
        activity_dao = MarketActivityDAO(settings.postgres)
        activity_result = activity_dao.list_entries()
        stats_map["market_activity"] = {
            "count": len(activity_result.get("items", [])),
            "updated_at": activity_result.get("dataset_timestamp"),
        }
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect market_activity stats: %s", exc)
        stats_map["market_activity"] = {}
    try:
        fund_flow_dao = MarketFundFlowDAO(settings.postgres)
        entries = fund_flow_dao.list_entries(limit=1)
        items = entries.get("items", [])
        latest = items[0] if items else None
        stats_map["market_fund_flow"] = {
            "count": len(items),
            "updated_at": latest.get("trade_date") if latest else None,
        }
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect market_fund_flow stats: %s", exc)
        stats_map["market_fund_flow"] = {}
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
    news_dao = NewsArticleDAO(settings.postgres)
    try:
        stats_map["finance_breakfast"] = news_dao.stats(source="finance_breakfast")
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect finance_breakfast stats: %s", exc)
        stats_map["finance_breakfast"] = {}
    try:
        stats_map["global_flash"] = news_dao.stats(source="global_flash")
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect global_flash stats: %s", exc)
        stats_map["global_flash"] = {}
    try:
        stats_map["trade_calendar"] = TradeCalendarDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect trade_calendar stats: %s", exc)
        stats_map["trade_calendar"] = {}
    try:
        stats_map["market_overview"] = MarketOverviewInsightDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect market_overview stats: %s", exc)
        stats_map["market_overview"] = {}
    else:
        stats = stats_map["market_overview"]
        if isinstance(stats, dict) and "latest" in stats and "updated_at" not in stats:
            stats["updated_at"] = stats.get("latest")
    try:
        stats_map["market_insight"] = NewsMarketInsightDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect market_insight stats: %s", exc)
        stats_map["market_insight"] = {}
    else:
        stats = stats_map["market_insight"]
        if isinstance(stats, dict) and "latest" in stats and "updated_at" not in stats:
            stats["updated_at"] = stats.get("latest")
    try:
        stats_map["sector_insight"] = NewsSectorInsightDAO(settings.postgres).stats()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to collect sector_insight stats: %s", exc)
        stats_map["sector_insight"] = {}

    stats_map.setdefault("fund_flow_aggregate", {})

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
    existing = load_runtime_config()
    time_input = payload.peripheral_aggregate_time if payload.peripheral_aggregate_time else existing.peripheral_aggregate_time
    peripheral_time = _normalize_time_string(time_input or "06:00")
    frequency_value = payload.global_flash_frequency_minutes or existing.global_flash_frequency_minutes
    try:
        frequency_value = int(frequency_value)
    except (TypeError, ValueError):
        frequency_value = existing.global_flash_frequency_minutes
    if frequency_value <= 0:
        frequency_value = existing.global_flash_frequency_minutes or 180
    frequency_value = max(10, min(frequency_value, 1440))
    alias_field_provided = "concept_alias_map" in payload.__fields_set__
    if alias_field_provided:
        alias_map = normalize_concept_alias_map(payload.concept_alias_map or {})
    else:
        alias_map = existing.concept_alias_map
    volume_field_provided = "volume_surge_config" in payload.__fields_set__
    if volume_field_provided and payload.volume_surge_config is not None:
        surge_payload = payload.volume_surge_config
        volume_surge = VolumeSurgeConfig(
            min_volume_ratio=surge_payload.min_volume_ratio,
            breakout_threshold_percent=surge_payload.breakout_threshold_percent,
            daily_change_threshold_percent=surge_payload.daily_change_threshold_percent,
            max_range_percent=surge_payload.max_range_percent,
        )
    else:
        volume_surge = existing.volume_surge_config
    config = RuntimeConfig(
        include_st=payload.include_st,
        include_delisted=payload.include_delisted,
        daily_trade_window_days=payload.daily_trade_window_days,
        peripheral_aggregate_time=peripheral_time,
        global_flash_frequency_minutes=frequency_value,
        concept_alias_map=alias_map,
        volume_surge_config=volume_surge,
    )
    save_runtime_config(config)
    if scheduler.running:
        schedule_peripheral_aggregate_job(config)
        schedule_global_flash_job(config)
        schedule_trade_calendar_job()
        schedule_global_flash_classification_job()
    return _runtime_config_to_payload(config)


def _build_market_insight_payload(summary: Dict[str, object]) -> Dict[str, object]:
    summary_json = summary.get("summary_json")
    if isinstance(summary_json, str):
        try:
            summary_json = json.loads(summary_json)
        except json.JSONDecodeError:
            summary_json = {"text": summary_json}
    elif summary_json is not None and not isinstance(summary_json, dict):
        summary_json = {"text": str(summary_json)}

    referenced = summary.get("referenced_articles") or []
    articles: List[MarketInsightArticleItem] = []
    if isinstance(referenced, str):
        try:
            referenced = json.loads(referenced)
        except json.JSONDecodeError:
            referenced = []
    if isinstance(referenced, list):
        for entry in referenced:
            if not isinstance(entry, dict):
                continue
            markets = entry.get("markets") or entry.get("impact_markets") or []
            if isinstance(markets, str):
                markets = [part.strip() for part in markets.split(",") if part.strip()]
            published_at = entry.get("published_at") or entry.get("publishedAt")
            article_payload = {
                "articleId": entry.get("article_id"),
                "source": entry.get("source"),
                "title": entry.get("title"),
                "impactSummary": entry.get("impact_summary"),
                "impactAnalysis": entry.get("impact_analysis"),
                "impactConfidence": entry.get("impact_confidence"),
                "markets": markets if isinstance(markets, list) else [],
                "publishedAt": _parse_datetime(published_at),
                "url": entry.get("url"),
            }
            articles.append(MarketInsightArticleItem(**article_payload))

    generated_at = _localize_datetime(summary.get("generated_at"))
    window_start = _localize_datetime(summary.get("window_start"))
    window_end = _localize_datetime(summary.get("window_end"))
    elapsed_ms = summary.get("elapsed_ms")
    elapsed_seconds = None
    if isinstance(elapsed_ms, (int, float)):
        elapsed_seconds = float(elapsed_ms) / 1000.0

    summary_payload = MarketInsightSummaryPayload(
        summaryId=str(summary.get("summary_id")),
        generatedAt=generated_at or _local_now(),
        windowStart=window_start or generated_at or _local_now(),
        windowEnd=window_end or generated_at or _local_now(),
        headlineCount=int(summary.get("headline_count") or 0),
        summary=summary_json,
        rawResponse=summary.get("raw_response"),
        promptTokens=summary.get("prompt_tokens"),
        completionTokens=summary.get("completion_tokens"),
        totalTokens=summary.get("total_tokens"),
        elapsedSeconds=elapsed_seconds,
        modelUsed=summary.get("model_used"),
    )

    return {
        "summary": summary_payload,
        "articles": articles,
    }


def _journal_entry_to_payload(entry: Optional[Dict[str, object]]) -> Optional[InvestmentJournalEntryResponse]:
    if entry is None:
        return None
    return InvestmentJournalEntryResponse(
        entryDate=entry.get("entry_date"),
        reviewHtml=entry.get("review_html"),
        planHtml=entry.get("plan_html"),
        createdAt=_localize_datetime(entry.get("created_at")),
        updatedAt=_localize_datetime(entry.get("updated_at")),
    )


def _build_sector_insight_payload(summary: Dict[str, object]) -> Dict[str, object]:
    summary_json = summary.get("summary_json")
    if isinstance(summary_json, str):
        try:
            summary_json = json.loads(summary_json)
        except json.JSONDecodeError:
            summary_json = {"text": summary_json}
    elif summary_json is not None and not isinstance(summary_json, dict):
        summary_json = {"text": str(summary_json)}

    snapshot = summary.get("group_snapshot")
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except json.JSONDecodeError:
            snapshot = None

    snapshot_payload = None
    if isinstance(snapshot, dict):
        snapshot_payload = SectorInsightSnapshot(**snapshot)

    referenced = summary.get("referenced_articles")
    if isinstance(referenced, str):
        try:
            referenced = json.loads(referenced)
        except json.JSONDecodeError:
            referenced = []

    referenced_items: List[SectorInsightArticleAssignment] = []
    if isinstance(referenced, list):
        for item in referenced:
            if isinstance(item, dict):
                referenced_items.append(SectorInsightArticleAssignment(**item))

    generated_at = _localize_datetime(summary.get("generated_at"))
    window_start = _localize_datetime(summary.get("window_start"))
    window_end = _localize_datetime(summary.get("window_end"))
    elapsed_ms = summary.get("elapsed_ms")
    elapsed_seconds = None
    if isinstance(elapsed_ms, (int, float)):
        elapsed_seconds = float(elapsed_ms) / 1000.0

    summary_payload = SectorInsightSummaryPayload(
        summaryId=str(summary.get("summary_id")),
        generatedAt=generated_at or _local_now(),
        windowStart=window_start or generated_at or _local_now(),
        windowEnd=window_end or generated_at or _local_now(),
        headlineCount=int(summary.get("headline_count") or 0),
        groupCount=int(summary.get("group_count") or 0),
        summary=summary_json,
        groupSnapshot=snapshot_payload,
        rawResponse=summary.get("raw_response"),
        promptTokens=summary.get("prompt_tokens"),
        completionTokens=summary.get("completion_tokens"),
        totalTokens=summary.get("total_tokens"),
        elapsedSeconds=elapsed_seconds,
        modelUsed=summary.get("model_used"),
        referencedArticles=referenced_items,
    )

    return {
        "summary": summary_payload,
        "snapshot": snapshot_payload,
    }


def _build_sector_insight_response(
    *,
    summary: Optional[Dict[str, Any]],
    snapshot: Optional[Dict[str, Any]],
) -> SectorInsightResponse:
    summary_payload: Optional[SectorInsightSummaryPayload] = None
    snapshot_payload: Optional[SectorInsightSnapshot] = None

    if summary:
        payload = _build_sector_insight_payload(summary)
        summary_payload = payload.get("summary")  # type: ignore[assignment]
        snapshot_payload = payload.get("snapshot")  # type: ignore[assignment]

    if snapshot and not snapshot_payload:
        snapshot_payload = SectorInsightSnapshot(**snapshot)

    return SectorInsightResponse(summary=summary_payload, snapshot=snapshot_payload)



@app.post("/control/sync/market-overview")
async def control_sync_market_overview(payload: SyncMarketOverviewRequest) -> dict[str, str]:
    await start_market_overview_job(payload)
    return {"status": "started"}


@app.post("/control/sync/market-insight")
async def control_sync_market_insight(payload: SyncMarketInsightRequest) -> dict[str, str]:
    await start_market_insight_job(payload)
    return {"status": "started"}


@app.post("/control/sync/sector-insight")
async def control_sync_sector_insight(payload: SyncSectorInsightRequest) -> dict[str, str]:
    await start_sector_insight_job(payload)
    return {"status": "started"}


@app.post("/control/sync/stock-basic")
async def control_sync_stock_basic(payload: SyncStockBasicRequest) -> dict[str, str]:
    await start_stock_basic_job(payload)
    return {"status": "started"}


@app.post("/control/sync/concept-directory")
async def control_sync_concept_directory(payload: SyncConceptDirectoryRequest) -> dict[str, str]:
    if not payload.refresh:
        return {"status": "skipped"}
    await start_concept_directory_job()
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
async def control_sync_global_indices(
    payload: Optional[SyncGlobalIndexRequest] = Body(default=None),
) -> dict[str, str]:
    await start_global_index_job(payload or SyncGlobalIndexRequest())
    return {"status": "started"}


@app.post("/control/sync/dollar-index")
async def control_sync_dollar_index(payload: SyncDollarIndexRequest) -> dict[str, str]:
    await start_dollar_index_job(payload)
    return {"status": "started"}


@app.post("/control/sync/rmb-midpoint")
async def control_sync_rmb_midpoint(payload: SyncRmbMidpointRequest) -> dict[str, str]:
    await start_rmb_midpoint_job(payload)
    return {"status": "started"}


@app.post("/control/sync/leverage-ratio")
async def control_sync_leverage_ratio(payload: SyncMacroLeverageRequest) -> dict[str, str]:
    await start_macro_leverage_job(payload)
    return {"status": "started"}


@app.post("/control/sync/cpi")
async def control_sync_cpi(payload: SyncMacroCpiRequest) -> dict[str, str]:
    await start_macro_cpi_job(payload)
    return {"status": "started"}


@app.post("/control/sync/pmi")
async def control_sync_pmi(payload: SyncMacroPmiRequest) -> dict[str, str]:
    await start_macro_pmi_job(payload)
    return {"status": "started"}


@app.post("/control/sync/m2")
async def control_sync_m2(payload: SyncMacroM2Request) -> dict[str, str]:
    await start_macro_m2_job(payload)
    return {"status": "started"}


@app.post("/control/sync/ppi")
async def control_sync_ppi(payload: SyncMacroPpiRequest) -> dict[str, str]:
    await start_macro_ppi_job(payload)
    return {"status": "started"}


@app.post("/control/sync/lpr")
async def control_sync_lpr(payload: SyncMacroLprRequest) -> dict[str, str]:
    await start_macro_lpr_job(payload)
    return {"status": "started"}


@app.post("/control/sync/pbc-rate")
async def control_sync_pbc_rate(payload: SyncMacroLprRequest) -> dict[str, str]:
    await start_macro_lpr_job(payload)
    return {"status": "started"}


@app.post("/control/sync/shibor")
async def control_sync_shibor(payload: SyncMacroShiborRequest) -> dict[str, str]:
    await start_macro_shibor_job(payload)
    return {"status": "started"}


@app.post("/control/sync/social-financing")
async def control_sync_social_financing(payload: SyncSocialFinancingRequest) -> dict[str, str]:
    await start_social_financing_job(payload)
    return {"status": "started"}


@app.post("/control/sync/futures-realtime")
async def control_sync_futures_realtime(payload: SyncFuturesRealtimeRequest) -> dict[str, str]:
    await start_futures_realtime_job(payload)
    return {"status": "started"}


@app.post("/control/sync/fed-statements")
async def control_sync_fed_statements(payload: SyncFedStatementRequest) -> dict[str, str]:
    await start_fed_statement_job(payload)
    return {"status": "started"}


@app.post("/control/sync/macro-aggregate")
async def control_sync_macro_aggregate(payload: SyncMacroAggregateRequest) -> dict[str, str]:
    await start_macro_aggregate_job(payload)
    return {"status": "started"}


@app.post("/control/sync/macro-insight")
async def control_sync_macro_insight(payload: SyncMacroInsightRequest) -> dict[str, str]:
    await start_macro_insight_job(payload)
    return {"status": "started"}


@app.post("/control/sync/fund-flow-aggregate")
async def control_sync_fund_flow_aggregate(payload: SyncFundFlowAggregateRequest) -> dict[str, str]:
    await start_fund_flow_aggregate_job(payload)
    return {"status": "started"}


@app.post("/control/sync/peripheral-aggregate")
async def control_sync_peripheral_aggregate(payload: SyncPeripheralAggregateRequest) -> dict[str, str]:
    await start_peripheral_aggregate_job(payload)
    return {"status": "started"}


@app.post("/control/sync/peripheral-summary")
async def control_sync_peripheral_summary(payload: SyncPeripheralInsightRequest) -> dict[str, str]:
    await start_peripheral_insight_job(payload)
    return {"status": "started"}


@app.post("/control/sync/industry-fund-flow")
async def control_sync_industry_fund_flow(payload: SyncIndustryFundFlowRequest) -> dict[str, str]:
    await start_industry_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/concept-fund-flow")
async def control_sync_concept_fund_flow(payload: SyncConceptFundFlowRequest) -> dict[str, str]:
    await start_concept_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/concept-index-history")
async def control_sync_concept_index_history(payload: SyncConceptIndexHistoryRequest) -> dict[str, str]:
    await start_concept_index_history_job(payload)
    return {"status": "started"}


@app.post("/control/sync/concept-insight")
async def control_sync_concept_insight(payload: SyncConceptInsightRequest) -> dict[str, str]:
    await start_concept_insight_job(payload)
    return {"status": "started"}


@app.post("/control/sync/industry-insight")
async def control_sync_industry_insight(payload: SyncIndustryInsightRequest) -> dict[str, str]:
    await start_industry_insight_job(payload)
    return {"status": "started"}


@app.post("/control/sync/individual-fund-flow")
async def control_sync_individual_fund_flow(payload: SyncIndividualFundFlowRequest) -> dict[str, str]:
    await start_individual_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/big-deal-fund-flow")
async def control_sync_big_deal_fund_flow(payload: SyncBigDealFundFlowRequest) -> dict[str, str]:
    await start_big_deal_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/margin-account")
async def control_sync_margin_account(payload: SyncMarginAccountRequest) -> dict[str, str]:
    await start_margin_account_job(payload)
    return {"status": "started"}


@app.post("/control/sync/market-fund-flow")
async def control_sync_market_fund_flow(payload: SyncMarketFundFlowRequest) -> dict[str, str]:
    await start_market_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/hsgt-fund-flow")
async def control_sync_hsgt_fund_flow(payload: SyncHsgtFundFlowRequest) -> dict[str, str]:
    await start_hsgt_fund_flow_job(payload)
    return {"status": "started"}


@app.post("/control/sync/market-activity")
async def control_sync_market_activity(payload: SyncMarketActivityRequest) -> dict[str, str]:
    await start_market_activity_job(payload)
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


@app.post("/control/sync/global-flash")
async def control_sync_global_flash(payload: SyncGlobalFlashRequest) -> dict[str, str]:
    await start_global_flash_job(payload)
    return {"status": "started"}


@app.post("/control/sync/realtime-indices")
async def control_sync_realtime_indices(payload: SyncRealtimeIndexRequest) -> dict[str, str]:
    await start_realtime_index_job(payload)
    return {"status": "started"}


@app.post("/control/sync/index-history")
async def control_sync_index_history(payload: SyncIndexHistoryRequest) -> dict[str, str]:
    await start_index_history_job(payload)
    return {"status": "started"}


@app.post("/control/sync/global-flash-classification")
async def control_sync_global_flash_classification(payload: SyncGlobalFlashClassifyRequest) -> dict[str, str]:
    await start_global_flash_classification_job(payload)
    return {"status": "started"}


@app.post("/control/sync/trade-calendar")
async def control_sync_trade_calendar(payload: SyncTradeCalendarRequest) -> dict[str, str]:
    await start_trade_calendar_job(payload)
    return {"status": "started"}


@app.get("/control/debug/stats", include_in_schema=False)
def control_debug_stats() -> dict[str, object]:
    settings = load_settings()
    news_dao = NewsArticleDAO(settings.postgres)
    return {
        "stats": {
            "stock_basic": StockBasicDAO(settings.postgres).stats(),
            "daily_trade": DailyTradeDAO(settings.postgres).stats(),
            "daily_indicator": DailyIndicatorDAO(settings.postgres).stats(),
            "daily_trade_metrics": DailyTradeMetricsDAO(settings.postgres).stats(),
            "income_statement": IncomeStatementDAO(settings.postgres).stats(),
            "financial_indicator": FinancialIndicatorDAO(settings.postgres).stats(),
            "finance_breakfast": news_dao.stats(source="finance_breakfast"),
            "global_flash": news_dao.stats(source="global_flash"),
            "realtime_index": RealtimeIndexDAO(settings.postgres).stats(),
            "index_history": IndexHistoryDAO(settings.postgres).stats(),
            "trade_calendar": TradeCalendarDAO(settings.postgres).stats(),
            "fundamental_metrics": FundamentalMetricsDAO(settings.postgres).stats(),
            "stock_main_business": StockMainBusinessDAO(settings.postgres).stats(),
            "stock_main_composition": StockMainCompositionDAO(settings.postgres).stats(),
            "market_insight": NewsMarketInsightDAO(settings.postgres).stats(),
            "sector_insight": NewsSectorInsightDAO(settings.postgres).stats(),
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
        elapsed_seconds=float(result.get("elapsedSeconds", result.get("elapsed_seconds", 0.0))),
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
        elapsed_seconds=float(result.get("elapsedSeconds", result.get("elapsed_seconds", 0.0))),
    )


@app.post("/sync/finance-breakfast", response_model=SyncFinanceBreakfastResponse)
def trigger_finance_breakfast_sync(payload: SyncFinanceBreakfastRequest) -> SyncFinanceBreakfastResponse:
    del payload
    result = sync_finance_breakfast()
    return SyncFinanceBreakfastResponse(
        rows=int(result["rows"]),
        elapsedSeconds=float(result.get("elapsedSeconds", result.get("elapsed_seconds", 0.0))),
    )


@app.post("/sync/global-flash", response_model=SyncGlobalFlashResponse)
def trigger_global_flash_sync(payload: SyncGlobalFlashRequest) -> SyncGlobalFlashResponse:
    del payload
    result = sync_global_flash()
    return SyncGlobalFlashResponse(
        rows=int(result.get("rows", 0)),
        elapsedSeconds=float(result.get("elapsedSeconds", result.get("elapsed_seconds", 0.0))),
    )


@app.post("/sync/global-flash/classification", response_model=SyncGlobalFlashClassifyResponse)
def trigger_global_flash_classification_sync(payload: SyncGlobalFlashClassifyRequest) -> SyncGlobalFlashClassifyResponse:
    relevance_result = classify_relevance_batch(batch_size=payload.batch_size)
    impact_result = classify_impact_batch(batch_size=payload.batch_size)

    relevance_rows = int(relevance_result.get("rows", 0) or 0)
    impact_rows = int(impact_result.get("rows", 0) or 0)
    relevance_requested = int(relevance_result.get("requested", relevance_rows) or relevance_rows)
    impact_requested = int(impact_result.get("requested", impact_rows) or impact_rows)
    elapsed_seconds = float(relevance_result.get("elapsedSeconds", 0.0) or 0.0) + float(
        impact_result.get("elapsedSeconds", 0.0) or 0.0
    )

    return SyncGlobalFlashClassifyResponse(
        rows=relevance_rows + impact_rows,
        relevanceRows=relevance_rows,
        relevanceRequested=relevance_requested,
        impactRows=impact_rows,
        impactRequested=impact_requested,
        elapsedSeconds=elapsed_seconds,
    )


__all__ = ["app"]
def _build_industry_insight_payload(
    summary: Dict[str, Any],
    *,
    include_snapshot: bool = True,
) -> Dict[str, Any]:
    snapshot_obj = summary.get("summary_snapshot") if include_snapshot else None
    generated_dt = _parse_datetime(summary.get("generated_at"))
    window_start_dt = _parse_datetime(summary.get("window_start"))
    window_end_dt = _parse_datetime(summary.get("window_end"))
    summary_payload = IndustryInsightSummary(
        summaryId=str(summary.get("summary_id")),
        generatedAt=generated_dt.isoformat() if generated_dt else None,
        windowStart=window_start_dt.isoformat() if window_start_dt else None,
        windowEnd=window_end_dt.isoformat() if window_end_dt else None,
        industryCount=summary.get("industry_count"),
        summarySnapshot=snapshot_obj if isinstance(snapshot_obj, dict) else (snapshot_obj if include_snapshot else None),
        summaryJson=summary.get("summary_json"),
        rawResponse=summary.get("raw_response"),
        referencedIndustries=summary.get("referenced_industries"),
        referencedArticles=summary.get("referenced_articles"),
        promptTokens=summary.get("prompt_tokens"),
        completionTokens=summary.get("completion_tokens"),
        totalTokens=summary.get("total_tokens"),
        elapsedMs=summary.get("elapsed_ms"),
        modelUsed=summary.get("model_used"),
    )

    snapshot_payload: Optional[IndustrySnapshot] = None
    if include_snapshot and isinstance(snapshot_obj, dict):
        try:
            snapshot_payload = IndustrySnapshot(**snapshot_obj)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to parse stored industry snapshot for %s: %s", summary.get("summary_id"), exc)
            snapshot_payload = None

    return {"summary": summary_payload, "snapshot": snapshot_payload}


def _build_industry_insight_response(
    *,
    summary: Optional[Dict[str, Any]],
    snapshot: Optional[Dict[str, Any]],
) -> IndustryInsightResponse:
    summary_payload: Optional[IndustryInsightSummary] = None
    snapshot_payload: Optional[IndustrySnapshot] = None

    if summary:
        payload = _build_industry_insight_payload(summary, include_snapshot=True)
        summary_payload = payload.get("summary")  # type: ignore[assignment]
        snapshot_payload = payload.get("snapshot")  # type: ignore[assignment]

    if snapshot and not snapshot_payload:
        try:
            snapshot_payload = IndustrySnapshot(**snapshot)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to parse on-demand industry snapshot: %s", exc)
            snapshot_payload = None

    return IndustryInsightResponse(insight=summary_payload, snapshot=snapshot_payload)
