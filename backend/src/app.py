"""
FastAPI application exposing Trend View backend services and control panel APIs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query
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
    StockBasicDAO,
)
from .services import (
    get_stock_overview,
    list_finance_breakfast,
    list_fundamental_metrics,
    sync_daily_indicator,
    sync_financial_indicators,
    sync_finance_breakfast,
    sync_income_statements,
    sync_daily_trade,
    sync_daily_trade_metrics,
    sync_fundamental_metrics,
    sync_stock_basic,
)
from .state import monitor

scheduler = AsyncIOScheduler(timezone=ZoneInfo("Asia/Shanghai"))
logger = logging.getLogger(__name__)


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


class StockListResponse(BaseModel):
    total: int
    items: List[StockItem]


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

    class Config:
        allow_population_by_field_name = True


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
        monitor.update(
            "fundamental_metrics",
            last_market=f"PER:{request.per_code}" if request.per_code is not None else "PER:AUTO",
        )
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


def _job_running(job: str) -> bool:
    return monitor.snapshot()[job]["status"] == "running"


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
    market: Optional[str] = Query(None, description="Filter by market"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    volume_spike_min: float = Query(
        1.8,
        alias="volumeSpikeMin",
        ge=0.0,
        description="Minimum volume spike ratio (latest volume / 10-day average).",
    ),
    pe_min: float = Query(
        0.0,
        alias="peMin",
        description="Minimum PE ratio filter.",
    ),
    roe_min: float = Query(
        3.0,
        alias="roeMin",
        description="Minimum ROE filter.",
    ),
    net_income_qoq_min: float = Query(
        0.0,
        alias="netIncomeQoqMin",
        description="Minimum net income QoQ ratio filter.",
    ),
    net_income_yoy_min: float = Query(
        0.1,
        alias="netIncomeYoyMin",
        description="Minimum net income YoY ratio filter.",
    ),
) -> StockListResponse:
    """Return paginated stock fundamentals enriched with latest trading data."""
    result = get_stock_overview(
        keyword=keyword,
        market=market,
        exchange=exchange,
        limit=None,
        offset=0,
    )

    def _passes_filters(payload: dict[str, object]) -> bool:
        def _gt(key: str, threshold: float) -> bool:
            value = payload.get(key)
            if value is None:
                return False
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return False
            return numeric > threshold

        return all(
            (
                _gt("volume_spike", volume_spike_min),
                _gt("pe_ratio", pe_min),
                _gt("roe", roe_min),
                _gt("net_income_qoq_latest", net_income_qoq_min),
                _gt("net_income_yoy_latest", net_income_yoy_min),
            )
        )

    filtered_items = [item for item in result["items"] if _passes_filters(item)]

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
        )
        for item in paged_items
    ]
    return StockListResponse(total=len(filtered_items), items=items)


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





















