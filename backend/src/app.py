"""
FastAPI application exposing Trend View backend services and control panel APIs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date
from typing import Dict, List, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config.runtime_config import RuntimeConfig, load_runtime_config, save_runtime_config
from .config.settings import load_settings
from .dao import DailyTradeDAO, StockBasicDAO
from .services import (
    get_stock_overview,
    sync_daily_trade,
    sync_stock_basic,
)
from .state import monitor

scheduler = AsyncIOScheduler()
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

    class Config:
        allow_population_by_field_name = True


class StockListResponse(BaseModel):
    total: int
    items: List[StockItem]


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


class SyncStockBasicRequest(BaseModel):
    list_statuses: Optional[List[str]] = Field(default_factory=lambda: ["L", "D", "P"])
    market: Optional[str] = None


class SyncStockBasicResponse(BaseModel):
    rows: int


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


@app.on_event("startup")
async def startup_event() -> None:
    if not scheduler.running:
        scheduler.start()
        scheduler.add_job(
            lambda: asyncio.get_running_loop().create_task(
                safe_start_stock_basic_job(SyncStockBasicRequest(list_statuses=["L", "D", "P"], market=None))
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
) -> StockListResponse:
    """Return paginated stock fundamentals enriched with latest trading data."""
    result = get_stock_overview(
        keyword=keyword,
        market=market,
        exchange=exchange,
        limit=limit,
        offset=offset,
    )
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
        )
        for item in result["items"]
    ]
    return StockListResponse(total=result["total"], items=items)


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


@app.get("/control/debug/stats", include_in_schema=False)
def control_debug_stats() -> dict[str, object]:
    settings = load_settings()
    return {
        "stats": {
            "stock_basic": StockBasicDAO(settings.postgres).stats(),
            "daily_trade": DailyTradeDAO(settings.postgres).stats(),
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


__all__ = ["app"]










