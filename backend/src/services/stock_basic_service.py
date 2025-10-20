"""
Service layer for synchronising Tushare stock basic data into PostgreSQL.
"""

from __future__ import annotations

import logging
from typing import Dict, Sequence

from ..api_clients import fetch_stock_basic
from ..config.settings import AppSettings, load_settings
from ..dao import DailyTradeDAO, StockBasicDAO

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
    list_statuses: Sequence[str] = ("L", "D", "P"),
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

    logger.info(
        "Upserting %s stock_basic rows into %s.%s",
        len(dataframe),
        settings.postgres.schema,
        settings.postgres.stock_table,
    )

    affected = dao.upsert(dataframe)
    logger.info("Upsert completed, affected rows: %s", affected)
    return affected


def get_stock_overview(
    *,
    keyword: str | None = None,
    market: str | None = None,
    exchange: str | None = None,
    limit: int = 50,
    offset: int = 0,
    settings_path: str | None = None,
) -> dict[str, object]:
    """
    Retrieve stock fundamentals enriched with latest trading metrics.
    """
    settings = load_settings(settings_path)
    stock_dao = StockBasicDAO(settings.postgres)
    result = stock_dao.query_fundamentals(
        keyword=keyword,
        market=market,
        exchange=exchange,
        exclude_statuses=("D", "P"),
        exclude_name_prefixes=("ST", "*ST"),
        limit=limit,
        offset=offset,
    )

    codes = [item["code"] for item in result["items"]]
    daily_dao = DailyTradeDAO(settings.postgres)
    metrics: Dict[str, dict] = daily_dao.fetch_latest_metrics(codes)

    for item in result["items"]:
        metric = metrics.get(item["code"], {})
        item["last_price"] = metric.get("last_price")
        item["pct_change"] = metric.get("pct_change")
        item["volume"] = metric.get("volume")
        item["trade_date"] = metric.get("trade_date")

    return result


__all__ = [
    "sync_stock_basic",
    "get_stock_overview",
]

