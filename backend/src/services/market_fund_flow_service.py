"""Service layer for market fund flow history."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

import pandas as pd

from ..api_clients import MARKET_FUND_FLOW_COLUMN_MAP, fetch_market_fund_flow
from ..config.settings import load_settings
from ..dao import MarketFundFlowDAO

logger = logging.getLogger(__name__)

PERCENT_COLUMNS: tuple[str, ...] = (
    "shanghai_change_percent",
    "shenzhen_change_percent",
    "main_net_inflow_ratio",
    "huge_order_net_inflow_ratio",
    "large_order_net_inflow_ratio",
    "medium_order_net_inflow_ratio",
    "small_order_net_inflow_ratio",
)


def _parse_numeric(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not (isinstance(value, float) and pd.isna(value)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _prepare_market_fund_flow_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date

    for column in frame.columns:
        if column == "trade_date":
            continue
        frame[column] = frame[column].map(_parse_numeric)

    for column in PERCENT_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(
                lambda x: x / 100 if isinstance(x, (int, float)) else x
            )

    prepared = frame.loc[frame["trade_date"].notnull(), MARKET_FUND_FLOW_COLUMN_MAP.values()].copy()
    return prepared.sort_values("trade_date").reset_index(drop=True)


def sync_market_fund_flow(*, settings_path: Optional[str] = None) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MarketFundFlowDAO(settings.postgres)

    dataframe = fetch_market_fund_flow()
    if dataframe.empty:
        logger.warning("No market fund flow data returned from source.")
        return {"rows": 0}

    prepared = _prepare_market_fund_flow_frame(dataframe)
    if prepared.empty:
        logger.warning("Market fund flow frame empty after preparation.")
        return {"rows": 0}

    affected = dao.upsert(prepared)
    logger.info("Upserted %s market fund flow rows", affected)
    return {"rows": int(affected)}


def list_market_fund_flow(
    *,
    limit: int = 100,
    offset: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MarketFundFlowDAO(settings.postgres)

    parsed_start: Optional[date] = None
    parsed_end: Optional[date] = None

    if start_date:
        try:
            parsed_start = datetime.fromisoformat(start_date).date()
        except ValueError:
            parsed_start = None
    if end_date:
        try:
            parsed_end = datetime.fromisoformat(end_date).date()
        except ValueError:
            parsed_end = None

    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))

    entries = dao.list_entries(
        limit=limit,
        offset=offset,
        start_date=parsed_start,
        end_date=parsed_end,
    )
    items = entries.get("items", [])
    return {
        "items": items,
        "total": int(entries.get("total", len(items)) or 0),
        "count": len(items),
        "latest_trade_date": entries.get("latest_trade_date"),
        "updated_at": entries.get("updated_at"),
        "available_years": entries.get("available_years", []),
    }


__all__ = ["sync_market_fund_flow", "list_market_fund_flow", "_prepare_market_fund_flow_frame"]
