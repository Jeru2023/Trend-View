"""Industry market utilities: directory search, watchlist, and history refresh."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

from ..config.settings import load_settings
from ..dao import IndustryIndexHistoryDAO, IndustryWatchlistDAO
from .industry_directory_service import (
    list_industry_directory,
    resolve_industry_label,
    search_industry_directory,
)
from .industry_index_history_service import sync_industry_index_history


def search_industries(query: Optional[str] = None, limit: int = 20) -> List[Dict[str, str]]:
    return search_industry_directory(query=query, limit=limit)


def list_all_industries() -> List[Dict[str, str]]:
    return [{"name": name, "code": code} for name, code in list_industry_directory()]


def list_industry_watchlist(*, settings_path: Optional[str] = None) -> List[Dict[str, object]]:
    settings = load_settings(settings_path)
    watchlist_dao = IndustryWatchlistDAO(settings.postgres)
    history_dao = IndustryIndexHistoryDAO(settings.postgres)
    entries = watchlist_dao.list_entries()
    results: List[Dict[str, object]] = []
    for entry in entries:
        industry_name = entry["industry_name"]
        latest_trade = history_dao.get_latest_trade_date(industry_name)
        results.append(
            {
                "industry": industry_name,
                "industryCode": entry["industry_code"],
                "isWatched": bool(entry.get("is_watched", True)),
                "lastSyncedAt": entry.get("last_synced_at"),
                "createdAt": entry.get("created_at"),
                "updatedAt": entry.get("updated_at"),
                "latestTradeDate": latest_trade,
            }
        )
    return results


def get_industry_status(industry: str, *, settings_path: Optional[str] = None) -> Dict[str, object]:
    resolved = resolve_industry_label(industry, settings_path=settings_path)
    settings = load_settings(settings_path)
    watchlist_dao = IndustryWatchlistDAO(settings.postgres)
    history_dao = IndustryIndexHistoryDAO(settings.postgres)
    entry = watchlist_dao.get_entry(resolved["name"])
    latest_trade = history_dao.get_latest_trade_date(resolved["name"])
    return {
        "industry": resolved["name"],
        "industryCode": resolved["code"],
        "isWatched": bool(entry and entry.get("is_watched")),
        "lastSyncedAt": entry.get("last_synced_at") if entry else None,
        "latestTradeDate": latest_trade,
    }


def set_industry_watch_state(
    industry: str,
    *,
    watch: bool,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    resolved = resolve_industry_label(industry, settings_path=settings_path)
    settings = load_settings(settings_path)
    watchlist_dao = IndustryWatchlistDAO(settings.postgres)
    history_dao = IndustryIndexHistoryDAO(settings.postgres)
    if watch:
        entry = watchlist_dao.upsert(resolved["name"], resolved["code"])
    else:
        entry = watchlist_dao.set_watch_state(resolved["name"], watch)
        if entry is None:
            entry = watchlist_dao.upsert(resolved["name"], resolved["code"])
            if not watch:
                entry = watchlist_dao.set_watch_state(resolved["name"], False) or entry
    latest_trade = history_dao.get_latest_trade_date(resolved["name"])
    return {
        "industry": resolved["name"],
        "industryCode": resolved["code"],
        "isWatched": entry.get("is_watched", watch),
        "lastSyncedAt": entry.get("last_synced_at") if entry else None,
        "latestTradeDate": latest_trade,
    }


def delete_industry_watch_entry(industry: str, *, settings_path: Optional[str] = None) -> Dict[str, object]:
    resolved = resolve_industry_label(industry, settings_path=settings_path)
    settings = load_settings(settings_path)
    watchlist_dao = IndustryWatchlistDAO(settings.postgres)
    history_dao = IndustryIndexHistoryDAO(settings.postgres)
    entry = watchlist_dao.delete_entry(resolved["name"])
    latest_trade = history_dao.get_latest_trade_date(resolved["name"])
    fallback = {
        "industry_name": resolved["name"],
        "industry_code": resolved["code"],
        "last_synced_at": None,
        "created_at": None,
        "updated_at": None,
        "is_watched": False,
    }
    payload = entry or fallback
    return {
        "industry": resolved["name"],
        "industryCode": resolved["code"],
        "isWatched": False,
        "lastSyncedAt": payload.get("last_synced_at"),
        "createdAt": payload.get("created_at"),
        "updatedAt": payload.get("updated_at"),
        "latestTradeDate": latest_trade,
    }


def refresh_industry_history(
    industry: str,
    *,
    lookback_days: int = 180,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    resolved = resolve_industry_label(industry, settings_path=settings_path)
    settings = load_settings(settings_path)
    watchlist_dao = IndustryWatchlistDAO(settings.postgres)
    history_dao = IndustryIndexHistoryDAO(settings.postgres)

    entry = watchlist_dao.get_entry(resolved["name"])

    start_date: Optional[str] = None
    if entry and entry.get("last_synced_at"):
        start_date = entry["last_synced_at"].strftime("%Y%m%d")
    elif lookback_days:
        start_date = (date.today() - timedelta(days=int(lookback_days))).strftime("%Y%m%d")

    result = sync_industry_index_history(
        [resolved["name"]],
        start_date=start_date,
        settings_path=settings_path,
    )

    now = datetime.now(timezone.utc)
    upserted = watchlist_dao.upsert(resolved["name"], resolved["code"], last_synced_at=now)
    latest_trade = history_dao.get_latest_trade_date(resolved["name"])

    return {
        "industry": resolved["name"],
        "industryCode": resolved["code"],
        "startDate": result.get("startDate"),
        "endDate": result.get("endDate"),
        "totalRows": result.get("totalRows"),
        "lastSyncedAt": now,
        "latestTradeDate": latest_trade,
        "isWatched": upserted.get("is_watched", True),
        "errors": result.get("errors", []),
    }


__all__ = [
    "search_industries",
    "list_all_industries",
    "list_industry_watchlist",
    "get_industry_status",
    "set_industry_watch_state",
    "delete_industry_watch_entry",
    "refresh_industry_history",
]
