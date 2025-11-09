"""Concept market utilities: search, watchlist, and history refresh."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

from ..config.settings import load_settings
from ..dao import ConceptIndexHistoryDAO, ConceptWatchlistDAO
from .concept_index_history_service import sync_concept_index_history
from .concept_constituent_service import (
    resolve_concept_label,
    search_concept_directory,
    list_concept_directory,
)


def search_concepts(query: Optional[str] = None, limit: int = 20) -> List[Dict[str, str]]:
    """Return concept search results (name + THS code)."""
    return search_concept_directory(query=query, limit=limit)


def list_all_concepts() -> List[Dict[str, str]]:
    """Return the full concept directory."""
    mapping = list_concept_directory()
    return [{"name": name, "code": code} for name, code in mapping.items()]


def list_concept_watchlist(*, settings_path: Optional[str] = None) -> List[Dict[str, object]]:
    """Return the monitored concept list along with metadata."""
    settings = load_settings(settings_path)
    watchlist_dao = ConceptWatchlistDAO(settings.postgres)
    history_dao = ConceptIndexHistoryDAO(settings.postgres)
    entries = watchlist_dao.list_entries()
    results: List[Dict[str, object]] = []
    for entry in entries:
        concept_name = entry["concept_name"]
        latest_trade = history_dao.get_latest_trade_date(concept_name)
        results.append(
            {
                "concept": concept_name,
                "conceptCode": entry["concept_code"],
                "isWatched": bool(entry.get("is_watched", True)),
                "lastSyncedAt": entry.get("last_synced_at"),
                "createdAt": entry.get("created_at"),
                "updatedAt": entry.get("updated_at"),
                "latestTradeDate": latest_trade,
            }
        )
    return results


def get_concept_status(concept: str, *, settings_path: Optional[str] = None) -> Dict[str, object]:
    """Return watchlist and history metadata for a concept."""
    resolved = resolve_concept_label(concept, settings_path=settings_path)
    settings = load_settings(settings_path)
    watchlist_dao = ConceptWatchlistDAO(settings.postgres)
    history_dao = ConceptIndexHistoryDAO(settings.postgres)
    entry = watchlist_dao.get_entry(resolved["name"])
    latest_trade = history_dao.get_latest_trade_date(resolved["name"])
    return {
        "concept": resolved["name"],
        "conceptCode": resolved["code"],
        "isWatched": bool(entry and entry.get("is_watched")),
        "lastSyncedAt": entry.get("last_synced_at") if entry else None,
        "latestTradeDate": latest_trade,
    }


def set_concept_watch_state(
    concept: str,
    *,
    watch: bool,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    """Enable or disable monitoring for the concept."""
    resolved = resolve_concept_label(concept, settings_path=settings_path)
    settings = load_settings(settings_path)
    watchlist_dao = ConceptWatchlistDAO(settings.postgres)
    history_dao = ConceptIndexHistoryDAO(settings.postgres)
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
        "concept": resolved["name"],
        "conceptCode": resolved["code"],
        "isWatched": entry.get("is_watched", watch),
        "lastSyncedAt": entry.get("last_synced_at") if entry else None,
        "latestTradeDate": latest_trade,
    }


def delete_concept_watch_entry(concept: str, *, settings_path: Optional[str] = None) -> Dict[str, object]:
    """Remove the concept from the watchlist entirely."""
    resolved = resolve_concept_label(concept, settings_path=settings_path)
    settings = load_settings(settings_path)
    watchlist_dao = ConceptWatchlistDAO(settings.postgres)
    history_dao = ConceptIndexHistoryDAO(settings.postgres)
    entry = watchlist_dao.delete_entry(resolved["name"])
    latest_trade = history_dao.get_latest_trade_date(resolved["name"])
    fallback = {
        "concept_name": resolved["name"],
        "concept_code": resolved["code"],
        "last_synced_at": None,
        "created_at": None,
        "updated_at": None,
        "is_watched": False,
    }
    payload = entry or fallback
    return {
        "concept": resolved["name"],
        "conceptCode": resolved["code"],
        "isWatched": False,
        "lastSyncedAt": payload.get("last_synced_at"),
        "createdAt": payload.get("created_at"),
        "updatedAt": payload.get("updated_at"),
        "latestTradeDate": latest_trade,
    }


def refresh_concept_history(
    concept: str,
    *,
    lookback_days: int = 180,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    """Trigger concept history sync (incremental when possible)."""
    resolved = resolve_concept_label(concept, settings_path=settings_path)
    settings = load_settings(settings_path)
    watchlist_dao = ConceptWatchlistDAO(settings.postgres)
    history_dao = ConceptIndexHistoryDAO(settings.postgres)

    entry = watchlist_dao.get_entry(resolved["name"])

    start_date: Optional[str] = None
    if entry and entry.get("last_synced_at"):
        start_date = entry["last_synced_at"].strftime("%Y%m%d")
    elif lookback_days:
        start_date = (date.today() - timedelta(days=int(lookback_days))).strftime("%Y%m%d")

    result = sync_concept_index_history(
        [resolved["name"]],
        start_date=start_date,
        settings_path=settings_path,
    )

    now = datetime.now(timezone.utc)
    upserted = watchlist_dao.upsert(resolved["name"], resolved["code"], last_synced_at=now)
    latest_trade = history_dao.get_latest_trade_date(resolved["name"])

    return {
        "concept": resolved["name"],
        "conceptCode": resolved["code"],
        "startDate": result.get("startDate"),
        "endDate": result.get("endDate"),
        "totalRows": result.get("totalRows"),
        "lastSyncedAt": now,
        "latestTradeDate": latest_trade,
        "isWatched": upserted.get("is_watched", True),
        "errors": result.get("errors", []),
    }


__all__ = [
    "search_concepts",
    "list_all_concepts",
    "list_concept_watchlist",
    "get_concept_status",
    "set_concept_watch_state",
    "delete_concept_watch_entry",
    "refresh_concept_history",
]
