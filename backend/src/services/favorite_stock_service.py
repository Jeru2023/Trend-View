"""
Service helpers for managing the stock favorites watchlist.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..config.settings import AppSettings, load_settings
from ..dao import FavoriteStockDAO, StockBasicDAO

FAVORITE_GROUP_NONE_SENTINEL = "__ungrouped__"


def _normalize_code(code: str) -> str:
    normalized = (code or "").strip().upper()
    if not normalized:
        raise ValueError("Stock code is required.")
    return normalized


def _normalize_group(group: Optional[str]) -> Optional[str]:
    if group is None:
        return None
    if group == FAVORITE_GROUP_NONE_SENTINEL:
        return FAVORITE_GROUP_NONE_SENTINEL
    trimmed = group.strip()
    if not trimmed:
        return None
    if trimmed == FAVORITE_GROUP_NONE_SENTINEL:
        return FAVORITE_GROUP_NONE_SENTINEL
    return trimmed


def _get_daos(settings: AppSettings) -> tuple[FavoriteStockDAO, StockBasicDAO]:
    return FavoriteStockDAO(settings.postgres), StockBasicDAO(settings.postgres)


def list_favorite_codes(settings_path: str | None = None) -> List[str]:
    """Return all favorite stock codes."""
    entries = list_favorite_entries(settings_path=settings_path)
    return [entry["code"] for entry in entries]


def list_favorite_entries(
    group: Optional[str] = None, *, settings_path: str | None = None
) -> List[Dict[str, object]]:
    """Return favorite entries with timestamps (optionally filtered by group)."""
    settings = load_settings(settings_path)
    favorites_dao, _ = _get_daos(settings)
    normalized_group = _normalize_group(group)
    if normalized_group == FAVORITE_GROUP_NONE_SENTINEL:
        entries = favorites_dao.list_entries(group="")
    else:
        entries = favorites_dao.list_entries(group=normalized_group)
    return [
        {
            "code": entry["code"],
            "group": entry.get("group"),
            "created_at": entry.get("created_at"),
            "updated_at": entry.get("updated_at"),
        }
        for entry in entries
    ]


def list_favorite_groups(settings_path: str | None = None) -> List[Dict[str, object]]:
    """Return distinct favorite groups and their counts."""
    settings = load_settings(settings_path)
    favorites_dao, _ = _get_daos(settings)
    return favorites_dao.list_groups()


def add_stock_to_favorites(
    code: str, *, group: Optional[str] = None, settings_path: str | None = None
) -> bool:
    """
    Mark the provided stock as favorite.

    Raises:
        ValueError: When the stock code is empty or not found in stock_basic.
    """
    normalized_code = _normalize_code(code)
    normalized_group = _normalize_group(group)
    if normalized_group == FAVORITE_GROUP_NONE_SENTINEL:
        normalized_group = None
    settings = load_settings(settings_path)
    favorites_dao, stock_dao = _get_daos(settings)
    if not stock_dao.exists(normalized_code):
        raise ValueError(f"Stock '{normalized_code}' not found.")
    favorites_dao.add(normalized_code, normalized_group)
    return True


def remove_stock_from_favorites(
    code: str, *, settings_path: str | None = None
) -> bool:
    """Remove the stock from favorites."""
    normalized = _normalize_code(code)
    settings = load_settings(settings_path)
    favorites_dao, _ = _get_daos(settings)
    return favorites_dao.remove(normalized) is not None


def is_stock_favorite(code: str, settings_path: str | None = None) -> bool:
    """Return True if the stock is currently marked as favorite."""
    normalized = _normalize_code(code)
    settings = load_settings(settings_path)
    favorites_dao, _ = _get_daos(settings)
    return favorites_dao.is_favorite(normalized)


def set_favorite_state(
    code: str,
    *,
    favorite: bool,
    group: Optional[str] = None,
    settings_path: str | None = None,
) -> Dict[str, object]:
    """Toggle favorite state and return summary metadata."""
    normalized_code = _normalize_code(code)
    normalized_group = _normalize_group(group)
    if normalized_group == FAVORITE_GROUP_NONE_SENTINEL:
        normalized_group = None
    settings = load_settings(settings_path)
    favorites_dao, stock_dao = _get_daos(settings)
    if not stock_dao.exists(normalized_code):
        raise ValueError(f"Stock '{normalized_code}' not found.")

    if favorite:
        operation_entry = favorites_dao.add(normalized_code, normalized_group)
    else:
        operation_entry = favorites_dao.remove(normalized_code)

    current_entry = favorites_dao.get_entry(normalized_code)
    current_group = (
        current_entry.get("group")
        if current_entry is not None
        else operation_entry.get("group") if operation_entry else None
    )

    return {
        "code": normalized_code,
        "isFavorite": current_entry is not None,
        "group": current_group,
        "total": favorites_dao.count(),
    }


def get_favorite_status(code: str, settings_path: str | None = None) -> Dict[str, object]:
    """Return the current favorite status plus total count."""
    normalized = _normalize_code(code)
    settings = load_settings(settings_path)
    favorites_dao, stock_dao = _get_daos(settings)
    if not stock_dao.exists(normalized):
        raise ValueError(f"Stock '{normalized}' not found.")
    entry = favorites_dao.get_entry(normalized)
    return {
        "code": normalized,
        "isFavorite": entry is not None,
        "group": entry.get("group") if entry else None,
        "total": favorites_dao.count(),
    }


__all__ = [
    "add_stock_to_favorites",
    "remove_stock_from_favorites",
    "list_favorite_codes",
    "list_favorite_entries",
    "list_favorite_groups",
    "is_stock_favorite",
    "set_favorite_state",
    "get_favorite_status",
]
