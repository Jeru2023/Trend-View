"""Service helpers for stock notes."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Optional

from ..config.settings import load_settings
from ..dao import StockBasicDAO, StockNoteDAO

MAX_NOTE_LENGTH = 1000


def _normalize_code(code: str) -> str:
    normalized = (code or "").strip().upper()
    if not normalized:
        raise ValueError("Stock code is required.")
    return normalized


def _normalize_content(content: str) -> str:
    if content is None:
        raise ValueError("Note content is required.")
    text = str(content).strip()
    if not text:
        raise ValueError("Note content is required.")
    if len(text) > MAX_NOTE_LENGTH:
        return text[:MAX_NOTE_LENGTH]
    return text


def add_stock_note(
    code: str,
    content: str,
    *,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    normalized_code = _normalize_code(code)
    normalized_content = _normalize_content(content)
    settings = load_settings(settings_path)
    dao = StockNoteDAO(settings.postgres)
    return dao.insert_note(normalized_code, normalized_content)


def list_stock_notes(
    code: str,
    *,
    limit: int = 50,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    normalized_code = _normalize_code(code)
    sanitized_limit = max(1, min(limit, 200))
    sanitized_offset = max(0, offset)
    settings = load_settings(settings_path)
    dao = StockNoteDAO(settings.postgres)
    stock_basic_dao = StockBasicDAO(settings.postgres)
    result = dao.list_notes(
        normalized_code,
        limit=sanitized_limit,
        offset=sanitized_offset,
    )
    _attach_stock_names(result, stock_basic_dao)
    return result


def list_recent_stock_notes(
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 200,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    today = datetime.now().date()
    start = start_date or (today - timedelta(days=90))
    end = end_date or today
    if start > end:
        start, end = end, start
    settings = load_settings(settings_path)
    dao = StockNoteDAO(settings.postgres)
    stock_basic_dao = StockBasicDAO(settings.postgres)
    result = dao.list_recent_notes(start, end, limit=limit)
    _attach_stock_names(result, stock_basic_dao)
    return result


def _attach_stock_names(result: Dict[str, object], stock_basic_dao: StockBasicDAO) -> None:
    if not result:
        return
    items = result.get("items")
    if not isinstance(items, list) or not items:
        return
    codes = sorted({(item.get("stock_code") or "").strip() for item in items if item.get("stock_code")})
    if not codes:
        return
    name_map = stock_basic_dao.fetch_names(codes)
    for item in items:
        code = (item.get("stock_code") or "").strip()
        if code and code in name_map:
            item["stock_name"] = name_map[code]


__all__ = ["add_stock_note", "list_stock_notes", "list_recent_stock_notes"]
