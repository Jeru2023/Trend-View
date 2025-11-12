"""Service helpers for stock notes."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Optional

from ..config.settings import load_settings
from ..dao import StockNoteDAO

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
    return dao.list_notes(
        normalized_code,
        limit=sanitized_limit,
        offset=sanitized_offset,
    )


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
    return dao.list_recent_notes(start, end, limit=limit)


__all__ = ["add_stock_note", "list_stock_notes", "list_recent_stock_notes"]
