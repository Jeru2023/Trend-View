"""
Service layer for creating and retrieving investment journal entries.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from ..config.settings import load_settings
from ..dao import InvestmentJournalDAO


def upsert_investment_journal_entry(
    entry_date: date,
    *,
    review_html: Optional[str],
    plan_html: Optional[str],
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    settings = load_settings(settings_path)
    dao = InvestmentJournalDAO(settings.postgres)
    return dao.upsert_entry(entry_date, review_html, plan_html)


def get_investment_journal_entry(
    entry_date: date,
    *,
    settings_path: Optional[str] = None,
) -> Optional[Dict[str, object]]:
    settings = load_settings(settings_path)
    dao = InvestmentJournalDAO(settings.postgres)
    return dao.get_entry(entry_date)


def list_investment_journal_entries(
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    settings_path: Optional[str] = None,
) -> List[Dict[str, object]]:
    today = datetime.now().date()
    start = start_date or (today - timedelta(days=30))
    end = end_date or today
    if start > end:
        start, end = end, start
    settings = load_settings(settings_path)
    dao = InvestmentJournalDAO(settings.postgres)
    return dao.list_entries(start, end)


__all__ = [
    "upsert_investment_journal_entry",
    "get_investment_journal_entry",
    "list_investment_journal_entries",
]
