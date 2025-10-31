"""
Service layer for synchronising and querying Federal Reserve press statements.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

import pandas as pd

from ..api_clients import fetch_fed_press_releases
from ..config.settings import load_settings
from ..dao import FedStatementDAO

logger = logging.getLogger(__name__)

FED_STATEMENT_COLUMNS = ["url", "title", "statement_date", "content", "raw_text", "position"]


def _prepare_fed_statement_frame(entries: List[Dict[str, object]]) -> pd.DataFrame:
    if not entries:
        return pd.DataFrame(columns=FED_STATEMENT_COLUMNS)

    frame = pd.DataFrame(entries)
    frame["url"] = frame.get("url", "").astype(str).str.strip()
    frame["title"] = frame.get("title", "").astype(str).str.strip()
    frame["content"] = frame.get("content", "").fillna("").astype(str).str.strip()
    frame["raw_text"] = frame.get("rawText", "").fillna("").astype(str).str.strip()
    frame["position"] = pd.to_numeric(frame.get("position"), errors="coerce").fillna(0).astype(int)

    if "publishedDate" in frame.columns:
        frame["statement_date"] = pd.to_datetime(
            frame["publishedDate"], errors="coerce"
        ).dt.date
    elif "statement_date" in frame.columns:
        frame["statement_date"] = pd.to_datetime(
            frame["statement_date"], errors="coerce"
        ).dt.date
    else:
        frame["statement_date"] = pd.NaT

    prepared = frame.loc[:, FED_STATEMENT_COLUMNS].copy()
    prepared = prepared.dropna(subset=["url"])
    return prepared.reset_index(drop=True)


def sync_fed_statements(
    *,
    limit: int = 5,
    max_records: int = 200,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    started = time.perf_counter()
    entries = fetch_fed_press_releases(limit=limit)
    frame = _prepare_fed_statement_frame(entries)
    if frame.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Fed statement sync skipped: no entries returned.")
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "urls": [],
            "urlCount": 0,
            "pruned": 0,
        }

    settings = load_settings(settings_path)
    dao = FedStatementDAO(settings.postgres)

    affected = dao.upsert(frame)
    pruned = dao.prune(max_records)
    elapsed = time.perf_counter() - started

    urls = frame["url"].dropna().unique().tolist()
    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "urls": urls,
        "urlCount": len(urls),
        "pruned": pruned,
    }


def list_fed_statements(
    *,
    limit: int = 20,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    settings = load_settings(settings_path)
    dao = FedStatementDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset)
    stats = dao.stats()
    items: List[Dict[str, object]] = []
    for entry in result.get("items", []):
        items.append(
            {
                "title": entry.get("title"),
                "url": entry.get("url"),
                "statement_date": entry.get("statement_date"),
                "content": entry.get("content"),
                "raw_text": entry.get("raw_text"),
                "position": entry.get("position"),
                "updated_at": entry.get("updated_at"),
            }
        )

    return {
        "total": int(result.get("total", 0)),
        "items": items,
        "lastSyncedAt": stats.get("updated_at"),
    }


__all__ = ["sync_fed_statements", "list_fed_statements", "_prepare_fed_statement_frame"]
