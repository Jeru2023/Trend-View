"""Service helpers for syncing and querying stock-specific news articles."""

from __future__ import annotations

import logging
from datetime import datetime
from contextlib import suppress
from typing import Dict, List, Optional

import pandas as pd
from zoneinfo import ZoneInfo

from ..api_clients import fetch_stock_news
from ..config.settings import load_settings
from ..dao import StockNewsDAO

logger = logging.getLogger(__name__)
LOCAL_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_LIMIT = 100


def _normalize_code(code: str) -> str:
    if not code:
        raise ValueError("Stock code is required.")
    trimmed = code.strip().upper()
    if not trimmed:
        raise ValueError("Stock code is required.")
    if "." in trimmed:
        return trimmed
    if len(trimmed) == 6 and trimmed.isdigit():
        prefix = "SH" if trimmed.startswith(("5", "6")) else "SZ"
        return f"{trimmed}.{prefix}"
    return trimmed


def _normalize_symbol(code: str) -> str:
    normalized = _normalize_code(code)
    return normalized.split(".")[0]


def _as_naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo:
        return dt.astimezone(LOCAL_TZ).replace(tzinfo=None)
    return dt


def sync_stock_news(
    code: str,
    *,
    settings_path: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
) -> Dict[str, int]:
    """Fetch latest stock news entries via AkShare and persist them."""
    normalized_code = _normalize_code(code)
    symbol = _normalize_symbol(code)
    settings = load_settings(settings_path)
    dao = StockNewsDAO(settings.postgres)

    dataframe = fetch_stock_news(symbol)
    if dataframe is None or dataframe.empty:
        logger.info("Stock news sync for %s returned no rows.", symbol)
        return {"fetched": 0, "inserted": 0}

    latest_published = dao.latest_published_at(normalized_code)
    prepared = dataframe.head(max(1, min(limit, DEFAULT_LIMIT * 2)))
    records: List[Dict[str, object]] = []
    for row in prepared.to_dict(orient="records"):
        title = (row.get("title") or "").strip()
        if not title:
            continue
        published_at = _as_naive(_coerce_datetime(row.get("published_at")))
        if latest_published and published_at and published_at <= latest_published:
            continue
        sanitized_payload = {key: _sanitize_payload(value) for key, value in row.items()}
        record = {
            "stock_code": normalized_code,
            "keyword": row.get("keyword"),
            "title": title,
            "content": row.get("content"),
            "source": row.get("source"),
            "url": row.get("url"),
            "normalized_url": row.get("normalized_url"),
            "published_at": published_at,
            "raw_payload": sanitized_payload,
        }
        records.append(record)

    if not records:
        logger.info("Stock news sync for %s produced no usable rows.", symbol)
        return {"fetched": int(len(prepared)), "inserted": 0}

    inserted = dao.upsert_many(records)
    logger.info(
        "Stock news sync stored %s rows for %s (requested=%s).",
        inserted,
        normalized_code,
        len(records),
    )
    return {"fetched": len(prepared), "inserted": inserted}


def list_stock_news(
    code: str,
    *,
    limit: int = 50,
    settings_path: Optional[str] = None,
) -> List[Dict[str, object]]:
    normalized_code = _normalize_code(code)
    settings = load_settings(settings_path)
    dao = StockNewsDAO(settings.postgres)
    rows = dao.list_recent(normalized_code, limit=limit)
    results: List[Dict[str, object]] = []
    for row in rows:
        results.append(
            {
                "id": row.get("id"),
                "stock_code": row.get("stock_code"),
                "keyword": row.get("keyword"),
                "title": row.get("title"),
                "content": row.get("content"),
                "source": row.get("source"),
                "url": row.get("url"),
                "published_at": _as_iso(row.get("published_at")),
                "created_at": _as_iso(row.get("created_at")),
                "updated_at": _as_iso(row.get("updated_at")),
            }
        )
    return results


def _coerce_datetime(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=LOCAL_TZ)
    if isinstance(value, str):
        with suppress(ValueError):
            parsed = pd.to_datetime(value)
            if isinstance(parsed, pd.Timestamp):
                return parsed.to_pydatetime()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return None


def _as_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=LOCAL_TZ).isoformat()
    return value.astimezone(LOCAL_TZ).isoformat()


def _sanitize_payload(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, datetime):
        return _as_iso(value)
    return value


__all__ = ["sync_stock_news", "list_stock_news"]
