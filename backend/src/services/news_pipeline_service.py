"""Unified news ingestion and processing pipeline."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import pandas as pd
from psycopg2 import sql
from zoneinfo import ZoneInfo

from ..config.settings import load_settings
from ..dao import NewsArticleDAO, NewsInsightDAO

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def ingest_articles(
    source: str,
    items: Iterable[Dict[str, object]],
    *,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    """Persist a batch of raw articles for a given source."""

    items = list(items or [])
    if not items:
        return {"rows": 0}

    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    records: List[Dict[str, object]] = []
    now_local = _local_now()

    for item in items:
        published_at = normalize_datetime(item.get("published_at"))
        title = (item.get("title") or "").strip()
        summary = _clean_text(item.get("summary"))
        content = _clean_text(item.get("content"))
        source_item_id = (item.get("source_item_id") or "").strip() or None
        url = (item.get("url") or "").strip() or None

        if not title:
            logger.debug("Skipping article without title from source %s", source)
            continue

        article_id = generate_article_id(
            source=source,
            source_item_id=source_item_id or url or title,
            published_at=published_at,
        )

        record = {
            "article_id": article_id,
            "source": source,
            "source_item_id": source_item_id,
            "title": title,
            "summary": summary,
            "content": content,
            "content_type": item.get("content_type"),
            "published_at": published_at,
            "url": url,
            "language": item.get("language"),
            "content_fetched": bool(content),
            "content_fetched_at": now_local if content else None,
            "processing_status": "pending",
            "raw_payload": json.dumps(item, ensure_ascii=False) if item else None,
        }
        records.append(record)

    if not records:
        return {"rows": 0}

    dataframe = pd.DataFrame(records)
    affected = article_dao.upsert(dataframe)
    return {"rows": int(affected)}


def acquire_for_relevance(*, limit: int = 20, settings_path: Optional[str] = None) -> List[Dict[str, object]]:
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    return article_dao.acquire_for_relevance(limit=limit)


def acquire_for_impact(*, limit: int = 20, settings_path: Optional[str] = None) -> List[Dict[str, object]]:
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    return article_dao.acquire_for_impact(limit=limit)


def save_relevance_results(
    results: Iterable[Dict[str, object]],
    *,
    settings_path: Optional[str] = None,
) -> None:
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    insight_dao = NewsInsightDAO(settings.postgres)

    now_local = _local_now()
    records: List[Dict[str, object]] = []
    ready_for_impact: List[str] = []
    completed: List[str] = []
    errors: List[Dict[str, object]] = []

    for result in results or []:
        article_id = result.get("article_id")
        if not article_id:
            continue
        if result.get("error"):
            errors.append({"article_id": article_id, "error": result["error"]})
            continue

        is_relevant = bool(result.get("is_relevant"))
        confidence = result.get("confidence")
        reason = result.get("reason")

        records.append(
            {
                "article_id": article_id,
                "is_relevant": is_relevant,
                "relevance_confidence": confidence,
                "relevance_reason": reason,
                "relevance_checked_at": now_local,
            }
        )

        if is_relevant:
            ready_for_impact.append(article_id)
        else:
            completed.append(article_id)

    if records:
        dataframe = pd.DataFrame(records)
        insight_dao.upsert(dataframe)

    if ready_for_impact:
        article_dao.update_status(article_ids=ready_for_impact, status="ready_for_impact", last_error=None)
    if completed:
        article_dao.update_status(article_ids=completed, status="completed", last_error=None)

    for entry in errors:
        article_dao.update_status(
            article_ids=[entry["article_id"]],
            status="error",
            last_error=str(entry.get("error")),
        )


def save_impact_results(
    results: Iterable[Dict[str, object]],
    *,
    settings_path: Optional[str] = None,
) -> None:
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    insight_dao = NewsInsightDAO(settings.postgres)

    now_local = _local_now()
    records: List[Dict[str, object]] = []
    completed: List[str] = []
    errors: List[Dict[str, object]] = []

    for result in results or []:
        article_id = result.get("article_id")
        if not article_id:
            continue
        if result.get("error"):
            errors.append({"article_id": article_id, "error": result["error"]})
            continue

        impact_levels = _serialize_list(result.get("impact_levels"))
        impact_markets = _serialize_list(result.get("impact_markets"))
        impact_industries = _serialize_list(result.get("impact_industries"))
        impact_sectors = _serialize_list(result.get("impact_sectors"))
        impact_themes = _serialize_list(result.get("impact_themes"))
        impact_stocks = _serialize_list(result.get("impact_stocks"))

        records.append(
            {
                "article_id": article_id,
                "impact_levels": impact_levels,
                "impact_markets": impact_markets,
                "impact_industries": impact_industries,
                "impact_sectors": impact_sectors,
                "impact_themes": impact_themes,
                "impact_stocks": impact_stocks,
                "impact_summary": result.get("impact_summary"),
                "impact_analysis": result.get("impact_analysis"),
                "impact_confidence": result.get("impact_confidence"),
                "impact_checked_at": now_local,
                "extra_metadata": json.dumps(result.get("extra_metadata"), ensure_ascii=False)
                if result.get("extra_metadata")
                else None,
            }
        )
        completed.append(article_id)

    if records:
        dataframe = pd.DataFrame(records)
        insight_dao.upsert(dataframe)

    if completed:
        article_dao.update_status(article_ids=completed, status="completed", last_error=None)

    for entry in errors:
        article_dao.update_status(
            article_ids=[entry["article_id"]],
            status="error",
            last_error=str(entry.get("error")),
        )


def reset_in_progress_articles(*, settings_path: Optional[str] = None) -> None:
    """Reset articles stuck in in-progress states back to pending."""

    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    with article_dao.connect() as conn:
        article_dao.ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {schema}.{table} SET processing_status = %s, updated_at = CURRENT_TIMESTAMP "
                    "WHERE processing_status IN (%s, %s)"
                ).format(
                    schema=sql.Identifier(article_dao.config.schema),
                    table=sql.Identifier(article_dao._table_name),
                ),
                ("pending", "relevance_in_progress", "impact_in_progress"),
            )


def generate_article_id(source: str, source_item_id: Optional[str], published_at: datetime) -> str:
    key = "|".join(
        [
            source or "unknown",
            source_item_id or "",
            published_at.isoformat() if isinstance(published_at, datetime) else "",
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def normalize_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return _local_now()
        dt = parsed.to_pydatetime()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TZ).replace(tzinfo=None)
    return dt.astimezone(LOCAL_TZ).replace(tzinfo=None)


def _serialize_list(values: Optional[Iterable[str]]) -> Optional[str]:
    if not values:
        return None
    cleaned = [str(item).strip() for item in values if str(item).strip()]
    if not cleaned:
        return None
    return json.dumps(cleaned, ensure_ascii=False)


def _clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _local_now() -> datetime:
    return datetime.now(LOCAL_TZ).replace(tzinfo=None)


__all__ = [
    "ingest_articles",
    "acquire_for_relevance",
    "acquire_for_impact",
    "save_relevance_results",
    "save_impact_results",
    "reset_in_progress_articles",
    "generate_article_id",
    "normalize_datetime",
]
