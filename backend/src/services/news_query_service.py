"""
Query helpers for unified news articles and insights.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from zoneinfo import ZoneInfo

from ..config.settings import load_settings
from ..dao import NewsArticleDAO, NewsInsightDAO

LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def list_news_articles(
    *,
    source: Optional[str] = None,
    limit: int = 100,
    only_relevant: bool = False,
    settings_path: Optional[str] = None,
) -> List[Dict[str, object]]:
    """
    Retrieve recent news articles with their associated insights.

    Args:
        source: Optional source identifier (e.g. "global_flash" or "finance_breakfast").
        limit: Maximum number of rows to return.
        only_relevant: When True, filter to articles marked as relevant by the LLM.
        settings_path: Optional override to load a different configuration file.
    """
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    insight_dao = NewsInsightDAO(settings.postgres)

    articles = article_dao.list_articles(source=source, limit=limit)
    if not articles:
        return []

    article_ids = [article["article_id"] for article in articles]
    insight_map = insight_dao.fetch_many(article_ids)

    results: List[Dict[str, object]] = []
    for article in articles:
        insight = insight_map.get(article["article_id"], {})
        if only_relevant and not insight.get("is_relevant"):
            continue
        merged = _merge_article_insight(article, insight)
        results.append(merged)
    return results


def _merge_article_insight(article: Dict[str, object], insight: Dict[str, object]) -> Dict[str, object]:
    relevance_checked_at = insight.get("relevance_checked_at")
    impact_checked_at = insight.get("impact_checked_at")
    extra_metadata_raw = insight.get("extra_metadata")
    extra_metadata = _coerce_metadata(extra_metadata_raw)

    record: Dict[str, object] = {
        "articleId": article.get("article_id"),
        "source": article.get("source"),
        "title": article.get("title"),
        "summary": article.get("summary"),
        "content": article.get("content"),
        "contentType": article.get("content_type"),
        "publishedAt": _format_datetime(article.get("published_at")),
        "url": article.get("url"),
        "language": article.get("language"),
        "processingStatus": article.get("processing_status"),
        "contentFetched": bool(article.get("content_fetched")),
        "contentFetchedAt": _format_datetime(article.get("content_fetched_at")),
        "relevanceAttempts": article.get("relevance_attempts"),
        "impactAttempts": article.get("impact_attempts"),
        "lastError": article.get("last_error"),
        "relevance": {
            "isRelevant": insight.get("is_relevant"),
            "confidence": insight.get("relevance_confidence"),
            "reason": insight.get("relevance_reason"),
            "checkedAt": _format_datetime(relevance_checked_at),
        },
        "impact": {
            "summary": insight.get("impact_summary"),
            "analysis": insight.get("impact_analysis"),
            "confidence": insight.get("impact_confidence"),
            "checkedAt": _format_datetime(impact_checked_at),
            "levels": insight.get("impact_levels", []),
            "markets": insight.get("impact_markets", []),
            "industries": insight.get("impact_industries", []),
            "sectors": insight.get("impact_sectors", []),
            "themes": insight.get("impact_themes", []),
            "stocks": insight.get("impact_stocks", []),
            "metadata": extra_metadata,
        },
    }
    return record


def _format_datetime(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ)


def _coerce_metadata(value: object) -> Optional[object]:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text
        else:
            return parsed
    return value


__all__ = ["list_news_articles"]
