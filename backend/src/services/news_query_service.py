"""
Query helpers for unified news articles and insights.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence

from zoneinfo import ZoneInfo

from ..config.settings import load_settings
from ..dao import NewsArticleDAO, NewsInsightDAO

LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def list_news_articles(
    *,
    source: Optional[str] = None,
    limit: int = 100,
    only_relevant: bool = False,
    stock: Optional[str] = None,
    lookback_hours: Optional[int] = None,
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

    fetch_limit = limit
    stock_tokens = _normalize_stock_filters(stock)
    if stock_tokens:
        fetch_limit = min(limit * 5, 500)
    articles = article_dao.list_articles(source=source, limit=fetch_limit)
    if not articles:
        return []

    article_ids = [article["article_id"] for article in articles]
    insight_map = insight_dao.fetch_many(article_ids)

    min_published_at: Optional[datetime] = None
    if lookback_hours and lookback_hours > 0:
        min_published_at = datetime.now(LOCAL_TZ) - timedelta(hours=int(lookback_hours))

    results: List[Dict[str, object]] = []
    for article in articles:
        insight = insight_map.get(article["article_id"], {})
        if only_relevant and not insight.get("is_relevant"):
            continue
        merged = _merge_article_insight(article, insight)
        if min_published_at:
            published_at = article.get("published_at")
            if isinstance(published_at, datetime):
                reference = (
                    published_at.replace(tzinfo=LOCAL_TZ)
                    if published_at.tzinfo is None
                    else published_at.astimezone(LOCAL_TZ)
                )
                if reference < min_published_at:
                    continue
        if stock_tokens:
            impact_stocks = merged.get("impact", {}).get("stocks") or []
            normalized_stocks = {_normalize_stock_token(value) for value in impact_stocks if value}
            if not normalized_stocks.intersection(stock_tokens):
                continue
        results.append(merged)
        if len(results) >= limit:
            break
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


def _normalize_stock_filters(stock: Optional[str]) -> set[str]:
    if not stock:
        return set()
    if isinstance(stock, str):
        tokens = [token.strip() for token in stock.split(",")]
    elif isinstance(stock, Sequence):
        tokens = [str(token).strip() for token in stock]
    else:
        return set()
    return {token.lower() for token in tokens if token}


def _normalize_stock_token(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


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
