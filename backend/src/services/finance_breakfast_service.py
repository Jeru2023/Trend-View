"""
Service layer for ingesting AkShare finance breakfast articles into the unified news pipeline.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

import pandas as pd

from ..api_clients import fetch_eastmoney_detail, fetch_finance_breakfast
from ..config.settings import load_settings
from ..dao import NewsArticleDAO
from .news_pipeline_service import generate_article_id, ingest_articles, normalize_datetime

logger = logging.getLogger(__name__)

FINANCE_BREAKFAST_COLUMNS = ["title", "summary", "published_at", "url"]
SOURCE_NAME = "finance_breakfast"


def sync_finance_breakfast(*, settings_path: Optional[str] = None) -> Dict[str, object]:
    """Fetch finance breakfast data and persist new articles via the unified pipeline."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)

    raw_frame = fetch_finance_breakfast()
    prepared = _prepare_finance_breakfast_frame(raw_frame)

    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.info("Finance breakfast sync skipped: upstream returned no usable rows.")
        return {"rows": 0, "elapsedSeconds": elapsed, "fetched": 0, "newRecords": 0}

    records = _frame_to_pipeline_records(prepared)
    article_ids = [record["article_id"] for record in records]
    existing_ids = article_dao.existing_article_ids(article_ids)

    new_records = [record for record in records if record["article_id"] not in existing_ids]
    if not new_records:
        elapsed = time.perf_counter() - started
        logger.info("Finance breakfast sync found no new records (fetched=%s).", len(prepared))
        return {"rows": 0, "elapsedSeconds": elapsed, "fetched": len(prepared), "newRecords": 0}

    _enrich_with_content(new_records)
    payloads = [record["payload"] for record in new_records]
    ingest_result = ingest_articles(SOURCE_NAME, payloads, settings_path=settings_path)

    rows = int(ingest_result.get("rows", 0))
    elapsed = time.perf_counter() - started
    logger.info(
        "Finance breakfast sync stored %s new articles (fetched=%s, new=%s).",
        rows,
        len(prepared),
        len(new_records),
    )
    return {
        "rows": rows,
        "elapsedSeconds": elapsed,
        "fetched": len(prepared),
        "newRecords": len(new_records),
    }


def _prepare_finance_breakfast_frame(dataframe: Optional[pd.DataFrame]) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=FINANCE_BREAKFAST_COLUMNS)

    frame = dataframe.copy()
    for column in FINANCE_BREAKFAST_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    with pd.option_context("mode.chained_assignment", None):
        frame["title"] = frame["title"].apply(_clean_text)
        frame["summary"] = frame["summary"].apply(_clean_optional_text)
        frame["url"] = frame["url"].apply(_clean_text)
        frame["published_at"] = pd.to_datetime(frame["published_at"], errors="coerce")

    prepared = (
        frame.loc[:, FINANCE_BREAKFAST_COLUMNS]
        .dropna(subset=["title", "published_at"])
        .drop_duplicates(subset=["title", "published_at"])
        .sort_values("published_at")
        .reset_index(drop=True)
    )
    return prepared


def _frame_to_pipeline_records(frame: pd.DataFrame) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for row in frame.itertuples(index=False):
        title = getattr(row, "title", None)
        summary = getattr(row, "summary", None)
        url = getattr(row, "url", None)
        published_raw = getattr(row, "published_at", None)

        if not title or published_raw is None:
            continue

        published_dt = normalize_datetime(published_raw)
        source_key = url or title
        article_id = generate_article_id(SOURCE_NAME, source_key, published_dt)

        payload = {
            "source_item_id": source_key,
            "title": title,
            "summary": summary,
            "published_at": published_dt.isoformat(),
            "url": url,
            "language": "zh-CN",
            "content_type": "morning_brief",
        }
        records.append({"article_id": article_id, "payload": payload})
    return records


def _enrich_with_content(records: List[Dict[str, object]]) -> None:
    """Fetch detailed article content for new finance breakfast entries."""
    for record in records:
        payload = record.get("payload", {})
        url = payload.get("url")
        if not url:
            continue
        try:
            detail = fetch_eastmoney_detail(str(url))
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("Finance breakfast content fetch failed (%s): %s", url, exc)
            continue

        content = _clean_optional_text(getattr(detail, "content", None))
        if content:
            payload["content"] = content


def _clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"nan", "none", "null"}:
        return None
    return text


def _clean_optional_text(value: object) -> Optional[str]:
    return _clean_text(value)


__all__ = [
    "sync_finance_breakfast",
    "FINANCE_BREAKFAST_COLUMNS",
]
