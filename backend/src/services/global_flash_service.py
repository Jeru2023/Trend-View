"""
Service layer for ingesting Eastmoney global flash headlines into the unified news pipeline.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from ..api_clients import fetch_global_flash_news
from ..config.settings import load_settings
from ..dao import NewsArticleDAO
from .news_pipeline_service import generate_article_id, ingest_articles, normalize_datetime

logger = logging.getLogger(__name__)

GLOBAL_FLASH_COLUMNS = ["url", "title", "summary", "published_at"]
SOURCE_NAME = "global_flash"


def sync_global_flash(*, settings_path: Optional[str] = None) -> Dict[str, object]:
    """Fetch the latest global flash data and persist new entries via the unified pipeline."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)

    raw_frame = fetch_global_flash_news()
    prepared = _prepare_global_flash_frame(raw_frame)

    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.info("Global flash sync skipped: upstream returned no usable rows.")
        return {"rows": 0, "elapsedSeconds": elapsed, "fetched": 0, "newRecords": 0}

    records = _frame_to_pipeline_records(prepared)
    article_ids = [record["article_id"] for record in records]
    existing_ids = article_dao.existing_article_ids(article_ids)
    source_keys = [record["payload"].get("source_item_id") or record["payload"].get("url") for record in records]
    existing_sources = article_dao.existing_source_items(SOURCE_NAME, source_keys)

    new_items = [
        record["payload"]
        for record in records
        if record["article_id"] not in existing_ids
        and (record["payload"].get("source_item_id") or record["payload"].get("url")) not in existing_sources
    ]

    if not new_items:
        elapsed = time.perf_counter() - started
        logger.info("Global flash sync found no new records (fetched=%s).", len(prepared))
        return {"rows": 0, "elapsedSeconds": elapsed, "fetched": len(prepared), "newRecords": 0}

    ingest_result = ingest_articles(SOURCE_NAME, new_items, settings_path=settings_path)
    elapsed = time.perf_counter() - started

    rows = int(ingest_result.get("rows", 0))
    logger.info(
        "Global flash sync stored %s new articles (fetched=%s, new=%s).",
        rows,
        len(prepared),
        len(new_items),
    )
    return {
        "rows": rows,
        "elapsedSeconds": elapsed,
        "fetched": len(prepared),
        "newRecords": len(new_items),
    }


def _prepare_global_flash_frame(dataframe: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normalise the upstream dataframe into a canonical structure."""
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=GLOBAL_FLASH_COLUMNS)

    frame = dataframe.copy()

    def _canonical(name: str) -> str:
        return "".join(ch for ch in name.lower() if ch.isalnum())

    canonical_map: Dict[str, str] = {}
    for column in frame.columns:
        key = _canonical(str(column))
        if key and key not in canonical_map:
            canonical_map[key] = column

    rename_map: Dict[str, str] = {}
    for target in GLOBAL_FLASH_COLUMNS:
        canonical_key = _canonical(target)
        source_column = canonical_map.get(canonical_key)
        if source_column and source_column != target:
            rename_map[source_column] = target

    if rename_map:
        frame = frame.rename(columns=rename_map)

    for column in GLOBAL_FLASH_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    with pd.option_context("mode.chained_assignment", None):
        frame["title"] = frame["title"].apply(_clean_text)
        frame["summary"] = frame["summary"].apply(_clean_optional_text)
        frame["url"] = frame["url"].apply(_clean_text)
        frame["published_at"] = pd.to_datetime(frame["published_at"], errors="coerce")

    prepared = (
        frame.loc[:, GLOBAL_FLASH_COLUMNS]
        .dropna(subset=["title", "url", "published_at"])
        .drop_duplicates(subset=["url"])
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

        if not title or not url or published_raw is None:
            continue

        published_dt = normalize_datetime(published_raw)
        article_id = generate_article_id(SOURCE_NAME, url, published_dt)

        payload = {
            "source_item_id": url,
            "title": title,
            "summary": summary,
            "published_at": published_dt.isoformat(),
            "url": url,
            "language": "zh-CN",
            "content_type": "flash",
        }
        records.append({"article_id": article_id, "payload": payload})
    return records


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
    text = _clean_text(value)
    if text is None:
        return None
    return text


__all__ = [
    "GLOBAL_FLASH_COLUMNS",
    "sync_global_flash",
    "_prepare_global_flash_frame",
]
