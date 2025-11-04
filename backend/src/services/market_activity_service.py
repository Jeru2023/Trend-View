"""Service layer for market activity (赚钱效应) data."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from ..api_clients import fetch_market_activity_legu
from ..config.settings import load_settings
from ..dao import MarketActivityDAO

logger = logging.getLogger(__name__)


def _parse_numeric(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _prepare_market_activity_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()
    frame.insert(0, "display_order", range(len(frame)))

    frame["metric"] = frame["metric"].astype(str).str.strip()
    frame["value_text"] = frame["value"].astype(str).str.strip()
    frame["value_number"] = frame["value"].map(_parse_numeric)

    dataset_timestamp: Optional[datetime] = None
    timestamp_candidates = frame.loc[
        frame["metric"].str.contains("统计日期", na=False), "value_text"
    ]
    if not timestamp_candidates.empty:
        for candidate in timestamp_candidates:
            try:
                parsed = pd.to_datetime(candidate)
                if pd.notnull(parsed):
                    dataset_timestamp = parsed.to_pydatetime()
                    break
            except Exception:
                continue

    frame["dataset_timestamp"] = dataset_timestamp

    prepared = frame.loc[:, ["metric", "display_order", "value_text", "value_number", "dataset_timestamp"]]
    return prepared


def sync_market_activity(*, settings_path: Optional[str] = None) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MarketActivityDAO(settings.postgres)

    dataframe = fetch_market_activity_legu()
    if dataframe.empty:
        logger.warning("No market activity data returned from source.")
        return {"rows": 0, "datasetTimestamp": None}

    prepared = _prepare_market_activity_frame(dataframe)
    if prepared.empty:
        logger.warning("Market activity frame empty after preparation.")
        return {"rows": 0, "datasetTimestamp": None}

    affected = dao.upsert(prepared)
    dataset_timestamp = None
    if prepared["dataset_timestamp"].notnull().any():
        dataset_timestamp = prepared["dataset_timestamp"].dropna().iloc[0]

    logger.info("Upserted %s market activity rows", affected)

    return {
        "rows": int(affected),
        "datasetTimestamp": dataset_timestamp.isoformat() if dataset_timestamp else None,
    }


def list_market_activity(*, settings_path: Optional[str] = None) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MarketActivityDAO(settings.postgres)
    result = dao.list_entries()
    items = result.get("items", [])
    dataset_timestamp = result.get("dataset_timestamp")

    return {
        "items": items,
        "datasetTimestamp": dataset_timestamp.isoformat() if dataset_timestamp else None,
    }


__all__ = ["sync_market_activity", "list_market_activity", "_prepare_market_activity_frame"]

