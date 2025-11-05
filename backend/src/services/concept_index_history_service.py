"""Service utilities for syncing concept index history from Tushare."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import tushare as ts

from ..config.settings import load_settings
from ..dao import ConceptIndexHistoryDAO

logger = logging.getLogger(__name__)


def _normalise_dates(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    default_span_days: int = 365,
) -> Tuple[str, str]:
    today = date.today()
    end = datetime.strptime(end_date, "%Y%m%d").date() if end_date else today
    if start_date:
        start = datetime.strptime(start_date, "%Y%m%d").date()
    else:
        start = end - timedelta(days=default_span_days)
    if start > end:
        start = end - timedelta(days=default_span_days)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _concept_ts_code_map(pro, *, fuzzy: bool = True) -> Dict[str, str]:
    """Fetch mapping from concept names to ts_code via index_basic."""
    try:
        index_df = pro.index_basic()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch index_basic from Tushare: %s", exc)
        return {}

    if index_df.empty:
        logger.warning("Tushare index_basic returned empty dataset.")
        return {}

    index_df = index_df.loc[index_df["name"].notna()].copy()
    index_df["name"] = index_df["name"].astype(str).str.strip()
    mapping: Dict[str, str] = {}
    for _, row in index_df.iterrows():
        name = row["name"]
        ts_code = str(row.get("ts_code") or "").strip()
        if not name or not ts_code:
            continue
        mapping[name] = ts_code
    if not mapping and fuzzy:
        logger.warning("No concept mapping generated from Tushare index_basic.")
    return mapping


def _resolve_concept_ts_code(pro, concept_name: str) -> Optional[str]:
    concept_name = concept_name.strip()
    mapping = _concept_ts_code_map(pro)
    if concept_name in mapping:
        return mapping[concept_name]
    # fallback fuzzy match
    candidates = {name: code for name, code in mapping.items() if concept_name in name or name in concept_name}
    if candidates:
        # choose the longest matching name to avoid partial collisions
        selected = max(candidates.items(), key=lambda item: len(item[0]))
        logger.info("Resolved concept '%s' to '%s' via fuzzy match (%s).", concept_name, selected[1], selected[0])
        return selected[1]
    logger.warning("Unable to resolve concept '%s' to a ts_code via Tushare index_basic.", concept_name)
    return None


def _prepare_history_frame(
    raw: pd.DataFrame,
    *, concept_name: str, ts_code: str,
) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["ts_code", "concept_name", "trade_date"])
    frame = raw.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")

    frame = frame.loc[frame["trade_date"].notna()].copy()
    frame["trade_date"] = frame["trade_date"].dt.date
    frame["ts_code"] = ts_code
    frame["concept_name"] = concept_name
    ordered = [
        "ts_code",
        "concept_name",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]
    return frame.loc[:, ordered]


def sync_concept_index_history(
    concept_names: Sequence[str],
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    """Fetch and persist concept index history for the given concept names."""
    if not concept_names:
        raise ValueError("concept_names must not be empty")

    start, end = _normalise_dates(start_date, end_date)

    settings = load_settings(settings_path)
    tushare_token = getattr(settings.tushare, "token", None)
    if not tushare_token:
        raise RuntimeError("Tushare token is required to sync concept index history.")

    ts.set_token(tushare_token)
    pro = ts.pro_api()

    dao = ConceptIndexHistoryDAO(settings.postgres)

    total_rows = 0
    synced_concepts: List[Dict[str, object]] = []
    errors: List[Dict[str, object]] = []

    for concept_name in concept_names:
        concept_name = concept_name.strip()
        if not concept_name:
            continue

        ts_code = _resolve_concept_ts_code(pro, concept_name)
        if not ts_code:
            errors.append({"concept": concept_name, "error": "ts_code_not_found"})
            continue

        try:
            raw = pro.index_daily(ts_code=ts_code, start_date=start, end_date=end)
        except Exception as exc:  # pragma: no cover - external dependency
            logger.error("Tushare index_daily failed for %s (%s): %s", concept_name, ts_code, exc)
            errors.append({"concept": concept_name, "ts_code": ts_code, "error": str(exc)})
            continue

        prepared = _prepare_history_frame(raw, concept_name=concept_name, ts_code=ts_code)
        if prepared.empty:
            logger.info("No index history rows for concept %s (%s) within %s-%s", concept_name, ts_code, start, end)
            synced_concepts.append({"concept": concept_name, "ts_code": ts_code, "rows": 0})
            continue

        affected = dao.upsert(prepared)
        total_rows += affected
        synced_concepts.append({"concept": concept_name, "ts_code": ts_code, "rows": int(affected)})
        logger.info(
            "Stored %s concept index rows for %s (%s) covering %s-%s",
            affected,
            concept_name,
            ts_code,
            start,
            end,
        )

    return {
        "concepts": synced_concepts,
        "errors": errors,
        "startDate": start,
        "endDate": end,
        "totalRows": total_rows,
    }


def list_concept_index_history(
    *,
    ts_code: Optional[str] = None,
    concept_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = ConceptIndexHistoryDAO(settings.postgres)
    return dao.list_entries(
        ts_code=ts_code,
        concept_name=concept_name,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )


__all__ = ["sync_concept_index_history", "list_concept_index_history"]
