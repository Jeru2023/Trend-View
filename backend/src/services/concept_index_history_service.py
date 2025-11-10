"""Service utilities for syncing concept index history via AkShare."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Sequence, Tuple
import re

import numpy as np
import pandas as pd
import akshare as ak

from ..config.settings import load_settings
from ..dao import ConceptDirectoryDAO, ConceptIndexHistoryDAO

logger = logging.getLogger(__name__)

_CONCEPT_NAME_LOOKUP: Dict[str, str] | None = None
_NORMALIZED_CONCEPT_MAP: Dict[str, str] | None = None


def _load_directory_concept_names() -> List[str]:
    try:
        settings = load_settings()
        dao = ConceptDirectoryDAO(settings.postgres)
        rows = dao.list_entries()
        if not rows:
            return []
        names: List[str] = []
        for row in rows:
            name = str(row.get("concept_name") or "").strip()
            if name:
                names.append(name)
        return names
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to load concept directory cache: %s", exc)
        return []

CONCEPT_SYNONYM_MAP: Dict[str, str] = {
    "ai算力": "东数西算(算力)",
    "光伏建筑一体化": "光伏概念",
    "算力": "东数西算(算力)",
}

CONCEPT_KEYWORD_HINTS: Tuple[Tuple[str, str], ...] = (
    ("算力", "东数西算(算力)"),
    ("建筑一体化", "光伏概念"),
    ("光伏", "光伏概念"),
)

_NORMALIZE_PATTERN = re.compile(r"[\s·•()（）-]+")


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


def _normalize_concept_label(label: str) -> str:
    if not label:
        return ""
    text = str(label).strip().lower()
    text = text.replace("概念", "")
    text = re.sub(r"[·•]+", "", text)
    text = _NORMALIZE_PATTERN.sub("", text)
    return text


def _ensure_concept_name_lookup() -> None:
    global _CONCEPT_NAME_LOOKUP, _NORMALIZED_CONCEPT_MAP
    if _CONCEPT_NAME_LOOKUP is not None and _NORMALIZED_CONCEPT_MAP is not None:
        return
    names = _load_directory_concept_names()
    if not names:
        try:
            frame = ak.stock_board_concept_name_ths()
        except Exception as exc:  # pragma: no cover - external dependency
            logger.warning("Failed to load THS concept name list: %s", exc)
            frame = pd.DataFrame()
        if isinstance(frame, pd.DataFrame) and "name" in frame.columns:
            values = frame["name"].dropna().astype(str)
            names = [value.strip() for value in values if value.strip()]

    if not names:
        _CONCEPT_NAME_LOOKUP = {}
        _NORMALIZED_CONCEPT_MAP = {}
        return

    lookup = {name: name for name in names}
    normalized: Dict[str, str] = {}
    for name in names:
        norm = _normalize_concept_label(name)
        if norm and norm not in normalized:
            normalized[norm] = name

    _CONCEPT_NAME_LOOKUP = lookup
    _NORMALIZED_CONCEPT_MAP = normalized


def _overlap_score(a: str, b: str) -> int:
    if not a or not b:
        return 0
    set_a = {ch for ch in a if "\u4e00" <= ch <= "\u9fff"}
    set_b = {ch for ch in b if "\u4e00" <= ch <= "\u9fff"}
    return len(set_a & set_b)


def _resolve_concept_symbol(concept_name: str) -> Optional[str]:
    label = (concept_name or "").strip()
    if not label:
        return None

    _ensure_concept_name_lookup()
    if not _CONCEPT_NAME_LOOKUP:
        return None

    if label in _CONCEPT_NAME_LOOKUP:
        return label

    normalized = _normalize_concept_label(label)
    alias_target = CONCEPT_SYNONYM_MAP.get(normalized) or CONCEPT_SYNONYM_MAP.get(label.lower())
    if alias_target and alias_target in _CONCEPT_NAME_LOOKUP:
        return alias_target

    if normalized in _NORMALIZED_CONCEPT_MAP:
        return _NORMALIZED_CONCEPT_MAP[normalized]

    for keyword, target in CONCEPT_KEYWORD_HINTS:
        if keyword in label and target in _CONCEPT_NAME_LOOKUP:
            return target

    best_name = None
    best_score = 0.0
    for candidate_norm, candidate_name in _NORMALIZED_CONCEPT_MAP.items():
        overlap = _overlap_score(normalized, candidate_norm)
        if overlap >= 2:
            score = 0.8 + overlap * 0.05
        else:
            score = SequenceMatcher(None, normalized, candidate_norm).ratio()
        if score > best_score:
            best_score = score
            best_name = candidate_name

    if best_name and best_score >= 0.65:
        return best_name

    return None

def _fetch_history_from_ths(concept_name: str, start: str, end: str) -> pd.DataFrame:
    resolved_symbol = _resolve_concept_symbol(concept_name)
    if not resolved_symbol:
        logger.warning("Unable to resolve THS concept symbol for %s", concept_name)
        return pd.DataFrame()

    if resolved_symbol != concept_name:
        logger.info("Resolved concept %s to %s for THS index fetch", concept_name, resolved_symbol)

    try:
        raw = ak.stock_board_concept_index_ths(symbol=resolved_symbol, start_date=start, end_date=end)
    except Exception as exc:  # pragma: no cover - external dependency
        logger.warning("THS concept index history failed for %s (resolved %s): %s", concept_name, resolved_symbol, exc)
        return pd.DataFrame()

    if raw.empty:
        return pd.DataFrame()

    frame = raw.copy()
    rename_map = {
        "日期": "trade_date",
        "开盘价": "open",
        "最高价": "high",
        "最低价": "low",
        "收盘价": "close",
        "涨跌幅": "pct_chg",
        "涨跌额": "change",
        "成交量": "vol",
        "成交额": "amount",
    }
    # support potential english column names
    rename_map.update({col.lower(): rename_map[col] for col in list(rename_map.keys()) if col.lower() not in rename_map})
    frame = frame.rename(columns=rename_map)

    if "trade_date" not in frame.columns:
        logger.warning("THS concept index history missing trade_date column for %s", concept_name)
        return pd.DataFrame()

    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.loc[frame["trade_date"].notna()].copy()
    frame["trade_date"] = frame["trade_date"].dt.date
    numeric_columns = ["open", "high", "low", "close", "vol", "amount"]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.sort_values("trade_date")
    frame["pre_close"] = frame["close"].shift(1)
    frame["change"] = frame["close"] - frame["pre_close"]
    frame["pct_chg"] = frame["change"] / frame["pre_close"] * 100
    frame.loc[frame["pre_close"].isna(), ["change", "pct_chg"]] = None
    frame.loc[frame["pre_close"] == 0, "pct_chg"] = None
    frame["pct_chg"] = frame["pct_chg"].where(np.isfinite(frame["pct_chg"]))

    frame["ts_code"] = f"THS-{concept_name}"
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
    for column in ordered:
        if column not in frame.columns:
            frame[column] = None
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
    dao = ConceptIndexHistoryDAO(settings.postgres)

    total_rows = 0
    synced_concepts: List[Dict[str, object]] = []
    errors: List[Dict[str, object]] = []

    for concept_name in concept_names:
        concept_name = concept_name.strip()
        if not concept_name:
            continue

        ths_frame = _fetch_history_from_ths(concept_name, start, end)
        if ths_frame.empty:
            logger.warning("No THS concept index rows for %s within %s-%s", concept_name, start, end)
            errors.append({"concept": concept_name, "error": "ths_no_data"})
            continue

        affected = dao.upsert(ths_frame)
        total_rows += affected
        synced_concepts.append(
            {
                "concept": concept_name,
                "ts_code": ths_frame["ts_code"].iloc[0],
                "rows": int(affected),
                "source": "ths",
            }
        )
        logger.info(
            "Stored %s concept index rows for %s via THS covering %s-%s",
            affected,
            concept_name,
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
