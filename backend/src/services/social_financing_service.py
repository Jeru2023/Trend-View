"""
Service layer for synchronising social financing incremental statistics.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from ..api_clients import MACRO_SOCIAL_FINANCING_COLUMN_MAP, fetch_macro_social_financing
from ..config.settings import load_settings
from ..dao import MacroSocialFinancingDAO

logger = logging.getLogger(__name__)


def _prepare_social_financing_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        columns = ["period_date"] + list(MACRO_SOCIAL_FINANCING_COLUMN_MAP.values())
        return pd.DataFrame(columns=columns)

    frame = dataframe.copy()

    for column in MACRO_SOCIAL_FINANCING_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    with pd.option_context("mode.chained_assignment", None):
        period_series = pd.to_datetime(frame["period_label"], format="%Y%m", errors="coerce")
        period_series = period_series + MonthEnd(0)
        frame["period_date"] = period_series.dt.date

    numeric_columns = [
        "total_financing",
        "renminbi_loans",
        "entrusted_and_fx_loans",
        "entrusted_loans",
        "trust_loans",
        "undiscounted_bankers_acceptance",
        "corporate_bonds",
        "domestic_equity_financing",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    columns = ["period_date", "period_label", *numeric_columns]
    prepared = frame.loc[:, columns].dropna(subset=["period_date"]).sort_values("period_date").reset_index(drop=True)
    return prepared


def sync_social_financing_ratios(*, settings_path: Optional[str] = None) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = MacroSocialFinancingDAO(settings.postgres)

    raw = fetch_macro_social_financing()
    prepared = _prepare_social_financing_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Social financing sync skipped: no data returned.")
        return {"rows": 0, "elapsedSeconds": elapsed}

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    return {"rows": int(affected), "elapsedSeconds": elapsed}


def list_social_financing_ratios(
    *,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = MacroSocialFinancingDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset)
    stats = dao.stats()
    return {
        "total": int(result.get("total", 0) or 0),
        "items": result.get("items", []),
        "lastSyncedAt": stats.get("updated_at") if isinstance(stats, dict) else None,
    }


__all__ = [
    "sync_social_financing_ratios",
    "list_social_financing_ratios",
    "_prepare_social_financing_frame",
]
