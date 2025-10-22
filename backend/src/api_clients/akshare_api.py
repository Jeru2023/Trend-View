"""
Helpers for interacting with AkShare endpoints.
"""

from __future__ import annotations

import logging
from typing import Final

import pandas as pd

import akshare as ak

logger = logging.getLogger(__name__)

FINANCE_BREAKFAST_COLUMNS: Final[dict[str, str]] = {
    "标题": "title",
    "摘要": "summary",
    "发布时间": "published_at",
    "链接": "url",
}


def fetch_finance_breakfast() -> pd.DataFrame:
    """
    Fetch finance breakfast summaries from AkShare.
    """
    try:
        df = ak.stock_info_cjzc_em()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to fetch finance breakfast data: %s", exc)
        return pd.DataFrame(columns=FINANCE_BREAKFAST_COLUMNS.values())

    if df is None or df.empty:
        logger.warning("AkShare returned no finance breakfast data.")
        return pd.DataFrame(columns=FINANCE_BREAKFAST_COLUMNS.values())

    renamed = df.rename(columns=FINANCE_BREAKFAST_COLUMNS)
    for column in FINANCE_BREAKFAST_COLUMNS.values():
        if column not in renamed.columns:
            renamed[column] = None

    subset = renamed.loc[:, list(FINANCE_BREAKFAST_COLUMNS.values())]
    subset["published_at"] = pd.to_datetime(subset["published_at"], errors="coerce")
    return subset


__all__ = ["FINANCE_BREAKFAST_COLUMNS", "fetch_finance_breakfast"]
