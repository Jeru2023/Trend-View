"""
Utilities for interacting with the Tushare Pro API.

This module only contains helpers responsible for fetching data from Tushare.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Sequence

import pandas as pd
import tushare as ts

logger = logging.getLogger(__name__)


STOCK_BASIC_FIELDS: Sequence[str] = (
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
)

DATE_COLUMNS: Sequence[str] = ("list_date", "delist_date")


def _fetch_stock_basic_frames(
    pro: ts.pro_api,
    list_statuses: Sequence[str],
    fields: Sequence[str],
    market: Optional[str],
) -> List[pd.DataFrame]:
    """Fetch stock basics for each list status and return non-empty frames."""
    frames: List[pd.DataFrame] = []
    for status in list_statuses:
        params = {
            "exchange": "",
            "list_status": status,
        }
        if market:
            params["market"] = market

        logger.debug("Requesting stock_basic with params=%s", params)
        frame = pro.stock_basic(fields=",".join(fields), **params)
        if frame is None or frame.empty:
            logger.warning("No stock_basic data returned for list_status=%s", status)
            continue

        frames.append(frame)

    return frames


def fetch_stock_basic(
    token: str,
    list_statuses: Sequence[str] = ("L", "D", "P"),
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch A-share stock basics from Tushare and return them as a DataFrame.

    Args:
        token: Tushare authentication token.
        list_statuses: Sequence of list statuses to request from Tushare. By
            default this covers listed (L), delisted (D), and paused (P) A-shares.
        market: Optional Tushare ``market`` filter. Leave empty to include all
            A-share markets.
    """
    if not token:
        raise RuntimeError("Tushare token is required to fetch stock basics.")

    pro = ts.pro_api(token)
    frames = _fetch_stock_basic_frames(
        pro=pro,
        list_statuses=list_statuses,
        fields=STOCK_BASIC_FIELDS,
        market=market,
    )

    if not frames:
        logger.warning("No stock_basic data retrieved.")
        return pd.DataFrame(columns=STOCK_BASIC_FIELDS)

    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"])


__all__ = [
    "DATE_COLUMNS",
    "STOCK_BASIC_FIELDS",
    "fetch_stock_basic",
]
