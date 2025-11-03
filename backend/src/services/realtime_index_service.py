"""
Service layer for syncing Sina real-time China index data.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Dict, Optional

import akshare as ak
import pandas as pd

from ..config.settings import load_settings
from ..dao import RealtimeIndexDAO

logger = logging.getLogger(__name__)

REALTIME_INDEX_COLUMN_MAP: Dict[str, str] = {
    "代码": "code",
    "名称": "name",
    "最新价": "latest_price",
    "涨跌额": "change_amount",
    "涨跌幅": "change_percent",
    "昨收": "prev_close",
    "今开": "open_price",
    "最高": "high_price",
    "最低": "low_price",
    "成交量": "volume",
    "成交额": "turnover",
}

NUMERIC_COLUMNS = [
    "latest_price",
    "change_amount",
    "change_percent",
    "prev_close",
    "open_price",
    "high_price",
    "low_price",
    "volume",
    "turnover",
]


def _install_dataframe_map_shim(*, force: bool = False) -> None:
    """Ensure pandas.DataFrame has a map method for older pandas releases."""
    if not force and hasattr(pd.DataFrame, "map"):
        return

    def _dataframe_map(
        self: pd.DataFrame,
        func: Callable[[object], object],
        na_action: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        if kwargs:
            raise TypeError(
                f"DataFrame.map shim received unsupported keyword arguments: {list(kwargs)}",
            )
        if na_action not in (None, "ignore"):
            raise ValueError(
                f"DataFrame.map shim received unsupported na_action: {na_action!r}",
            )
        mapped_columns = {
            column: self[column].map(func, na_action=na_action)
            for column in self.columns
        }
        return self.__class__(mapped_columns, index=self.index)

    setattr(pd.DataFrame, "map", _dataframe_map)  # type: ignore[attr-defined]


_install_dataframe_map_shim()


def _prepare_realtime_index_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(REALTIME_INDEX_COLUMN_MAP.values()))

    prepared = frame.copy()
    prepared = prepared.rename(columns=REALTIME_INDEX_COLUMN_MAP)

    if "code" in prepared.columns:
        prepared["code"] = prepared["code"].astype(str).str.strip()
    if "name" in prepared.columns:
        prepared["name"] = prepared["name"].astype(str).str.strip()

    for column in NUMERIC_COLUMNS:
        if column not in prepared.columns:
            continue
        series = prepared[column]
        if series.dtype == object:
            series = series.astype(str).str.replace("%", "", regex=False).str.strip()
        prepared[column] = pd.to_numeric(series, errors="coerce")

    prepared = prepared.loc[:, list(REALTIME_INDEX_COLUMN_MAP.values())].copy()
    prepared = prepared.dropna(subset=["code"])
    return prepared.reset_index(drop=True)


def _fetch_realtime_index_raw() -> pd.DataFrame:
    """Call AkShare with compatibility shim for older pandas versions."""

    _install_dataframe_map_shim()

    try:
        return ak.stock_zh_index_spot_sina()
    except AttributeError as exc:
        if "has no attribute 'map'" not in str(exc):
            raise

        logger.warning(
            "AkShare stock_zh_index_spot_sina encountered DataFrame.map AttributeError; forcing shim reinstall",
        )
        _install_dataframe_map_shim(force=True)
        return ak.stock_zh_index_spot_sina()


def sync_realtime_indices(*, settings_path: Optional[str] = None) -> Dict[str, object]:
    """Fetch and persist Sina real-time China index data."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = RealtimeIndexDAO(settings.postgres)

    try:
        raw_frame = _fetch_realtime_index_raw()
    except Exception as exc:  # pragma: no cover - network call
        logger.exception("Failed to fetch realtime indices from AkShare: %s", exc)
        raise

    prepared = _prepare_realtime_index_frame(raw_frame)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Realtime index sync returned no data; skipping persistence.")
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
            "codes": [],
            "codeCount": 0,
        }

    dao.clear_table()
    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    codes = prepared["code"].dropna().tolist()

    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "codes": codes[:20],
        "codeCount": len(codes),
    }


def list_realtime_indices(
    *,
    limit: int = 500,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    settings = load_settings(settings_path)
    dao = RealtimeIndexDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset)
    stats = dao.stats()
    return {
        "total": result.get("total", 0),
        "items": result.get("items", []),
        "lastSyncedAt": stats.get("updated_at"),
    }


__all__ = ["sync_realtime_indices", "list_realtime_indices", "_prepare_realtime_index_frame"]
