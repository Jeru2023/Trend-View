"""Services for indicator-based stock screening (e.g.,持续放量)."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict

import pandas as pd
from zoneinfo import ZoneInfo

from ..api_clients import fetch_stock_rank_cxfl_ths
from ..config.settings import load_settings
from ..dao import IndicatorScreeningDAO

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

CONTINUOUS_VOLUME_CODE = "continuous_volume"
CONTINUOUS_VOLUME_NAME = "持续放量"


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isnan(value):
            return None
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("%", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_volume_text(text: Any) -> tuple[int | None, str | None]:
    if not text:
        return None, None
    raw = str(text).strip()
    if not raw:
        return None, None
    date_note = None
    if "(" in raw and raw.endswith(")"):
        start = raw.find("(")
        date_note = raw[start + 1 : -1]
        raw = raw[:start].strip()

    multiplier = 1
    cleaned = raw
    for suffix, factor in (("万手", 10000 * 100), ("亿手", 100000000 * 100), ("万", 10000), ("亿", 100000000)):
        if cleaned.endswith(suffix):
            multiplier *= factor
            cleaned = cleaned[: -len(suffix)]
            break
    if cleaned.endswith("手"):
        multiplier *= 100
        cleaned = cleaned[:-1]

    numeric = _safe_float(cleaned)
    if numeric is None:
        return None, date_note
    return int(round(numeric * multiplier)), date_note


def _format_stock_code_full(code: str) -> str:
    if not code:
        return ""
    cleaned = code.strip().upper()
    if len(cleaned) != 6:
        return cleaned
    if cleaned.startswith(("6", "9", "5")):
        return f"{cleaned}.SH"
    return f"{cleaned}.SZ"


def _normalize_continuous_volume_frame(dataframe: pd.DataFrame, captured_at: datetime) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.copy()
    frame["rank"] = pd.to_numeric(frame.get("rank"), errors="coerce")
    frame["stock_code"] = frame.get("stock_code", "").astype(str).str.zfill(6)
    frame["stock_code_full"] = frame["stock_code"].apply(_format_stock_code_full)
    frame["price_change_percent"] = frame["price_change_percent"].apply(_safe_float)
    frame["stage_change_percent"] = frame["stage_change_percent"].apply(_safe_float)
    frame["last_price"] = frame["last_price"].apply(_safe_float)
    frame["volume_days"] = pd.to_numeric(frame.get("volume_days"), errors="coerce").astype("Int64")

    volume_values = frame.get("volume_text")
    baseline_values = frame.get("baseline_volume_text")

    frame["volume_shares"] = volume_values.apply(lambda x: _parse_volume_text(x)[0])
    frame["baseline_volume_shares"] = baseline_values.apply(lambda x: _parse_volume_text(x)[0])

    frame["indicator_code"] = CONTINUOUS_VOLUME_CODE
    frame["indicator_name"] = CONTINUOUS_VOLUME_NAME
    frame["captured_at"] = captured_at
    return frame.reindex(columns=INDICATOR_SCREENING_COLUMNS_FRAME)


INDICATOR_SCREENING_COLUMNS_FRAME = [
    "indicator_code",
    "indicator_name",
    "captured_at",
    "rank",
    "stock_code",
    "stock_code_full",
    "stock_name",
    "price_change_percent",
    "stage_change_percent",
    "last_price",
    "volume_shares",
    "volume_text",
    "baseline_volume_shares",
    "baseline_volume_text",
    "volume_days",
    "industry",
]


def sync_indicator_continuous_volume(*, settings_path: str | None = None) -> dict[str, Any]:
    """Fetch 最新持续放量列表并写入数据库。"""
    settings = load_settings(settings_path)
    dataframe = fetch_stock_rank_cxfl_ths()
    captured_at = datetime.now(LOCAL_TZ).replace(tzinfo=None)
    if dataframe is None or dataframe.empty:
        return {
            "indicatorCode": CONTINUOUS_VOLUME_CODE,
            "indicatorName": CONTINUOUS_VOLUME_NAME,
            "rows": 0,
            "capturedAt": None,
        }
    normalized = _normalize_continuous_volume_frame(dataframe, captured_at)
    dao = IndicatorScreeningDAO(settings.postgres)
    rows = dao.upsert(normalized)
    return {
        "indicatorCode": CONTINUOUS_VOLUME_CODE,
        "indicatorName": CONTINUOUS_VOLUME_NAME,
        "rows": rows,
        "capturedAt": datetime.fromtimestamp(captured_at.timestamp(), LOCAL_TZ),
    }


def list_indicator_screenings(
    *,
    indicator_code: str,
    limit: int = 200,
    offset: int = 0,
    settings_path: str | None = None,
) -> dict[str, Any]:
    """List indicator screening entries ordered by rank."""
    settings = load_settings(settings_path)
    dao = IndicatorScreeningDAO(settings.postgres)
    result = dao.list_entries(indicator_code=indicator_code, limit=limit, offset=offset)

    localized_latest = _localize_datetime(result.get("latest_captured_at"))

    items: list[dict[str, Any]] = []
    for entry in result.get("items", []):
        items.append(
            {
                "indicatorCode": entry.get("indicator_code"),
                "indicatorName": entry.get("indicator_name"),
                "capturedAt": _localize_datetime(entry.get("captured_at")),
                "rank": entry.get("rank"),
                "stockCode": entry.get("stock_code"),
                "stockCodeFull": entry.get("stock_code_full"),
                "stockName": entry.get("stock_name"),
                "priceChangePercent": _safe_float(entry.get("price_change_percent")),
                "stageChangePercent": _safe_float(entry.get("stage_change_percent")),
                "lastPrice": _safe_float(entry.get("last_price")),
                "volumeShares": _safe_float(entry.get("volume_shares")),
                "volumeText": entry.get("volume_text"),
                "baselineVolumeShares": _safe_float(entry.get("baseline_volume_shares")),
                "baselineVolumeText": entry.get("baseline_volume_text"),
                "volumeDays": entry.get("volume_days"),
                "industry": entry.get("industry"),
            }
        )

    indicator_name = items[0]["indicatorName"] if items else CONTINUOUS_VOLUME_NAME
    return {
        "indicatorCode": indicator_code,
        "indicatorName": indicator_name,
        "capturedAt": localized_latest,
        "total": int(result.get("total", 0)),
        "items": items,
    }


def _localize_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=LOCAL_TZ)
        return value.astimezone(LOCAL_TZ)
    return None


__all__ = [
    "sync_indicator_continuous_volume",
    "list_indicator_screenings",
]
