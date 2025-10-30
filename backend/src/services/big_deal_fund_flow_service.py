"""Service for syncing AkShare big deal (large order) fund flow data."""

from __future__ import annotations

import logging
import re
import time
from typing import Callable, Optional, Sequence

import pandas as pd

from ..api_clients import BIG_DEAL_FUND_FLOW_COLUMN_MAP, fetch_big_deal_fund_flow
from ..config.settings import load_settings
from ..dao import BigDealFundFlowDAO

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "trade_price",
    "price_change",
)

AMOUNT_COLUMNS: tuple[str, ...] = ("trade_amount",)
VOLUME_COLUMNS: tuple[str, ...] = ("trade_volume",)
PERCENT_COLUMNS: tuple[str, ...] = ("price_change_percent",)

_UNIT_PATTERN = re.compile(r"([+-]?\d+(?:\.\d+)?)([万亿兆]?)", re.IGNORECASE)
_UNIT_MULTIPLIER = {
    "": 1.0,
    "万": 1e4,
    "亿": 1e8,
    "兆": 1e12,
}


def _normalize_stock_code(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if text.isdigit():
        return text.zfill(6)
    if "." in text:
        symbol, _suffix = text.split(".", 1)
        if symbol.isdigit():
            return symbol.zfill(6)
        return symbol
    return text


def _parse_numeric(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"--", "-"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_percent(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"--", "-"}:
        return None
    if text.endswith("%"):
        text = text[:-1]
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_amount(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"--", "-"}:
        return None
    text = text.replace(",", "").replace("人民币", "").replace("元", "")
    match = _UNIT_PATTERN.match(text)
    if not match:
        return _parse_numeric(text)
    number, unit = match.groups()
    try:
        numeric = float(number)
    except ValueError:
        return None
    multiplier = _UNIT_MULTIPLIER.get(unit, 1.0)
    return numeric * multiplier


def _parse_volume(value: object) -> Optional[int]:
    numeric = _parse_numeric(value)
    if numeric is None:
        return None
    return int(round(numeric))


def _prepare_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()
    for column in BIG_DEAL_FUND_FLOW_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None

    frame["trade_time"] = pd.to_datetime(frame.get("trade_time"), errors="coerce")
    if "stock_code" in frame.columns:
        frame["stock_code"] = frame["stock_code"].map(_normalize_stock_code)
    if "trade_side" in frame.columns:
        frame["trade_side"] = frame["trade_side"].astype(str).str.strip()

    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_parse_numeric)

    for column in AMOUNT_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_parse_amount)
            frame[column] = frame[column].map(lambda val: round(val, 2) if val is not None else None)

    for column in VOLUME_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(_parse_volume)

    for column in PERCENT_COLUMNS:
        if column in frame.columns:
            parsed = frame[column].map(_parse_percent)
            frame[column] = parsed.map(lambda val: round(val, 4) if val is not None else None)

    if "price_change" in frame.columns:
        frame["price_change"] = frame["price_change"].map(_parse_numeric)
        frame["price_change"] = frame["price_change"].map(lambda val: round(val, 4) if val is not None else None)

    required = list(BIG_DEAL_FUND_FLOW_COLUMN_MAP.values())
    frame = frame.dropna(subset=["trade_time", "stock_code", "trade_side", "trade_volume", "trade_amount"])
    if not frame.empty:
        key_columns = ["trade_time", "stock_code", "trade_side", "trade_volume", "trade_amount"]

        def _build_key(row: pd.Series) -> str:
            trade_time = row["trade_time"]
            if pd.isna(trade_time):
                time_key = ""
            else:
                time_key = trade_time.isoformat()
            volume = row["trade_volume"]
            amount = row["trade_amount"]
            volume_key = str(int(volume)) if volume is not None else ""
            amount_key = ""
            if amount is not None:
                amount_key = str(int(round(amount * 100)))
            side_key = (row["trade_side"] or "").strip()
            code_key = (row["stock_code"] or "").strip()
            return "|".join([time_key, code_key, side_key, volume_key, amount_key])

        frame = frame.assign(__dedupe_key=frame.apply(_build_key, axis=1))
        frame = frame.sort_values(["__dedupe_key"]).drop_duplicates(subset="__dedupe_key", keep="last")
        frame = frame.drop(columns="__dedupe_key")

    return frame.loc[:, required]


def sync_big_deal_fund_flow(
    *,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = BigDealFundFlowDAO(settings.postgres)

    if progress_callback:
        progress_callback(0.1, "Fetching big deal fund flow data", None)

    frame = fetch_big_deal_fund_flow()
    if frame.empty:
        elapsed = time.perf_counter() - started
        if progress_callback:
            progress_callback(1.0, "No big deal records fetched", 0)
        return {
            "rows": 0,
            "elapsedSeconds": elapsed,
        }

    prepared = _prepare_frame(frame)

    if progress_callback:
        progress_callback(0.6, f"Upserting {len(prepared)} big deal rows", len(prepared))

    with dao.connect() as conn:
        dao.ensure_table(conn)
        affected = dao.upsert(prepared, conn=conn)
        conn.commit()

    elapsed = time.perf_counter() - started

    if progress_callback:
        progress_callback(1.0, f"Upserted {affected} big deal rows", int(affected))

    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
    }


def list_big_deal_fund_flow(
    *,
    limit: int = 100,
    offset: int = 0,
    side: Optional[str] = None,
    stock_code: Optional[str] = None,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = BigDealFundFlowDAO(settings.postgres)
    normalized_side = side.strip() if isinstance(side, str) else None
    if normalized_side:
        normalized_side = normalized_side[:10]

    normalized_codes: Sequence[str] | None = None
    if stock_code:
        normalized_codes = _normalize_query_codes(stock_code)
        if not normalized_codes:
            return {"total": 0, "items": []}

    return dao.list_entries(
        limit=limit,
        offset=offset,
        side=normalized_side or None,
        stock_codes=normalized_codes,
    )


def _normalize_query_codes(value: str) -> Sequence[str]:
    text = (value or "").strip().upper()
    if not text:
        return ()
    codes: list[str] = []

    def _add(candidate: str) -> None:
        normalized = candidate.strip()
        if normalized and normalized not in codes:
            codes.append(normalized)

    if "." in text:
        symbol, suffix = text.split(".", 1)
        symbol = symbol.strip()
        if symbol:
            if symbol.isdigit():
                _add(symbol.zfill(6))
            _add(symbol)
        suffix = suffix.strip()
        if suffix and symbol:
            normalized_suffix = suffix.upper()
            _add(f"{symbol}.{normalized_suffix}")

    if text.isdigit():
        _add(text.zfill(6))

    digits_only = "".join(ch for ch in text if ch.isdigit())
    if digits_only:
        _add(digits_only.zfill(6))

    return tuple(codes)


__all__ = [
    "_prepare_frame",
    "sync_big_deal_fund_flow",
    "list_big_deal_fund_flow",
]
