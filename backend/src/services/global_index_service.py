"""Service layer for synchronising global index data via Yahoo Finance."""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd
import tushare as ts

from ..api_clients import fetch_yahoo_daily_history, fetch_yahoo_daily_history_range
from ..config.settings import AppSettings, load_settings
from ..dao import GlobalIndexHistoryDAO
from ..dao.global_index_history_dao import GLOBAL_INDEX_HISTORY_FIELDS

logger = logging.getLogger(__name__)


_UTC = timezone.utc
_HISTORY_LOOKBACK_DAYS = 370  # roughly 1 calendar year
_LOOKBACK_DAYS = _HISTORY_LOOKBACK_DAYS
_FTSE_SYMBOL = "XIN9.FGI"
_FTSE_TS_CODE = "XIN9"
_FTSE_DISPLAY_NAME = "富时中国A50指数"

YAHOO_INDEX_SPECS: List[Dict[str, object]] = [
    {"symbol": "^DJI", "display_name": "道琼斯工业指数", "seq": 1},
    {"symbol": "^GSPC", "display_name": "标普500指数", "seq": 2},
    {"symbol": "^IXIC", "display_name": "纳斯达克综合指数", "seq": 3},
    {"symbol": "^N225", "display_name": "日经225指数", "seq": 4},
    {"symbol": "^HSI", "display_name": "恒生指数", "seq": 5},
    {"symbol": "XIN9.FGI", "display_name": "富时中国A50指数", "seq": 6},
    {"symbol": "^STOXX50E", "display_name": "欧洲斯托克50指数", "seq": 7},
]


def _resolve_settings(settings: Optional[AppSettings], settings_path: Optional[str]) -> AppSettings:
    if settings is not None:
        return settings
    return load_settings(settings_path)


def _display_name_for(symbol: str) -> Optional[str]:
    target = symbol.lower()
    for spec in YAHOO_INDEX_SPECS:
        candidate = str(spec.get("symbol") or "").lower()
        if candidate == target:
            return spec.get("display_name")
    return None


def _to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if math.isnan(numeric):
            return None
        return numeric
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if pd.notna(numeric) else None


def _first_present(row: Dict[str, object], *keys: str) -> object:
    for key in keys:
        if not key:
            continue
        if key in row:
            value = row.get(key)
            if value is not None:
                return value
    for key in keys:
        if not key:
            continue
        if key in row:
            return row.get(key)
    return None


def _normalize_history_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    if not rows:
        return []

    working: List[Dict[str, object]] = [
        {
            **row,
            "trade_date": row.get("trade_date") or row.get("tradeDate"),
            "open_price": _first_present(row, "open_price", "openPrice", "open"),
            "high_price": _first_present(row, "high_price", "highPrice", "high"),
            "low_price": _first_present(row, "low_price", "lowPrice", "low"),
            "close_price": _first_present(row, "close_price", "closePrice", "close"),
            "prev_close": _first_present(row, "prev_close", "prevClose", "previousClose", "pre_close"),
            "change_amount": _first_present(row, "change_amount", "changeAmount", "change"),
            "change_percent": _first_present(
                row,
                "change_percent",
                "changePercent",
                "pct_change",
                "pctChange",
                "pctChg",
            ),
            "volume": _first_present(row, "volume", "vol"),
        }
        for row in rows
    ]

    ascending = sorted(
        working,
        key=lambda item: item.get("trade_date") or datetime.min.date(),
    )

    last_close: Optional[float] = None
    for entry in ascending:
        open_price = _to_float(entry.get("open_price"))
        close_price = _to_float(entry.get("close_price"))
        prev_close = _to_float(entry.get("prev_close"))
        entry["open_price"] = open_price
        entry["close_price"] = close_price

        if prev_close is None and last_close is not None:
            prev_close = last_close
        elif (
            prev_close is not None
            and close_price is not None
            and last_close is not None
            and abs(prev_close - close_price) < 1e-3
            and abs(last_close - close_price) > 1e-6
        ):
            prev_close = last_close

        if prev_close is not None:
            entry["prev_close"] = prev_close

        change_amount = _to_float(entry.get("change_amount"))
        if change_amount is None or abs(change_amount) < 1e-9:
            if close_price is not None and prev_close is not None:
                change_amount = close_price - prev_close
            elif close_price is not None and open_price is not None:
                change_amount = close_price - open_price
        if change_amount is not None:
            entry["change_amount"] = change_amount

        change_percent = _to_float(entry.get("change_percent"))
        if change_percent is None or abs(change_percent) < 1e-9:
            baseline = prev_close if prev_close not in (None, 0) else open_price
            if change_amount is not None and baseline not in (None, 0):
                change_percent = (change_amount / baseline) * 100.0
        if change_percent is not None:
            entry["change_percent"] = change_percent

        volume_value = _to_float(entry.get("volume"))
        entry["volume"] = volume_value

        if close_price is not None:
            last_close = close_price

    descending = sorted(ascending, key=lambda item: item.get("trade_date") or datetime.min.date(), reverse=True)
    return descending


def _is_ftse_symbol(symbol: str) -> bool:
    return str(symbol or "").upper() == _FTSE_SYMBOL.upper()


def _filter_recent_history(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "trade_date" not in frame.columns:
        return frame
    cutoff = (_utc_now() - timedelta(days=_HISTORY_LOOKBACK_DAYS)).date()
    filtered = frame.copy()
    filtered["trade_date"] = pd.to_datetime(filtered["trade_date"]).dt.date
    return filtered[filtered["trade_date"] >= cutoff]


def _rows_to_dataframe(rows: List[Dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=GLOBAL_INDEX_HISTORY_FIELDS)
    dataframe = pd.DataFrame(rows)
    for column in GLOBAL_INDEX_HISTORY_FIELDS:
        if column not in dataframe.columns:
            dataframe[column] = None
    return dataframe.loc[:, GLOBAL_INDEX_HISTORY_FIELDS]


def _row_requires_change_backfill(row: Dict[str, object]) -> bool:
    for key in ("change_amount", "change_percent"):
        value = row.get(key)
        if value is None:
            return True
        if isinstance(value, float) and math.isnan(value):
            return True
    return False


def _row_missing_core_prices(row: Dict[str, object]) -> bool:
    for key in ("open_price", "high_price", "low_price", "close_price"):
        value = row.get(key)
        if value is None:
            return True
        if isinstance(value, float) and math.isnan(value):
            return True
        if isinstance(value, (int, float)) and float(value) == 0.0:
            return True
    return False


def _fetch_ftse_a50_history_from_tushare(
    settings: AppSettings,
    *,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    token = getattr(settings, "tushare", None)
    token_value = getattr(token, "token", None) if token else None
    if not token_value:
        logger.warning("Tushare token missing; cannot backfill FTSE A50 history.")
        return pd.DataFrame(columns=GLOBAL_INDEX_HISTORY_FIELDS)

    params: Dict[str, object] = {"ts_code": _FTSE_TS_CODE}
    effective_start = start_date or (_utc_now() - timedelta(days=_HISTORY_LOOKBACK_DAYS))
    params["start_date"] = effective_start.strftime("%Y%m%d")
    if end_date is not None:
        params["end_date"] = end_date.strftime("%Y%m%d")

    try:
        pro_client = ts.pro_api(token_value)
        df = pro_client.index_global(**params)
    except Exception as exc:  # pragma: no cover - network errors
        logger.error("Failed to fetch FTSE A50 via Tushare: %s", exc)
        return pd.DataFrame(columns=GLOBAL_INDEX_HISTORY_FIELDS)

    if df is None or df.empty:
        return pd.DataFrame(columns=GLOBAL_INDEX_HISTORY_FIELDS)

    frame = df.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], format="%Y%m%d", errors="coerce").dt.date
    mapping = {
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
        "pre_close": "prev_close",
        "change": "change_amount",
        "pct_change": "change_percent",
        "vol": "volume",
    }
    frame = frame.rename(columns=mapping)
    frame["code"] = _FTSE_SYMBOL
    frame["name"] = _FTSE_DISPLAY_NAME
    frame["currency"] = frame.get("currency") or "CNY"
    frame["timezone"] = frame.get("exchange_timezone") or "Asia/Shanghai"

    ordered = _rows_to_dataframe(frame.to_dict("records"))
    ordered = ordered.dropna(subset=["trade_date"]).sort_values("trade_date", ascending=False)
    ordered = _filter_recent_history(ordered)
    normalized_rows = _normalize_history_rows(ordered.to_dict("records"))
    normalized_df = _rows_to_dataframe(normalized_rows)
    if limit is not None and limit > 0:
        normalized_df = normalized_df.head(int(limit))
    return normalized_df.reset_index(drop=True)


def _utc_now() -> datetime:
    return datetime.now(tz=_UTC)


def _first_fetch_start() -> datetime:
    return _utc_now() - timedelta(days=_HISTORY_LOOKBACK_DAYS)


def _compute_prev_close_series(df: pd.DataFrame, initial_prev_close: Optional[float]) -> pd.Series:
    shifted = df["close"].shift(1)
    if initial_prev_close is not None and not df.empty:
        shifted.iloc[0] = initial_prev_close
    return shifted


def _build_snapshot_records(recent_rows: Dict[str, List[Dict[str, object]]]) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for spec in YAHOO_INDEX_SPECS:
        symbol = spec["symbol"]
        rows = recent_rows.get(symbol)
        if not rows:
            continue
        latest = rows[0]
        prev = rows[1] if len(rows) > 1 else None
        latest_close = latest.get("close_price")
        if latest_close is None:
            continue
        prev_close = prev.get("close_price") if prev else None
        if prev_close in (None,):
            prev_close = latest.get("prev_close")
        change_amount = None
        change_percent = None
        if prev_close not in (None, 0):
            change_amount = latest_close - prev_close
            change_percent = (change_amount / prev_close) * 100.0
        amplitude = None
        high_price = latest.get("high_price")
        low_price = latest.get("low_price")
        if prev_close not in (None, 0) and high_price is not None and low_price is not None:
            amplitude = ((high_price - low_price) / prev_close) * 100.0
        trade_date = latest.get("trade_date")
        last_quote_time = None
        if isinstance(trade_date, datetime):
            last_quote_time = trade_date
        elif trade_date is not None:
            try:
                last_quote_time = datetime.combine(trade_date, datetime.min.time()).replace(hour=16)
            except Exception:  # pragma: no cover - defensive
                last_quote_time = None
        records.append(
            {
                "code": symbol,
                "seq": spec.get("seq"),
                "name": spec.get("display_name"),
                "latest_price": latest_close,
                "change_amount": change_amount,
                "change_percent": change_percent,
                "open_price": latest.get("open_price"),
                "high_price": high_price,
                "low_price": low_price,
                "prev_close": prev_close,
                "amplitude": amplitude,
                "last_quote_time": last_quote_time,
                "updated_at": last_quote_time,
            }
        )
    return records


def sync_global_indices(*, settings_path: Optional[str] = None) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    history_dao = GlobalIndexHistoryDAO(settings.postgres)

    symbols = [spec["symbol"] for spec in YAHOO_INDEX_SPECS]
    latest_rows = history_dao.fetch_recent_rows(symbols, per_code=1)
    latest_dates_map = history_dao.latest_trade_dates(symbols)
    history_frames: List[pd.DataFrame] = []
    total_history_rows = 0

    for spec in YAHOO_INDEX_SPECS:
        symbol = spec["symbol"]
        latest_date = latest_dates_map.get(symbol)
        existing_sample = history_dao.list_history(symbol, limit=10)
        insufficient_history = len(existing_sample) <= 5
        needs_core_backfill = any(_row_missing_core_prices(row) for row in existing_sample)

        if _is_ftse_symbol(symbol):
            start_param = None
            if latest_date is not None and not needs_core_backfill:
                start_param = datetime.combine(latest_date, datetime.min.time())
            history_df = _fetch_ftse_a50_history_from_tushare(
                settings,
                start_date=start_param,
                end_date=None,
            )
            history_df = _filter_recent_history(history_df)
        else:
            force_full_refetch = latest_date is None or needs_core_backfill
            if force_full_refetch:
                start_dt = _first_fetch_start()
            else:
                start_dt = datetime.combine(latest_date + timedelta(days=1), datetime.min.time()).replace(tzinfo=_UTC)
                if start_dt > _utc_now():
                    continue
            history_df = fetch_yahoo_daily_history(symbol, start=start_dt, end=None, max_lookback_days=_LOOKBACK_DAYS)
            history_df = _filter_recent_history(history_df)
            if (force_full_refetch or insufficient_history) and (history_df.empty or len(history_df) <= 5):
                range_df = fetch_yahoo_daily_history_range(symbol, range_period="5y", interval="1d")
                if not range_df.empty:
                    history_df = _filter_recent_history(range_df)

        if history_df.empty:
            continue
        history_df = history_df.sort_values("trade_date").reset_index(drop=True)
        if "close" not in history_df.columns:
            if "close_price" in history_df.columns:
                history_df["close"] = history_df["close_price"]
            else:
                history_df["close"] = None
        prev_close_initial = None
        rows_for_symbol = latest_rows.get(symbol)
        if rows_for_symbol and not needs_core_backfill:
            prev_close_initial = rows_for_symbol[0].get("close_price")
        history_df["prev_close"] = _compute_prev_close_series(history_df, prev_close_initial)
        history_df["change_amount"] = history_df["close"] - history_df["prev_close"]
        history_df["change_percent"] = None
        mask = history_df["prev_close"].notnull() & (history_df["prev_close"] != 0)
        history_df.loc[mask, "change_percent"] = (
            history_df.loc[mask, "change_amount"] / history_df.loc[mask, "prev_close"]
        ) * 100.0
        normalized_rows = _normalize_history_rows(history_df.to_dict("records"))
        for entry in normalized_rows:
            entry["timezone"] = entry.get("timezone") or entry.get("exchange_timezone")
        history_df = _rows_to_dataframe(normalized_rows)
        history_df["code"] = symbol
        history_df["name"] = spec.get("display_name")
        history_frames.append(
            history_df
        )
        total_history_rows += len(history_df)

    if history_frames:
        history_payload = pd.concat(history_frames, ignore_index=True)
        history_dao.upsert(history_payload)

    _refresh_recent_history(history_dao, symbols)

    recent_rows = history_dao.fetch_recent_rows(symbols, per_code=2)
    snapshot_records = _build_snapshot_records(recent_rows)
    snapshot_rows = len(snapshot_records)

    elapsed = time.perf_counter() - started
    codes = [record["code"] for record in snapshot_records]
    return {
        "rows": int(snapshot_rows),
        "historyRows": int(total_history_rows),
        "elapsedSeconds": elapsed,
        "codes": codes,
        "codeCount": len(codes),
    }


def _refresh_recent_history(history_dao: GlobalIndexHistoryDAO, symbols: List[str], per_code: int = 12) -> None:
    recent = history_dao.fetch_recent_rows(symbols, per_code=per_code)
    rows_to_upsert: List[Dict[str, object]] = []
    for rows in recent.values():
        rows_to_upsert.extend(_normalize_history_rows(rows))
    if not rows_to_upsert:
        return
    history_df = pd.DataFrame(rows_to_upsert)
    history_dao.upsert(history_df.reindex(columns=GLOBAL_INDEX_HISTORY_FIELDS))


def list_global_indices(
    *,
    limit: int = 200,
    offset: int = 0,
    settings_path: Optional[str] = None,
    settings: Optional[AppSettings] = None,
) -> dict[str, object]:
    resolved_settings = _resolve_settings(settings, settings_path)
    history_dao = GlobalIndexHistoryDAO(resolved_settings.postgres)
    symbols = [spec["symbol"] for spec in YAHOO_INDEX_SPECS]
    recent = history_dao.fetch_recent_rows(symbols, per_code=2)
    snapshot_records = _build_snapshot_records(recent)
    total = len(snapshot_records)
    start = max(0, int(offset))
    if start >= total:
        items: List[Dict[str, object]] = []
    else:
        window = max(1, int(limit))
        end = min(total, start + window)
        items = snapshot_records[start:end]
    stats = history_dao.stats()
    return {
        "total": total,
        "items": items,
        "lastSyncedAt": stats.get("updated_at"),
    }


def list_global_index_history(
    *,
    code: str,
    limit: int = 260,
    settings_path: Optional[str] = None,
    settings: Optional[AppSettings] = None,
) -> dict[str, object]:
    normalized_code = (code or "").strip()
    if not normalized_code:
        raise ValueError("code must be provided")

    limit_value = max(10, min(int(limit), 1000))
    resolved_settings = _resolve_settings(settings, settings_path)
    dao = GlobalIndexHistoryDAO(resolved_settings.postgres)
    rows = dao.list_history(normalized_code, limit=limit_value)
    if _is_ftse_symbol(normalized_code) and len(rows) <= 5:
        ftse_df = _fetch_ftse_a50_history_from_tushare(resolved_settings, limit=limit_value)
        if not ftse_df.empty:
            dao.upsert(ftse_df)
            rows = dao.list_history(normalized_code, limit=limit_value)
    needs_backfill = any(_row_requires_change_backfill(row) for row in rows)
    normalized_rows = _normalize_history_rows(rows)
    if (needs_backfill or any(_row_requires_change_backfill(row) for row in normalized_rows)) and normalized_rows:
        dao.upsert(_rows_to_dataframe(normalized_rows))
    name = next((row.get("name") for row in normalized_rows if row.get("name")), None) or _display_name_for(normalized_code)
    return {
        "code": normalized_code,
        "name": name,
        "items": normalized_rows,
    }


__all__ = [
    "sync_global_indices",
    "list_global_indices",
    "list_global_index_history",
]
