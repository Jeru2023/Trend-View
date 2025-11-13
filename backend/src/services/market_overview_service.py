"""Service building aggregated market overview payload."""

from __future__ import annotations

import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from ..config.settings import load_settings
from ..dao import (
    HSGTFundFlowDAO,
    IndexHistoryDAO,
    MarginAccountDAO,
    MarketActivityDAO,
    MarketFundFlowDAO,
    MarketInsightDAO,
    PeripheralInsightDAO,
    RealtimeIndexDAO,
)
from .macro_insight_service import get_latest_macro_insight

_INDEX_CODES = [
    "000001.SH",
    "399001.SZ",
    "399006.SZ",
    "588040.SH",
]

_LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def _serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_LOCAL_TZ)
        else:
            value = value.astimezone(_LOCAL_TZ)
        return value.isoformat()
    try:
        parsed = datetime.fromisoformat(str(value).replace(" ", "T"))
    except ValueError:
        return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_LOCAL_TZ)
    else:
        parsed = parsed.astimezone(_LOCAL_TZ)
    return parsed.isoformat()


def _serialise_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _serialize_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _serialise_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_serialise_value(item) for item in value]
    return value


def _fetch_latest_market_insight(settings) -> Optional[Dict[str, Any]]:
    dao = MarketInsightDAO(settings.postgres)
    record = dao.latest_summary()
    if not record:
        return None

    def _ensure_local(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=_LOCAL_TZ)
        else:
            value = value.astimezone(_LOCAL_TZ)
        return value.isoformat()

    record["generated_at"] = _ensure_local(record.get("generated_at"))
    record["window_start"] = _ensure_local(record.get("window_start"))
    record["window_end"] = _ensure_local(record.get("window_end"))
    return record


def build_market_overview_payload(*, settings_path: Optional[str] = None) -> Dict[str, Any]:
    settings = load_settings(settings_path)

    realtime_dao = RealtimeIndexDAO(settings.postgres)
    history_dao = IndexHistoryDAO(settings.postgres)
    market_fund_flow_dao = MarketFundFlowDAO(settings.postgres)
    hsgt_dao = HSGTFundFlowDAO(settings.postgres)
    margin_dao = MarginAccountDAO(settings.postgres)
    peripheral_dao = PeripheralInsightDAO(settings.postgres)
    activity_dao = MarketActivityDAO(settings.postgres)

    realtime_rows = realtime_dao.list_entries(limit=500)["items"]
    realtime_filtered: List[Dict[str, Any]] = []
    for row in realtime_rows:
        if (row.get("turnover") or 0) <= 5e11:
            continue
        entry = dict(row)
        pct_value = entry.get("change_percent")
        if pct_value is not None:
            try:
                percent = float(pct_value)
            except (TypeError, ValueError):
                percent = None
            if percent is not None:
                entry["change_percent"] = percent / 100.0
            else:
                entry["change_percent"] = None
        realtime_filtered.append(entry)

    index_history: Dict[str, List[Dict[str, Any]]] = {}
    for code in _INDEX_CODES:
        history_rows = history_dao.list_history(index_code=code, limit=10)
        normalised_rows: List[Dict[str, Any]] = []
        for row in history_rows:
            entry = dict(row)
            pct_change = entry.get("pct_change")
            if pct_change is not None:
                try:
                    pct_value = float(pct_change)
                except (TypeError, ValueError):
                    pct_value = None
                if pct_value is not None:
                    entry["pct_change"] = pct_value / 100.0
            for numeric_key in ("open", "close", "high", "low", "volume", "amount", "change_amount", "turnover"):
                value = entry.get(numeric_key)
                if value is None:
                    continue
                try:
                    entry[numeric_key] = float(value)
                except (TypeError, ValueError):
                    pass
            normalised_rows.append(entry)
        index_history[code] = normalised_rows

    market_insight = _fetch_latest_market_insight(settings)
    if market_insight:
        market_insight.pop("referenced_articles", None)
        for key in ("generated_at", "window_start", "window_end"):
            if key in market_insight and market_insight[key] is not None:
                market_insight[key] = _serialize_datetime(market_insight[key])

    macro_insight = get_latest_macro_insight()
    if macro_insight:
        for key in ("generated_at", "updated_at", "created_at"):
            if macro_insight.get(key) is not None:
                macro_insight[key] = _serialize_datetime(macro_insight[key])

    market_fund_flow = market_fund_flow_dao.list_entries(limit=10).get("items", [])
    hsgt_flow = hsgt_dao.list_entries(symbol="北向资金", limit=10).get("items", [])
    margin_stats = margin_dao.list_entries(limit=10).get("items", [])

    peripheral = peripheral_dao.fetch_latest()
    if peripheral:
        metrics = peripheral.get("metrics")
        if isinstance(metrics, str):
            try:
                peripheral["metrics"] = json.loads(metrics)
            except json.JSONDecodeError:
                peripheral["metrics"] = metrics
        for key in ("generated_at", "created_at", "updated_at"):
            if peripheral.get(key) is not None:
                peripheral[key] = _serialize_datetime(peripheral[key])

    activity_rows = activity_dao.list_entries().get("items", [])

    now_local = datetime.now(_LOCAL_TZ)

    payload = {
        "generatedAt": now_local.isoformat(),
        "realtimeIndices": realtime_filtered,
        "indexHistory": index_history,
        "marketInsight": market_insight,
        "macroInsight": macro_insight,
        "marketFundFlow": market_fund_flow,
        "hsgtFundFlow": hsgt_flow,
        "marginAccount": margin_stats,
        "peripheralInsight": peripheral,
        "marketActivity": activity_rows,
        "latestReasoning": None,
    }

    return _serialise_value(payload)
__all__ = ["build_market_overview_payload"]
