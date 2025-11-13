"""Client helpers for Yahoo Finance chart endpoints."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_YAHOO_BASE_URL = "https://query1.finance.yahoo.com"
_UTC = timezone.utc


def _build_headers() -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (compatible; TrendViewBot/1.0; +https://github.com/Jeru2023/Trend-View)",
        "Accept": "application/json",
    }


def _to_timestamp(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=_UTC)
    else:
        value = value.astimezone(_UTC)
    return int(value.timestamp())


def _chart_endpoint(symbol: str) -> str:
    safe_symbol = symbol.strip()
    if not safe_symbol:
        raise ValueError("symbol must be non-empty")
    return f"{_YAHOO_BASE_URL}/v8/finance/chart/{safe_symbol}"


def _empty_chart_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adjclose",
            "currency",
            "exchange_timezone",
        ]
    )


def _parse_chart_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    result = (payload or {}).get("chart", {}).get("result")
    if not result:
        return _empty_chart_frame()

    chart = result[0]
    timestamps = chart.get("timestamp") or []
    indicators = chart.get("indicators", {})
    quote = indicators.get("quote", [{}])[0]
    adjclose = (indicators.get("adjclose") or [{}])[0].get("adjclose")
    meta = chart.get("meta") or {}

    rows = []
    for idx, ts in enumerate(timestamps):
        try:
            trade_dt = datetime.fromtimestamp(ts, tz=_UTC)
        except (TypeError, ValueError):
            continue
        row = {
            "trade_date": trade_dt.date(),
            "open": _get_series_value(quote.get("open"), idx),
            "high": _get_series_value(quote.get("high"), idx),
            "low": _get_series_value(quote.get("low"), idx),
            "close": _get_series_value(quote.get("close"), idx),
            "volume": _get_series_value(quote.get("volume"), idx),
            "adjclose": _get_series_value(adjclose, idx) if adjclose is not None else None,
            "currency": meta.get("currency"),
            "exchange_timezone": meta.get("exchangeTimezoneName"),
        }
        if row["close"] is None:
            continue
        rows.append(row)

    return pd.DataFrame(rows)


def _request_chart(symbol: str, params: Dict[str, Any]) -> pd.DataFrame:
    url = _chart_endpoint(symbol)
    try:
        response = requests.get(url, params=params, headers=_build_headers(), timeout=15)
        response.raise_for_status()
        return _parse_chart_payload(response.json())
    except Exception as exc:  # pragma: no cover - network/IO
        logger.error("Yahoo Finance request failed for %s: %s", symbol, exc)
        return _empty_chart_frame()


def fetch_yahoo_daily_history(
    symbol: str,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    max_lookback_days: int = 400,
) -> pd.DataFrame:
    """Fetch daily historical bars for the given symbol using explicit timestamps."""

    params: Dict[str, Any] = {
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,split",
    }

    if start is not None:
        params["period1"] = _to_timestamp(start)
    else:
        fallback_start = datetime.now(tz=_UTC) - timedelta(days=max_lookback_days)
        params["period1"] = _to_timestamp(fallback_start)

    if end is not None:
        params["period2"] = _to_timestamp(end)
    else:
        params["period2"] = int(time.time())

    frame = _request_chart(symbol, params)
    if frame.empty:
        return frame
    return frame


def fetch_yahoo_daily_history_range(
    symbol: str,
    *,
    range_period: str = "5y",
    interval: str = "1d",
) -> pd.DataFrame:
    """Fetch daily history using Yahoo's ``range`` parameter (useful when period bounds fail)."""

    params: Dict[str, Any] = {
        "interval": interval,
        "range": range_period,
        "includePrePost": "false",
        "events": "div,split",
    }
    return _request_chart(symbol, params)


def _get_series_value(series: Any, index: int) -> Optional[float]:
    if series is None:
        return None
    try:
        value = series[index]
    except (TypeError, IndexError):
        return None
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
