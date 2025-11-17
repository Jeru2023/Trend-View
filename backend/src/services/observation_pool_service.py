"""Observation pool generator for strategy-driven watchlist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd

from ..config.settings import AppSettings, load_settings
from ..dao import DailyTradeDAO

@dataclass
class StrategyParameters:
    lookback_days: int = 60
    min_history: int = 45
    breakout_buffer: float = 0.005  # 0.5%
    max_range_amplitude: float = 0.15  # 15%
    volume_ratio_threshold: float = 2.0
    volume_average_window: int = 20


def _normalize_float(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return round(numeric, digits)


def _load_trade_window(daily_dao: DailyTradeDAO, settings: AppSettings, lookback_days: int) -> pd.DataFrame:
    latest_date = daily_dao.latest_trade_date(include_intraday=False)
    if latest_date is None:
        return pd.DataFrame()
    start_date = latest_date - timedelta(days=lookback_days * 2)
    schema = settings.postgres.schema
    trade_table = daily_dao._table_name
    stock_table = settings.postgres.stock_table
    query = f"""
        SELECT t.ts_code,
               t.trade_date,
               t.open,
               t.high,
               t.low,
               t.close,
               t.vol,
               sb.name,
               sb.symbol
        FROM {schema}.{trade_table} AS t
        JOIN {schema}.{stock_table} AS sb ON sb.ts_code = t.ts_code
        WHERE t.is_intraday = FALSE
          AND t.trade_date >= %s
          AND sb.list_status = 'L'
    """
    with daily_dao.connect() as conn:
        daily_dao.ensure_table(conn)
        frame = pd.read_sql_query(query, conn, params=[start_date])
    return frame


def _detect_range_breakouts(frame: pd.DataFrame, params: StrategyParameters) -> List[Dict[str, object]]:
    if frame.empty:
        return []
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    candidates: List[Dict[str, object]] = []
    grouped = frame.groupby("ts_code", sort=False)
    for ts_code, group in grouped:
        ordered = group.sort_values("trade_date")
        if len(ordered) < params.min_history:
            continue
        recent = ordered.tail(params.lookback_days)
        if len(recent) < params.min_history:
            continue
        low_min = recent["low"].min(skipna=True)
        high_max = recent["high"].max(skipna=True)
        if pd.isna(low_min) or pd.isna(high_max) or low_min <= 0:
            continue
        amplitude = (high_max - low_min) / low_min
        if amplitude > params.max_range_amplitude:
            continue
        last_row = recent.iloc[-1]
        prev_high_series = recent["high"].iloc[:-1]
        if prev_high_series.empty:
            continue
        prev_high = prev_high_series.max()
        if pd.isna(prev_high) or prev_high <= 0:
            continue
        breakout_price = prev_high
        last_close = last_row["close"]
        if pd.isna(last_close) or last_close <= 0:
            continue
        breakout = last_close >= breakout_price * (1 + params.breakout_buffer)
        if not breakout:
            continue
        prev_vol = recent["vol"].iloc[:-1].tail(params.volume_average_window)
        avg_vol = prev_vol.mean(skipna=True)
        last_vol = last_row["vol"]
        if pd.isna(avg_vol) or avg_vol <= 0 or pd.isna(last_vol):
            continue
        volume_ratio = last_vol / avg_vol
        if volume_ratio < params.volume_ratio_threshold:
            continue
        prev_close = recent["close"].iloc[-2] if len(recent) >= 2 else None
        pct_change = None
        if prev_close and prev_close != 0:
            pct_change = (last_close - prev_close) / prev_close * 100
        candidates.append(
            {
                "ts_code": ts_code,
                "symbol": ordered["symbol"].iloc[-1] if "symbol" in ordered else None,
                "name": ordered["name"].iloc[-1] if "name" in ordered else None,
                "latest_trade_date": last_row["trade_date"].date().isoformat()
                if pd.notna(last_row["trade_date"])
                else None,
                "close": _normalize_float(last_close, 2),
                "pct_change": _normalize_float(pct_change, 2),
                "volume_ratio": _normalize_float(volume_ratio, 2),
                "range_amplitude": _normalize_float(amplitude * 100, 2),
                "range_high": _normalize_float(high_max, 2),
                "range_low": _normalize_float(low_min, 2),
                "breakout_level": _normalize_float(breakout_price, 2),
            }
        )
    candidates.sort(key=lambda item: (item.get("volume_ratio") or 0), reverse=True)
    return candidates


def generate_observation_pool(*, settings_path: Optional[str] = None) -> Dict[str, object]:
    settings = load_settings(settings_path)
    daily_dao = DailyTradeDAO(settings.postgres)
    params = StrategyParameters()
    frame = _load_trade_window(daily_dao, settings, params.lookback_days)
    universe_total = frame["ts_code"].nunique() if not frame.empty else 0
    candidates = _detect_range_breakouts(frame, params)
    total_candidates = len(candidates)
    now_ts = datetime.now(timezone.utc).isoformat()
    latest_date = daily_dao.latest_trade_date(include_intraday=False)
    summary_notes: List[str] = []
    if total_candidates:
        summary_notes.append(f"盘整突破策略共发现 {total_candidates} 只个股，平均放量 {_normalize_float(pd.Series([c['volume_ratio'] for c in candidates]).mean(), 2)} 倍。")
    else:
        summary_notes.append("暂无个股满足盘整突破条件，保持关注。")

    strategy_payload = {
        "id": "range_breakout",
        "name": "盘整突破",
        "description": "扫描近60日横盘区间并放量突破上沿的个股。",
        "parameters": {
            "lookbackDays": params.lookback_days,
            "maxRangePercent": params.max_range_amplitude * 100,
            "volumeRatio": params.volume_ratio_threshold,
        },
        "candidate_count": total_candidates,
        "candidates": candidates[:100],
    }

    return {
        "generated_at": now_ts,
        "latest_trade_date": latest_date.isoformat() if latest_date else None,
        "universe_total": universe_total,
        "total_candidates": total_candidates,
        "summary_notes": summary_notes,
        "strategies": [strategy_payload],
    }
