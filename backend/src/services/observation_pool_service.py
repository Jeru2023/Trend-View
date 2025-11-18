"""Observation pool generator for strategy-driven watchlist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd

from ..config.runtime_config import ObservationStrategyConfig, load_runtime_config
from ..config.settings import AppSettings, load_settings
from ..dao import BigDealFundFlowDAO, DailyTradeDAO


@dataclass
class StrategyParameters:
    lookback_days: int = 60
    min_history: int = 45
    breakout_buffer: float = 0.005  # 0.5%
    max_range_amplitude: float = 0.15  # 15%
    volume_ratio_threshold: float = 2.0
    volume_average_window: int = 20
    max_weekly_gain: Optional[float] = None
    require_big_deal_inflow: bool = False


@dataclass
class BottomingParameters:
    ma_long: int = 200
    ma_mid: int = 60
    slope_lookback: int = 20
    slope_max_pct: float = 1.0  # percent
    distance_low_min: float = 10.0
    distance_low_max: float = 30.0
    distance_lookback: int = 250
    volume_ma_fast: int = 50
    volume_ma_slow: int = 150
    atr_period: int = 14
    atr_lookback: int = 252
    pulse_window: int = 20
    pulse_multiplier: float = 2.0


@dataclass
class MainRallyParameters:
    ma_fast: int = 50
    ma_mid: int = 120
    ma_long: int = 200
    breakout_window: int = 100
    vol_ma: int = 50
    vol_multiplier: float = 1.5
    atr_period: int = 20
    atr_multiplier: float = 1.5
    rs_window: int = 250


@dataclass
class VolatilityContractionParameters:
    lookback_days: int = 120
    contraction_window: int = 60
    breakout_high_window: int = 20
    min_drawdown_pct: float = 15.0
    atr_period: int = 14
    atr_compare_offset: int = 20
    atr_ratio_threshold: float = 0.75
    volume_ma: int = 50
    volume_multiplier: float = 2.0


def _normalize_float(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return round(numeric, digits)


def _load_trade_window(daily_dao: DailyTradeDAO, settings: AppSettings, lookback_days: int) -> pd.DataFrame:
    latest_date = daily_dao.latest_trade_date(include_intraday=True)
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
               t.pre_close,
               t.vol,
               sb.name,
               sb.symbol
        FROM {schema}.{trade_table} AS t
        JOIN {schema}.{stock_table} AS sb ON sb.ts_code = t.ts_code
        WHERE t.trade_date >= %s
          AND sb.list_status = 'L'
    """
    with daily_dao.connect() as conn:
        daily_dao.ensure_table(conn)
        frame = pd.read_sql_query(query, conn, params=[start_date])
    return frame


WEEK_WINDOW_DAYS = 5


def _detect_range_breakouts(
    frame: pd.DataFrame,
    params: StrategyParameters,
    *,
    big_deal_dao: Optional[BigDealFundFlowDAO] = None,
    trade_date: Optional[date] = None,
) -> List[Dict[str, object]]:
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
        weekly_gain = None
        if params.max_weekly_gain is not None and params.max_weekly_gain >= 0:
            if len(recent) > WEEK_WINDOW_DAYS:
                try:
                    reference_close = recent["close"].iloc[-(WEEK_WINDOW_DAYS + 1)]
                except (IndexError, KeyError):
                    reference_close = None
            else:
                reference_close = None
            if reference_close and reference_close > 0:
                weekly_gain = (last_close - reference_close) / reference_close * 100
                if weekly_gain > params.max_weekly_gain:
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
                "weekly_change": _normalize_float(weekly_gain, 2),
            }
        )
    candidates.sort(key=lambda item: (item.get("volume_ratio") or 0), reverse=True)

    if params.require_big_deal_inflow and big_deal_dao is not None:
        inflow_map = big_deal_dao.fetch_buy_amount_map([c["ts_code"] for c in candidates if c.get("ts_code")], trade_date=trade_date)
        filtered: List[Dict[str, object]] = []
        for item in candidates:
            code = item.get("ts_code")
            stats = inflow_map.get(code) if code else None
            net_amount = stats.get("netAmount") if stats else None
            item["big_deal_net_amount"] = _normalize_float(net_amount, 2)
            if net_amount and net_amount > 0:
                filtered.append(item)
        candidates = filtered

    return candidates


def _compute_true_range(row: pd.Series, prev_close: float) -> Optional[float]:
    high = row.get("high")
    low = row.get("low")
    if high is None or low is None:
        return None
    if prev_close is None:
        return float(high) - float(low)
    return max(float(high) - float(low), abs(float(high) - prev_close), abs(float(low) - prev_close))


def _detect_bottoming_stage(
    frame: pd.DataFrame,
    params: BottomingParameters,
) -> List[Dict[str, object]]:
    if frame.empty:
        return []
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    candidates: List[Dict[str, object]] = []
    grouped = frame.groupby("ts_code", sort=False)
    for ts_code, group in grouped:
        ordered = group.sort_values("trade_date")
        if len(ordered) < max(params.distance_lookback, params.ma_long, params.volume_ma_slow):
            continue
        ordered["ma_long"] = ordered["close"].rolling(params.ma_long, min_periods=params.ma_long // 2).mean()
        ordered["ma_mid"] = ordered["close"].rolling(params.ma_mid, min_periods=params.ma_mid // 2).mean()
        ordered["ma_long_shift"] = ordered["ma_long"].shift(params.slope_lookback)
        ordered["vol_ma_fast"] = ordered["vol"].rolling(params.volume_ma_fast, min_periods=params.volume_ma_fast // 2).mean()
        ordered["vol_ma_slow"] = ordered["vol"].rolling(params.volume_ma_slow, min_periods=params.volume_ma_slow // 2).mean()
        ordered["low_min"] = ordered["low"].rolling(params.distance_lookback, min_periods=params.distance_lookback // 2).min()

        # ATR
        ordered["prev_close"] = ordered["close"].shift(1)
        ordered["true_range"] = ordered.apply(
            lambda row: _compute_true_range(row, row["prev_close"]) if pd.notna(row["close"]) else None, axis=1
        )
        ordered["atr"] = ordered["true_range"].rolling(params.atr_period, min_periods=params.atr_period // 2).mean()
        ordered["atr_pct_rank"] = ordered["atr"].rolling(params.atr_lookback, min_periods=params.atr_period).apply(
            lambda x: pd.Series(x).rank(pct=True).iloc[-1] if len(x) and pd.notna(x.iloc[-1]) else None,
            raw=False,
        )

        last_row = ordered.iloc[-1]
        close = last_row.get("close")
        ma_long = last_row.get("ma_long")
        ma_long_prev = last_row.get("ma_long_shift")
        ma_mid = last_row.get("ma_mid")
        low_min = last_row.get("low_min")
        vol_fast = last_row.get("vol_ma_fast")
        vol_slow = last_row.get("vol_ma_slow")

        if pd.isna(close) or pd.isna(ma_long) or pd.isna(ma_mid) or pd.isna(low_min) or low_min <= 0:
            continue

        # Conditions
        if close <= ma_long:
            # price must be above long MA
            continue
        if pd.isna(ma_long_prev) or ma_long_prev <= 0:
            continue
        slope_pct = ((ma_long - ma_long_prev) / ma_long_prev) * 100
        if slope_pct < 0 or slope_pct > params.slope_max_pct:
            continue
        distance_low_pct = ((close / low_min) - 1.0) * 100
        if distance_low_pct < params.distance_low_min or distance_low_pct > params.distance_low_max:
            continue
        if close >= ma_mid:
            # still want price under mid-term MA
            continue
        if pd.isna(vol_fast) or pd.isna(vol_slow) or vol_slow <= 0 or vol_fast >= vol_slow:
            continue

        # volume pulse count in recent pulse_window
        window = ordered.tail(params.pulse_window)
        pulse_count = 0
        if not window.empty:
            pulse_count = int(
                (
                    (window["vol"] >= (window["vol_ma_fast"] * params.pulse_multiplier))
                    & (window["close"] > window["open"])
                ).sum()
            )

        candidate = {
            "ts_code": ts_code,
            "symbol": ordered["symbol"].iloc[-1] if "symbol" in ordered else None,
            "name": ordered["name"].iloc[-1] if "name" in ordered else None,
            "latest_trade_date": last_row["trade_date"].date().isoformat()
            if pd.notna(last_row["trade_date"])
            else None,
            "close": _normalize_float(close, 2),
            "pct_change": _normalize_float(((close - ma_mid) / ma_mid * 100) if ma_mid else None, 2),
            "volume_ratio": _normalize_float(vol_fast / vol_slow if vol_slow else None, 2),
            "range_amplitude": _normalize_float(distance_low_pct, 2),
            "breakout_level": _normalize_float(ma_long, 2),
            "ma_long": _normalize_float(ma_long, 2),
            "ma_mid": _normalize_float(ma_mid, 2),
            "ma_long_slope_pct": _normalize_float(slope_pct, 2),
            "distance_from_low_pct": _normalize_float(distance_low_pct, 2),
            "vol_fast": _normalize_float(vol_fast, 2),
            "vol_slow": _normalize_float(vol_slow, 2),
            "atr_percentile": _normalize_float(last_row.get("atr_pct_rank") * 100 if pd.notna(last_row.get("atr_pct_rank")) else None, 2),
            "pulse_count": pulse_count,
        }
        candidates.append(candidate)

    candidates.sort(key=lambda item: (item.get("atr_percentile") or 1000, item.get("distance_from_low_pct") or 1000))
    return candidates


def _detect_main_rally(
    frame: pd.DataFrame,
    params: MainRallyParameters,
) -> List[Dict[str, object]]:
    if frame.empty:
        return []
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    candidates: List[Dict[str, object]] = []
    grouped = frame.groupby("ts_code", sort=False)
    for ts_code, group in grouped:
        ordered = group.sort_values("trade_date")
        if len(ordered) < max(params.ma_long, params.breakout_window, params.vol_ma):
            continue
        ordered["ma_fast"] = ordered["close"].rolling(params.ma_fast, min_periods=params.ma_fast // 2).mean()
        ordered["ma_mid"] = ordered["close"].rolling(params.ma_mid, min_periods=params.ma_mid // 2).mean()
        ordered["ma_long"] = ordered["close"].rolling(params.ma_long, min_periods=params.ma_long // 2).mean()
        ordered["ma20"] = ordered["close"].rolling(20, min_periods=10).mean()
        ordered["high_max"] = ordered["high"].rolling(params.breakout_window, min_periods=params.breakout_window // 2).max()

        ordered["prev_close"] = ordered["close"].shift(1)
        ordered["true_range"] = ordered.apply(
            lambda row: _compute_true_range(row, row["prev_close"]) if pd.notna(row["close"]) else None, axis=1
        )
        ordered["atr"] = ordered["true_range"].rolling(params.atr_period, min_periods=params.atr_period // 2).mean()

        ordered["vol_ma"] = ordered["vol"].rolling(params.vol_ma, min_periods=params.vol_ma // 2).mean()

        last_row = ordered.iloc[-1]
        close = last_row.get("close")
        ma_fast = last_row.get("ma_fast")
        ma_mid = last_row.get("ma_mid")
        ma_long = last_row.get("ma_long")
        ma20 = last_row.get("ma20")
        high_max = last_row.get("high_max")
        vol_ma = last_row.get("vol_ma")
        atr = last_row.get("atr")
        vol = last_row.get("vol")

        # basic validity
        if pd.isna(close) or pd.isna(ma_fast) or pd.isna(ma_mid) or pd.isna(ma_long) or pd.isna(high_max):
            continue

        # MA alignment
        if not (ma_fast > ma_mid > ma_long):
            continue
        if not (close > ma_fast and close > ma_mid and close > ma_long):
            continue

        # breakout & momentum
        if pd.isna(high_max) or close < high_max:
            continue
        if pd.notna(ma20) and pd.notna(atr) and close <= ma20 + params.atr_multiplier * atr:
            continue

        # volume confirmation
        if pd.isna(vol_ma) or vol_ma <= 0:
            continue
        if pd.isna(vol) or vol < vol_ma * params.vol_multiplier:
            continue

        candidate = {
            "ts_code": ts_code,
            "symbol": ordered["symbol"].iloc[-1] if "symbol" in ordered else None,
            "name": ordered["name"].iloc[-1] if "name" in ordered else None,
            "latest_trade_date": last_row["trade_date"].date().isoformat()
            if pd.notna(last_row["trade_date"])
            else None,
            "close": _normalize_float(close, 2),
            "pct_change": _normalize_float(((close - ma_fast) / ma_fast * 100) if ma_fast else None, 2),
            "volume_ratio": _normalize_float(vol / vol_ma if vol_ma else None, 2),
            "range_amplitude": None,
            "breakout_level": _normalize_float(high_max, 2),
            "ma_fast": _normalize_float(ma_fast, 2),
            "ma_mid": _normalize_float(ma_mid, 2),
            "ma_long": _normalize_float(ma_long, 2),
            "atr": _normalize_float(atr, 3),
        }
        candidates.append(candidate)

    candidates.sort(key=lambda item: (-(item.get("pct_change") or 0), -(item.get("volume_ratio") or 0)))
    return candidates


def _detect_volatility_contraction(
    frame: pd.DataFrame,
    params: VolatilityContractionParameters,
) -> List[Dict[str, object]]:
    if frame.empty:
        return []
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    candidates: List[Dict[str, object]] = []
    grouped = frame.groupby("ts_code", sort=False)
    for ts_code, group in grouped:
        ordered = group.sort_values("trade_date")
        if len(ordered) < params.lookback_days:
            continue
        scope = ordered.tail(params.lookback_days).copy()
        scope["rolling_high"] = scope["close"].rolling(params.contraction_window, min_periods=params.contraction_window // 2).max()
        scope["drawdown_pct"] = (
            (scope["rolling_high"] - scope["close"]) / scope["rolling_high"]
        ) * 100
        max_drawdown = scope["drawdown_pct"].max(skipna=True)
        if max_drawdown is None or max_drawdown < params.min_drawdown_pct:
            continue

        scope["prev_close"] = scope["close"].shift(1)
        scope["true_range"] = scope.apply(
            lambda row: _compute_true_range(row, row["prev_close"]) if pd.notna(row["close"]) else None,
            axis=1,
        )
        scope["atr"] = scope["true_range"].rolling(params.atr_period, min_periods=params.atr_period // 2).mean()
        scope["vol_ma"] = scope["vol"].rolling(params.volume_ma, min_periods=params.volume_ma // 2).mean()
        scope["breakout_high"] = scope["high"].rolling(params.breakout_high_window, min_periods=5).max().shift(1)

        last_row = scope.iloc[-1]
        close = last_row.get("close")
        vol = last_row.get("vol")
        atr = last_row.get("atr")
        dropout_high = last_row.get("breakout_high")
        vol_ma_latest = last_row.get("vol_ma")

        if pd.isna(close) or pd.isna(dropout_high) or pd.isna(vol_ma_latest) or vol_ma_latest <= 0:
            continue

        atr_compare = scope["atr"].shift(params.atr_compare_offset).iloc[-1] if len(scope) > params.atr_compare_offset else None
        if pd.isna(atr) or pd.isna(atr_compare) or atr >= atr_compare * params.atr_ratio_threshold:
            continue

        recent_vol_avg = scope["vol"].tail(5).mean()
        if pd.isna(recent_vol_avg) or recent_vol_avg >= vol_ma_latest:
            continue

        if close <= dropout_high or vol < vol_ma_latest * params.volume_multiplier:
            continue

        candidate = {
            "ts_code": ts_code,
            "symbol": scope["symbol"].iloc[-1] if "symbol" in scope else None,
            "name": scope["name"].iloc[-1] if "name" in scope else None,
            "latest_trade_date": last_row["trade_date"].date().isoformat()
            if pd.notna(last_row["trade_date"])
            else None,
            "close": _normalize_float(close, 2),
            "pct_change": None,
            "volume_ratio": _normalize_float(vol / vol_ma_latest if vol_ma_latest else None, 2),
            "range_amplitude": _normalize_float(max_drawdown, 2),
            "breakout_level": _normalize_float(dropout_high, 2),
            "atr_reduction_pct": _normalize_float((atr / atr_compare) * 100 if atr_compare else None, 2),
            "vol_contraction_ratio": _normalize_float(recent_vol_avg / vol_ma_latest if vol_ma_latest else None, 2),
        }
        candidates.append(candidate)

    candidates.sort(
        key=lambda item: (
            (item.get("vol_contraction_ratio") or 1),
            -(item.get("volume_ratio") or 0),
        )
    )
    return candidates


def generate_observation_pool(*, settings_path: Optional[str] = None) -> Dict[str, object]:
    settings = load_settings(settings_path)
    daily_dao = DailyTradeDAO(settings.postgres)
    runtime_config = load_runtime_config()
    range_params = StrategyParameters(
        lookback_days=runtime_config.observation_strategy_config.lookback_days,
        min_history=runtime_config.observation_strategy_config.min_history,
        breakout_buffer=max(runtime_config.observation_strategy_config.breakout_buffer_percent / 100, 0.0),
        max_range_amplitude=max(runtime_config.observation_strategy_config.max_range_percent / 100, 0.0001),
        volume_ratio_threshold=runtime_config.observation_strategy_config.volume_ratio_threshold,
        volume_average_window=runtime_config.observation_strategy_config.volume_average_window,
        max_weekly_gain=runtime_config.observation_strategy_config.max_weekly_gain_percent,
        require_big_deal_inflow=runtime_config.observation_strategy_config.require_big_deal_inflow,
    )
    bottoming_params = BottomingParameters()
    main_rally_params = MainRallyParameters()
    vcp_params = VolatilityContractionParameters()

    frame = _load_trade_window(
        daily_dao,
        settings,
        max(
            range_params.lookback_days,
            bottoming_params.distance_lookback,
            main_rally_params.rs_window,
            vcp_params.lookback_days,
        ),
    )
    universe_total = frame["ts_code"].nunique() if not frame.empty else 0
    big_deal_dao = BigDealFundFlowDAO(settings.postgres) if range_params.require_big_deal_inflow else None
    latest_date = daily_dao.latest_trade_date(include_intraday=False)
    if isinstance(latest_date, datetime):
        trade_day = latest_date.date()
    else:
        trade_day = latest_date
    range_candidates = _detect_range_breakouts(
        frame,
        range_params,
        big_deal_dao=big_deal_dao,
        trade_date=trade_day,
    )
    bottoming_candidates = _detect_bottoming_stage(frame, bottoming_params)
    main_rally_candidates = _detect_main_rally(frame, main_rally_params)
    vcp_candidates = _detect_volatility_contraction(frame, vcp_params)

    now_ts = datetime.now(timezone.utc).isoformat()
    summary_notes: List[str] = []
    if range_candidates:
        summary_notes.append(
            f"盘整突破策略共发现 {len(range_candidates)} 只个股，平均放量 {_normalize_float(pd.Series([c['volume_ratio'] for c in range_candidates]).mean(), 2)} 倍。"
        )
    if bottoming_candidates:
        summary_notes.append(f"底部构筑策略捕捉到 {len(bottoming_candidates)} 只个股，侧重长期均线走平与地量特征。")
    if main_rally_candidates:
        summary_notes.append(
            f"主升浪策略找到 {len(main_rally_candidates)} 只个股，均线多头排列且放量突破新高。"
        )
    if vcp_candidates:
        summary_notes.append(f"波动收缩策略捕捉到 {len(vcp_candidates)} 只即将突破的收敛形态。")

    strategy_payload = {
        "id": "range_breakout",
        "name": "盘整突破",
        "description": "",
        "parameters": {
            "lookbackDays": range_params.lookback_days,
            "minHistoryDays": range_params.min_history,
            "breakoutBufferPercent": round(range_params.breakout_buffer * 100, 3),
            "maxRangePercent": range_params.max_range_amplitude * 100,
            "volumeRatio": range_params.volume_ratio_threshold,
            "volumeAverageWindow": range_params.volume_average_window,
            "maxWeeklyGainPercent": range_params.max_weekly_gain,
            "requireBigDealInflow": range_params.require_big_deal_inflow,
        },
        "candidate_count": len(range_candidates),
        "candidates": range_candidates[:100],
    }

    bottoming_payload = {
        "id": "bottoming_stage1",
        "name": "底部构筑",
        "description": "",
        "parameters": {
            "longMaDays": bottoming_params.ma_long,
            "midMaDays": bottoming_params.ma_mid,
            "distanceFromLowMin": bottoming_params.distance_low_min,
            "distanceFromLowMax": bottoming_params.distance_low_max,
            "volumeCompressionRatio": round(bottoming_params.volume_ma_fast / bottoming_params.volume_ma_slow, 2),
            "atrLookback": bottoming_params.atr_lookback,
        },
        "candidate_count": len(bottoming_candidates),
        "candidates": bottoming_candidates[:100],
    }
    main_rally_payload = {
        "id": "main_rally_stage2",
        "name": "主升浪",
        "description": "",
        "parameters": {
            "maFast": main_rally_params.ma_fast,
            "maMid": main_rally_params.ma_mid,
            "maLong": main_rally_params.ma_long,
            "breakoutWindow": main_rally_params.breakout_window,
            "volMa": main_rally_params.vol_ma,
            "volMultiplier": main_rally_params.vol_multiplier,
        },
        "candidate_count": len(main_rally_candidates),
        "candidates": main_rally_candidates[:100],
    }
    vcp_payload = {
        "id": "volatility_contraction",
        "name": "波动收缩",
        "description": "",
        "parameters": {
            "lookbackDays": vcp_params.lookback_days,
            "contractionWindow": vcp_params.contraction_window,
            "breakoutWindow": vcp_params.breakout_high_window,
            "minDrawdownPct": vcp_params.min_drawdown_pct,
            "atrCompareOffset": vcp_params.atr_compare_offset,
            "atrRatioThreshold": vcp_params.atr_ratio_threshold,
            "volumeMa": vcp_params.volume_ma,
            "volumeMultiplier": vcp_params.volume_multiplier,
        },
        "candidate_count": len(vcp_candidates),
        "candidates": vcp_candidates[:100],
    }

    return {
        "generated_at": now_ts,
        "latest_trade_date": latest_date.isoformat() if latest_date else None,
        "universe_total": universe_total,
        "total_candidates": len(range_candidates),
        "summary_notes": summary_notes,
        "strategies": [strategy_payload, bottoming_payload, main_rally_payload, vcp_payload],
    }
