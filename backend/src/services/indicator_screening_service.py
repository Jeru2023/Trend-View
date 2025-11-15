"""Services for indicator-based stock screening (e.g.,持续放量/量价齐升)."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, time
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from zoneinfo import ZoneInfo

from ..api_clients import (
    DAILY_TRADE_FIELDS,
    fetch_stock_rank_cxfl_ths,
    fetch_stock_rank_ljqs_ths,
    fetch_stock_rank_xstp_ths,
    fetch_stock_rank_lxsz_ths,
    get_realtime_quotes,
)
from ..config.runtime_config import VolumeSurgeConfig, load_runtime_config
from ..config.settings import load_settings
from ..dao import (
    BigDealFundFlowDAO,
    DailyIndicatorDAO,
    DailyTradeDAO,
    DailyTradeMetricsDAO,
    FundamentalMetricsDAO,
    IndicatorScreeningDAO,
    StockBasicDAO,
)
from .intraday_volume_profile_service import (
    estimate_full_day_volume,
    load_average_profile_map,
)
from .daily_trade_metrics_service import recompute_trade_metrics_for_codes

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

CONTINUOUS_VOLUME_CODE = "continuous_volume"
CONTINUOUS_VOLUME_NAME = "持续放量"
VOLUME_PRICE_RISE_CODE = "volume_price_rise"
VOLUME_PRICE_RISE_NAME = "量价齐升"
UPWARD_BREAKOUT_CODE = "upward_breakout"
UPWARD_BREAKOUT_NAME = "向上突破"
UPWARD_BREAKOUT_SYMBOL = "500日均线"
CONTINUOUS_RISE_CODE = "continuous_rise"
CONTINUOUS_RISE_NAME = "连续上涨"
VOLUME_SURGE_BREAKOUT_CODE = "volume_surge_breakout"
VOLUME_SURGE_BREAKOUT_NAME = "爆量启动"
BIG_DEAL_INDICATOR_CODE = "big_deal_inflow"
BIG_DEAL_INDICATOR_NAME = "当日大单净流入"

DEFAULT_INDICATOR_CODE = CONTINUOUS_VOLUME_CODE
MAX_INTERSECTION_FETCH = 2000
MAX_SINGLE_INDICATOR_FETCH = 5000

FINAL_SYNC_CUTOFF = time(16, 0)

VOLUME_SURGE_FETCH_DAYS = 90
VOLUME_SURGE_CONSOLIDATION_WINDOW = 30
VOLUME_SURGE_MIN_HISTORY = 35
VOLUME_SURGE_VOLUME_WINDOW = 20
VOLUME_SURGE_MIN_VOLUME_RATIO = 3.0
VOLUME_SURGE_BREAKOUT_THRESHOLD = 0.03
VOLUME_SURGE_DAILY_CHANGE_THRESHOLD = 7.0
VOLUME_SURGE_RANGE_LIMIT = 0.25


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    numeric: float
    if isinstance(value, (int, float)):
        numeric = float(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        text = text.replace("%", "").replace(",", "")
        try:
            numeric = float(text)
        except (ValueError, OverflowError):
            return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _round_decimal(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _parse_volume_text(text: Any) -> tuple[float | None, str | None]:
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
    return float(numeric * multiplier), date_note


def _parse_amount_text(text: Any) -> float | None:
    if not text:
        return None
    raw = str(text).strip()
    if not raw:
        return None
    multiplier = 1.0
    for suffix, factor in (("亿", 100000000.0), ("万", 10000.0)):
        if raw.endswith(suffix):
            multiplier = factor
            raw = raw[: -len(suffix)]
            break
    raw = raw.replace(",", "")
    numeric = _safe_float(raw)
    if numeric is None:
        return None
    return numeric * multiplier


def _format_stock_code_full(code: str) -> str:
    if not code:
        return ""
    cleaned = code.strip().upper()
    if len(cleaned) != 6:
        return cleaned
    if cleaned.startswith(("6", "9", "5")):
        return f"{cleaned}.SH"
    return f"{cleaned}.SZ"


def _normalize_ts_code(code: str) -> str | None:
    if not code:
        return None
    text = code.strip().upper()
    if not text:
        return None
    if "." in text:
        symbol, suffix = text.split(".", 1)
        symbol = symbol.strip().zfill(6)
        suffix = suffix.strip()[:2]
        return f"{symbol}.{suffix}" if symbol else None
    digits = "".join(filter(str.isdigit, text))
    if not digits:
        return None
    digits = digits[-6:].zfill(6)
    if digits.startswith(("4", "8")):
        suffix = "BJ"
    elif digits.startswith(("6", "9", "5")):
        suffix = "SH"
    else:
        suffix = "SZ"
    return f"{digits}.{suffix}"


def _format_volume_label(value: Any) -> str | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    absolute = abs(numeric)
    if absolute >= 100000000:
        return f"{numeric / 100000000:.2f}亿股"
    if absolute >= 10000:
        return f"{numeric / 10000:.2f}万股"
    return f"{numeric:.0f}股"


def _format_amount_label(value: Any) -> str | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    absolute = abs(numeric)
    if absolute >= 100000000:
        return f"{numeric / 100000000:.2f}亿"
    if absolute >= 10000:
        return f"{numeric / 10000:.2f}万"
    return f"{numeric:.0f}"


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
def _normalize_volume_price_rise_frame(dataframe: pd.DataFrame, captured_at: datetime) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.copy()
    frame["rank"] = pd.to_numeric(frame.get("rank"), errors="coerce")
    frame["stock_code"] = frame.get("stock_code", "").astype(str).str.zfill(6)
    frame["stock_code_full"] = frame["stock_code"].apply(_format_stock_code_full)
    frame["last_price"] = frame["last_price"].apply(_safe_float)
    frame["volume_days"] = pd.to_numeric(frame.get("volume_days"), errors="coerce").astype("Int64")
    frame["stage_change_percent"] = frame["stage_change_percent"].apply(_safe_float)
    frame["turnover_percent"] = frame.get("turnover_percent", None)
    frame["turnover_percent"] = frame["turnover_percent"].apply(_safe_float)

    frame["indicator_code"] = VOLUME_PRICE_RISE_CODE
    frame["indicator_name"] = VOLUME_PRICE_RISE_NAME
    frame["captured_at"] = captured_at
    return frame.reindex(columns=INDICATOR_SCREENING_COLUMNS_FRAME)


def _normalize_upward_breakout_frame(dataframe: pd.DataFrame, captured_at: datetime) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.copy()
    frame["rank"] = pd.to_numeric(frame.get("rank"), errors="coerce")
    frame["stock_code"] = frame.get("stock_code", "").astype(str).str.zfill(6)
    frame["stock_code_full"] = frame["stock_code"].apply(_format_stock_code_full)
    frame["last_price"] = frame["last_price"].apply(_safe_float)
    frame["price_change_percent"] = frame.get("price_change_percent").apply(_safe_float)
    frame["turnover_rate"] = frame.get("turnover_rate").apply(_safe_float)

    volume_values = frame.get("volume_text")
    frame["volume_shares"] = volume_values.apply(lambda x: _parse_volume_text(x)[0])
    frame["baseline_volume_text"] = None
    frame["baseline_volume_shares"] = None
    frame["volume_days"] = None
    frame["turnover_percent"] = None

    frame["turnover_amount_text"] = frame.get("turnover_amount_text")
    frame["turnover_amount"] = frame["turnover_amount_text"].apply(_parse_amount_text)

    frame["indicator_code"] = UPWARD_BREAKOUT_CODE
    frame["indicator_name"] = UPWARD_BREAKOUT_NAME
    frame["captured_at"] = captured_at
    return frame.reindex(columns=INDICATOR_SCREENING_COLUMNS_FRAME)


def _normalize_continuous_rise_frame(dataframe: pd.DataFrame, captured_at: datetime) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.copy()
    frame["rank"] = pd.to_numeric(frame.get("rank"), errors="coerce")
    frame["stock_code"] = frame.get("stock_code", "").astype(str).str.zfill(6)
    frame["stock_code_full"] = frame["stock_code"].apply(_format_stock_code_full)
    frame["last_price"] = frame["last_price"].apply(_safe_float)
    frame["high_price"] = frame.get("high_price").apply(_safe_float)
    frame["low_price"] = frame.get("low_price").apply(_safe_float)
    frame["volume_days"] = pd.to_numeric(frame.get("volume_days"), errors="coerce").astype("Int64")
    frame["stage_change_percent"] = frame.get("stage_change_percent").apply(_safe_float)
    frame["turnover_percent"] = frame.get("turnover_percent").apply(_safe_float)
    if "net_income_yoy_latest" in frame.columns:
        frame["net_income_yoy_latest"] = frame["net_income_yoy_latest"].apply(_safe_float)
    else:
        frame["net_income_yoy_latest"] = pd.Series([None] * len(frame))
    if "net_income_qoq_latest" in frame.columns:
        frame["net_income_qoq_latest"] = frame["net_income_qoq_latest"].apply(_safe_float)
    else:
        frame["net_income_qoq_latest"] = pd.Series([None] * len(frame))
    frame["pe_ratio"] = frame.get("pe_ratio").apply(_safe_float) if "pe_ratio" in frame.columns else pd.Series([None] * len(frame))
    frame["volume_text"] = None
    frame["volume_shares"] = None
    frame["baseline_volume_text"] = None
    frame["baseline_volume_shares"] = None
    frame["turnover_rate"] = None
    frame["turnover_amount"] = None
    frame["turnover_amount_text"] = None

    frame["indicator_code"] = CONTINUOUS_RISE_CODE
    frame["indicator_name"] = CONTINUOUS_RISE_NAME
    frame["captured_at"] = captured_at
    return frame.reindex(columns=INDICATOR_SCREENING_COLUMNS_FRAME)


def _normalize_volume_surge_breakout_frame(dataframe: pd.DataFrame, captured_at: datetime) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.copy()
    frame["stock_code"] = frame.get("stock_code", "").astype(str).str.zfill(6)
    frame["stock_code_full"] = frame.get("stock_code_full")
    frame.loc[frame["stock_code_full"].isna(), "stock_code_full"] = frame.loc[
        frame["stock_code_full"].isna(), "stock_code"
    ].apply(_format_stock_code_full)

    frame["rank"] = pd.to_numeric(frame.get("rank"), errors="coerce")
    frame["price_change_percent"] = frame.get("price_change_percent").apply(_safe_float)
    frame["stage_change_percent"] = frame.get("stage_change_percent").apply(_safe_float)
    frame["last_price"] = frame.get("last_price").apply(_safe_float)

    frame["volume_shares"] = frame.get("last_volume").apply(_safe_float)
    frame["baseline_volume_shares"] = frame.get("avg_volume").apply(_safe_float)
    frame["volume_text"] = frame.get("volume_ratio").apply(
        lambda value: f"{value:.2f}x" if value is not None and math.isfinite(value) else None
    )
    frame["baseline_volume_text"] = frame.get("avg_volume").apply(_format_volume_label)
    frame["volume_days"] = pd.to_numeric(frame.get("volume_days"), errors="coerce").astype("Int64")

    frame["turnover_percent"] = frame.get("volume_ratio").apply(_safe_float)
    frame["turnover_rate"] = frame.get("breakout_percent").apply(_safe_float)
    frame["turnover_amount_text"] = frame.get("range_percent").apply(
        lambda value: f"振幅 {value:.1f}%" if value is not None and math.isfinite(value) else None
    )

    frame["indicator_code"] = VOLUME_SURGE_BREAKOUT_CODE
    frame["indicator_name"] = VOLUME_SURGE_BREAKOUT_NAME
    frame["captured_at"] = captured_at
    frame["volume_shares"] = frame["volume_shares"].apply(_safe_float)
    frame["baseline_volume_shares"] = frame["baseline_volume_shares"].apply(_safe_float)

    return frame.reindex(columns=INDICATOR_SCREENING_COLUMNS_FRAME)


def _fetch_big_deal_indicator_frame(settings) -> pd.DataFrame:
    dao = BigDealFundFlowDAO(settings.postgres)
    trade_day = datetime.now(LOCAL_TZ).date()
    dataset = dao.list_positive_inflows(trade_date=trade_day, limit=MAX_SINGLE_INDICATOR_FETCH, offset=0)
    items = dataset.get("items", [])
    if not items:
        return pd.DataFrame(
            columns=[
                "rank",
                "stock_code",
                "stock_code_full",
                "stock_name",
                "volume_days",
                "big_deal_net_amount",
                "big_deal_buy_amount",
                "big_deal_sell_amount",
                "big_deal_net_text",
                "big_deal_buy_text",
                "big_deal_sell_text",
            ]
        )

    records: list[dict[str, Any]] = []
    for idx, entry in enumerate(items, start=1):
        code = str(entry.get("stock_code") or "").zfill(6)
        code_full = _format_stock_code_full(code)
        net_amount = _safe_float(entry.get("net_amount"))
        buy_amount = _safe_float(entry.get("buy_amount"))
        sell_amount = _safe_float(entry.get("sell_amount"))
        trade_count = entry.get("trade_count")
        records.append(
            {
                "rank": idx,
                "stock_code": code,
                "stock_code_full": code_full,
                "stock_name": entry.get("stock_name"),
                "industry": None,
                "volume_days": trade_count,
                "big_deal_net_amount": net_amount,
                "big_deal_buy_amount": buy_amount,
                "big_deal_sell_amount": sell_amount,
                "big_deal_net_text": _format_amount_label(net_amount),
                "big_deal_buy_text": _format_amount_label(buy_amount),
                "big_deal_sell_text": _format_amount_label(sell_amount),
            }
        )

    codes_full = [record["stock_code_full"] for record in records if record.get("stock_code_full")]
    if codes_full:
        trade_dao = DailyTradeDAO(settings.postgres)
        trade_metrics = trade_dao.fetch_latest_metrics(codes_full, include_intraday=True)
        stock_dao = StockBasicDAO(settings.postgres)
        fundamentals = stock_dao.query_fundamentals(
            include_st=True,
            include_delisted=True,
            limit=len(codes_full),
            offset=0,
            codes=codes_full,
        )
        industry_map = {
            item.get("code"): item.get("industry")
            for item in fundamentals.get("items", [])
            if item.get("code")
        }
        for record in records:
            code_full = record.get("stock_code_full")
            metrics = trade_metrics.get(code_full or "")
            if metrics:
                record["last_price"] = metrics.get("last_price")
                record["price_change_percent"] = metrics.get("pct_chg")
            industry = industry_map.get(code_full or "")
            if industry:
                record["industry"] = industry

    return pd.DataFrame.from_records(records)


def _normalize_big_deal_inflow_frame(dataframe: pd.DataFrame, captured_at: datetime) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.copy()
    frame["indicator_code"] = BIG_DEAL_INDICATOR_CODE
    frame["indicator_name"] = BIG_DEAL_INDICATOR_NAME
    frame["captured_at"] = captured_at
    frame["rank"] = pd.to_numeric(frame.get("rank"), errors="coerce")
    frame["stock_code"] = frame.get("stock_code", "").astype(str).str.zfill(6)
    frame["stock_code_full"] = frame["stock_code"].apply(_format_stock_code_full)
    frame["stock_name"] = frame.get("stock_name")
    if "price_change_percent" in frame.columns:
        frame["price_change_percent"] = frame["price_change_percent"].apply(_safe_float)
    else:
        frame["price_change_percent"] = None
    frame["stage_change_percent"] = None
    if "last_price" in frame.columns:
        frame["last_price"] = frame["last_price"].apply(_safe_float)
    else:
        frame["last_price"] = None

    frame["volume_days"] = pd.to_numeric(frame.get("volume_days"), errors="coerce").astype("Int64")
    frame["volume_shares"] = frame.get("big_deal_buy_amount").apply(_safe_float)
    frame["baseline_volume_shares"] = frame.get("big_deal_sell_amount").apply(_safe_float)
    frame["volume_text"] = frame.get("big_deal_buy_text")
    frame["baseline_volume_text"] = frame.get("big_deal_sell_text")

    frame["turnover_percent"] = None
    frame["turnover_rate"] = None
    frame["turnover_amount"] = frame.get("big_deal_net_amount").apply(_safe_float)
    frame["turnover_amount_text"] = frame.get("big_deal_net_text")

    frame["industry"] = frame.get("industry")
    frame["high_price"] = None
    frame["low_price"] = None

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
    "turnover_percent",
    "turnover_rate",
    "turnover_amount",
    "turnover_amount_text",
    "industry",
    "high_price",
    "low_price",
]


INDICATOR_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    CONTINUOUS_VOLUME_CODE: {
        "name": CONTINUOUS_VOLUME_NAME,
        "fetcher": lambda _settings: fetch_stock_rank_cxfl_ths(),
        "normalizer": _normalize_continuous_volume_frame,
    },
    VOLUME_PRICE_RISE_CODE: {
        "name": VOLUME_PRICE_RISE_NAME,
        "fetcher": lambda _settings: fetch_stock_rank_ljqs_ths(),
        "normalizer": _normalize_volume_price_rise_frame,
    },
    UPWARD_BREAKOUT_CODE: {
        "name": UPWARD_BREAKOUT_NAME,
        "fetcher": lambda _settings: fetch_stock_rank_xstp_ths(symbol=UPWARD_BREAKOUT_SYMBOL),
        "normalizer": _normalize_upward_breakout_frame,
    },
    CONTINUOUS_RISE_CODE: {
        "name": CONTINUOUS_RISE_NAME,
        "fetcher": lambda _settings: fetch_stock_rank_lxsz_ths(),
        "normalizer": _normalize_continuous_rise_frame,
    },
    VOLUME_SURGE_BREAKOUT_CODE: {
        "name": VOLUME_SURGE_BREAKOUT_NAME,
        "fetcher": lambda settings: _fetch_volume_surge_breakout_candidates(settings),
        "normalizer": _normalize_volume_surge_breakout_frame,
    },
    BIG_DEAL_INDICATOR_CODE: {
        "name": BIG_DEAL_INDICATOR_NAME,
        "fetcher": lambda settings: _fetch_big_deal_indicator_frame(settings),
        "normalizer": _normalize_big_deal_inflow_frame,
    },
}


def _fetch_volume_surge_breakout_candidates(settings) -> pd.DataFrame:
    runtime_config = load_runtime_config()
    surge_config = getattr(runtime_config, "volume_surge_config", None)
    if surge_config is None:
        surge_config = VolumeSurgeConfig()
    stock_basic_dao = StockBasicDAO(settings.postgres)
    fundamentals = stock_basic_dao.query_fundamentals(
        include_delisted=False,
        include_st=False,
        limit=0,
    )
    items = fundamentals.get("items", [])
    metadata = {item["code"]: item for item in items if item.get("code")}
    if not metadata:
        return pd.DataFrame()

    daily_trade_dao = DailyTradeDAO(settings.postgres)
    start_date = (datetime.now(LOCAL_TZ) - timedelta(days=VOLUME_SURGE_FETCH_DAYS)).strftime("%Y%m%d")
    trade_frame = daily_trade_dao.fetch_close_prices(start_date=start_date)
    if trade_frame.empty:
        return trade_frame

    trade_frame = trade_frame[trade_frame["ts_code"].isin(metadata)]
    if trade_frame.empty:
        return trade_frame

    trade_frame["trade_date"] = pd.to_datetime(trade_frame["trade_date"], errors="coerce")
    trade_frame["close"] = pd.to_numeric(trade_frame["close"], errors="coerce")
    trade_frame["volume"] = pd.to_numeric(trade_frame["volume"], errors="coerce")
    trade_frame = trade_frame.dropna(subset=["trade_date", "close", "volume"])

    records: list[dict[str, Any]] = []
    grouped = trade_frame.groupby("ts_code", sort=False)
    for ts_code, group in grouped:
        meta = metadata.get(ts_code)
        if not meta:
            continue
        candidate = _analyze_volume_surge_group(ts_code, group, meta, surge_config)
        if candidate:
            records.append(candidate)

    if not records:
        return pd.DataFrame()

    frame = pd.DataFrame(records)
    frame.sort_values(by="score", ascending=False, inplace=True)
    frame["rank"] = range(1, len(frame) + 1)
    return frame


def _analyze_volume_surge_group(
    ts_code: str,
    group: pd.DataFrame,
    meta: dict[str, Any],
    surge_config: VolumeSurgeConfig,
) -> dict[str, Any] | None:
    ordered = group.sort_values("trade_date").copy()
    ordered = ordered.dropna(subset=["close", "volume"])
    if len(ordered) < VOLUME_SURGE_MIN_HISTORY:
        return None

    closes = ordered["close"].astype(float).tolist()
    volumes = ordered["volume"].astype(float).tolist()
    last_close = closes[-1]
    last_volume = volumes[-1]
    if last_volume <= 0 or last_close is None:
        return None

    volume_window = ordered["volume"].tail(VOLUME_SURGE_VOLUME_WINDOW)
    avg_volume = float(volume_window.mean()) if not volume_window.empty else None
    if avg_volume in (None, 0) or not math.isfinite(avg_volume):
        return None

    volume_ratio = last_volume / avg_volume if avg_volume else None
    min_volume_ratio = surge_config.min_volume_ratio or VOLUME_SURGE_MIN_VOLUME_RATIO
    if volume_ratio is None or not math.isfinite(volume_ratio) or volume_ratio < min_volume_ratio:
        return None

    consolidation = ordered.tail(VOLUME_SURGE_CONSOLIDATION_WINDOW + 1)
    if len(consolidation) <= 1:
        return None
    prior_closes = consolidation["close"].iloc[:-1]
    if prior_closes.empty:
        return None

    max_prior_close = float(prior_closes.max())
    min_prior_close = float(prior_closes.min())
    if max_prior_close <= 0 or min_prior_close <= 0:
        return None

    range_ratio = (max_prior_close - min_prior_close) / min_prior_close
    max_range_ratio = (surge_config.max_range_percent or (VOLUME_SURGE_RANGE_LIMIT * 100)) / 100
    if range_ratio > max_range_ratio:
        return None

    breakout_percent = None
    if max_prior_close > 0:
        breakout_percent = (last_close - max_prior_close) / max_prior_close

    prev_close = closes[-2] if len(closes) >= 2 else None
    price_change = None
    if prev_close and prev_close > 0:
        price_change = ((last_close - prev_close) / prev_close) * 100

    breakout_threshold = (surge_config.breakout_threshold_percent or (VOLUME_SURGE_BREAKOUT_THRESHOLD * 100)) / 100
    daily_change_threshold = surge_config.daily_change_threshold_percent or VOLUME_SURGE_DAILY_CHANGE_THRESHOLD

    passes_breakout = breakout_percent is not None and breakout_percent >= breakout_threshold
    passes_strong_move = price_change is not None and price_change >= daily_change_threshold
    if not (passes_breakout or passes_strong_move):
        return None

    stage_change = None
    if len(closes) > 20:
        base_close = closes[-21]
        if base_close and base_close > 0:
            stage_change = ((last_close - base_close) / base_close) * 100

    score = (volume_ratio * 100) + max(breakout_percent or 0, 0) * 500 + max(price_change or 0, 0)
    short_code = (ts_code.split(".")[0] if isinstance(ts_code, str) else ts_code) or ""

    return {
        "stock_code": short_code,
        "stock_code_full": ts_code,
        "stock_name": meta.get("name"),
        "industry": meta.get("industry"),
        "last_price": last_close,
        "last_volume": last_volume * 100,
        "avg_volume": avg_volume * 100,
        "volume_ratio": volume_ratio,
        "price_change_percent": price_change,
        "stage_change_percent": stage_change,
        "volume_days": min(len(prior_closes), VOLUME_SURGE_CONSOLIDATION_WINDOW),
        "breakout_percent": (breakout_percent * 100) if breakout_percent is not None else None,
        "range_percent": (range_ratio * 100) if range_ratio is not None else None,
        "score": score,
    }


def sync_indicator_screening(
    indicator_code: str | None = None,
    *,
    settings_path: str | None = None,
) -> dict[str, Any]:
    code = _normalize_indicator_code(indicator_code)
    settings = load_settings(settings_path)
    dao = IndicatorScreeningDAO(settings.postgres)
    return _perform_indicator_sync(code, settings, dao)


def sync_indicator_continuous_volume(*, settings_path: str | None = None) -> dict[str, Any]:
    result = sync_indicator_screening(CONTINUOUS_VOLUME_CODE, settings_path=settings_path)
    result["skipped"] = False
    return result


def sync_all_indicator_screenings(
    *,
    force: bool = False,
    settings_path: str | None = None,
) -> list[dict[str, Any]]:
    settings = load_settings(settings_path)
    dao = IndicatorScreeningDAO(settings.postgres)
    results: list[dict[str, Any]] = []
    for code in INDICATOR_DEFINITIONS:
        sync_result = _perform_indicator_sync(code, settings, dao)
        results.append(sync_result)
    return results


def _perform_indicator_sync(
    code: str,
    settings,
    dao: IndicatorScreeningDAO,
) -> dict[str, Any]:
    definition = INDICATOR_DEFINITIONS[code]
    dataframe = definition["fetcher"](settings)
    captured_at = datetime.now(LOCAL_TZ).replace(tzinfo=None)
    if dataframe is None or dataframe.empty:
        return {
            "indicatorCode": code,
            "indicatorName": definition["name"],
            "rows": 0,
            "capturedAt": None,
            "skipped": False,
        }
    normalized = definition["normalizer"](dataframe, captured_at)
    rows = dao.upsert(normalized)
    return {
        "indicatorCode": code,
        "indicatorName": definition["name"],
        "rows": rows,
        "capturedAt": datetime.fromtimestamp(captured_at.timestamp(), LOCAL_TZ),
        "skipped": False,
    }


def list_indicator_screenings(
    *,
    indicator_codes: Sequence[str] | None = None,
    limit: int = 200,
    offset: int = 0,
    net_income_yoy_min: float | None = None,
    net_income_qoq_min: float | None = None,
    pe_min: float | None = None,
    pe_max: float | None = None,
    turnover_rate_min: float | None = None,
    turnover_rate_max: float | None = None,
    daily_change_min: float | None = None,
    daily_change_max: float | None = None,
    pct_change_1w_max: float | None = None,
    pct_change_1m_max: float | None = None,
    has_big_deal_inflow: bool | None = None,
    settings_path: str | None = None,
) -> dict[str, Any]:
    codes = _normalize_indicator_codes(indicator_codes)
    settings = load_settings(settings_path)
    dao = IndicatorScreeningDAO(settings.postgres)
    daily_trade_dao = DailyTradeDAO(settings.postgres)
    trade_date = daily_trade_dao.latest_trade_date(include_intraday=False)
    if trade_date is None:
        raise RuntimeError("Daily trade dataset is empty; run the daily trade sync first.")

    daily_indicator_dao = DailyIndicatorDAO(settings.postgres)
    trade_metrics_dao = DailyTradeMetricsDAO(settings.postgres)
    fundamental_dao = FundamentalMetricsDAO(settings.postgres)
    require_big_deal_filter = has_big_deal_inflow is True or BIG_DEAL_INDICATOR_CODE in codes
    primary_code = codes[0] if codes else None

    table_names = {
        "daily_trade": daily_trade_dao._table_name,  # noqa: SLF001
        "stock_basic": settings.postgres.stock_table,
        "daily_indicator": daily_indicator_dao._table_name,  # noqa: SLF001
        "daily_trade_metrics": trade_metrics_dao._table_name,  # noqa: SLF001
        "fundamental_metrics": fundamental_dao._table_name,  # noqa: SLF001
        "big_deal": settings.postgres.big_deal_fund_flow_table,
    }

    dataset = dao.query_full_universe(
        trade_date=trade_date,
        indicator_codes=codes,
        primary_indicator_code=primary_code,
        limit=limit,
        offset=offset,
        net_income_yoy_min=net_income_yoy_min,
        net_income_qoq_min=net_income_qoq_min,
        pe_min=pe_min,
        pe_max=pe_max,
        turnover_rate_min=turnover_rate_min,
        turnover_rate_max=turnover_rate_max,
        daily_change_min=daily_change_min,
        daily_change_max=daily_change_max,
        pct_change_1w_max=pct_change_1w_max,
        pct_change_1m_max=pct_change_1m_max,
        require_big_deal_inflow=require_big_deal_filter,
        tables=table_names,
    )

    rows: list[dict[str, Any]] = dataset.get("items", [])
    stock_codes = [row.get("ts_code") for row in rows if row.get("ts_code")]
    indicator_detail_map = _build_indicator_detail_map(dao, codes, stock_codes)

    entries = _compose_screening_entries(
        rows,
        trade_date=trade_date,
        indicator_codes=codes,
        primary_indicator_code=primary_code,
        indicator_details=indicator_detail_map,
    )

    response_captured = _resolve_response_captured_at(entries, trade_date)
    indicator_name = INDICATOR_DEFINITIONS[primary_code]["name"] if primary_code else "All Indicators"

    return {
        "indicatorCode": primary_code or "all",
        "indicatorCodes": list(codes),
        "indicatorName": indicator_name,
        "capturedAt": response_captured,
        "total": dataset.get("total", 0),
        "items": entries,
    }

def run_indicator_realtime_refresh(
    codes: Sequence[str] | None,
    *,
    sync_all: bool = False,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    stock_dao = StockBasicDAO(settings.postgres)

    target_codes: List[str]
    if sync_all or not codes:
        target_codes = stock_dao.list_codes()
    else:
        target_codes = list(codes)

    normalized_codes = list(dict.fromkeys(filter(None, (_normalize_ts_code(code) for code in target_codes))))
    if not normalized_codes:
        raise ValueError("No valid stock codes were provided for realtime refresh.")

    daily_trade_dao = DailyTradeDAO(settings.postgres)
    total_processed = 0
    total_metrics = 0
    all_codes: list[str] = []
    chunk_size = 50  # Tushare realtime endpoint limits 50 codes per request

    for start in range(0, len(normalized_codes), chunk_size):
        chunk = normalized_codes[start : start + chunk_size]
        try:
            realtime_df = get_realtime_quotes(chunk, token=settings.tushare.token)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Realtime quote batch failed for %s: %s", chunk[:3], exc)
            continue
        if realtime_df.empty:
            continue
        profile_map = load_average_profile_map(chunk, settings_path=settings_path)
        rows: list[dict[str, object]] = []
        processed_codes: list[str] = []

        for record in realtime_df.to_dict("records"):
            ts_code = _normalize_ts_code(str(record.get("code") or ""))
            if not ts_code:
                continue

            trade_date_raw = record.get("trade_date")
            trade_time = str(record.get("trade_time") or "15:00:00")
            try:
                trade_date = datetime.strptime(str(trade_date_raw).strip(), "%Y-%m-%d").date()
            except (ValueError, TypeError):
                try:
                    trade_date = datetime.strptime(str(trade_date_raw).strip(), "%Y%m%d").date()
                except (ValueError, TypeError):
                    trade_date = datetime.now(LOCAL_TZ).date()

            current_volume_shares = _safe_float(record.get("volume"))
            if current_volume_shares is None:
                continue
            estimated_shares, _ = estimate_full_day_volume(
                ts_code,
                trade_time,
                current_volume_shares,
                profile_map=profile_map,
                settings_path=settings_path,
            )
            estimated_hands = estimated_shares / 100

            close_price = _safe_float(record.get("close"))
            pre_close = _safe_float(record.get("pre_close"))
            change_value = None
            pct_change = None
            if close_price is not None and pre_close not in (None, 0):
                change_value = close_price - pre_close
                pct_change = (change_value / pre_close) * 100

            rows.append(
                {
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                    "open": _safe_float(record.get("open")),
                    "high": _safe_float(record.get("high")),
                    "low": _safe_float(record.get("low")),
                    "close": close_price,
                    "pre_close": pre_close,
                    "change": change_value,
                    "pct_chg": pct_change,
                    "vol": estimated_hands,
                    "amount": _safe_float(record.get("amount")),
                    "is_intraday": True,
                }
            )
            processed_codes.append(ts_code)

        if not rows:
            continue

        dataframe = pd.DataFrame(rows, columns=list(DAILY_TRADE_FIELDS))
        affected = daily_trade_dao.upsert(dataframe)
        total_processed += affected
        all_codes.extend(processed_codes)

        metrics_result = recompute_trade_metrics_for_codes(
            processed_codes,
            include_intraday=True,
            settings_path=settings_path,
        )
        total_metrics += metrics_result.get("rows", 0)

    if total_processed == 0:
        raise RuntimeError("Realtime quotes did not contain usable rows.")

    return {
        "processed": total_processed,
        "metricsUpdated": total_metrics,
        "codes": all_codes,
        "updatedAt": datetime.now(LOCAL_TZ),
    }


def _attach_indicator_metrics(
    entries: Sequence[dict[str, Any]],
    fundamental_dao: FundamentalMetricsDAO,
    daily_indicator_dao: DailyIndicatorDAO,
) -> None:
    if not entries:
        return
    ts_codes: list[str] = []
    for entry in entries:
        stock_code_full = entry.get("stock_code_full")
        stock_code = entry.get("stock_code")
        if not stock_code_full:
            stock_code_full = _format_stock_code_full(stock_code or "")
            entry["stock_code_full"] = stock_code_full
        if stock_code_full:
            ts_codes.append(stock_code_full)
    if not ts_codes:
        return
    unique_codes = sorted(set(ts_codes))
    fundamentals = fundamental_dao.fetch_metrics(unique_codes)
    indicator_metrics = daily_indicator_dao.fetch_latest_indicators(unique_codes)
    for entry in entries:
        code = entry.get("stock_code_full")
        fundamentals_data = fundamentals.get(code, {})
        indicator_data = indicator_metrics.get(code, {})
        entry["net_income_yoy_latest"] = fundamentals_data.get("net_income_yoy_latest")
        entry["net_income_qoq_latest"] = fundamentals_data.get("net_income_qoq_latest")
        entry["pe_ratio"] = indicator_data.get("pe")


def _enrich_entries_with_market_data(entries: Sequence[dict[str, Any]], *, settings) -> None:
    if not entries:
        return
    codes: list[str] = []
    for entry in entries:
        code_full = entry.get("stockCodeFull") or entry.get("stock_code_full")
        if code_full:
            codes.append(code_full)
    if not codes:
        return
    unique_codes = sorted(set(codes))
    trade_dao = DailyTradeDAO(settings.postgres)
    trade_metrics = trade_dao.fetch_latest_metrics(unique_codes, include_intraday=True)
    stock_dao = StockBasicDAO(settings.postgres)
    fundamentals = stock_dao.query_fundamentals(
        include_st=True,
        include_delisted=True,
        limit=len(unique_codes),
        offset=0,
        codes=unique_codes,
    )
    metrics_dao = DailyTradeMetricsDAO(settings.postgres)
    trend_metrics = metrics_dao.fetch_metrics(unique_codes)
    missing_pct_codes = [
        code
        for code in unique_codes
        if code not in trend_metrics
        or trend_metrics[code].get("pct_change_1w") is None
        or trend_metrics[code].get("pct_change_1m") is None
    ]
    fallback_pct_changes: Dict[str, Dict[str, float | None]] = {}
    if missing_pct_codes:
        fallback_pct_changes = trade_dao.fetch_pct_change_windows(missing_pct_codes, windows=(5, 20))
    indicator_dao = DailyIndicatorDAO(settings.postgres)
    indicator_snapshots = indicator_dao.fetch_latest_indicators(unique_codes)
    industry_map = {
        item.get("code"): item.get("industry")
        for item in fundamentals.get("items", [])
        if item.get("code")
    }
    for entry in entries:
        code_full = entry.get("stockCodeFull") or entry.get("stock_code_full")
        if not code_full:
            continue
        metrics = trade_metrics.get(code_full)
        if metrics:
            last_price = metrics.get("last_price")
            pct_chg = metrics.get("pct_change")
            if last_price is not None:
                entry["lastPrice"] = last_price
                entry["last_price"] = last_price
            if pct_chg is not None:
                entry["priceChangePercent"] = pct_chg
                entry["price_change_percent"] = pct_chg
            details = entry.get("indicatorDetails")
            if isinstance(details, dict):
                for detail in details.values():
                    if last_price is not None:
                        detail["lastPrice"] = last_price
                    if pct_chg is not None:
                        detail["priceChangePercent"] = pct_chg
        industry = industry_map.get(code_full)
        if industry:
            entry["industry"] = industry
        trend = trend_metrics.get(code_full) or {}
        pct_1w = _safe_float(trend.get("pct_change_1w"))
        pct_1m = _safe_float(trend.get("pct_change_1m"))
        if pct_1w is None or pct_1m is None:
            fallback_entry = fallback_pct_changes.get(code_full) or {}
            if pct_1w is None:
                pct_1w = _safe_float(fallback_entry.get("pct_change_5"))
            if pct_1m is None:
                pct_1m = _safe_float(fallback_entry.get("pct_change_20"))
        if pct_1w is not None:
            entry["pctChange1W"] = pct_1w
            entry["pct_change_1w"] = pct_1w
        if pct_1m is not None:
            entry["pctChange1M"] = pct_1m
            entry["pct_change_1m"] = pct_1m
        indicator_snapshot = indicator_snapshots.get(code_full)
        if indicator_snapshot:
            turnover_value = _safe_float(indicator_snapshot.get("turnover_rate"))
            if turnover_value is not None:
                entry["turnoverRate"] = turnover_value
                entry["turnover_rate"] = turnover_value


def _apply_indicator_filters(
    items: Sequence[dict[str, Any]],
    net_income_yoy_min: float | None,
    net_income_qoq_min: float | None,
    pe_min: float | None,
    pe_max: float | None,
    turnover_rate_min: float | None,
    turnover_rate_max: float | None,
    daily_change_min: float | None,
    daily_change_max: float | None,
    pct_change_1w_max: float | None,
    pct_change_1m_max: float | None,
) -> list[dict[str, Any]]:
    if (
        net_income_yoy_min is None
        and net_income_qoq_min is None
        and pe_min is None
        and pe_max is None
        and turnover_rate_min is None
        and turnover_rate_max is None
        and daily_change_min is None
        and daily_change_max is None
        and pct_change_1w_max is None
        and pct_change_1m_max is None
    ):
        return list(items)

    filtered: list[dict[str, Any]] = []
    for entry in items:
        net_income = _safe_float(entry.get("netIncomeYoyLatest"))
        net_income_qoq = _safe_float(entry.get("netIncomeQoqLatest"))
        pe_value = _safe_float(entry.get("peRatio"))
        turnover_value = _safe_float(entry.get("turnoverRate") or entry.get("turnover_rate"))
        daily_change = _safe_float(entry.get("priceChangePercent") or entry.get("price_change_percent"))
        pct_1w = _safe_float(entry.get("pctChange1W") or entry.get("pct_change_1w"))
        pct_1m = _safe_float(entry.get("pctChange1M") or entry.get("pct_change_1m"))

        if net_income_yoy_min is not None:
            if net_income is None or net_income < net_income_yoy_min:
                continue
        if net_income_qoq_min is not None:
            if net_income_qoq is None or net_income_qoq < net_income_qoq_min:
                continue
        if pe_min is not None:
            if pe_value is None or pe_value < pe_min:
                continue
        if pe_max is not None:
            if pe_value is None or pe_value > pe_max:
                continue
        if turnover_rate_min is not None:
            if turnover_value is None or turnover_value < turnover_rate_min:
                continue
        if turnover_rate_max is not None:
            if turnover_value is None or turnover_value > turnover_rate_max:
                continue
        if daily_change_min is not None:
            if daily_change is None or daily_change < daily_change_min:
                continue
        if daily_change_max is not None:
            if daily_change is None or daily_change > daily_change_max:
                continue
        if pct_change_1w_max is not None:
            if pct_1w is None or pct_1w > pct_change_1w_max:
                continue
        if pct_change_1m_max is not None:
            if pct_1m is None or pct_1m > pct_change_1m_max:
                continue
        filtered.append(entry)
    return filtered


def _deduplicate_entries(entries: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        key = entry.get("stockCodeFull") or entry.get("stockCode") or entry.get("stock_code_full") or entry.get("stock_code")
        if key:
            if key in seen:
                continue
            seen.add(key)
        deduped.append(entry)
    return deduped


def _serialize_entry(entry: dict[str, Any]) -> dict[str, Any]:
    indicator_code = entry.get("indicator_code")
    net_amount = _safe_float(entry.get("turnover_amount"))
    turnover_value = _round_decimal(_safe_float(entry.get("turnover_percent")))
    serialized = {
        "indicatorCode": indicator_code,
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
        "turnoverPercent": turnover_value,
        "turnoverRate": _safe_float(entry.get("turnover_rate")),
        "turnoverAmount": net_amount,
        "turnoverAmountText": entry.get("turnover_amount_text"),
        "highPrice": _safe_float(entry.get("high_price")),
        "lowPrice": _safe_float(entry.get("low_price")),
        "bigDealNetAmount": _safe_float(entry.get("bigDealNetAmount")),
        "bigDealBuyAmount": _safe_float(entry.get("bigDealBuyAmount")),
        "bigDealSellAmount": _safe_float(entry.get("bigDealSellAmount")),
        "bigDealTradeCount": entry.get("bigDealTradeCount"),
        "netIncomeYoyLatest": _safe_float(entry.get("net_income_yoy_latest")),
        "netIncomeQoqLatest": _safe_float(entry.get("net_income_qoq_latest")),
        "peRatio": _safe_float(entry.get("pe_ratio")),
        "industry": entry.get("industry"),
        "matchedIndicators": [indicator_code] if indicator_code else [],
        "hasBigDealInflow": False,
    }
    serialized["indicatorDetails"] = {}
    if indicator_code:
        detail = _extract_indicator_details(entry, indicator_code)
        if indicator_code == BIG_DEAL_INDICATOR_CODE:
            serialized["hasBigDealInflow"] = bool(net_amount is not None and net_amount > 0)
        serialized["indicatorDetails"][indicator_code] = detail
    return serialized


def _extract_indicator_details(entry: dict[str, Any], indicator_code: str) -> dict[str, Any]:
    detail = {
        "priceChangePercent": _safe_float(entry.get("price_change_percent")),
        "stageChangePercent": _safe_float(entry.get("stage_change_percent")),
        "lastPrice": _safe_float(entry.get("last_price")),
        "volumeDays": entry.get("volume_days"),
        "volumeText": entry.get("volume_text"),
        "baselineVolumeText": entry.get("baseline_volume_text"),
        "turnoverPercent": _round_decimal(_safe_float(entry.get("turnover_percent"))),
        "turnoverRate": _safe_float(entry.get("turnover_rate")),
        "turnoverAmount": _safe_float(entry.get("turnover_amount")),
        "turnoverAmountText": entry.get("turnover_amount_text"),
        "highPrice": _safe_float(entry.get("high_price")),
        "lowPrice": _safe_float(entry.get("low_price")),
    }
    if indicator_code == BIG_DEAL_INDICATOR_CODE:
        detail.update(
            {
                "bigDealNetAmount": _safe_float(entry.get("turnover_amount")),
                "bigDealBuyAmount": _safe_float(entry.get("volume_shares")),
                "bigDealSellAmount": _safe_float(entry.get("baseline_volume_shares")),
                "bigDealTradeCount": entry.get("volume_days"),
            }
        )
    return detail


def _build_indicator_detail_map(
    dao: IndicatorScreeningDAO,
    indicator_codes: Sequence[str],
    stock_codes: Sequence[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    if not indicator_codes or not stock_codes:
        return {}
    detail_map: dict[str, dict[str, dict[str, Any]]] = {}
    unique_codes = list(dict.fromkeys(stock_codes))
    for code in indicator_codes:
        per_code = dao.fetch_latest_entries_for_codes(indicator_code=code, stock_codes=unique_codes)
        if not per_code:
            continue
        for ts_code, record in per_code.items():
            detail_map.setdefault(ts_code, {})[code] = record
    return detail_map


def _compose_screening_entries(
    rows: Sequence[dict[str, Any]],
    *,
    trade_date: date,
    indicator_codes: Sequence[str],
    primary_indicator_code: str | None,
    indicator_details: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    default_captured = datetime.combine(trade_date, FINAL_SYNC_CUTOFF).replace(tzinfo=LOCAL_TZ)
    indicator_name = INDICATOR_DEFINITIONS[primary_indicator_code]["name"] if primary_indicator_code else "All Indicators"

    for row in rows:
        ts_code = row.get("ts_code")
        if not ts_code:
            continue
        stock_code = row.get("stock_code")
        if not stock_code and isinstance(ts_code, str):
            stock_code = ts_code.split(".", 1)[0]

        entry = {
            "indicatorCode": primary_indicator_code or "all",
            "indicatorName": indicator_name,
            "capturedAt": default_captured,
            "rank": None,
            "stockCode": stock_code,
            "stockCodeFull": ts_code,
            "stockName": row.get("stock_name"),
            "industry": row.get("industry"),
            "priceChangePercent": _safe_float(row.get("pct_chg")),
            "stageChangePercent": None,
            "lastPrice": _safe_float(row.get("close")),
            "volumeShares": None,
            "volumeText": None,
            "baselineVolumeShares": None,
            "baselineVolumeText": None,
            "volumeDays": None,
            "turnoverPercent": None,
            "turnoverRate": _safe_float(row.get("turnover_rate")),
            "turnoverAmount": None,
            "turnoverAmountText": None,
            "highPrice": None,
            "lowPrice": None,
            "netIncomeYoyLatest": _safe_float(row.get("net_income_yoy_latest")),
            "netIncomeQoqLatest": _safe_float(row.get("net_income_qoq_latest")),
            "peRatio": _safe_float(row.get("pe_ratio")),
            "pctChange1W": _safe_float(row.get("pct_change_1w")),
            "pctChange1M": _safe_float(row.get("pct_change_1m")),
            "bigDealNetAmount": _safe_float(row.get("big_deal_net_amount")),
            "bigDealBuyAmount": _safe_float(row.get("big_deal_buy_amount")),
            "bigDealSellAmount": _safe_float(row.get("big_deal_sell_amount")),
            "bigDealTradeCount": row.get("big_deal_trade_count"),
            "hasBigDealInflow": bool((_safe_float(row.get("big_deal_net_amount")) or 0) > 0),
            "matchedIndicators": [],
            "indicatorDetails": {},
        }

        stock_detail_map = indicator_details.get(ts_code, {})
        if stock_detail_map:
            matched = list(stock_detail_map.keys())
            entry["matchedIndicators"] = matched
            for code, detail in stock_detail_map.items():
                entry["indicatorDetails"][code] = _extract_indicator_details(detail, code)

        if primary_indicator_code and primary_indicator_code in stock_detail_map:
            detail = stock_detail_map[primary_indicator_code]
            entry["capturedAt"] = _localize_datetime(detail.get("captured_at")) or default_captured
            entry["rank"] = detail.get("rank")
            entry["stageChangePercent"] = _safe_float(detail.get("stage_change_percent"))
            entry["volumeText"] = detail.get("volume_text")
            entry["volumeShares"] = _safe_float(detail.get("volume_shares"))
            entry["baselineVolumeText"] = detail.get("baseline_volume_text")
            entry["baselineVolumeShares"] = _safe_float(detail.get("baseline_volume_shares"))
            entry["volumeDays"] = detail.get("volume_days")
            entry["turnoverPercent"] = _round_decimal(_safe_float(detail.get("turnover_percent")))
            entry["turnoverAmount"] = _safe_float(detail.get("turnover_amount"))
            entry["turnoverAmountText"] = detail.get("turnover_amount_text")
            entry["highPrice"] = _safe_float(detail.get("high_price"))
            entry["lowPrice"] = _safe_float(detail.get("low_price"))
            if entry.get("priceChangePercent") is None:
                entry["priceChangePercent"] = _safe_float(detail.get("price_change_percent"))
            if entry.get("lastPrice") is None:
                entry["lastPrice"] = _safe_float(detail.get("last_price"))

        entries.append(entry)
    return entries


def _resolve_response_captured_at(entries: Sequence[dict[str, Any]], trade_date: date) -> datetime:
    default_value = datetime.combine(trade_date, FINAL_SYNC_CUTOFF).replace(tzinfo=LOCAL_TZ)
    latest = default_value
    for entry in entries:
        candidate = entry.get("capturedAt")
        if isinstance(candidate, datetime):
            if latest is None or candidate > latest:
                latest = candidate
    return latest or default_value


def _normalize_indicator_code(code: str | None) -> str:
    if code and code in INDICATOR_DEFINITIONS:
        return code
    return DEFAULT_INDICATOR_CODE


def _normalize_indicator_codes(codes: Sequence[str] | None) -> List[str]:
    if not codes:
        return []
    normalized = []
    for code in codes:
        if code in INDICATOR_DEFINITIONS and code not in normalized:
            normalized.append(code)
    return normalized


def _resolve_latest_captured(per_code_records: Dict[str, Dict[str, dict]]) -> datetime | None:
    latest: datetime | None = None
    for record_map in per_code_records.values():
        for entry in record_map.values():
            localized = _localize_datetime(entry.get("captured_at"))
            if localized and (latest is None or localized > latest):
                latest = localized
    return latest


def _annotate_big_deal_inflow(
    entries: Sequence[dict[str, Any]],
    *,
    big_deal_dao: BigDealFundFlowDAO | None,
    trade_date: Optional[date] = None,
) -> None:
    if not entries or big_deal_dao is None:
        return
    codes = sorted({entry.get("stockCode") for entry in entries if entry.get("stockCode")})
    if not codes:
        return
    target_date = trade_date or datetime.now(LOCAL_TZ).date()
    inflow_map = big_deal_dao.fetch_buy_amount_map(codes, trade_date=target_date)
    for entry in entries:
        code = entry.get("stockCode")
        detail = inflow_map.get(code or "")
        net_amount = _safe_float(detail.get("netAmount")) if detail else None
        buy_amount = _safe_float(detail.get("buyAmount")) if detail else None
        sell_amount = _safe_float(detail.get("sellAmount")) if detail else None
        trade_count = detail.get("tradeCount") if detail else None
        entry["hasBigDealInflow"] = bool(net_amount is not None and net_amount > 0)
        entry["bigDealNetAmount"] = net_amount
        entry["bigDealBuyAmount"] = buy_amount
        entry["bigDealSellAmount"] = sell_amount
        entry["bigDealTradeCount"] = trade_count
        indicator_details = entry.get("indicatorDetails")
        if not isinstance(indicator_details, dict):
            indicator_details = {}
            entry["indicatorDetails"] = indicator_details
        detail_entry = indicator_details.get(BIG_DEAL_INDICATOR_CODE, {})
        detail_entry.update(
            {
                "bigDealNetAmount": net_amount,
                "bigDealBuyAmount": buy_amount,
                "bigDealSellAmount": sell_amount,
                "bigDealTradeCount": trade_count,
            }
        )
        indicator_details[BIG_DEAL_INDICATOR_CODE] = detail_entry


def _apply_big_deal_filter(
    items: Sequence[dict[str, Any]],
    require_inflow: bool,
) -> list[dict[str, Any]]:
    if not require_inflow:
        return list(items)
    return [entry for entry in items if entry.get("hasBigDealInflow")]


def _localize_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=LOCAL_TZ)
        return value.astimezone(LOCAL_TZ)
    return None


__all__ = [
    "sync_indicator_screening",
    "sync_indicator_continuous_volume",
    "list_indicator_screenings",
    "run_indicator_realtime_refresh",
]
