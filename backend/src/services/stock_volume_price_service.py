"""Run Wyckoff-style volume/price reasoning for individual stocks."""

from __future__ import annotations

import json
from datetime import datetime
import math
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence

from zoneinfo import ZoneInfo

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from ..dao import BigDealFundFlowDAO, DailyTradeDAO, IndividualFundFlowDAO, StockVolumePriceReasoningDAO
from .stock_basic_service import get_stock_overview

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
INDIVIDUAL_FLOW_LIMIT = 40
BIG_DEAL_SNAPSHOT_LIMIT = 30

VOLUME_PRICE_PROMPT = """
你是一名精通威科夫量价分析法的资深A股策略顾问。以下是个股 {stock_name} ({stock_code}) 最近 {lookback_days} 个交易日的量价 + 资金 JSON 数据（history、statistics、individualFundFlow.items、bigDealTrades.items）。请输出结构化 JSON，格式如下：
{{
  "wyckoffPhase": "吸筹/上涨/再吸筹/派发/下跌/再派发/震荡/不确定",
  "stageSummary": "不少于80字，结合威科夫原理与数据的综合分析。",
  "trendContext": "描述整体趋势、价格区间与关键阻力/支撑。",
  "keySignals": {{
    "volumeSignals": ["引用具体量能数据，如“近3日成交量为20日均量的1.4倍”"],
    "priceAction": ["引用具体价格行为或威科夫术语，如“Spring 后放量上攻”"]
  }},
  "marketNarrative": "基于量价对“聪明钱”意图的解读。",
  "strategyOutlook": ["基于当前阶段的操作展望，例如“突破回踩可小仓试多”"],
  "keyRisks": ["至少一条潜在风险或失败信号"],
  "nextWatchlist": ["为确认当前判断，需要跟踪的信号清单"],
  "confidence": 0.0-1.0
}}

要求：
1. 先判断趋势与价格相对位置，再给出阶段。只有在出现高位滞涨、放量破位、区间涨幅转负等明确信号时才判定为“派发/下跌”；否则可使用吸筹/上涨/再吸筹/震荡/不确定。
2. 所有观点必须引用 JSON 数据中的具体数值或统计（例如最高/最低价、statistics.window20.changePercent、volume 相对 avgVolume 等）。
3. 若数据不足以确认阶段，需将 “wyckoffPhase” 设为“震荡”或“不确定”，并在 stageSummary 中说明原因与待确认事项。
4. 若 individualFundFlow.items 有记录，需要评价净流入/排名/阶段涨跌幅等资金行为；若 bigDealTrades.items 不为空，需点名最近大单方向（trade_side）、成交额（trade_amount）或占比，以印证量价推理。

以下为输入数据：
{payload}
"""


def _format_trade_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _percent_change(start: Optional[float], end: Optional[float]) -> Optional[float]:
    if start in (None, 0) or end is None:
        return None
    return round(((end - start) / start) * 100, 2)


def _mean(values: List[float]) -> Optional[float]:
    return round(mean(values), 2) if values else None


def _std(values: List[float]) -> Optional[float]:
    return round(pstdev(values), 2) if len(values) > 1 else None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _serialize_datetime(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=LOCAL_TZ).isoformat()
        return value.astimezone(LOCAL_TZ).isoformat()
    return value


def _normalize_record_for_json(record: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in record.items():
        normalized[key] = _serialize_datetime(value)
    return normalized


def _normalize_code_aliases(ts_code: str) -> List[str]:
    text = (ts_code or "").strip().upper()
    if not text:
        return []
    aliases = {text}
    if "." in text:
        prefix, _suffix = text.split(".", 1)
        prefix = prefix.strip()
        if prefix:
            aliases.add(prefix)
            if prefix.isdigit():
                aliases.add(prefix.zfill(6))
    elif text.isdigit():
        aliases.add(text.zfill(6))
    digits_only = "".join(ch for ch in text if ch.isdigit())
    if digits_only:
        aliases.add(digits_only.zfill(6))
    return [alias for alias in aliases if alias]


def _load_individual_fund_flow_snapshot(code_aliases: Sequence[str], settings) -> Dict[str, Any]:
    dao = IndividualFundFlowDAO(settings.postgres)
    snapshot = dao.list_entries(stock_codes=code_aliases or None, limit=INDIVIDUAL_FLOW_LIMIT, offset=0)
    items = snapshot.get("items") or []
    snapshot["items"] = [_normalize_record_for_json(item) for item in items]
    return snapshot


def _load_big_deal_snapshot(code_aliases: Sequence[str], settings) -> Dict[str, Any]:
    dao = BigDealFundFlowDAO(settings.postgres)
    snapshot = dao.list_entries(
        stock_codes=code_aliases or None,
        limit=BIG_DEAL_SNAPSHOT_LIMIT,
        offset=0,
    )
    items = snapshot.get("items") or []
    snapshot["items"] = [_normalize_record_for_json(item) for item in items]
    return snapshot


def _resolve_stock_profile(code: str, *, settings_path: Optional[str] = None) -> dict[str, Any]:
    overview = get_stock_overview(
        codes=[code],
        limit=None,
        offset=0,
        settings_path=settings_path,
    )
    if not overview["items"]:
        raise ValueError(f"Stock '{code}' not found.")
    return overview["items"][0]


def build_stock_volume_price_dataset(
    code: str,
    *,
    lookback_days: int = 90,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    profile = _resolve_stock_profile(code, settings_path=settings_path)
    settings = load_settings(settings_path)
    daily_trade_dao = DailyTradeDAO(settings.postgres)
    code_aliases = _normalize_code_aliases(profile["code"])
    individual_fund_flow = _load_individual_fund_flow_snapshot(code_aliases, settings)
    big_deal_snapshot = _load_big_deal_snapshot(code_aliases, settings)
    history_rows = daily_trade_dao.fetch_price_history(profile["code"], limit=min(max(lookback_days + 30, 120), 400))
    if not history_rows:
        return {
            "code": profile["code"],
            "name": profile.get("name"),
            "lookbackDays": lookback_days,
            "history": [],
            "statistics": {},
            "individualFundFlow": individual_fund_flow,
            "bigDealTrades": big_deal_snapshot,
        }

    trimmed = history_rows[-lookback_days:]
    normalised: List[Dict[str, Any]] = []
    for row in trimmed:
        normalised.append(
            {
                "date": _format_trade_date(row.get("trade_date")),
                "open": _safe_float(row.get("open")),
                "high": _safe_float(row.get("high")),
                "low": _safe_float(row.get("low")),
                "close": _safe_float(row.get("close")),
                "volume": _safe_float(row.get("volume")),
            }
        )

    closes = [entry["close"] for entry in normalised if isinstance(entry.get("close"), (int, float))]
    volumes = [entry["volume"] for entry in normalised if isinstance(entry.get("volume"), (int, float))]
    highs = [entry["high"] for entry in normalised if isinstance(entry.get("high"), (int, float))]
    lows = [entry["low"] for entry in normalised if isinstance(entry.get("low"), (int, float))]

    first_close = closes[0] if closes else None
    last_close = closes[-1] if closes else None

    stats = {
        "firstDate": normalised[0]["date"] if normalised else None,
        "lastDate": normalised[-1]["date"] if normalised else None,
        "changePercent": _percent_change(first_close, last_close),
        "maxClose": max(closes) if closes else None,
        "minClose": min(closes) if closes else None,
        "maxHigh": max(highs) if highs else None,
        "minLow": min(lows) if lows else None,
        "avgVolume": _mean(volumes),
        "volumeStd": _std(volumes),
        "maxVolume": max(volumes) if volumes else None,
    }

    def _window_metrics(window: int) -> Dict[str, Optional[float]]:
        subset = normalised[-window:] if len(normalised) >= window else normalised[:]
        closes_subset = [entry["close"] for entry in subset if isinstance(entry.get("close"), (int, float))]
        volumes_subset = [entry["volume"] for entry in subset if isinstance(entry.get("volume"), (int, float))]
        first_c = closes_subset[0] if closes_subset else None
        last_c = closes_subset[-1] if closes_subset else None
        return {
            "window": len(subset),
            "changePercent": _percent_change(first_c, last_c),
            "avgVolume": _mean(volumes_subset),
        }

    stats["window5"] = _window_metrics(5)
    stats["window20"] = _window_metrics(20)
    stats["window60"] = _window_metrics(60)

    return {
        "code": profile["code"],
        "name": profile.get("name"),
        "lookbackDays": lookback_days,
        "history": normalised,
        "statistics": stats,
        "individualFundFlow": individual_fund_flow,
        "bigDealTrades": big_deal_snapshot,
    }


def generate_stock_volume_price_reasoning(
    code: str,
    *,
    lookback_days: int = 90,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    dataset = build_stock_volume_price_dataset(code, lookback_days=lookback_days, settings_path=settings_path)
    settings = load_settings(settings_path)
    generated_at = datetime.now(LOCAL_TZ)
    generated_at_db = generated_at.replace(tzinfo=None)
    reasoning_dao = StockVolumePriceReasoningDAO(settings.postgres)

    summary_text: Optional[str] = None
    model_name: Optional[str] = None
    if run_llm and dataset.get("history"):
        if settings.deepseek is None:
            run_llm = False
        else:
            prompt_payload = json.dumps(dataset, ensure_ascii=False, separators=(",", ":"))
            prompt = VOLUME_PRICE_PROMPT.format(
                stock_name=dataset.get("name") or dataset["code"],
                stock_code=dataset["code"],
                lookback_days=dataset["lookbackDays"],
                payload=prompt_payload,
            )
            result = generate_finance_analysis(
                prompt,
                settings=settings.deepseek,
                prompt_template="{news_content}",
                model_override="deepseek-reasoner",
                temperature=0.2,
                max_output_tokens=4096,
            )
            if isinstance(result, dict):
                summary_text = result.get("content")
                model_name = result.get("model") or "deepseek-reasoner"
            elif isinstance(result, str):
                summary_text = result
                model_name = "deepseek-reasoner"

    default_summary = {
        "wyckoffPhase": "未能判断",
        "stageSummary": "暂无模型输出，请稍后重试或检查是否有足够的量价数据。",
        "volumeSignals": [],
        "priceSignals": [],
        "compositeIntent": "未知",
        "strategy": [],
        "risks": [],
        "checklist": [],
        "confidence": 0,
    }

    summary_dict: Dict[str, Any]
    raw_payload: str
    if summary_text:
        raw_payload = summary_text
        try:
            parsed = json.loads(summary_text)
        except (TypeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, dict):
            summary_dict = parsed
        else:
            summary_dict = {**default_summary, "stageSummary": str(summary_text)}
    else:
        summary_dict = default_summary
        if not dataset.get("history"):
            summary_dict["stageSummary"] = "暂无该股票的历史行情记录，无法进行量价推理。"
        raw_payload = json.dumps(summary_dict, ensure_ascii=False)

    record = {
        "code": dataset["code"],
        "name": dataset.get("name"),
        "lookbackDays": dataset["lookbackDays"],
        "historySize": len(dataset.get("history") or []),
        "statistics": dataset.get("statistics") or {},
        "summary": summary_dict,
        "rawText": raw_payload,
        "model": model_name,
        "generatedAt": generated_at.isoformat(),
    }

    record_id = reasoning_dao.insert_snapshot(
        stock_code=dataset["code"],
        stock_name=dataset.get("name"),
        lookback_days=dataset["lookbackDays"],
        summary_json=summary_dict,
        raw_text=raw_payload,
        model=model_name,
        generated_at=generated_at_db,
    )
    record["id"] = record_id
    return record


def get_latest_stock_volume_price_reasoning(
    code: str,
    *,
    settings_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    settings = load_settings(settings_path)
    reasoning_dao = StockVolumePriceReasoningDAO(settings.postgres)
    record = reasoning_dao.fetch_latest(code)
    if not record:
        return None
    record["generatedAt"] = (
        record["generatedAt"].replace(tzinfo=LOCAL_TZ) if isinstance(record["generatedAt"], datetime) else record["generatedAt"]
    )
    return record


def list_stock_volume_price_history(
    code: str,
    *,
    limit: int = 10,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    if not code:
        return {"total": 0, "items": []}
    settings = load_settings(settings_path)
    reasoning_dao = StockVolumePriceReasoningDAO(settings.postgres)
    history = reasoning_dao.list_history(code, limit=limit, offset=offset)
    items: List[Dict[str, Any]] = []
    for entry in history.get("items", []):
        generated_at = entry.get("generatedAt")
        if isinstance(generated_at, datetime) and generated_at.tzinfo is None:
            entry["generatedAt"] = generated_at.replace(tzinfo=LOCAL_TZ)
        items.append(entry)
    return {"total": history.get("total", 0), "items": items}


__all__ = [
    "build_stock_volume_price_dataset",
    "generate_stock_volume_price_reasoning",
    "get_latest_stock_volume_price_reasoning",
    "list_stock_volume_price_history",
]
