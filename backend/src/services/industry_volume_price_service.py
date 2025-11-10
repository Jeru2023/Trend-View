"""Run Wyckoff-style volume/price reasoning for industries."""

from __future__ import annotations

import json
from datetime import datetime
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional

from zoneinfo import ZoneInfo

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from ..dao import IndustryIndexHistoryDAO, IndustryVolumePriceReasoningDAO
from .industry_directory_service import resolve_industry_label

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

VOLUME_PRICE_PROMPT = """
你是一名精通威科夫量价分析法的资深A股策略顾问。以下是行业 {industry_name} ({industry_code}) 最近 {lookback_days} 个交易日的指数行情 JSON 数据（包含 statistics.changePercent、window5/20/60、avgVolume、volumeStd 等字段）。请输出一段 JSON 推理，必须严格遵循下述结构：
{{
  "wyckoffPhase": "吸筹/上涨/再吸筹/派发/下跌/再派发/震荡/不确定",
  "stageSummary": "不少于80字，结合威科夫原理与数据的综合分析。",
  "trendContext": "描述整体趋势、价格区间与关键位置，例如“位于XX-XX区间，上攻前高遇阻”。",
  "keySignals": {{
    "volumeSignals": ["引用具体量能数据，如“近3日成交量为20日均量的1.4倍”"],
    "priceAction": ["引用具体价格行为或威科夫术语，如“UTAD后放量回落”"]
  }},
  "marketNarrative": "基于量价对“聪明钱”意图的解读。",
  "strategyOutlook": ["基于当前阶段给出的策略展望，例如“观望等待回踩确认”"],
  "keyRisks": ["至少一条潜在风险或失败信号"],
  "nextWatchlist": ["后续需要确认的信号列表"],
  "confidence": 0.0-1.0
}}

额外要求：
1. 先判断趋势与价格相对位置，再给出阶段，仅在出现高位滞涨、放量破位、区间涨幅转负等明确信号时才判定“派发/下跌”；否则可使用吸筹/上涨/再吸筹/震荡/不确定。
2. 所有推理必须引用 JSON 中的具体数值或统计（如 statistics.window20.changePercent、history 的最高/最低价、volume 与 avgVolume 的对比）。
3. 若数据不足以确认阶段，需将 “wyckoffPhase” 设为“震荡”或“不确定”，并在 stageSummary 中说明原因与待确认事项。

以下是输入数据：
{payload}
"""


def _format_trade_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    try:
        return value.isoformat()  # type: ignore[attr-defined]
    except Exception:
        text = str(value)
        if len(text) == 8 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:]}"
        return text


def _percent_change(start: Optional[float], end: Optional[float]) -> Optional[float]:
    if start in (None, 0) or end is None:
        return None
    return round(((end - start) / start) * 100, 2)


def _mean(values: List[float]) -> Optional[float]:
    return round(mean(values), 2) if values else None


def _std(values: List[float]) -> Optional[float]:
    return round(pstdev(values), 2) if len(values) > 1 else None


def build_industry_volume_price_dataset(
    industry: str,
    *,
    lookback_days: int = 90,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    resolved = resolve_industry_label(industry, settings_path=settings_path)
    settings = load_settings(settings_path)
    dao = IndustryIndexHistoryDAO(settings.postgres)
    query = dao.list_entries(
        industry_name=resolved["name"],
        limit=min(max(lookback_days + 20, 90), 200),
    )
    items = query.get("items") or []
    if not items:
        return {
            "industry": resolved["name"],
            "industryCode": resolved["code"],
            "lookbackDays": lookback_days,
            "history": [],
            "statistics": {},
        }

    history_sorted = sorted(items, key=lambda row: row.get("trade_date") or "")
    trimmed = history_sorted[-lookback_days:]
    normalised: List[Dict[str, Any]] = []
    for row in trimmed:
        normalised.append(
            {
                "date": _format_trade_date(row.get("trade_date")),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "preClose": row.get("pre_close"),
                "pctChange": row.get("pct_chg"),
                "volume": row.get("vol"),
                "amount": row.get("amount"),
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
        "industry": resolved["name"],
        "industryCode": resolved["code"],
        "lookbackDays": lookback_days,
        "history": normalised,
        "statistics": stats,
    }


def generate_industry_volume_price_reasoning(
    industry: str,
    *,
    lookback_days: int = 90,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    dataset = build_industry_volume_price_dataset(industry, lookback_days=lookback_days, settings_path=settings_path)
    settings = load_settings(settings_path)
    generated_at = datetime.now(LOCAL_TZ)
    generated_at_db = generated_at.replace(tzinfo=None)
    reasoning_dao = IndustryVolumePriceReasoningDAO(settings.postgres)

    summary_text: Optional[str] = None
    model_name: Optional[str] = None
    if run_llm and dataset.get("history"):
        if settings.deepseek is None:
            run_llm = False
        else:
            prompt_payload = json.dumps(dataset, ensure_ascii=False, separators=(",", ":"))
            prompt = VOLUME_PRICE_PROMPT.format(
                industry_name=dataset["industry"],
                industry_code=dataset["industryCode"],
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
            summary_dict["stageSummary"] = "暂无该行业的历史指数记录，无法进行量价推理。"
        raw_payload = json.dumps(summary_dict, ensure_ascii=False)

    record = {
        "industry": dataset["industry"],
        "industryCode": dataset["industryCode"],
        "lookbackDays": dataset["lookbackDays"],
        "historySize": len(dataset.get("history") or []),
        "statistics": dataset.get("statistics") or {},
        "summary": summary_dict,
        "rawText": raw_payload,
        "model": model_name,
        "generatedAt": generated_at.isoformat(),
    }

    reasoning_dao.insert_snapshot(
        industry_name=record["industry"],
        industry_code=record["industryCode"],
        lookback_days=record["lookbackDays"],
        summary_json=summary_dict,
        raw_text=raw_payload,
        model=model_name,
        generated_at=generated_at_db,
    )

    return record


def _normalize_record(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not record:
        return None
    normalized = dict(record)
    generated_at = normalized.get("generatedAt")
    if isinstance(generated_at, datetime):
        normalized["generatedAt"] = generated_at.replace(tzinfo=LOCAL_TZ).isoformat()
    return normalized


def get_latest_industry_volume_price_reasoning(
    industry: str,
    *,
    settings_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not industry:
        return None
    resolved = resolve_industry_label(industry, settings_path=settings_path)
    settings = load_settings(settings_path)
    dao = IndustryVolumePriceReasoningDAO(settings.postgres)
    record = dao.fetch_latest(resolved["name"])
    return _normalize_record(record)


def list_industry_volume_price_history(
    industry: str,
    *,
    limit: int = 10,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    resolved = resolve_industry_label(industry, settings_path=settings_path)
    settings = load_settings(settings_path)
    dao = IndustryVolumePriceReasoningDAO(settings.postgres)
    result = dao.list_history(resolved["name"], limit=limit, offset=offset)
    items = [
        normalized
        for normalized in (_normalize_record(item) for item in result.get("items", []))
        if normalized
    ]
    return {"total": int(result.get("total", 0)), "items": items}


__all__ = [
    "build_industry_volume_price_dataset",
    "generate_industry_volume_price_reasoning",
    "get_latest_industry_volume_price_reasoning",
    "list_industry_volume_price_history",
]
