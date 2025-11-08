"""Run Wyckoff-style volume/price reasoning for concepts."""

from __future__ import annotations

import json
from datetime import datetime
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional

from zoneinfo import ZoneInfo

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from ..dao import ConceptIndexHistoryDAO, ConceptVolumePriceReasoningDAO
from .concept_constituent_service import resolve_concept_label

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

VOLUME_PRICE_PROMPT = """
你是一名熟悉威科夫量价分析法的A股策略顾问。我们提供了概念 {concept_name} ({concept_code}) 最近 {lookback_days} 个交易日的指数行情数据（单位：价格为元，vol为万手，amount为百万元）。

请基于 JSON 数据输出一段 JSON 推理，结构如下：
{{
  "wyckoffPhase": "吸筹/上涨/派发/下跌/震荡",
  "stageSummary": "不少于80字，说明当前阶段判断与理由",
  "volumeSignals": ["至少3条量能观察，需引用具体日期或倍数"],
  "priceSignals": ["至少3条价格或结构信号，说明支撑/阻力/影线等"],
  "compositeIntent": "主力意图，例如吸筹/派发/测试/不明",
  "strategy": ["至少2条操作建议，包含触发条件或仓位思路"],
  "risks": ["至少2条风险提示"],
  "checklist": ["至少3条后续需要跟踪的量价或事件条件"],
  "confidence": 0-1之间的小数"
}}

要求：
1. 务必引用数据中的具体涨跌幅、价格区间或“最近X日平均成交量”这类指标。
2. 以汉语输出，尽量结合威科夫阶段术语（PS、SC、ST、SOS、LPSY等）解释。
3. 若数据不足以推理，也要说明原因并列出需要补充的观察点。

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


def build_volume_price_dataset(
    concept: str,
    *,
    lookback_days: int = 90,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    resolved = resolve_concept_label(concept, settings_path=settings_path)
    settings = load_settings(settings_path)
    dao = ConceptIndexHistoryDAO(settings.postgres)
    query = dao.list_entries(
        concept_name=resolved["name"],
        limit=min(max(lookback_days + 20, 90), 200),
    )
    items = query.get("items") or []
    if not items:
        return {
            "concept": resolved["name"],
            "conceptCode": resolved["code"],
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
        "concept": resolved["name"],
        "conceptCode": resolved["code"],
        "lookbackDays": lookback_days,
        "history": normalised,
        "statistics": stats,
    }


def generate_concept_volume_price_reasoning(
    concept: str,
    *,
    lookback_days: int = 90,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    dataset = build_volume_price_dataset(concept, lookback_days=lookback_days, settings_path=settings_path)
    settings = load_settings(settings_path)
    generated_at = datetime.now(LOCAL_TZ)
    generated_at_db = generated_at.replace(tzinfo=None)
    reasoning_dao = ConceptVolumePriceReasoningDAO(settings.postgres)

    summary_text: Optional[str] = None
    model_name: Optional[str] = None
    if run_llm and dataset.get("history"):
        if settings.deepseek is None:
            run_llm = False
        else:
            prompt_payload = json.dumps(dataset, ensure_ascii=False, separators=(",", ":"))
            prompt = VOLUME_PRICE_PROMPT.format(
                concept_name=dataset["concept"],
                concept_code=dataset["conceptCode"],
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
            summary_dict["stageSummary"] = "暂无该概念的历史指数记录，无法进行量价推理。"
        raw_payload = json.dumps(summary_dict, ensure_ascii=False)

    record = {
        "concept": dataset["concept"],
        "conceptCode": dataset["conceptCode"],
        "lookbackDays": dataset["lookbackDays"],
        "historySize": len(dataset.get("history") or []),
        "statistics": dataset.get("statistics") or {},
        "summary": summary_dict,
        "rawText": raw_payload,
        "model": model_name,
        "generatedAt": generated_at.isoformat(),
    }

    reasoning_dao.insert_snapshot(
        concept_name=record["concept"],
        concept_code=record["conceptCode"],
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


def get_latest_volume_price_reasoning(
    concept: str,
    *,
    settings_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not concept:
        return None
    resolved = resolve_concept_label(concept, settings_path=settings_path)
    settings = load_settings(settings_path)
    dao = ConceptVolumePriceReasoningDAO(settings.postgres)
    record = dao.fetch_latest(resolved["name"])
    return _normalize_record(record)


def list_volume_price_history(
    concept: str,
    *,
    limit: int = 10,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    resolved = resolve_concept_label(concept, settings_path=settings_path)
    settings = load_settings(settings_path)
    dao = ConceptVolumePriceReasoningDAO(settings.postgres)
    result = dao.list_history(resolved["name"], limit=limit, offset=offset)
    items = [
        normalized
        for normalized in (_normalize_record(item) for item in result.get("items", []))
        if normalized
    ]
    return {"total": int(result.get("total", 0)), "items": items}


__all__ = [
    "build_volume_price_dataset",
    "generate_concept_volume_price_reasoning",
    "get_latest_volume_price_reasoning",
    "list_volume_price_history",
]
