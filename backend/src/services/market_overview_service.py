"""Service building aggregated market overview payload and reasoning."""

from __future__ import annotations

import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from ..config.settings import load_settings
from ..api_clients import generate_finance_analysis
from ..dao import (
    HSGTFundFlowDAO,
    IndexHistoryDAO,
    MarginAccountDAO,
    MarketActivityDAO,
    MarketFundFlowDAO,
    MarketOverviewInsightDAO,
    PeripheralInsightDAO,
    RealtimeIndexDAO,
)
from . import (
    get_latest_macro_insight,
    get_latest_market_insight,
)

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

    market_insight = get_latest_market_insight()
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

    insight = MarketOverviewInsightDAO(settings.postgres).fetch_latest()
    latest_reasoning = None
    if insight:
        latest_reasoning = {
            "summary": insight.get("summary_json"),
            "rawText": insight.get("raw_response"),
            "model": insight.get("model"),
            "generatedAt": _serialize_datetime(insight.get("generated_at")),
        }

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
        "latestReasoning": latest_reasoning,
    }

    return _serialise_value(payload)


MARKET_OVERVIEW_PROMPT = """
你是一名熟悉中国A股的策略分析师。下面提供的 JSON 数据包括指数实时/历史表现、已有的市场与宏观推理结果、资金流向、外围洞察以及市场活跃度。

请根据这些信息判断当前大盘的整体状态，输出严格的 JSON 对象，字段为：
- "bias": 在 ["bullish","neutral","bearish"] 中选择，代表多头/中性/空头倾向；
- "confidence": 介于 0 和 1 之间的小数，表示结论把握度；
- "summary": 中文综述，不少于 120 字，需要同时覆盖指数走势、资金流向、外围与宏观要素；
- "key_signals": 数组，列出 4-6 个关键信号，每个元素包含 {"title": 标题, "detail": 中文描述}；
- "position_suggestion": 中文仓位建议，不少于 80 字，需兼顾短线和中线视角；
- "risks": 数组，列出至少 3 个潜在风险或不确定性。

以下是数据：
{news_content}
"""


def generate_market_overview_reasoning(
    *,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    overview = build_market_overview_payload(settings_path=settings_path)
    settings = load_settings(settings_path)
    generated_at_local = datetime.now(_LOCAL_TZ)
    generated_at_db = generated_at_local.replace(tzinfo=None)

    summary_text: Optional[str] = None
    model_name: Optional[str] = None
    if run_llm:
        if settings.deepseek is None:
            run_llm = False
        else:
            model_name = "deepseek-reasoner"
            prompt_payload = json.dumps(overview, ensure_ascii=False, separators=(",", ":"))
            prompt = MARKET_OVERVIEW_PROMPT.replace("{news_content}", prompt_payload)
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
                model_name = result.get("model") or model_name
            elif isinstance(result, str):
                summary_text = result

    default_summary = {
        "bias": "neutral",
        "confidence": 0,
        "summary": "暂无推理结果，待模型输出后再评估。",
        "key_signals": [],
        "position_suggestion": "暂无模型建议，请等待任务完成。",
        "risks": [],
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
            summary_dict = {**default_summary, "summary": str(summary_text)}
    else:
        summary_dict = default_summary
        raw_payload = json.dumps(default_summary, ensure_ascii=False)

    summary_dict.setdefault("bias", "neutral")
    summary_dict.setdefault("confidence", 0)
    summary_dict.setdefault("key_signals", [])
    summary_dict.setdefault("position_suggestion", "")
    summary_dict.setdefault("risks", [])

    serialised_summary = _serialise_value(summary_dict)

    MarketOverviewInsightDAO(settings.postgres).insert_snapshot(
        generated_at=generated_at_db,
        summary_json=serialised_summary,
        raw_response=raw_payload,
        model=model_name,
    )

    return {
        "overview": overview,
        "summary": serialised_summary,
        "rawText": raw_payload,
        "model": model_name,
        "generatedAt": generated_at_local.isoformat(),
    }


__all__ = ["build_market_overview_payload", "generate_market_overview_reasoning"]
