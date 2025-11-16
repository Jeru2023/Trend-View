"""Generate valuation-focused analysis for a single stock."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from zoneinfo import ZoneInfo

from ..api_clients import generate_finance_analysis, run_coze_agent
from ..config.settings import load_settings
from ..dao import StockValuationAnalysisDAO
from .stock_integrated_analysis_service import build_stock_integrated_context

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
VALUATION_COOLDOWN_MINUTES = 0

VALUATION_PROMPT_TEMPLATE = "今天是 {date}。请直接分析股票 {name}，输出你的估值观点及结论，格式不限。"

DEFAULT_VALUATION_SUMMARY: Dict[str, Any] = {
    "valuationSummary": "尚未生成估值分析。",
    "valuationRange": {"bear": None, "base": None, "bull": None},
    "valuationMethods": [],
    "peerComparison": [],
    "drivers": [],
    "risks": [],
    "recommendation": {"stance": "", "targetPrice": "", "actions": []},
    "confidence": 0,
}


def _normalize_summary(content: Optional[str]) -> Dict[str, Any]:
    if not content:
        return {**DEFAULT_VALUATION_SUMMARY}
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return {**DEFAULT_VALUATION_SUMMARY, "valuationSummary": content}
    if isinstance(parsed, dict):
        merged = {**DEFAULT_VALUATION_SUMMARY, **parsed}
        recommendation = merged.get("recommendation") or {}
        merged["recommendation"] = {
            "stance": recommendation.get("stance") or "",
            "targetPrice": recommendation.get("targetPrice") or "",
            "actions": recommendation.get("actions") or [],
        }
        range_payload = merged.get("valuationRange") or {}
        merged["valuationRange"] = {
            "bear": range_payload.get("bear"),
            "base": range_payload.get("base"),
            "bull": range_payload.get("bull"),
        }
        return merged
    return {**DEFAULT_VALUATION_SUMMARY, "valuationSummary": str(parsed)}


def generate_stock_valuation_analysis(
    code: str,
    *,
    run_llm: bool = True,
    force: bool = False,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    if not code:
        raise ValueError("Stock code is required.")

    settings = load_settings(settings_path)
    dao = StockValuationAnalysisDAO(settings.postgres)
    context = build_stock_integrated_context(code, settings_path=settings_path)

    generated_at = datetime.now(LOCAL_TZ)
    generated_at_db = generated_at.replace(tzinfo=None)

    latest = dao.fetch_latest(context["code"])
    if not force and latest:
        latest_generated = latest.get("generatedAt")
        if isinstance(latest_generated, datetime):
            latest_dt = latest_generated
        else:
            try:
                latest_dt = datetime.fromisoformat(str(latest_generated))
            except (TypeError, ValueError):
                latest_dt = None
        if latest_dt:
            if latest_dt.tzinfo is None:
                latest_local = latest_dt.replace(tzinfo=LOCAL_TZ)
            else:
                latest_local = latest_dt.astimezone(LOCAL_TZ)
            elapsed = generated_at - latest_local
            if elapsed < timedelta(minutes=VALUATION_COOLDOWN_MINUTES):
                raise RuntimeError("估值分析冷却中，请稍后再试或开启强制刷新。")

    summary_text: Optional[str] = None
    model_name: Optional[str] = None
    llm_available = settings.coze is not None or settings.deepseek is not None

    if run_llm and llm_available:
        payload = context.get("name") or context["code"]
        today_label = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
        if settings.coze is not None:
            coze_text = f"今天是{today_label}。请分析股票 {payload}，给出估值观点。"
            coze_result = run_coze_agent(
                coze_text,
                settings=settings.coze,
            )
            if coze_result:
                summary_text = coze_result.get("content")
                model_name = coze_result.get("model") or "coze-agent"

        if not summary_text and settings.deepseek is not None:
            prompt = VALUATION_PROMPT_TEMPLATE.format(name=payload, date=today_label)
            result = generate_finance_analysis(
                prompt,
                settings=settings.deepseek,
                prompt_template="{news_content}",
                model_override="deepseek-reasoner",
                temperature=0.15,
                max_output_tokens=4096,
            )
            if isinstance(result, dict):
                summary_text = result.get("content")
                model_name = result.get("model") or "deepseek-reasoner"
            elif isinstance(result, str):
                summary_text = result
                model_name = "deepseek-reasoner"

    if not summary_text:
        if not llm_available:
            fallback = {**DEFAULT_VALUATION_SUMMARY, "valuationSummary": "系统未配置 LLM 凭据，无法生成估值分析。"}
        else:
            fallback = {**DEFAULT_VALUATION_SUMMARY}
        summary_dict = fallback
        raw_payload = json.dumps(fallback, ensure_ascii=False)
    else:
        summary_dict = _normalize_summary(summary_text)
        raw_payload = summary_text

    record_id = dao.insert_snapshot(
        stock_code=context["code"],
        stock_name=context.get("name"),
        summary_json=summary_dict,
        raw_text=raw_payload,
        model=model_name,
        context_json=context,
        generated_at=generated_at_db,
    )

    return {
        "id": record_id,
        "code": context["code"],
        "name": context.get("name"),
        "summary": summary_dict,
        "rawText": raw_payload,
        "model": model_name,
        "generatedAt": generated_at_db,
        "context": context,
    }


def get_latest_stock_valuation_analysis(code: str, *, settings_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    settings = load_settings(settings_path)
    dao = StockValuationAnalysisDAO(settings.postgres)
    return dao.fetch_latest(code)


def list_stock_valuation_analysis_history(
    code: str,
    *,
    limit: int = 10,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    if not code:
        return {"total": 0, "items": []}
    settings = load_settings(settings_path)
    dao = StockValuationAnalysisDAO(settings.postgres)
    return dao.list_history(code, limit=limit, offset=offset)


__all__ = [
    "generate_stock_valuation_analysis",
    "get_latest_stock_valuation_analysis",
    "list_stock_valuation_analysis_history",
]
