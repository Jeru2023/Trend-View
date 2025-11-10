"""Generate integrated stock analysis by aggregating multi-source signals."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from zoneinfo import ZoneInfo

from ..api_clients import generate_finance_analysis, run_coze_agent
from ..config.settings import load_settings
from ..dao import StockIntegratedAnalysisDAO, StockNewsDAO
from .big_deal_fund_flow_service import list_big_deal_fund_flow
from .individual_fund_flow_service import list_individual_fund_flow
from .stock_basic_service import get_stock_detail
from .stock_volume_price_service import get_latest_stock_volume_price_reasoning

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_NEWS_DAYS = 10
DEFAULT_TRADE_DAYS = 10
MIN_NEWS_DAYS = 1
MAX_NEWS_DAYS = 30
MIN_TRADE_DAYS = 5
MAX_TRADE_DAYS = 30
COOLDOWN_MINUTES = 0
NEWS_LIMIT = 80
BIG_DEAL_LIMIT = 12

INTEGRATED_PROMPT_TEMPLATE = """
你是资深买方策略顾问，请基于以下 JSON 数据生成结构化综合分析，用中文输出，引用数字时必须带来源字段名或日期。
输出 JSON 模板：
{{
  "overview": "50-80字概览，包含交易/财务/资金流关键结论与数据引用",
  "keyFindings": ["句子1", "句子2"],
  "bullBearFactors": {{
    "bull": ["多头因素（引用概览/资金/财务/新闻/量价数据）"],
    "bear": ["空头因素（同上）"]
  }},
  "strategy": {{
    "timeframe": "短线/中线/长线之一，说明逻辑",
    "actions": ["操作建议1（含触发条件/参考价）", "操作建议2"]
  }},
  "risks": ["至少一条风险或待验证信号，引用数据"],
  "confidence": 0.0-1.0
}}

要求：
1. 不得编造上下文没有的数据，如某模块为空必须说明“暂无 XXX 数据”。
2. 充分引用概览卡片（交易统计、财务指标、财务摘要、资金流、个股大单）、10 日交易序列、10 日内新闻列表、量价推理结果。
3. 新闻引用需包含标题或日期，量价结果需引用阶段/信号字段。
4. 若数据互相矛盾，请解释原因并在 risks 中提示。

以下是输入数据：
{payload}
"""


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float):
        return value
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    numeric = _safe_float(value)
    return int(round(numeric)) if numeric is not None else None


def _format_trade_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        if len(text) == 8 and text.isdigit():
            try:
                parsed = datetime.strptime(text, "%Y%m%d")
            except ValueError:
                return text
        else:
            return text
    return parsed.strftime("%Y-%m-%d")


def _normalize_trade_history(history: List[Dict[str, Any]], trade_days: int) -> List[Dict[str, Any]]:
    if not history:
        return []
    normalized: List[Dict[str, Any]] = []
    for entry in history:
        trade_date_raw = entry.get("time") or entry.get("date")
        formatted_date = _format_trade_date(trade_date_raw)
        if not formatted_date:
            continue
        normalized.append(
            {
                "date": formatted_date,
                "open": _safe_float(entry.get("open")),
                "high": _safe_float(entry.get("high")),
                "low": _safe_float(entry.get("low")),
                "close": _safe_float(entry.get("close")),
                "volume": _safe_float(entry.get("volume")),
            }
        )
    normalized.sort(key=lambda item: item["date"])
    if not normalized:
        return []
    lookback = max(MIN_TRADE_DAYS, min(trade_days, MAX_TRADE_DAYS))
    enriched: List[Dict[str, Any]] = []
    prev_close: Optional[float] = None
    for entry in normalized:
        close_value = entry.get("close")
        pct_change = None
        if prev_close not in (None, 0) and close_value is not None:
            pct_change = round(((close_value - prev_close) / prev_close) * 100, 2)
        enriched.append({**entry, "pctChange": pct_change})
        if close_value is not None:
            prev_close = close_value
    return enriched[-lookback:]


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(LOCAL_TZ).isoformat()
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=LOCAL_TZ).isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def _sanitize_news_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sanitized: List[Dict[str, Any]] = []
    for entry in records:
        title = (entry.get("title") or "").strip()
        if not title:
            continue
        sanitized.append(
            {
                "id": entry.get("id"),
                "title": title,
                "summary": (entry.get("content") or "").strip() or None,
                "source": entry.get("source"),
                "url": entry.get("url"),
                "publishedAt": _serialize_value(entry.get("published_at") or entry.get("publishedAt")),
            }
        )
    return sanitized


def build_stock_integrated_context(
    code: str,
    *,
    news_days: int = DEFAULT_NEWS_DAYS,
    trade_days: int = DEFAULT_TRADE_DAYS,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    detail = get_stock_detail(code, history_limit=180, settings_path=settings_path)
    if not detail:
        raise ValueError(f"Stock '{code}' not found.")

    normalized_news_days = max(MIN_NEWS_DAYS, min(news_days, MAX_NEWS_DAYS))
    normalized_trade_days = max(MIN_TRADE_DAYS, min(trade_days, MAX_TRADE_DAYS))

    settings = load_settings(settings_path)
    news_dao = StockNewsDAO(settings.postgres)
    news_since = datetime.now(LOCAL_TZ) - timedelta(days=normalized_news_days)
    since_naive = news_since.replace(tzinfo=None)
    news_records = news_dao.list_since(detail["profile"]["code"], since=since_naive, limit=NEWS_LIMIT)
    news_items = _sanitize_news_records(news_records)

    individual_flow = list_individual_fund_flow(stock_code=detail["profile"]["code"], limit=100, settings_path=settings_path)
    big_deals = list_big_deal_fund_flow(stock_code=detail["profile"]["code"], limit=BIG_DEAL_LIMIT, settings_path=settings_path)
    volume_reasoning = get_latest_stock_volume_price_reasoning(detail["profile"]["code"], settings_path=settings_path)

    trade_history = _normalize_trade_history(detail.get("dailyTradeHistory") or [], normalized_trade_days)

    context = {
        "code": detail["profile"]["code"],
        "name": detail["profile"].get("name"),
        "generatedAt": datetime.now(LOCAL_TZ).isoformat(),
        "newsWindowDays": normalized_news_days,
        "tradeWindowDays": normalized_trade_days,
        "profile": _serialize_value(detail.get("profile") or {}),
        "tradingData": _serialize_value(detail.get("tradingData") or {}),
        "tradingStats": _serialize_value(detail.get("tradingStats") or {}),
        "financialData": _serialize_value(detail.get("financialData") or {}),
        "financialStats": _serialize_value(detail.get("financialStats") or {}),
        "businessProfile": _serialize_value(detail.get("businessProfile") or {}),
        "businessComposition": _serialize_value(detail.get("businessComposition") or {}),
        "individualFundFlow": _serialize_value(individual_flow.get("items") if isinstance(individual_flow, dict) else individual_flow),
        "bigDeals": _serialize_value(big_deals.get("items") if isinstance(big_deals, dict) else big_deals),
        "dailyTrades": trade_history,
        "news": news_items,
        "volumeReasoning": _serialize_value(
            volume_reasoning.get("summary") if isinstance(volume_reasoning, dict) else None
        ),
        "volumeReasoningMeta": _serialize_value(volume_reasoning) if volume_reasoning else None,
        "volumeReasoningNotice": None if volume_reasoning else "暂无量价推理结果，可先运行量价分析模块。",
    }
    return context


def generate_stock_integrated_analysis(
    code: str,
    *,
    news_days: int = DEFAULT_NEWS_DAYS,
    trade_days: int = DEFAULT_TRADE_DAYS,
    run_llm: bool = True,
    force: bool = False,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    if not code:
        raise ValueError("Stock code is required.")

    settings = load_settings(settings_path)
    dao = StockIntegratedAnalysisDAO(settings.postgres)
    context = build_stock_integrated_context(
        code,
        news_days=news_days,
        trade_days=trade_days,
        settings_path=settings_path,
    )

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
            if elapsed < timedelta(minutes=COOLDOWN_MINUTES):
                raise RuntimeError("距离上次综合分析不到5分钟，请稍后再试或开启强制刷新。")

    summary_text: Optional[str] = None
    model_name: Optional[str] = None
    llm_available = bool(settings.coze or settings.deepseek)
    if run_llm and llm_available:
        payload = json.dumps(context, ensure_ascii=False, separators=(",", ":"))
        stock_label = context.get("name") or context.get("code") or "该股票"
        coze_prompt = (
            f"请阅读以下 JSON 数据，生成一份专业、全面、深入的投资分析报告，"
            f"重点评估{stock_label}的估值合理性并给出明确的投资建议。"
            "报告必须覆盖以下章节：公司概况、行业分析、行业前景、竞争优势、财务分析、技术分析、风险因素、估值分析、投资建议。\n\n"
            f"{payload}"
        )
        if settings.coze is not None:
            coze_result = run_coze_agent(
                coze_prompt,
                settings=settings.coze,
            )
            if coze_result:
                summary_text = coze_result.get("content")
                model_name = coze_result.get("model") or "coze-agent"

        if not summary_text and settings.deepseek is not None:
            prompt = INTEGRATED_PROMPT_TEMPLATE.format(payload=payload)
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

    default_summary = {
        "overview": "模型尚未生成综合分析。",
        "keyFindings": [],
        "bullBearFactors": {"bull": [], "bear": []},
        "strategy": {"timeframe": "", "actions": []},
        "risks": [],
        "confidence": 0,
    }

    if not summary_text:
        if not llm_available:
            default_summary["overview"] = "系统未配置 LLM 凭据，无法生成综合分析。"
        summary_payload = json.dumps(default_summary, ensure_ascii=False)
        summary_dict = default_summary
        raw_payload = summary_payload
    else:
        raw_payload = summary_text
        try:
            parsed = json.loads(summary_text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            summary_dict = {**default_summary, **parsed}
        else:
            summary_dict = {**default_summary, "overview": summary_text}

    record_id = dao.insert_snapshot(
        stock_code=context["code"],
        stock_name=context.get("name"),
        news_days=context.get("newsWindowDays") or news_days,
        trade_days=context.get("tradeWindowDays") or trade_days,
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
        "newsDays": context.get("newsWindowDays"),
        "tradeDays": context.get("tradeWindowDays"),
        "summary": summary_dict,
        "rawText": raw_payload,
        "model": model_name,
        "context": context,
        "generatedAt": generated_at,
    }


def get_latest_stock_integrated_analysis(
    code: str,
    *,
    settings_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    settings = load_settings(settings_path)
    dao = StockIntegratedAnalysisDAO(settings.postgres)
    record = dao.fetch_latest(code)
    if not record:
        return None
    generated_at = record.get("generatedAt")
    if isinstance(generated_at, datetime) and generated_at.tzinfo is None:
        record["generatedAt"] = generated_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(LOCAL_TZ)
    return record


def list_stock_integrated_analysis_history(
    code: str,
    *,
    limit: int = 10,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    if not code:
        return {"total": 0, "items": []}
    settings = load_settings(settings_path)
    dao = StockIntegratedAnalysisDAO(settings.postgres)
    history = dao.list_history(code, limit=limit, offset=offset)
    items: List[Dict[str, Any]] = []
    for entry in history.get("items", []):
        generated_at = entry.get("generatedAt")
        if isinstance(generated_at, datetime) and generated_at.tzinfo is None:
            entry["generatedAt"] = generated_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(LOCAL_TZ)
        items.append(entry)
    return {"total": history.get("total", 0), "items": items}


__all__ = [
    "build_stock_integrated_context",
    "generate_stock_integrated_analysis",
    "get_latest_stock_integrated_analysis",
    "list_stock_integrated_analysis_history",
]
