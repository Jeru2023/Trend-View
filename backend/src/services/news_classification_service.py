"""LLM-driven relevance and impact classification for unified news articles."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from .news_pipeline_service import (
    acquire_for_impact,
    acquire_for_relevance,
    save_impact_results,
    save_relevance_results,
)

logger = logging.getLogger(__name__)

RELEVANCE_PROMPT_TEMPLATE = (
    "你是一位中国A股市场的资深投研分析师。请阅读以下新闻内容，判断其与中国A股市场的相关性。\n"
    "请严格按照 JSON 格式回复，不得输出任何多余文字。字段定义如下：\n"
    "{\n"
    '  "is_relevant": true/false,  // 是否与A股高度相关\n'
    '  "confidence": 0-1,          // 置信度，小数形式\n'
    '  "reason": "简洁理由",        // 不超过40个汉字\n'
    '  "focus_topics": ["关键词1", "关键词2"],   // 关键词，可为空数组\n'
    '  "suggested_levels": ["market","industry","sector","theme","stock"] // 预计关注层级，可为空数组\n'
    "}\n"
    "新闻信息如下：\n{news_content}"
)

IMPACT_PROMPT_TEMPLATE = (
    "你是一位中国A股市场的资深策略分析师。请基于以下新闻内容，对其对A股的影响进行深入评估，并严格按照 JSON 格式输出。字段定义如下：\n"
    "{\n"
    '  "impact_summary": "一句话概要",\n'
    '  "impact_analysis": "详细分析，不少于80字，需同时陈述正面影响与潜在风险",\n'
    '  "impact_confidence": 0-1, // 置信度\n'
    '  "impact_levels": ["market","industry","sector","theme","stock"],\n'
    '  "impact_markets": ["指数或市场"],\n'
    '  "impact_industries": ["行业名称"],\n'
    '  "impact_sectors": ["板块名称"],\n'
    '  "impact_themes": ["题材概念"],\n'
    '  "impact_stocks": ["相关个股（代码或简称）"],\n'
    '  "extra_metadata": {\n'
    '      "subject_level": "国家级/部门/行业/企业等",\n'
    '      "impact_scope": "影响范围描述",\n'
    '      "impact_scope_levels": ["大盘","行业","板块","概念","个股"],\n'
    '      "impact_scope_details": {\n'
    '          "大盘": ["上证指数"...],\n'
    '          "行业": ["新能源"...],\n'
    '          "板块": ["锂电池"...],\n'
    '          "概念": ["机器人"...],\n'
    '          "个股": ["300750.SZ"...]\n'
    '      },\n'
    '      "event_type": "monetary_policy|fiscal_policy|macro_policy|macro_data|market_liquidity|regulation|geopolitics|global_macro|credit_policy|other",\n'
    '      "time_sensitivity": "短期/中期/长期/阶段性/持续性/一次性",\n'
    '      "quant_signal": "若存在量化信号则描述，否则写无",\n'
    '      "macro_score": 0-1, // 评估新闻与宏观/大盘相关度\n'
    '      "macro_tags": ["宏观主题关键词"],\n'
    '      "impact_severity": "critical|high|medium|low",\n'
    '      "severity_score": 0-1, // 影响力度评分\n'
    '      "focus_topics": ["相关焦点"],\n'
    '      "reasoning": "判定宏观影响与严重程度的依据"\n'
    "  }\n"
    "}\n"
    "若新闻主要涉及个股或局部行业，对大盘影响有限，请将 impact_severity 设为 low，macro_score ≤ 0.3，并说明原因。\n"
    "请仅输出一个JSON对象，不得包含注释或额外解释。以下是新闻内容：\n{news_content}"
)

IMPACT_LEVEL_ALIASES: Dict[str, str] = {
    "market": "market",
    "大盘": "market",
    "指数": "market",
    "industry": "industry",
    "产业": "industry",
    "行业": "industry",
    "sector": "sector",
    "板块": "sector",
    "theme": "theme",
    "概念": "theme",
    "题材": "theme",
    "stock": "stock",
    "个股": "stock",
    "公司": "stock",
}

SCOPE_KEY_ALIASES: Dict[str, str] = {
    "大盘": "market",
    "指数": "market",
    "market": "market",
    "行业": "industry",
    "产业": "industry",
    "industry": "industry",
    "板块": "sector",
    "sector": "sector",
    "概念": "theme",
    "题材": "theme",
    "theme": "theme",
    "个股": "stock",
    "公司": "stock",
    "stock": "stock",
}


def classify_relevance_batch(
    *,
    batch_size: int = 10,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    """Run relevance classification for pending articles."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    if not getattr(settings, "deepseek", None):
        elapsed = time.perf_counter() - started
        logger.warning("DeepSeek configuration missing; skipping relevance classification.")
        return {"rows": 0, "elapsedSeconds": elapsed, "skipped": True}

    batch_limit = max(1, min(int(batch_size), 50))
    articles = acquire_for_relevance(limit=batch_limit, settings_path=settings_path)
    if not articles:
        elapsed = time.perf_counter() - started
        return {"rows": 0, "elapsedSeconds": elapsed, "requested": 0}

    results: List[Dict[str, object]] = []
    for article in articles:
        article_id = article.get("article_id")
        if not article_id:
            continue
        news_text = _build_article_text(article)
        if not news_text:
            results.append(
                {
                    "article_id": article_id,
                    "error": "Empty article content",
                }
            )
            continue

        try:
            raw_response = generate_finance_analysis(
                news_text,
                settings=settings.deepseek,  # type: ignore[arg-type]
                prompt_template=RELEVANCE_PROMPT_TEMPLATE,
                temperature=0.2,
            )
        except Exception as exc:  # pragma: no cover - external call
            logger.exception("DeepSeek relevance request failed for article %s: %s", article_id, exc)
            results.append({"article_id": article_id, "error": str(exc)})
            continue

        parsed = _parse_relevance_response(raw_response)
        parsed["article_id"] = article_id
        results.append(parsed)

    if results:
        save_relevance_results(results, settings_path=settings_path)

    processed = sum(1 for item in results if not item.get("error"))
    elapsed = time.perf_counter() - started
    return {
        "rows": processed,
        "elapsedSeconds": elapsed,
        "requested": len(articles),
    }


def classify_impact_batch(
    *,
    batch_size: int = 10,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    """Run impact tagging for articles already marked as relevant."""
    started = time.perf_counter()
    settings = load_settings(settings_path)
    if not getattr(settings, "deepseek", None):
        elapsed = time.perf_counter() - started
        logger.warning("DeepSeek configuration missing; skipping impact classification.")
        return {"rows": 0, "elapsedSeconds": elapsed, "skipped": True}

    batch_limit = max(1, min(int(batch_size), 30))
    articles = acquire_for_impact(limit=batch_limit, settings_path=settings_path)
    if not articles:
        elapsed = time.perf_counter() - started
        return {"rows": 0, "elapsedSeconds": elapsed, "requested": 0}

    results: List[Dict[str, object]] = []
    for article in articles:
        article_id = article.get("article_id")
        if not article_id:
            continue

        news_text = _build_article_text(article, prefer_full_content=True)
        if not news_text:
            results.append({"article_id": article_id, "error": "Empty article content"})
            continue

        try:
            raw_response = generate_finance_analysis(
                news_text,
                settings=settings.deepseek,  # type: ignore[arg-type]
                prompt_template=IMPACT_PROMPT_TEMPLATE,
                temperature=0.25,
            )
        except Exception as exc:  # pragma: no cover - external call
            logger.exception("DeepSeek impact request failed for article %s: %s", article_id, exc)
            results.append({"article_id": article_id, "error": str(exc)})
            continue

        parsed = _parse_impact_response(raw_response)
        parsed["article_id"] = article_id
        results.append(parsed)

    if results:
        save_impact_results(results, settings_path=settings_path)

    processed = sum(1 for item in results if not item.get("error"))
    elapsed = time.perf_counter() - started
    return {
        "rows": processed,
        "elapsedSeconds": elapsed,
        "requested": len(articles),
    }


def _build_article_text(article: Dict[str, object], *, prefer_full_content: bool = False) -> str:
    title = _clean(article.get("title"))
    summary = _clean(article.get("summary"))
    content = _clean(article.get("content"))

    if prefer_full_content and content:
        primary_body = _truncate(content, 3000)
    elif summary:
        primary_body = _truncate(summary, 2000)
    elif content:
        primary_body = _truncate(content, 3000)
    else:
        primary_body = ""

    published_at = article.get("published_at")
    published_str = ""
    if isinstance(published_at, datetime):
        localized = published_at.replace(tzinfo=None)
        published_str = localized.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(published_at, str):
        published_str = published_at

    source = _clean(article.get("source"))
    raw_payload = article.get("raw_payload")
    additional_info = _extract_additional_text(raw_payload)

    parts: List[str] = []
    if source:
        parts.append(f"来源：{source}")
    if published_str:
        parts.append(f"发布时间：{published_str}")
    if title:
        parts.append(f"标题：{title}")
    if summary and summary != primary_body:
        parts.append(f"摘要：{summary}")
    if primary_body:
        parts.append(f"正文：{primary_body}")
    if additional_info:
        parts.append(f"附加信息：{additional_info}")

    return "\n".join(part for part in parts if part).strip()


def _extract_additional_text(raw_payload: Optional[object]) -> str:
    if not raw_payload:
        return ""
    try:
        payload = json.loads(raw_payload)
    except (TypeError, ValueError):
        return ""
    if not isinstance(payload, dict):
        return ""
    candidates: List[str] = []
    for key in ("content", "digest", "description", "intro", "detail"):
        value = payload.get(key)
        cleaned = _clean(value)
        if cleaned:
            candidates.append(cleaned)
    combined = " ".join(candidates)
    return _truncate(combined, 1500)


def _parse_relevance_response(raw: Optional[str]) -> Dict[str, object]:
    if not raw:
        return {"error": "Empty response from model"}

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"error": raw.strip()[:200]}

    if not isinstance(payload, dict):
        return {"error": str(payload)[:200]}

    is_relevant = _coerce_bool(payload.get("is_relevant"))
    confidence = _coerce_float(payload.get("confidence"))
    reason = _clean(payload.get("reason")) or "模型未给出原因"

    metadata: Dict[str, object] = {}
    focus_topics = _coerce_list(payload.get("focus_topics"))
    if focus_topics:
        metadata["focus_topics"] = focus_topics
    suggested_levels = _normalize_levels(payload.get("suggested_levels"))
    if suggested_levels:
        metadata["suggested_levels"] = suggested_levels

    result: Dict[str, object] = {
        "is_relevant": is_relevant,
        "confidence": confidence,
        "reason": reason[:120],
    }
    if metadata:
        result["extra_metadata"] = metadata
    return result


def _parse_impact_response(raw: Optional[str]) -> Dict[str, object]:
    if not raw:
        return {"error": "Empty response from model"}

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"error": raw.strip()[:200]}

    if not isinstance(payload, dict):
        return {"error": str(payload)[:200]}

    def _pick(source: dict, *keys: str) -> object:
        for key in keys:
            if key in source and source[key] is not None:
                return source[key]
        return None

    impact_summary = _clean(
        _pick(payload, "impact_summary", "impactSummary", "summary", "impactSynopsis")
    )
    impact_analysis = _clean(
        _pick(payload, "impact_analysis", "impactAnalysis", "analysis", "details", "impactDetail")
    )
    impact_confidence = _coerce_float(
        _pick(payload, "impact_confidence", "impactConfidence", "confidence")
    )

    impact_levels = _normalize_levels(
        _pick(payload, "impact_levels", "impactLevels", "levels")
    )
    impact_markets = _coerce_list(
        _pick(payload, "impact_markets", "impactMarkets", "markets", "marketImpact")
    )
    impact_industries = _coerce_list(
        _pick(payload, "impact_industries", "impactIndustries", "industries")
    )
    impact_sectors = _coerce_list(
        _pick(payload, "impact_sectors", "impactSectors", "sectors")
    )
    impact_themes = _coerce_list(
        _pick(payload, "impact_themes", "impactThemes", "themes", "concepts")
    )
    impact_stocks = _coerce_list(
        _pick(payload, "impact_stocks", "impactStocks", "stocks", "tickers", "symbols")
    )

    metadata = _normalize_extra_metadata(
        _pick(payload, "extra_metadata", "extraMetadata") or {}
    )

    fallback_metadata = _normalize_extra_metadata(payload)
    if fallback_metadata:
        metadata = {**fallback_metadata, **metadata}

    impact_severity_root = _pick(payload, "impact_severity", "impactSeverity")
    if impact_severity_root and isinstance(metadata, dict):
        cleaned_severity = _clean(impact_severity_root)
        if cleaned_severity:
            metadata.setdefault("impact_severity", cleaned_severity)

    severity_score_root = _pick(payload, "severity_score", "severityScore")
    severity_score_value = _coerce_float(severity_score_root)
    if severity_score_value is not None:
        metadata.setdefault("severity_score", severity_score_value)

    macro_score_root = _pick(payload, "macro_score", "macroScore")
    macro_score_value = _coerce_float(macro_score_root)
    if macro_score_value is not None:
        metadata.setdefault("macro_score", macro_score_value)

    macro_tags_root = _pick(payload, "macro_tags", "macroTags")
    macro_tags_list = _coerce_list(macro_tags_root)
    if macro_tags_list:
        metadata.setdefault("macro_tags", macro_tags_list)

    reasoning_root = _pick(payload, "reasoning", "analysisReasoning", "rationale")
    reasoning_value = _clean(reasoning_root)
    if reasoning_value:
        metadata.setdefault("reasoning", reasoning_value)

    metadata.setdefault("raw_response", raw)

    result: Dict[str, object] = {
        "impact_summary": impact_summary,
        "impact_analysis": impact_analysis,
        "impact_confidence": impact_confidence,
        "impact_levels": impact_levels,
        "impact_markets": impact_markets,
        "impact_industries": impact_industries,
        "impact_sectors": impact_sectors,
        "impact_themes": impact_themes,
        "impact_stocks": impact_stocks,
    }
    if metadata:
        result["extra_metadata"] = metadata
    return result


def _normalize_extra_metadata(raw_metadata: object) -> Dict[str, object]:
    if not raw_metadata:
        return {}
    if isinstance(raw_metadata, dict):
        metadata = raw_metadata.copy()
    else:
        return {"notes": _clean(raw_metadata)}

    normalized: Dict[str, object] = {}
    key_aliases: Dict[str, tuple[str, ...]] = {
        "subject_level": ("subject_level", "subjectLevel", "subject"),
        "impact_scope": ("impact_scope", "impactScope"),
        "event_type": ("event_type", "eventType"),
        "time_sensitivity": ("time_sensitivity", "timeSensitivity"),
        "quant_signal": ("quant_signal", "quantSignal"),
        "focus_topics": ("focus_topics", "focusTopics"),
        "macro_score": ("macro_score", "macroScore"),
        "impact_severity": ("impact_severity", "impactSeverity"),
        "severity_score": ("severity_score", "severityScore"),
        "macro_tags": ("macro_tags", "macroTags"),
        "macro_focus": ("macro_focus", "macroFocus"),
        "reasoning": ("reasoning", "analysisReasoning", "rationale"),
    }
    handled_aliases = {alias for aliases in key_aliases.values() for alias in aliases}

    for canonical, aliases in key_aliases.items():
        value = None
        for alias in aliases:
            if alias in metadata and metadata[alias] not in (None, "", []):
                value = metadata[alias]
                break
        if value is None:
            continue
        if canonical in {"focus_topics", "macro_tags", "macro_focus"}:
            topics = _coerce_list(value)
            if topics:
                normalized[canonical] = topics
            continue
        if canonical in {"macro_score", "severity_score"}:
            numeric_value = _coerce_float(value)
            if numeric_value is not None:
                normalized[canonical] = numeric_value
            continue
        if canonical == "impact_severity":
            cleaned_value = _clean(value)
            if cleaned_value:
                normalized[canonical] = cleaned_value.lower()
            continue
        if canonical == "reasoning":
            cleaned_value = _clean(value)
            if cleaned_value:
                normalized[canonical] = cleaned_value
            continue
        if isinstance(value, (list, tuple, set)):
            cleaned_iterable = [item for item in (_clean(element) for element in value) if item]
            if cleaned_iterable:
                normalized[canonical] = cleaned_iterable
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            normalized[canonical] = value
            continue
        cleaned = _clean(value)
        if cleaned:
            normalized[canonical] = cleaned

    scoped_levels = _coerce_list(metadata.get("impact_scope_levels"))
    if scoped_levels:
        normalized["impact_scope_levels"] = scoped_levels

    scope_details = metadata.get("impact_scope_details")
    if isinstance(scope_details, dict):
        normalized_scopes: Dict[str, List[str]] = {}
        for key, values in scope_details.items():
            canonical = SCOPE_KEY_ALIASES.get(str(key).strip(), str(key).strip())
            normalized_scopes[canonical] = _coerce_list(values)
        if normalized_scopes:
            normalized["impact_scope_details"] = normalized_scopes

    reserved = handled_aliases | {"impact_scope_levels", "impact_scope_details"}
    remaining = {k: v for k, v in metadata.items() if k not in reserved}
    if remaining:
        normalized.setdefault("extra_notes", remaining)

    return normalized


def _normalize_levels(values: object) -> List[str]:
    items = _coerce_list(values)
    normalized: List[str] = []
    for item in items:
        code = IMPACT_LEVEL_ALIASES.get(item, item)
        if code and code not in normalized:
            normalized.append(code)
    return normalized


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "y", "1"}:
            return True
        if lowered in {"false", "no", "n", "0"}:
            return False
    return False


def _coerce_float(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(numeric, 1.0))


def _coerce_list(value: object) -> List[str]:
    if value is None:
        return []
    items: List[str] = []
    if isinstance(value, (list, tuple, set)):
        iterable = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if (text.startswith("[") and text.endswith("]")) or (text.startswith("(") and text.endswith(")")):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, (list, tuple, set)):
                    iterable = parsed
                else:
                    iterable = text.split(",")
            except (json.JSONDecodeError, TypeError):
                iterable = text.split(",")
        else:
            iterable = text.split(",")
    else:
        return []
    for element in iterable:
        text = str(element).strip()
        if text:
            items.append(text)
    return items


def _clean(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _truncate(text: str, limit: int) -> str:
    if not text or len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


__all__ = [
    "classify_relevance_batch",
    "classify_impact_batch",
]
