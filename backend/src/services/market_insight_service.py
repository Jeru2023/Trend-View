"""Services for generating aggregated market insight summaries."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from zoneinfo import ZoneInfo
from psycopg2 import sql

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from ..dao import NewsArticleDAO, NewsInsightDAO, NewsMarketInsightDAO

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_ARTICLE_LIMIT = 40
DEEPSEEK_REASONER_MODEL = "deepseek-reasoner"
MAX_OUTPUT_TOKENS = 32000
MAX_CANDIDATE_LIMIT = 150
_CANDIDATE_MULTIPLIER = 3

ALLOWED_EVENT_TYPES = {
    "monetary_policy",
    "macro_policy",
    "fiscal_policy",
    "macro_data",
    "market_liquidity",
    "regulation",
    "geopolitics",
    "global_macro",
    "credit_policy",
}

EVENT_TYPE_ALIASES = {
    "货币政策": "monetary_policy",
    "宏观政策": "macro_policy",
    "财政政策": "fiscal_policy",
    "宏观数据": "macro_data",
    "流动性": "market_liquidity",
    "监管": "regulation",
    "地缘政治": "geopolitics",
    "全球宏观": "global_macro",
    "信贷政策": "credit_policy",
    "政策": "regulation",
    "宏观": "macro_policy",
    "宏观经济": "macro_data",
    "经济数据": "macro_data",
    "监管政策": "regulation",
}

PRIORITY_SUBJECT_LEVELS = {
    "国家级",
    "国务院",
    "中央政府",
    "央行",
    "人民银行",
    "财政部",
    "证监会",
    "银保监会",
    "证监局",
    "国家发展改革委",
    "国家统计局",
    "发改委",
}

SEVERITY_WEIGHTS = {
    "critical": 1.0,
    "high": 0.8,
    "medium": 0.55,
    "low": 0.25,
}

MARKET_INSIGHT_PROMPT = """
你是一名资深的中国A股策略分析师。请阅读以下最近24小时内与大盘直接相关的新闻摘要，并基于它们给出整体市场洞察。

【分析要求】
1. 全面梳理对大盘（上证指数、深证成指、沪深300等）影响显著的因素。
2. 明确整体情绪（利多/中性/利空），并给出0-1之间的综合置信度。
3. 总结主要驱动事件、受益/受压行业板块、潜在风险提示、重点关注指数或板块。
4. 若新闻观点存在分歧，请客观说明并指出潜在不确定性。
5. 请保证输出信息丰富：
   - \"market_overview\" 需不少于120字，覆盖宏观、政策、资金、情绪等维度。
   - \"key_drivers\" 至少列出3条，每条需概括驱动逻辑。
   - 如有风险因素与操作建议，分别不少于3条（若不足请写明原因）。
   - \"recommended_actions\" 不得少于80字，明确短期与中期操作框架。
   - \"detailed_notes\" 中每条 \"analysis\" 至少两句话，涵盖正面与潜在风险或不确定性。
6. 输出必须采用JSON格式，键名使用蛇形命名法。结构如下：
{
  "sentiment": "bullish|neutral|bearish",
  "confidence": 0-1 的小数,
  "market_overview": "总体研判",
  "key_drivers": ["驱动因素1", "驱动因素2", ...],
  "sectors_to_watch": ["行业或板块"...],
  "indices_to_watch": ["指数"...],
  "risk_factors": ["风险点"...],
  "recommended_actions": "给投资者的操作建议",
  "detailed_notes": [
      {
         "title": "新闻标题",
         "impact_summary": "一句话总结",
         "analysis": "该新闻对大盘的含义",
         "confidence": 0-1 的小数
      }
  ]
}
7. 若输入新闻不足以得出结论，请在 JSON 中说明原因。

以下是新闻列表（按时间倒序）：
{news_content}
"""


def _local_now() -> datetime:
    return datetime.now(LOCAL_TZ).replace(tzinfo=None)


def _format_article_for_prompt(records: List[Dict[str, object]]) -> str:
    lines: List[str] = []
    for idx, record in enumerate(records, start=1):
        published = record.get("published_at")
        if isinstance(published, datetime):
            published_str = published.strftime("%Y-%m-%d %H:%M")
        else:
            published_str = str(published)
        lines.append(
            f"[{idx}] {published_str} | {record.get('title','--')}\n"
            f"  ImpactSummary: {record.get('impact_summary') or '--'}\n"
            f"  ImpactAnalysis: {record.get('impact_analysis') or '--'}\n"
            f"  Markets: {', '.join(record.get('impact_markets') or []) or '--'} | Confidence: {record.get('impact_confidence') or 'N/A'}"
        )
    return "\n".join(lines)


def _decode_json_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except json.JSONDecodeError:
        pass
    return [part.strip() for part in str(value).split(",") if part.strip()]


def collect_recent_market_headlines(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    limit: int = DEFAULT_ARTICLE_LIMIT,
    settings_path: Optional[str] = None,
) -> List[Dict[str, object]]:
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    insight_dao = NewsInsightDAO(settings.postgres)

    window_end = _local_now()
    window_start = window_end - timedelta(hours=max(lookback_hours, 1))

    with article_dao.connect() as conn:
        article_dao.ensure_table(conn)
        insight_dao.ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT a.article_id, a.source, a.title, a.summary, a.published_at, a.url,
                           i.impact_summary, i.impact_analysis, i.impact_confidence,
                           i.impact_levels, i.impact_markets, i.impact_industries, i.impact_sectors,
                           i.impact_themes, i.impact_stocks, i.relevance_confidence,
                           i.extra_metadata
                    FROM {schema}.{articles} AS a
                    JOIN {schema}.{insights} AS i ON i.article_id = a.article_id
                    WHERE a.processing_status = 'completed'
                      AND i.impact_checked_at IS NOT NULL
                      AND a.published_at BETWEEN %s AND %s
                      AND (
                          (i.impact_levels IS NOT NULL AND POSITION('"market"' IN i.impact_levels) > 0)
                          OR (i.impact_markets IS NOT NULL AND LENGTH(TRIM(i.impact_markets)) > 0)
                      )
                    ORDER BY i.impact_confidence DESC NULLS LAST, a.published_at DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(article_dao.config.schema),
                    articles=sql.Identifier(article_dao._table_name),
                    insights=sql.Identifier(insight_dao._table_name),
                ),
                (
                    window_start,
                    window_end,
                    _candidate_limit(limit),
                ),
            )
            rows = cur.fetchall()

    columns = [
        "article_id",
        "source",
        "title",
        "summary",
        "published_at",
        "url",
        "impact_summary",
        "impact_analysis",
        "impact_confidence",
        "impact_levels",
        "impact_markets",
        "impact_industries",
        "impact_sectors",
        "impact_themes",
        "impact_stocks",
        "relevance_confidence",
        "extra_metadata",
    ]

    articles: List[Dict[str, object]] = []
    for row in rows:
        record = {column: value for column, value in zip(columns, row)}
        record["impact_levels"] = _decode_json_list(record.get("impact_levels"))
        record["impact_markets"] = _decode_json_list(record.get("impact_markets"))
        record["impact_industries"] = _decode_json_list(record.get("impact_industries"))
        record["impact_sectors"] = _decode_json_list(record.get("impact_sectors"))
        record["impact_themes"] = _decode_json_list(record.get("impact_themes"))
        record["impact_stocks"] = _decode_json_list(record.get("impact_stocks"))

        raw_meta = record.get("extra_metadata")
        metadata: Dict[str, object] = {}
        if isinstance(raw_meta, dict):
            metadata = raw_meta
        elif isinstance(raw_meta, str) and raw_meta.strip():
            try:
                metadata = json.loads(raw_meta)
            except json.JSONDecodeError:
                metadata = {}
        record["extra_metadata"] = metadata or {}

        macro_score = _clamp(_safe_float(metadata.get("macro_score")))
        severity_score = _clamp(_safe_float(metadata.get("severity_score")))
        impact_severity = str(metadata.get("impact_severity") or "").lower() or None
        event_type = str(metadata.get("event_type") or "").lower()
        subject_level = metadata.get("subject_level")
        macro_tags = metadata.get("macro_tags") if isinstance(metadata.get("macro_tags"), (list, tuple, set)) else []

        record["macro_score"] = macro_score
        record["severity_score"] = severity_score
        record["impact_severity"] = impact_severity
        record["event_type"] = event_type
        record["subject_level"] = subject_level
        record["macro_tags"] = [str(tag).strip() for tag in macro_tags if str(tag).strip()]

        articles.append(record)
    return _prioritize_articles(articles, limit, reference_time=window_end)


def generate_market_insight_summary(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    limit: int = DEFAULT_ARTICLE_LIMIT,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    settings = load_settings(settings_path)
    summary_dao = NewsMarketInsightDAO(settings.postgres)

    articles = collect_recent_market_headlines(
        lookback_hours=lookback_hours,
        limit=limit,
        settings_path=settings_path,
    )

    if not articles:
        raise ValueError("No market-impact headlines found in the specified window")

    window_end = _local_now()
    window_start = window_end - timedelta(hours=max(lookback_hours, 1))

    prompt_news_block = _format_article_for_prompt(articles)
    prompt = MARKET_INSIGHT_PROMPT.replace("{news_content}", prompt_news_block)

    deepseek_settings = settings.deepseek
    if deepseek_settings is None:
        raise RuntimeError("DeepSeek configuration is required for market insight generation")

    logger.info(
        "Generating market insight summary: headlines=%s lookback=%sh model=%s",
        len(articles),
        lookback_hours,
        DEEPSEEK_REASONER_MODEL,
    )

    started = time.perf_counter()
    response = generate_finance_analysis(
        prompt,
        settings=deepseek_settings,
        prompt_template="{news_content}",
        model_override=DEEPSEEK_REASONER_MODEL,
        response_format={"type": "json_object"},
        max_output_tokens=MAX_OUTPUT_TOKENS,
        temperature=0.2,
        return_usage=True,
    )
    elapsed_ms = int((time.perf_counter() - started) * 1000)

    if not response or not isinstance(response, dict):
        raise RuntimeError("DeepSeek reasoner did not return a valid response")

    content = response.get("content") or ""
    usage = response.get("usage") or {}
    prompt_tokens = usage.get("prompt_tokens") if isinstance(usage, dict) else None
    completion_tokens = usage.get("completion_tokens") if isinstance(usage, dict) else None
    total_tokens = usage.get("total_tokens") if isinstance(usage, dict) else None

    summary_json: Optional[dict] = None
    if content:
        try:
            summary_json = json.loads(content)
        except json.JSONDecodeError:
            logger.warning("Market insight response is not valid JSON; storing raw content")
            summary_json = None

    summary_payload = {
        "summary_id": None,
        "generated_at": window_end,
        "window_start": window_start,
        "window_end": window_end,
        "headline_count": len(articles),
        "summary_json": json.dumps(summary_json, ensure_ascii=False) if summary_json else None,
        "raw_response": content,
        "referenced_articles": json.dumps(
            [
                {
                    "article_id": item.get("article_id"),
                    "source": item.get("source"),
                    "title": item.get("title"),
                    "impact_summary": item.get("impact_summary"),
                    "impact_analysis": item.get("impact_analysis"),
                    "impact_confidence": item.get("impact_confidence"),
                    "markets": item.get("impact_markets"),
                    "published_at": item.get("published_at").isoformat() if isinstance(item.get("published_at"), datetime) else item.get("published_at"),
                    "url": item.get("url"),
                    "impact_severity": (item.get("impact_severity") or (item.get("extra_metadata") or {}).get("impact_severity")),
                    "severity_score": item.get("severity_score") or (item.get("extra_metadata") or {}).get("severity_score"),
                    "macro_score": item.get("macro_score") or (item.get("extra_metadata") or {}).get("macro_score"),
                    "event_type": item.get("event_type") or (item.get("extra_metadata") or {}).get("event_type"),
                    "subject_level": item.get("subject_level") or (item.get("extra_metadata") or {}).get("subject_level"),
                    "macro_tags": item.get("macro_tags") or (item.get("extra_metadata") or {}).get("macro_tags"),
                }
                for item in articles
            ],
            ensure_ascii=False,
        ),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "elapsed_ms": elapsed_ms,
        "model_used": DEEPSEEK_REASONER_MODEL,
    }

    summary_id = summary_dao.insert_summary(summary_payload)
    summary_payload["summary_id"] = summary_id
    summary_payload["summary_json"] = summary_json
    summary_payload["referenced_articles"] = json.loads(summary_payload["referenced_articles"])

    return summary_payload


def get_latest_market_insight(*, settings_path: Optional[str] = None) -> Optional[Dict[str, object]]:
    settings = load_settings(settings_path)
    summary_dao = NewsMarketInsightDAO(settings.postgres)
    return summary_dao.latest_summary()


def list_market_insights(*, limit: int = 10, settings_path: Optional[str] = None) -> List[Dict[str, object]]:
    settings = load_settings(settings_path)
    summary_dao = NewsMarketInsightDAO(settings.postgres)
    return summary_dao.list_summaries(limit=limit)


def _safe_float(value: object, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(value, upper))


def _candidate_limit(limit: int) -> int:
    base = max(1, int(limit))
    candidate = base * _CANDIDATE_MULTIPLIER
    candidate = max(base, candidate)
    return max(1, min(candidate, MAX_CANDIDATE_LIMIT))


def _prioritize_articles(
    records: List[Dict[str, object]],
    limit: int,
    *,
    reference_time: Optional[datetime] = None,
) -> List[Dict[str, object]]:
    if not records:
        return []

    limit = max(5, min(int(limit), 50))
    reference_time = reference_time or _local_now()

    scored: List[tuple[float, Dict[str, object]]] = []
    for record in records:
        metadata = record.get("extra_metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
            record["extra_metadata"] = metadata
        elif not isinstance(metadata, dict):
            metadata = {}
            record["extra_metadata"] = {}

        metadata_missing = not metadata

        macro_score = _clamp(_safe_float(metadata.get("macro_score")))
        severity_score = _clamp(_safe_float(metadata.get("severity_score")))
        impact_severity = str(metadata.get("impact_severity") or "").lower() or None
        severity_value = SEVERITY_WEIGHTS.get(impact_severity or "", severity_score)
        if impact_severity in SEVERITY_WEIGHTS:
            severity_value = max(SEVERITY_WEIGHTS[impact_severity], severity_score)
        else:
            severity_value = max(severity_value, severity_score)

        event_type_raw = str(metadata.get("event_type") or "").lower()
        event_type = EVENT_TYPE_ALIASES.get(event_type_raw, event_type_raw)

        subject_level_raw = metadata.get("subject_level")
        subject_level = str(subject_level_raw).strip() if subject_level_raw else None

        macro_tags = metadata.get("macro_tags") if isinstance(metadata.get("macro_tags"), (list, tuple, set)) else []
        macro_tags_list = [str(tag).strip() for tag in macro_tags if str(tag).strip()]

        record["macro_score"] = macro_score
        record["severity_score"] = severity_value
        record["impact_severity"] = impact_severity
        record["event_type"] = event_type
        record["subject_level"] = subject_level
        record["macro_tags"] = macro_tags_list

        has_macro_fields = any(
            key in metadata and metadata.get(key) not in (None, "")
            for key in ("macro_score", "impact_severity", "severity_score")
        )
        metadata_missing = metadata_missing or not has_macro_fields

        levels = record.get("impact_levels") or []
        if "market" not in levels and not record.get("impact_markets"):
            continue

        if not metadata_missing:
            if macro_score < 0.45 and severity_value < 0.5:
                continue
            if event_type and event_type not in ALLOWED_EVENT_TYPES and (macro_score < 0.65 or severity_value < 0.65):
                continue

        score = 0.0

        if not metadata_missing:
            score += macro_score * 5.0
            score += severity_value * 5.0
        else:
            score -= 1.5

        if event_type in ALLOWED_EVENT_TYPES:
            score += 1.8
        elif event_type:
            score -= 0.8

        if subject_level and subject_level in PRIORITY_SUBJECT_LEVELS:
            score += 1.4
        elif subject_level:
            score += 0.3

        if macro_tags_list:
            score += min(len(macro_tags_list), 4) * 0.25

        confidence = record.get("impact_confidence")
        try:
            confidence_value = max(0.0, min(float(confidence or 0), 1.0))
        except (TypeError, ValueError):
            confidence_value = 0.0
        score += confidence_value * 4.0

        if "market" in levels:
            score += 3.0
        if "industry" in levels:
            score += 1.0

        markets = record.get("impact_markets") or []
        industries = record.get("impact_industries") or []
        themes = record.get("impact_themes") or []

        if markets:
            score += 1.2
        if industries:
            score += 0.6
        if themes:
            score += 0.5

        published_at = record.get("published_at")
        if isinstance(published_at, datetime):
            age_hours = (reference_time - published_at).total_seconds() / 3600.0
            age_hours = max(0.0, age_hours)
            score += max(0.0, 24.0 - age_hours) * 0.15

        score += len(set(markets)) * 0.15
        score += len(set(industries)) * 0.1
        score += len(set(themes)) * 0.05

        record["impact_summary"] = _truncate_text(record.get("impact_summary"))
        record["impact_analysis"] = _truncate_text(record.get("impact_analysis"), max_length=220)

        scored.append((score, record))

    scored.sort(key=lambda item: item[0], reverse=True)

    primary_seen: set[str] = set()
    selected: List[Dict[str, object]] = []
    backup: List[Dict[str, object]] = []

    for _, record in scored:
        signature = _primary_signature(record)
        if signature and signature not in primary_seen:
            selected.append(record)
            primary_seen.add(signature)
        else:
            backup.append(record)

        if len(selected) >= limit:
            break

    if len(selected) < limit and backup:
        needed = limit - len(selected)
        selected.extend(backup[:needed])

    return selected[:limit]


def _truncate_text(value: Optional[object], *, max_length: int = 160) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "…"


def _primary_signature(record: Dict[str, object]) -> Optional[str]:
    for key in ("impact_markets", "impact_industries", "impact_themes", "impact_levels"):
        values = record.get(key)
        if values:
            first = str(values[0]).strip()
            if first:
                return f"{key}:{first}"
    title = record.get("title")
    if title:
        return f"title:{title[:40]}"
    return None


__all__ = [
    "collect_recent_market_headlines",
    "generate_market_insight_summary",
    "get_latest_market_insight",
    "list_market_insights",
]
