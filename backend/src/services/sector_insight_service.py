"""Services for building sector/theme insights from classified news headlines."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from zoneinfo import ZoneInfo
from psycopg2 import sql

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from ..dao import NewsArticleDAO, NewsInsightDAO, NewsSectorInsightDAO

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_ARTICLE_LIMIT = 60
MAX_CANDIDATE_LIMIT = 200
_CANDIDATE_MULTIPLIER = 3

DEEPSEEK_REASONER_MODEL = "deepseek-reasoner"
MAX_OUTPUT_TOKENS = 32000

MAX_GROUP_ARTICLES = 5
PROMPT_GROUP_LIMIT = 12
PROMPT_GROUP_ARTICLE_LIMIT = 2
PROMPT_GROUP_TEXT_LIMIT = 220
PROMPT_HEADLINE_LIMIT = 25

SECTOR_INSIGHT_PROMPT = """
你是一名专注中国A股产业、板块与题材轮动的策略分析师。我们已经基于新闻管线对最近{lookback_hours}小时内的资讯进行了结构化处理，按行业/板块/题材聚合成 JSON 数据。请阅读这些数据后，以 JSON 形式输出你的研判。

输出格式固定为：
{
  "bias": "bullish|neutral|bearish",               // 对整体板块风格的倾向判断
  "confidence": 0-1 之间的小数,                      // 结论把握度
  "headline_summary": "不少于120字的综述",             // 说明数据时间、主导线索、资金风格、节奏判断
  "highlight_sectors": [                           // 至少列出3个重点行业/板块/题材
      {
         "name": "板块名称",
         "tag_type": "industry|sector|theme",
         "stance": "bullish|bearish|watch",
         "confidence": 0-1 之间的小数,
         "drivers": "不少于60字，阐述推动逻辑、关联事件及催化",
         "risks": "指出潜在回撤信号与风险防线",
         "timeframe": "短线|中线|阶段性|持续性 等",
         "event_types": ["事件类型列表"],
         "focus_topics": ["关键词列表"]
      }
  ],
  "rotation_view": "不少于80字，总结资金风格、大小盘/成长价值的切换线索",
  "risk_warnings": ["至少3条风险提示"],
  "actionable_ideas": ["至少3条可执行策略建议，每条不少于40字"],
  "data_freshness": "说明数据最新时间戳与样本规模"
}

若数据不足以给出可靠判断，请在 JSON 中写明原因并给出后续观察方向。

以下是聚合后的 JSON 数据：
{sector_payload}
"""


SEVERITY_WEIGHT = {
    "critical": 1.0,
    "high": 0.85,
    "medium": 0.6,
    "low": 0.3,
}

TAG_TYPE_PRIORITY = {
    "industry": 3.0,
    "sector": 2.5,
    "theme": 2.0,
}


def _local_now() -> datetime:
    return datetime.now(LOCAL_TZ).replace(tzinfo=None)


def _candidate_limit(limit: int) -> int:
    return max(1, min(int(limit) * _CANDIDATE_MULTIPLIER, MAX_CANDIDATE_LIMIT))


def _decode_json_list(value: Optional[object]) -> List[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except (TypeError, json.JSONDecodeError):
        pass
    text = str(value)
    parts = [part.strip() for part in text.replace("|", ",").split(",")]
    return [part for part in parts if part]


def _safe_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp_unit(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return max(0.0, min(float(value), 1.0))


def _to_local_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ)


def _to_iso(value: Optional[datetime]) -> Optional[str]:
    dt = _to_local_datetime(value)
    return dt.isoformat() if dt else None


def _normalize_string_list(value: Optional[object]) -> List[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        items = value
    else:
        text = str(value)
        for delimiter in ["/", "|", "、", "，", ";"]:
            text = text.replace(delimiter, ",")
        items = text.split(",")
    result = []
    for item in items:
        trimmed = str(item).strip()
        if trimmed:
            result.append(trimmed)
    return result


def _extract_severity(metadata: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    raw = metadata.get("impact_severity") or metadata.get("impactSeverity")
    severity_label = str(raw).strip().lower() if raw else None
    if severity_label not in SEVERITY_WEIGHT:
        severity_label = None
    severity_score = _clamp_unit(_safe_float(metadata.get("severity_score")))
    if severity_score is None and severity_label:
        severity_score = SEVERITY_WEIGHT.get(severity_label)
    return severity_label, severity_score


def _normalize_event_type(metadata: Dict[str, Any]) -> Optional[str]:
    value = metadata.get("event_type") or metadata.get("eventType")
    if not value:
        return None
    text = str(value).strip().lower()
    return text or None


def _normalize_focus_topics(metadata: Dict[str, Any]) -> List[str]:
    topics = metadata.get("focus_topics") or metadata.get("focusTopics")
    return _normalize_string_list(topics)


def _normalize_time_sensitivity(metadata: Dict[str, Any]) -> List[str]:
    value = metadata.get("time_sensitivity") or metadata.get("timeSensitivity")
    return _normalize_string_list(value)


def _collect_tags(article: Dict[str, Any]) -> List[Tuple[str, str]]:
    tags: List[Tuple[str, str]] = []
    for tag in article.get("impact_industries", []) or []:
        trimmed = str(tag).strip()
        if trimmed:
            tags.append((trimmed, "industry"))
    for tag in article.get("impact_sectors", []) or []:
        trimmed = str(tag).strip()
        if trimmed:
            tags.append((trimmed, "sector"))
    for tag in article.get("impact_themes", []) or []:
        trimmed = str(tag).strip()
        if trimmed:
            tags.append((trimmed, "theme"))
    # Deduplicate while keeping order preference industry > sector > theme
    seen = set()
    ordered: List[Tuple[str, str]] = []
    for name, tag_type in sorted(tags, key=lambda item: TAG_TYPE_PRIORITY.get(item[1], 1.0), reverse=True):
        key = (name, tag_type)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def collect_recent_sector_headlines(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    limit: int = DEFAULT_ARTICLE_LIMIT,
    minimum_confidence: float = 0.3,
    settings_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Load recent news articles tagged with industry/sector/theme impact."""

    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    insight_dao = NewsInsightDAO(settings.postgres)

    window_end = _local_now()
    window_start = window_end - timedelta(hours=max(1, int(lookback_hours)))

    with article_dao.connect() as conn:
        article_dao.ensure_table(conn)
        insight_dao.ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT a.article_id, a.source, a.title, a.summary, a.published_at, a.url,
                           a.processing_status, a.content, a.language,
                           i.is_relevant, i.relevance_confidence,
                           i.impact_summary, i.impact_analysis, i.impact_confidence,
                           i.impact_levels, i.impact_markets, i.impact_industries, i.impact_sectors, i.impact_themes,
                           i.impact_stocks, i.extra_metadata
                    FROM {schema}.{articles} AS a
                    JOIN {schema}.{insights} AS i ON i.article_id = a.article_id
                    WHERE a.processing_status = 'completed'
                      AND i.impact_checked_at IS NOT NULL
                      AND a.published_at BETWEEN %s AND %s
                      AND (
                           (i.impact_industries IS NOT NULL AND LENGTH(TRIM(i.impact_industries)) > 0)
                        OR (i.impact_sectors IS NOT NULL AND LENGTH(TRIM(i.impact_sectors)) > 0)
                        OR (i.impact_themes IS NOT NULL AND LENGTH(TRIM(i.impact_themes)) > 0)
                      )
                    ORDER BY a.published_at DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(article_dao.config.schema),
                    articles=sql.Identifier(article_dao._table_name),
                    insights=sql.Identifier(insight_dao._table_name),
                ),
                (window_start, window_end, _candidate_limit(limit)),
            )
            rows = cur.fetchall()

    columns = [
        "article_id",
        "source",
        "title",
        "summary",
        "published_at",
        "url",
        "processing_status",
        "content",
        "language",
        "is_relevant",
        "relevance_confidence",
        "impact_summary",
        "impact_analysis",
        "impact_confidence",
        "impact_levels",
        "impact_markets",
        "impact_industries",
        "impact_sectors",
        "impact_themes",
        "impact_stocks",
        "extra_metadata",
    ]

    articles: List[Dict[str, Any]] = []
    for row in rows:
        record = {column: value for column, value in zip(columns, row)}
        record["published_at"] = _to_local_datetime(record.get("published_at"))
        record["impact_levels"] = _decode_json_list(record.get("impact_levels"))
        record["impact_markets"] = _decode_json_list(record.get("impact_markets"))
        record["impact_industries"] = _decode_json_list(record.get("impact_industries"))
        record["impact_sectors"] = _decode_json_list(record.get("impact_sectors"))
        record["impact_themes"] = _decode_json_list(record.get("impact_themes"))
        record["impact_stocks"] = _decode_json_list(record.get("impact_stocks"))

        metadata_raw = record.get("extra_metadata")
        metadata: Dict[str, Any] = {}
        if isinstance(metadata_raw, dict):
            metadata = dict(metadata_raw)
        elif isinstance(metadata_raw, str) and metadata_raw.strip():
            try:
                metadata = json.loads(metadata_raw)
            except json.JSONDecodeError:
                metadata = {}
        record["extra_metadata"] = metadata

        confidence = _clamp_unit(_safe_float(record.get("impact_confidence")))
        record["impact_confidence"] = confidence

        if confidence is not None and confidence < minimum_confidence:
            continue

        articles.append(record)

    return articles[:limit]


def build_sector_group_snapshot(
    articles: Sequence[Dict[str, Any]],
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Aggregate tagged articles into sector/theme groups."""

    if reference_time is None:
        reference_time = datetime.now(LOCAL_TZ)
    else:
        reference_time = _to_local_datetime(reference_time) or datetime.now(LOCAL_TZ)

    group_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    assignments: Dict[str, Dict[str, Any]] = {}
    excluded: List[str] = []

    for article in articles:
        article_id = str(article.get("article_id") or "")
        tags = _collect_tags(article)
        if not tags:
            if article_id:
                excluded.append(article_id)
            continue

        metadata = article.get("extra_metadata") or {}
        severity_label, severity_score = _extract_severity(metadata)
        event_type = _normalize_event_type(metadata)
        focus_topics = _normalize_focus_topics(metadata)
        time_sensitivity = _normalize_time_sensitivity(metadata)
        impact_levels = article.get("impact_levels") or []

        published_at = _to_local_datetime(article.get("published_at"))

        assignment = assignments.setdefault(
            article_id,
            {
                "articleId": article_id,
                "title": article.get("title"),
                "impactSummary": article.get("impact_summary"),
                "impactAnalysis": article.get("impact_analysis"),
                "confidence": article.get("impact_confidence"),
                "severity": severity_label,
                "severityScore": severity_score,
                "eventType": event_type,
                "timeSensitivity": time_sensitivity,
                "focusTopics": focus_topics,
                "publishedAt": _to_iso(published_at),
                "source": article.get("source"),
                "url": article.get("url"),
                "tags": [],
                "tagTypes": [],
                "impactLevels": impact_levels,
            },
        )

        for tag_name, tag_type in tags:
            if tag_name not in assignment["tags"]:
                assignment["tags"].append(tag_name)
            if tag_type not in assignment["tagTypes"]:
                assignment["tagTypes"].append(tag_type)

            key = (tag_name, tag_type)
            group = group_map.setdefault(
                key,
                {
                    "name": tag_name,
                    "tagType": tag_type,
                    "articles": [],
                    "articleCount": 0,
                    "confidenceSum": 0.0,
                    "confidenceCount": 0,
                    "severityScoreSum": 0.0,
                    "severityCount": 0,
                    "maxSeverityLabel": None,
                    "maxSeverityScore": None,
                    "latestPublished": None,
                    "eventTypes": set(),
                    "timeSensitivity": set(),
                    "focusTopics": set(),
                    "impactLevels": set(),
                    "sources": set(),
                },
            )

            confidence = article.get("impact_confidence")
            if confidence is not None:
                group["confidenceSum"] += float(confidence)
                group["confidenceCount"] += 1

            if severity_score is not None:
                group["severityScoreSum"] += float(severity_score)
                group["severityCount"] += 1

            weight = SEVERITY_WEIGHT.get(severity_label or "", 0.0)
            current_max_weight = SEVERITY_WEIGHT.get(group["maxSeverityLabel"] or "", 0.0)
            if weight > current_max_weight:
                group["maxSeverityLabel"] = severity_label
                group["maxSeverityScore"] = severity_score

            if published_at:
                if not isinstance(group["latestPublished"], datetime) or (
                    published_at > group["latestPublished"]
                ):
                    group["latestPublished"] = published_at

            if event_type:
                group["eventTypes"].add(event_type)
            for item in time_sensitivity:
                group["timeSensitivity"].add(item)
            for topic in focus_topics:
                group["focusTopics"].add(topic)
            for level in impact_levels:
                group["impactLevels"].add(level)
            if article.get("source"):
                group["sources"].add(str(article.get("source")))

            group["articles"].append(
                {
                    "articleId": article_id,
                    "title": article.get("title"),
                    "impactSummary": article.get("impact_summary"),
                    "impactAnalysis": article.get("impact_analysis"),
                    "confidence": article.get("impact_confidence"),
                    "severity": severity_label,
                    "severityScore": severity_score,
                    "eventType": event_type,
                    "timeSensitivity": time_sensitivity,
                    "publishedAt": _to_iso(published_at),
                    "source": article.get("source"),
                    "url": article.get("url"),
                    "impactLevels": impact_levels,
                }
            )

            group["articleCount"] += 1

    groups: List[Dict[str, Any]] = []
    for group in group_map.values():
        avg_confidence = (
            group["confidenceSum"] / group["confidenceCount"]
            if group["confidenceCount"]
            else None
        )
        avg_severity = (
            group["severityScoreSum"] / group["severityCount"]
            if group["severityCount"]
            else None
        )
        max_severity_label = group["maxSeverityLabel"]
        max_severity_score = group["maxSeverityScore"]
        latest_published = group.get("latestPublished")

        recency_hours: Optional[float] = None
        if isinstance(latest_published, datetime):
            delta = reference_time - latest_published
            recency_hours = max(0.0, delta.total_seconds() / 3600.0)

        score = _compute_group_score(
            article_count=group["articleCount"],
            avg_confidence=avg_confidence,
            max_severity_label=max_severity_label,
            max_severity_score=max_severity_score,
            avg_severity=avg_severity,
            recency_hours=recency_hours,
            tag_type=group["tagType"],
            event_types=len(group["eventTypes"]),
            time_sensitivity=len(group["timeSensitivity"]),
            focus_topics=len(group["focusTopics"]),
        )

        ranked_articles = sorted(
            group["articles"],
            key=lambda item: (
                SEVERITY_WEIGHT.get((item.get("severity") or ""), 0.0),
                _safe_float(item.get("confidence"), 0.0) or 0.0,
                item.get("publishedAt") or "",
            ),
            reverse=True,
        )

        groups.append(
            {
                "name": group["name"],
                "tagType": group["tagType"],
                "articleCount": group["articleCount"],
                "averageConfidence": _round_optional(avg_confidence),
                "averageSeverityScore": _round_optional(avg_severity),
                "maxSeverity": max_severity_label,
                "maxSeverityScore": _round_optional(max_severity_score),
                "latestPublishedAt": _to_iso(latest_published),
                "eventTypes": sorted(group["eventTypes"]),
                "timeSensitivity": sorted(group["timeSensitivity"]),
                "focusTopics": sorted(group["focusTopics"]),
                "impactLevels": sorted(group["impactLevels"]),
                "sources": sorted(group["sources"]),
                "score": _round_optional(score, digits=4),
                "sampleArticles": ranked_articles[:MAX_GROUP_ARTICLES],
            }
        )

    groups.sort(key=lambda item: item.get("score") or 0.0, reverse=True)

    snapshot = {
        "generatedAt": _to_iso(reference_time),
        "lookbackHours": int(lookback_hours),
        "headlineCount": len(articles),
        "groupCount": len(groups),
        "groups": groups,
        "articleAssignments": list(assignments.values()),
        "excludedCount": len(excluded),
    }
    if excluded:
        snapshot["excludedArticleIds"] = excluded
    return snapshot


def _round_optional(value: Optional[float], *, digits: int = 3) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def _truncate_article_text(value: Optional[object], limit: int = PROMPT_GROUP_TEXT_LIMIT) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _build_prompt_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    prompt: Dict[str, Any] = {
        "generatedAt": snapshot.get("generatedAt"),
        "lookbackHours": snapshot.get("lookbackHours"),
        "headlineCount": snapshot.get("headlineCount"),
        "groupCount": snapshot.get("groupCount"),
        "excludedCount": snapshot.get("excludedCount"),
    }

    groups = snapshot.get("groups") or []
    sorted_groups = sorted(groups, key=lambda item: item.get("score") or 0, reverse=True)
    trimmed_groups: List[Dict[str, Any]] = []
    for group in sorted_groups[:PROMPT_GROUP_LIMIT]:
        tag_type = group.get("tagType") or group.get("tag_type")
        articles = group.get("sampleArticles") or group.get("sample_articles") or []
        trimmed_articles: List[Dict[str, Any]] = []
        for article in articles[:PROMPT_GROUP_ARTICLE_LIMIT]:
            trimmed_articles.append(
                {
                    "title": article.get("title"),
                    "impactSummary": _truncate_article_text(article.get("impactSummary")),
                    "impactAnalysis": _truncate_article_text(article.get("impactAnalysis")),
                    "confidence": article.get("confidence"),
                    "severity": article.get("severity"),
                    "eventType": article.get("eventType"),
                    "timeSensitivity": article.get("timeSensitivity"),
                    "focusTopics": article.get("focusTopics"),
                    "publishedAt": article.get("publishedAt"),
                }
            )

        trimmed_groups.append(
            {
                "name": group.get("name"),
                "tagType": tag_type,
                "score": group.get("score"),
                "articleCount": group.get("articleCount"),
                "averageConfidence": group.get("averageConfidence"),
                "maxSeverity": group.get("maxSeverity"),
                "maxSeverityScore": group.get("maxSeverityScore"),
                "latestPublishedAt": group.get("latestPublishedAt"),
                "eventTypes": group.get("eventTypes"),
                "timeSensitivity": group.get("timeSensitivity"),
                "focusTopics": group.get("focusTopics"),
                "impactLevels": group.get("impactLevels"),
                "sources": group.get("sources"),
                "sampleHeadlines": trimmed_articles,
            }
        )

    if trimmed_groups:
        prompt["topGroups"] = trimmed_groups

    assignments = snapshot.get("articleAssignments") or []
    trimmed_assignments: List[Dict[str, Any]] = []
    for article in assignments[:PROMPT_HEADLINE_LIMIT]:
        trimmed_assignments.append(
            {
                "title": article.get("title"),
                "impactSummary": _truncate_article_text(article.get("impactSummary")),
                "impactAnalysis": _truncate_article_text(article.get("impactAnalysis")),
                "confidence": article.get("confidence"),
                "severity": article.get("severity"),
                "eventType": article.get("eventType"),
                "timeSensitivity": article.get("timeSensitivity"),
                "focusTopics": article.get("focusTopics"),
                "tags": article.get("tags"),
                "tagTypes": article.get("tagTypes"),
                "publishedAt": article.get("publishedAt"),
            }
        )

    if trimmed_assignments:
        prompt["sampleHeadlines"] = trimmed_assignments

    return prompt


def _compute_group_score(
    *,
    article_count: int,
    avg_confidence: Optional[float],
    max_severity_label: Optional[str],
    max_severity_score: Optional[float],
    avg_severity: Optional[float],
    recency_hours: Optional[float],
    tag_type: str,
    event_types: int,
    time_sensitivity: int,
    focus_topics: int,
) -> float:
    score = 0.0
    if avg_confidence is not None:
        score += float(avg_confidence) * 4.0
    severity_weight = SEVERITY_WEIGHT.get(max_severity_label or "", 0.0)
    if severity_weight:
        score += severity_weight * 3.0
    if max_severity_score:
        score += float(max_severity_score) * 2.0
    elif avg_severity:
        score += float(avg_severity) * 2.0

    score += min(article_count, 6) * 0.65

    if recency_hours is not None:
        score += max(0.0, 24.0 - recency_hours) * 0.18

    score += TAG_TYPE_PRIORITY.get(tag_type, 1.5)
    score += min(event_types, 4) * 0.35
    score += min(time_sensitivity, 4) * 0.2
    score += min(focus_topics, 6) * 0.15

    return score


def generate_sector_insight_summary(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    limit: int = DEFAULT_ARTICLE_LIMIT,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate a DeepSeek-powered sector insight summary and persist it."""

    articles = collect_recent_sector_headlines(
        lookback_hours=lookback_hours,
        limit=limit,
        settings_path=settings_path,
    )
    if not articles:
        raise ValueError("No sector-impact headlines found in the specified window")

    reference_time = datetime.now(LOCAL_TZ)
    snapshot = build_sector_group_snapshot(
        articles,
        lookback_hours=lookback_hours,
        reference_time=reference_time,
    )


    prompt_snapshot = _build_prompt_snapshot(snapshot)
    payload_json = json.dumps(prompt_snapshot, ensure_ascii=False, separators=(",", ":"))
    prompt = SECTOR_INSIGHT_PROMPT.replace("{sector_payload}", payload_json)
    prompt = prompt.replace("{lookback_hours}", str(int(lookback_hours)))

    settings = load_settings(settings_path)
    summary_dao = NewsSectorInsightDAO(settings.postgres)

    summary_json: Optional[Dict[str, Any]] = None
    raw_response: Optional[str] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    elapsed_ms: Optional[int] = None
    model_used: Optional[str] = None

    if run_llm:
        deepseek_settings = settings.deepseek
        if deepseek_settings is None:
            raise RuntimeError("DeepSeek configuration is required for sector insight generation")

        logger.info(
            "Generating sector insight summary: groups=%s lookback=%sh model=%s",
            snapshot.get("groupCount"),
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

        if not isinstance(response, dict):
            logger.info("DeepSeek reasoner returned non-JSON payload, retrying without enforced response_format")
            started = time.perf_counter()
            response = generate_finance_analysis(
                prompt,
                settings=deepseek_settings,
                prompt_template="{news_content}",
                model_override=DEEPSEEK_REASONER_MODEL,
                response_format=None,
                max_output_tokens=MAX_OUTPUT_TOKENS,
                temperature=0.2,
                return_usage=True,
            )
            elapsed_ms = int((time.perf_counter() - started) * 1000)

        if not isinstance(response, dict):
            raise RuntimeError("DeepSeek reasoner did not return a valid response")

        raw_response = response.get("content") or ""
        usage = response.get("usage") or {}
        if isinstance(usage, dict):
            prompt_tokens = usage.get("prompt_tokens")
            completion_tokens = usage.get("completion_tokens")
            total_tokens = usage.get("total_tokens")
        model_used = response.get("model") or DEEPSEEK_REASONER_MODEL

        if raw_response:
            try:
                summary_json = json.loads(raw_response)
            except json.JSONDecodeError:
                logger.warning("Sector insight response is not valid JSON; storing raw content")
                summary_json = None

    window_end = reference_time
    window_start = window_end - timedelta(hours=max(1, int(lookback_hours)))

    summary_payload = {
        "summary_id": None,
        "generated_at": window_end,
        "window_start": window_start,
        "window_end": window_end,
        "headline_count": len(articles),
        "group_count": snapshot.get("groupCount") or 0,
        "group_snapshot": json.dumps(snapshot, ensure_ascii=False),
        "summary_json": json.dumps(summary_json, ensure_ascii=False) if summary_json is not None else None,
        "raw_response": raw_response,
        "referenced_articles": json.dumps(snapshot.get("articleAssignments") or [], ensure_ascii=False),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "elapsed_ms": elapsed_ms,
        "model_used": model_used,
    }

    summary_id = summary_dao.insert_summary(summary_payload)
    summary_payload["summary_id"] = summary_id
    summary_payload["group_snapshot"] = snapshot
    summary_payload["summary_json"] = summary_json
    summary_payload["referenced_articles"] = snapshot.get("articleAssignments") or []

    return summary_payload


def get_latest_sector_insight(*, settings_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    settings = load_settings(settings_path)
    summary_dao = NewsSectorInsightDAO(settings.postgres)
    record = summary_dao.latest_summary()
    if not record:
        return None

    def _ensure_iso(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return _to_iso(value)

    record["generated_at"] = _ensure_iso(record.get("generated_at"))
    record["window_start"] = _ensure_iso(record.get("window_start"))
    record["window_end"] = _ensure_iso(record.get("window_end"))
    return record


def list_sector_insights(
    *, limit: int = 10, settings_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    settings = load_settings(settings_path)
    summary_dao = NewsSectorInsightDAO(settings.postgres)
    records = summary_dao.list_summaries(limit=limit)
    results: List[Dict[str, Any]] = []
    for record in records:
        item = dict(record)
        item["generated_at"] = _to_iso(item.get("generated_at"))
        item["window_start"] = _to_iso(item.get("window_start"))
        item["window_end"] = _to_iso(item.get("window_end"))
        results.append(item)
    return results


__all__ = [
    "collect_recent_sector_headlines",
    "build_sector_group_snapshot",
    "generate_sector_insight_summary",
    "get_latest_sector_insight",
    "list_sector_insights",
]