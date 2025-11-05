"""Services for building concept-level insights."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

from zoneinfo import ZoneInfo

from psycopg2 import sql

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from ..dao import (
    ConceptIndexHistoryDAO,
    ConceptInsightDAO,
    NewsArticleDAO,
    NewsInsightDAO,
)
from .concept_index_history_service import sync_concept_index_history
from .sector_fund_flow_service import build_sector_fund_flow_snapshot

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

DEFAULT_LOOKBACK_HOURS = 48
DEFAULT_CONCEPT_LIMIT = 10
MAX_NEWS_PER_CONCEPT = 4
INDEX_LOOKBACK_DAYS = 60

DEEPSEEK_REASONER_MODEL = "deepseek-reasoner"
MAX_OUTPUT_TOKENS = 26000

CONCEPT_INSIGHT_PROMPT = """
你是一名聚焦A股主题投资的策略顾问。我们已经汇总了最近{lookback_hours}小时的热门概念资金热度、指数行情以及相关新闻。请阅读以下JSON数据，为投资团队输出一份结构化建议。

输出必须是 JSON，格式如下：
{
  "headline": "不少于80字概述本轮概念热度和主线",
  "market_view": "不少于120字，概括资金流向、量价趋势、主题逻辑和整体风险",
  "top_concepts": [
     {
        "name": "概念名称",
        "stance": "bullish|watch|bearish",
        "confidence": 0-1 的小数，
        "drivers": "不少于60字，说明资金/行情/新闻驱动",
        "key_metrics": ["用短句列出核心指标，如：5日涨幅+6.3%"],
        "representative_stocks": ["列出1-3只值得跟踪的龙头或补涨股"],
        "risk_flags": ["至少2条风险提示，可引用成交放量、政策扰动等"],
        "suggested_actions": ["至少2条策略建议，如：回踩支撑布局、突破追随等"]
     }
  ],
  "rotation_notes": "不少于80字，说明资金风格与题材切换线索",
  "risk_summary": ["至少3条全局风险要点"],
  "next_steps": ["不少于3条后续跟踪/数据观察建议"],
  "data_timestamp": "说明数据提取时间和覆盖范围"
}

若数据不足以判断，请明确写出原因并给出建议的补充信息。

以下是概念数据：
{concept_payload}
"""


def _local_now() -> datetime:
    return datetime.now(LOCAL_TZ).replace(tzinfo=None)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TZ).isoformat()
    return dt.astimezone(LOCAL_TZ).isoformat()


def _truncate_text(value: Optional[object], limit: int = 200) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _calculate_change(latest_close: float, reference_close: Optional[float]) -> Optional[float]:
    if reference_close is None:
        return None
    if reference_close == 0:
        return None
    return round(((latest_close - reference_close) / reference_close) * 100, 2)


def _parse_iso_datetime(value: Optional[str]) -> datetime:
    if not value:
        raise ValueError("Missing datetime value")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO datetime: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TZ)
    return parsed


def _compute_index_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    if not rows:
        return {
            "latestClose": None,
            "change1d": None,
            "change5d": None,
            "change20d": None,
            "avgVolume5d": None,
        }

    sorted_rows = sorted(rows, key=lambda item: item.get("trade_date"), reverse=True)
    latest = sorted_rows[0]
    latest_close = latest.get("close")
    change1d = latest.get("pct_chg")

    def _close_at(days: int) -> Optional[float]:
        if len(sorted_rows) <= days:
            return None
        return sorted_rows[days].get("close")

    change5d = _calculate_change(latest_close, _close_at(5)) if latest_close is not None else None
    change20d = _calculate_change(latest_close, _close_at(20)) if latest_close is not None else None

    volumes = [row.get("vol") for row in sorted_rows[:5] if isinstance(row.get("vol"), (int, float))]
    avg_volume = round(sum(volumes) / len(volumes), 2) if volumes else None

    return {
        "latestClose": latest_close,
        "change1d": change1d,
        "change5d": change5d,
        "change20d": change20d,
        "avgVolume5d": avg_volume,
    }


def _fetch_concept_news(
    article_dao: NewsArticleDAO,
    insight_dao: NewsInsightDAO,
    concept_name: str,
    *,
    lookback_hours: int,
    limit: int,
) -> List[Dict[str, Any]]:
    concept_key = concept_name.strip().lower()
    if not concept_key:
        return []

    window_start = _local_now() - timedelta(hours=max(1, lookback_hours))

    with article_dao.connect() as conn:
        article_dao.ensure_table(conn)
        insight_dao.ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT a.article_id, a.source, a.title, a.summary, a.published_at, a.url,
                           i.impact_summary, i.impact_analysis, i.impact_confidence,
                           i.relevance_confidence, i.relevance_reason,
                           i.impact_themes, i.impact_industries, i.impact_sectors, i.impact_stocks,
                           i.extra_metadata
                    FROM {schema_articles}.{articles} AS a
                    JOIN {schema_insights}.{insights} AS i ON i.article_id = a.article_id
                    WHERE a.processing_status = 'completed'
                      AND a.published_at >= %s
                      AND (
                          LOWER(COALESCE(i.impact_themes, '')) LIKE %s
                       OR LOWER(COALESCE(i.impact_industries, '')) LIKE %s
                       OR LOWER(COALESCE(i.impact_sectors, '')) LIKE %s
                       OR LOWER(COALESCE(i.impact_summary, '')) LIKE %s
                       OR LOWER(COALESCE(a.title, '')) LIKE %s
                       OR LOWER(COALESCE(a.summary, '')) LIKE %s
                      )
                    ORDER BY a.published_at DESC
                    LIMIT %s
                    """
                ).format(
                    schema_articles=sql.Identifier(article_dao.config.schema),
                    articles=sql.Identifier(article_dao._table_name),
                    schema_insights=sql.Identifier(insight_dao.config.schema),
                    insights=sql.Identifier(insight_dao._table_name),
                ),
                (
                    window_start,
                    f"%{concept_key}%",
                    f"%{concept_key}%",
                    f"%{concept_key}%",
                    f"%{concept_key}%",
                    f"%{concept_key}%",
                    f"%{concept_key}%",
                    limit,
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
        "relevance_confidence",
        "relevance_reason",
        "impact_themes",
        "impact_industries",
        "impact_sectors",
        "impact_stocks",
        "extra_metadata",
    ]

    results: List[Dict[str, Any]] = []
    for row in rows:
        record = {column: value for column, value in zip(columns, row)}
        record["published_at"] = _iso(record.get("published_at"))
        record["impact_summary"] = _truncate_text(record.get("impact_summary"), 200)
        record["impact_analysis"] = _truncate_text(record.get("impact_analysis"), 320)
        if isinstance(record.get("extra_metadata"), str):
            try:
                record["extra_metadata"] = json.loads(record["extra_metadata"])
            except json.JSONDecodeError:
                pass
        results.append(record)
    return results


def _build_concept_entries(
    hot_concepts: Sequence[Dict[str, Any]],
    *,
    lookback_hours: int,
    index_history_dao: ConceptIndexHistoryDAO,
    news_article_dao: NewsArticleDAO,
    news_insight_dao: NewsInsightDAO,
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for concept in hot_concepts:
        concept_name = concept.get("name")
        if not concept_name:
            continue

        concept_entry: Dict[str, Any] = {
            "name": concept_name,
            "fundFlow": {
                "score": concept.get("score"),
                "totalNetAmount": concept.get("totalNetAmount"),
                "totalInflow": concept.get("totalInflow"),
                "totalOutflow": concept.get("totalOutflow"),
                "bestRank": concept.get("bestRank"),
                "bestSymbol": concept.get("bestSymbol"),
                "stages": concept.get("stages"),
            },
        }

        history = index_history_dao.list_entries(concept_name=concept_name, limit=INDEX_LOOKBACK_DAYS).get("items", [])
        if history:
            concept_entry["indexMetrics"] = _compute_index_metrics(history)
            if history:
                concept_entry["latestTradeDate"] = history[0].get("trade_date")
                concept_entry["tsCode"] = history[0].get("ts_code")
        else:
            concept_entry["indexMetrics"] = _compute_index_metrics([])

        news = _fetch_concept_news(news_article_dao, news_insight_dao, concept_name, lookback_hours=lookback_hours, limit=MAX_NEWS_PER_CONCEPT)
        concept_entry["news"] = news

        entries.append(concept_entry)

    return entries


def build_concept_snapshot(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    concept_limit: int = DEFAULT_CONCEPT_LIMIT,
    refresh_index_history: bool = False,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    snapshot_time = _local_now()
    settings = load_settings(settings_path)

    hotlist = build_sector_fund_flow_snapshot()
    concepts_raw = (hotlist.get("concepts") or [])[: concept_limit]

    if refresh_index_history and concepts_raw:
        concept_names = [entry.get("name") for entry in concepts_raw if entry.get("name")]
        if concept_names:
            try:
                sync_concept_index_history(
                    concept_names,
                    start_date=(snapshot_time - timedelta(days=400)).strftime("%Y%m%d"),
                    end_date=snapshot_time.strftime("%Y%m%d"),
                    settings_path=settings_path,
                )
            except Exception as exc:  # pragma: no cover - best effort
                logger.warning("Failed to refresh concept index history: %s", exc)

    index_history_dao = ConceptIndexHistoryDAO(settings.postgres)
    news_article_dao = NewsArticleDAO(settings.postgres)
    news_insight_dao = NewsInsightDAO(settings.postgres)

    concepts = _build_concept_entries(
        concepts_raw,
        lookback_hours=lookback_hours,
        index_history_dao=index_history_dao,
        news_article_dao=news_article_dao,
        news_insight_dao=news_insight_dao,
    )

    return {
        "generatedAt": _iso(snapshot_time),
        "lookbackHours": int(lookback_hours),
        "conceptCount": len(concepts),
        "concepts": concepts,
        "fundSnapshot": hotlist,
    }


def generate_concept_insight_summary(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    concept_limit: int = DEFAULT_CONCEPT_LIMIT,
    run_llm: bool = True,
    refresh_index_history: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    snapshot = build_concept_snapshot(
        lookback_hours=lookback_hours,
        concept_limit=concept_limit,
        refresh_index_history=refresh_index_history,
        settings_path=settings_path,
    )

    if not snapshot.get("concepts"):
        raise ValueError("No concept data available for insight generation")

    payload_json = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    prompt = CONCEPT_INSIGHT_PROMPT.replace("{concept_payload}", payload_json)
    prompt = prompt.replace("{lookback_hours}", str(int(lookback_hours)))

    settings = load_settings(settings_path)
    concept_dao = ConceptInsightDAO(settings.postgres)

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
            raise RuntimeError("DeepSeek configuration is required for concept insight generation")

        logger.info(
            "Generating concept insight summary: concepts=%s lookback=%sh model=%s",
            snapshot.get("conceptCount"),
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
                logger.warning("Concept insight response is not valid JSON; storing raw content")
                summary_json = None

    window_end = _parse_iso_datetime(snapshot["generatedAt"]).astimezone(LOCAL_TZ)
    window_start = window_end - timedelta(hours=max(1, int(lookback_hours)))

    summary_payload = {
        "summary_id": None,
        "generated_at": window_end.replace(tzinfo=None),
        "window_start": window_start.replace(tzinfo=None),
        "window_end": window_end.replace(tzinfo=None),
        "concept_count": snapshot.get("conceptCount"),
        "summary_snapshot": json.dumps(snapshot, ensure_ascii=False),
        "summary_json": json.dumps(summary_json, ensure_ascii=False) if summary_json is not None else None,
        "raw_response": raw_response,
        "referenced_concepts": json.dumps([concept.get("name") for concept in snapshot.get("concepts", [])], ensure_ascii=False),
        "referenced_articles": json.dumps([
            {
                "concept": entry.get("name"),
                "articles": entry.get("news", []),
            }
            for entry in snapshot.get("concepts", [])
        ], ensure_ascii=False),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "elapsed_ms": elapsed_ms,
        "model_used": model_used,
    }

    summary_id = concept_dao.insert_summary(summary_payload)
    summary_payload["summary_id"] = summary_id
    summary_payload["summary_snapshot"] = snapshot
    summary_payload["summary_json"] = summary_json
    summary_payload["referenced_concepts"] = [concept.get("name") for concept in snapshot.get("concepts", [])]
    summary_payload["referenced_articles"] = [
        {
            "concept": entry.get("name"),
            "articles": entry.get("news", []),
        }
        for entry in snapshot.get("concepts", [])
    ]

    return summary_payload


def get_latest_concept_insight(*, settings_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    settings = load_settings(settings_path)
    concept_dao = ConceptInsightDAO(settings.postgres)
    record = concept_dao.latest_summary()
    if not record:
        return None

    def _ensure_iso(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=LOCAL_TZ).isoformat()
        return value.astimezone(LOCAL_TZ).isoformat()

    record["generated_at"] = _ensure_iso(record.get("generated_at"))
    record["window_start"] = _ensure_iso(record.get("window_start"))
    record["window_end"] = _ensure_iso(record.get("window_end"))
    return record


def list_concept_insights(*, limit: int = 10, settings_path: Optional[str] = None) -> List[Dict[str, Any]]:
    settings = load_settings(settings_path)
    concept_dao = ConceptInsightDAO(settings.postgres)
    records = concept_dao.list_summaries(limit=limit)
    results: List[Dict[str, Any]] = []
    for record in records:
        item = dict(record)

        def _ensure_iso(value: Optional[datetime]) -> Optional[str]:
            if value is None:
                return None
            if value.tzinfo is None:
                return value.replace(tzinfo=LOCAL_TZ).isoformat()
            return value.astimezone(LOCAL_TZ).isoformat()

        item["generated_at"] = _ensure_iso(item.get("generated_at"))
        item["window_start"] = _ensure_iso(item.get("window_start"))
        item["window_end"] = _ensure_iso(item.get("window_end"))
        results.append(item)
    return results


__all__ = [
    "build_concept_snapshot",
    "generate_concept_insight_summary",
    "get_latest_concept_insight",
    "list_concept_insights",
]
