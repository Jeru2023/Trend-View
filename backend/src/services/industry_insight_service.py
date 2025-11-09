"""Services for building industry-level insights."""

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
from ..dao import IndustryInsightDAO, NewsArticleDAO, NewsInsightDAO
from .industry_directory_service import resolve_industry_label
from .sector_fund_flow_service import build_sector_fund_flow_snapshot

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

DEFAULT_LOOKBACK_HOURS = 48
DEFAULT_INDUSTRY_LIMIT = 5
MAX_NEWS_PER_INDUSTRY = 4

DEEPSEEK_REASONER_MODEL = "deepseek-reasoner"
MAX_OUTPUT_TOKENS = 26000

INDUSTRY_INSIGHT_PROMPT = """
你是一名聚焦A股行业轮动的策略顾问。我们已经汇总了最近{lookback_hours}小时的热门行业资金热度、阶段表现以及相关新闻。请阅读以下JSON数据，为投资团队输出一份结构化的行业推理建议。

输出必须是 JSON，格式如下：
{{
  "headline": "不少于80字的行业整体概述",
  "market_view": "不少于120字，归纳资金流向、量价表现、行业逻辑与风险点",
  "top_industries": [
     {{
        "name": "行业名称",
        "stance": "bullish|watch|bearish",
        "confidence": 0-1的小数，
        "drivers": "不少于60字，说明资金/行情/新闻驱动",
        "key_metrics": ["用短句列出核心指标，例如：20日净流入+12.5亿"],
        "leading_stocks": ["列出1-3只行业龙头或补涨股"],
        "risk_flags": ["至少2条风险提示，如：上游成本、政策扰动等"],
        "suggested_actions": ["至少2条策略建议，例如：逢低吸纳、逢高调仓等"]
     }}
  ],
  "rotation_notes": "不少于80字，说明资金风格与行业切换线索",
  "risk_summary": ["至少3条全局风险要点"],
  "next_steps": ["不少于3条后续跟踪/数据观察建议"],
  "data_timestamp": "说明数据提取时间和覆盖范围"
}}

若数据不足以判断，请明确写出原因并给出建议的补充信息。

以下是行业数据：
{industry_payload}
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


def _calculate_stage_metrics(stages: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    metrics: Dict[str, Optional[float]] = {
        "latestIndex": None,
        "change1d": None,
        "change3d": None,
        "change5d": None,
        "change10d": None,
        "change20d": None,
    }
    if not stages:
        return metrics

    for stage in stages:
        symbol = stage.get("symbol")
        price_change = stage.get("priceChangePercent")
        stage_change = stage.get("stageChangePercent")
        index_value = stage.get("indexValue")
        if metrics["latestIndex"] is None and index_value is not None:
            metrics["latestIndex"] = index_value
        if symbol == "即时":
            metrics["change1d"] = price_change
        elif symbol == "3日排行":
            metrics["change3d"] = stage_change
        elif symbol == "5日排行":
            metrics["change5d"] = stage_change
        elif symbol == "10日排行":
            metrics["change10d"] = stage_change
        elif symbol == "20日排行":
            metrics["change20d"] = stage_change
    return metrics


def _fetch_industry_news(
    article_dao: NewsArticleDAO,
    insight_dao: NewsInsightDAO,
    industry_name: str,
    *,
    lookback_hours: int,
    limit: int,
) -> List[Dict[str, Any]]:
    key = industry_name.strip().lower()
    if not key:
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
                    FROM {articles_schema}.{articles_table} AS a
                    JOIN {insights_schema}.{insights_table} AS i ON i.article_id = a.article_id
                    WHERE a.processing_status = 'completed'
                      AND a.published_at >= %s
                      AND (
                          LOWER(COALESCE(i.impact_industries, '')) LIKE %s
                       OR LOWER(COALESCE(i.impact_sectors, '')) LIKE %s
                       OR LOWER(COALESCE(i.impact_themes, '')) LIKE %s
                      )
                    ORDER BY a.published_at DESC
                    LIMIT %s
                    """
                ).format(
                    articles_schema=sql.Identifier(article_dao.config.schema),
                    articles_table=sql.Identifier(article_dao._table_name),  # type: ignore[attr-defined]
                    insights_schema=sql.Identifier(insight_dao.config.schema),
                    insights_table=sql.Identifier(insight_dao._table_name),  # type: ignore[attr-defined]
                ),
                (
                    window_start,
                    f"%{key}%",
                    f"%{key}%",
                    f"%{key}%",
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


def _build_industry_entries(
    industries: Sequence[Dict[str, Any]],
    *,
    lookback_hours: int,
    news_article_dao: NewsArticleDAO,
    news_insight_dao: NewsInsightDAO,
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for item in industries:
        industry_name = item.get("name")
        if not industry_name:
            continue

        stages = item.get("stages") or []

        industry_entry: Dict[str, Any] = {
            "name": industry_name,
            "fundFlow": {
                "score": item.get("score"),
                "totalNetAmount": item.get("totalNetAmount"),
                "totalInflow": item.get("totalInflow"),
                "totalOutflow": item.get("totalOutflow"),
                "bestRank": item.get("bestRank"),
                "bestSymbol": item.get("bestSymbol"),
                "stages": item.get("stages"),
            },
            "stageMetrics": _calculate_stage_metrics(stages),
        }

        latest_stage = stages[0] if stages else {}
        industry_entry["latestUpdatedAt"] = latest_stage.get("updatedAt")

        news = _fetch_industry_news(
            news_article_dao,
            news_insight_dao,
            industry_name,
            lookback_hours=lookback_hours,
            limit=MAX_NEWS_PER_INDUSTRY,
        )
        industry_entry["news"] = news

        entries.append(industry_entry)
    return entries


def build_industry_snapshot(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    industry_limit: int = DEFAULT_INDUSTRY_LIMIT,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    snapshot_time = _local_now()

    hotlist = build_sector_fund_flow_snapshot()
    industries_raw = (hotlist.get("industries") or [])[: industry_limit]

    settings = load_settings(settings_path)
    news_article_dao = NewsArticleDAO(settings.postgres)
    news_insight_dao = NewsInsightDAO(settings.postgres)

    industries = _build_industry_entries(
        industries_raw,
        lookback_hours=lookback_hours,
        news_article_dao=news_article_dao,
        news_insight_dao=news_insight_dao,
    )

    return {
        "generatedAt": _iso(snapshot_time),
        "lookbackHours": int(lookback_hours),
        "industryCount": len(industries),
        "industries": industries,
        "fundSnapshot": {
            "generatedAt": hotlist.get("generatedAt"),
            "symbols": hotlist.get("symbols"),
        },
    }


def generate_industry_insight_summary(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    industry_limit: int = DEFAULT_INDUSTRY_LIMIT,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    snapshot = build_industry_snapshot(
        lookback_hours=lookback_hours,
        industry_limit=industry_limit,
        settings_path=settings_path,
    )

    if not snapshot.get("industries"):
        raise ValueError("No industry data available for insight generation")

    payload_json = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    prompt = INDUSTRY_INSIGHT_PROMPT.replace("{industry_payload}", payload_json)
    prompt = prompt.replace("{lookback_hours}", str(int(lookback_hours)))

    settings = load_settings(settings_path)
    industry_dao = IndustryInsightDAO(settings.postgres)

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
            raise RuntimeError("DeepSeek configuration is required for industry insight generation")

        logger.info(
            "Generating industry insight summary: industries=%s lookback=%sh model=%s",
            snapshot.get("industryCount"),
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
                logger.warning("Industry insight response is not valid JSON; storing raw content")
                summary_json = None

    generated_at = snapshot.get("generatedAt")
    window_end = _parse_iso_datetime(generated_at).astimezone(LOCAL_TZ)
    window_start = window_end - timedelta(hours=max(1, int(lookback_hours)))

    summary_payload = {
        "summary_id": None,
        "generated_at": window_end.replace(tzinfo=None),
        "window_start": window_start.replace(tzinfo=None),
        "window_end": window_end.replace(tzinfo=None),
        "industry_count": snapshot.get("industryCount"),
        "summary_snapshot": json.dumps(snapshot, ensure_ascii=False),
        "summary_json": json.dumps(summary_json, ensure_ascii=False) if summary_json is not None else None,
        "raw_response": raw_response,
        "referenced_industries": json.dumps([entry.get("name") for entry in snapshot.get("industries", [])], ensure_ascii=False),
        "referenced_articles": json.dumps([
            {
                "industry": entry.get("name"),
                "articles": entry.get("news", []),
            }
            for entry in snapshot.get("industries", [])
        ], ensure_ascii=False),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "elapsed_ms": elapsed_ms,
        "model_used": model_used,
    }

    summary_id = industry_dao.insert_summary(summary_payload)
    summary_payload["summary_id"] = summary_id
    summary_payload["summary_snapshot"] = snapshot
    summary_payload["summary_json"] = summary_json
    summary_payload["referenced_industries"] = [entry.get("name") for entry in snapshot.get("industries", [])]
    summary_payload["referenced_articles"] = [
        {
            "industry": entry.get("name"),
            "articles": entry.get("news", []),
        }
        for entry in snapshot.get("industries", [])
    ]

    return summary_payload


def get_latest_industry_insight(*, settings_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    settings = load_settings(settings_path)
    industry_dao = IndustryInsightDAO(settings.postgres)
    record = industry_dao.latest_summary()
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


def list_industry_news(
    industry: str,
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    limit: int = 50,
    settings_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    resolved = resolve_industry_label(industry, settings_path=settings_path)
    settings = load_settings(settings_path)
    article_dao = NewsArticleDAO(settings.postgres)
    insight_dao = NewsInsightDAO(settings.postgres)
    return _fetch_industry_news(
        article_dao,
        insight_dao,
        resolved["name"],
        lookback_hours=lookback_hours,
        limit=max(1, min(limit, 200)),
    )


def list_industry_insights(*, limit: int = 10, settings_path: Optional[str] = None) -> List[Dict[str, Any]]:
    settings = load_settings(settings_path)
    industry_dao = IndustryInsightDAO(settings.postgres)
    records = industry_dao.list_summaries(limit=limit)
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
    "build_industry_snapshot",
    "generate_industry_insight_summary",
    "get_latest_industry_insight",
    "list_industry_insights",
    "list_industry_news",
]
