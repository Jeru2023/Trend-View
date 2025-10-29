"""
Service module to synchronise AkShare finance breakfast summaries.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta
from textwrap import dedent
from typing import Callable, Optional

import pandas as pd

from ..api_clients import fetch_eastmoney_detail, fetch_finance_breakfast, generate_finance_analysis
from ..config.settings import load_settings
from ..dao import FinanceBreakfastDAO

logger = logging.getLogger(__name__)

EMPTY_CONTENT_SENTINEL = "EMPTY"
MAX_CONSECUTIVE_CONTENT_FAILURES = 3
AI_LOOKBACK_DAYS = 31


def _deepseek_configured(settings) -> bool:
    if not settings:
        return False
    token = getattr(settings, "token", None)
    if not token:
        return False
    token_text = str(token).strip()
    if not token_text or token_text.upper().startswith("REPLACE_WITH"):
        return False
    return True


def _ai_cutoff_datetime() -> datetime:
    return datetime.utcnow() - timedelta(days=AI_LOOKBACK_DAYS)


def _within_ai_window(published_at: object, cutoff: datetime) -> bool:
    normalized = _coerce_datetime(published_at)
    if normalized is None:
        return False
    return normalized >= cutoff

AI_ANALYSIS_PROMPT_TEMPLATE = dedent(
    """\
    Please conduct a professional analysis of the impact on the A-share market based on the following financial content:

    [News Content]
    {news_content}

    Please output the analysis results in JSON format:

    {{
        "ImpactAnalysis": [
            {{
                "EventTitle": "Brief event title",
                "NewsSummary": "Summary of relevant news content",
                "ImpactNature": "Positive/Negative/Neutral",
                "ImpactMagnitude": 0-100,  // Numerical range, 0 being minimal, 100 being maximal, indicating the degree of impact on the market/sector/stock
                "ImpactScope": "Overall Market/Sector Level/Individual Stock Level",
                "AffectedSectors": ["Sector1", "Sector2"],
                "SectorTags": ["Tag1", "Tag2"],  // Tags for categorization and filtering
                "Rationale": "Detailed reasoning behind the impact analysis",
                "Duration": "Short-term (within 1 week)/Medium-term (1-3 months)/Long-term (over 3 months)",
                "InvestmentRecommendation": "Specific investment operation suggestions"
            }}
        ],
        "ComprehensiveAssessment": {{
            "OverallMarketImpact": "Positive/Negative/Neutral",
            "MarketImpactMagnitude": 0-100,  // Degree of impact on the overall market
            "KeySectorsToWatch": ["Sector1", "Sector2"],
            "RiskIndicators": ["Risk1", "Risk2"],
            "OpportunityIndicators": ["Opportunity1", "Opportunity2"],
            "AnalysisSummary": "Brief summary analysis"
        }}
    }}

    ## Analysis Standards:

    1. **Impact Magnitude Scoring Criteria**:
       - 80-100: Significant impact, likely to cause substantial fluctuations in the overall market
       - 60-79: Considerable impact, with notable effects on major sectors
       - 40-59: Moderate impact, with relatively obvious effects on specific sectors
       - 20-39: General impact, affecting certain stocks or short-term sentiment
       - 0-19: Minor impact, with limited scope of influence

    2. **Impact Scope Classification**:
       - Overall Market: Impacts the entire A-share market
       - Sector Level: Affects specific industries or thematic sectors
       - Individual Stock Level: Primarily impacts individual companies

    3. **Sector Tag Classification**:
       - Industries: Technology, Consumer, Healthcare, Finance, Cyclicals, Manufacturing, New Energy, etc.
       - Themes: AI, Robotics, Semiconductors, Defense, Rare Earth, Precious Metals, etc.
       - Attributes: High-Growth, Value, Defensive, Cyclical, etc.

    Please conduct an objective and professional analysis based on news facts, avoiding overinterpretation.
    """
)


def _u(hex_str: str) -> str:
    return bytes.fromhex(hex_str.replace(" ", "")).decode("utf-8")


SECTION_COMPREHENSIVE_ASSESS = _u("E7BBBCE59088E8AF84E4BCB0")
SECTION_COMPREHENSIVE_ANALYSIS = _u("E7BBBCE59088E58886E69E90")
SECTION_COMPREHENSIVE_RESEARCH = _u("E7BBBCE59088E7A094E588A4")
SECTION_COMPREHENSIVE_JUDGEMENT = _u("E7BBBCE59088E588A4E696AD")
SECTION_COMPREHENSIVE_COMMENT = _u("E7BBBCE59088E782B9E8AF84")
SECTION_COMPREHENSIVE_INTERPRET = _u("E7BBBCE59088E8A7A3E8AFBB")

FIELD_MARKET_IMPACT = _u("E5B882E59CBAE695B4E4BD93E5BDB1E5938D")
FIELD_MARKET_INTENSITY = _u("E5B882E59CBAE5BDB1E5938DE7A88BE5BAA6")
FIELD_FOCUS_SECTORS = _u("E9878DE782B9E585B3E6B3A8E69DBFE59D97")
FIELD_FOCUS_ITEMS = _u("E9878DE782B9E585B3E6B3A8E4BA8BE9A1B9")
FIELD_OPPORTUNITIES = _u("E69CBAE4BC9AE68F90E7A4BA")
FIELD_ACTIONS = _u("E6938DE4BD9CE5BBBAE8AEAE")
FIELD_RISKS = _u("E9A38EE999A9E68F90E7A4BA")
FIELD_ANALYSIS_SUMMARY = _u("E58886E69E90E680BBE7BB93")
FIELD_INVESTMENT_SUMMARY = _u("E68A95E8B584E680BBE7BB93")

ALIAS_FIELDS: dict[str, tuple[str, ...]] = {
    FIELD_MARKET_IMPACT: (
        _u("E695B4E4BD93E5BDB1E5938D"),
        _u("E5B882E59CBAE5BDB1E5938D"),
        _u("E5B882E59CBAE695B4E4BD93E8B5B0E58ABF"),
        "OverallMarketImpact",
        "OverallImpactAssessment",
    ),
    FIELD_MARKET_INTENSITY: (
        _u("E5B882E59CBAE5BDB1E5938DE5B985E5BAA6"),
        _u("E5B882E59CBAE5BDB1E5938DE5BCBAE5BAA6"),
        _u("E5B882E59CBAE5BDB1E5938DE7AD89E7BAA7"),
        "MarketImpactMagnitude",
        "MarketImpactScore",
    ),
    FIELD_FOCUS_SECTORS: (
        _u("E9878DE782B9E585B3E6B3A8E8A18CE4B89A"),
        _u("E585B3E6B3A8E69DBFE59D97"),
        "KeySectorsToWatch",
        "FocusSectors",
    ),
    FIELD_FOCUS_ITEMS: (
        _u("E9878DE782B9E585B3E6B3A8"),
        _u("E585B3E6B3A8E4BA8BE9A1B9"),
        _u("E585B3E6B3A8E8A681E782B9"),
        "FocusItems",
        "FocusTargets",
    ),
    FIELD_OPPORTUNITIES: (
        _u("E69CBAE4BC9AE7AD96E795A5"),
        _u("E69CBAE4BC9AE696B9E59091"),
        "OpportunityIndicators",
        "Opportunities",
    ),
    FIELD_ACTIONS: (
        _u("E68A95E8B584E5BBBAE8AEAE"),
        _u("E7AD96E795A5E5BBBAE8AEAE"),
        _u("E6938DE4BD9CE7AD96E795A5"),
        "ActionPlan",
        "RecommendedActions",
    ),
    FIELD_RISKS: (
        _u("E9A38EE999A9E8ADA6E7A4BA"),
        _u("E9A38EE999A9E68F90E98692"),
        "RiskIndicators",
        "Risks",
    ),
    FIELD_ANALYSIS_SUMMARY: (
        _u("E58886E69E90E7BBBCE8BFB0"),
        _u("E7BBBCE59088E7BB93E8AEBA"),
        _u("E695B4E4BD93E58886E69E90"),
        "AnalysisSummary",
        "OverallSummary",
    ),
    FIELD_INVESTMENT_SUMMARY: (
        _u("E695B4E4BD93E7BB93E8AEBA"),
        _u("E68A95E8B584E7BB93E8AEBA"),
        "InvestmentRecommendation",
        "InvestmentSummary",
    ),
}

COMPREHENSIVE_SECTION_KEYS = (
    SECTION_COMPREHENSIVE_ASSESS,
    SECTION_COMPREHENSIVE_ANALYSIS,
    SECTION_COMPREHENSIVE_RESEARCH,
    SECTION_COMPREHENSIVE_JUDGEMENT,
    SECTION_COMPREHENSIVE_COMMENT,
    SECTION_COMPREHENSIVE_INTERPRET,
    "ComprehensiveAssessment",
    "ComprehensiveAnalysis",
    "OverallAssessment",
)

SUMMARY_ORDER = (
    FIELD_MARKET_IMPACT,
    FIELD_MARKET_INTENSITY,
    FIELD_FOCUS_SECTORS,
    FIELD_FOCUS_ITEMS,
    FIELD_OPPORTUNITIES,
    FIELD_ACTIONS,
    FIELD_RISKS,
    FIELD_ANALYSIS_SUMMARY,
    FIELD_INVESTMENT_SUMMARY,
)



def _parse_ai_payload(raw_ai: object) -> Optional[object]:
    if raw_ai is None:
        return None
    if isinstance(raw_ai, (dict, list)):
        return raw_ai
    if isinstance(raw_ai, str):
        text = raw_ai.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if 0 <= start < end:
                try:
                    return json.loads(text[start : end + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _locate_comprehensive_section(payload: object) -> Optional[object]:
    if isinstance(payload, dict):
        for key in COMPREHENSIVE_SECTION_KEYS:
            if key in payload and payload[key] is not None:
                return payload[key]
        for value in payload.values():
            section = _locate_comprehensive_section(value)
            if section is not None:
                return section
    elif isinstance(payload, list):
        for item in payload:
            section = _locate_comprehensive_section(item)
            if section is not None:
                return section
    return None


def _normalize_to_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    parts = re.split(r"[、，,；;\s]+", text)
    return [part for part in parts if part]


def _select_field(section: dict[str, object], canonical: str, aliases: tuple[str, ...]) -> Optional[object]:
    for key in (canonical, *aliases):
        if key in section:
            value = section[key]
            if value not in (None, "", [], {}):
                return value
    return None


def _extract_numeric(value: object) -> Optional[float]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group())
    except ValueError:
        return None


def _format_value_for_summary(field: str, value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
        return "、".join(items) if items else None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if field == FIELD_MARKET_INTENSITY:
            return f"{int(number) if number.is_integer() else number}分"
        return str(int(number) if number.is_integer() else number)
    text = str(value).strip()
    if not text:
        return None
    if field == FIELD_MARKET_INTENSITY:
        number = _extract_numeric(text)
        if number is not None:
            return f"{int(number) if number.is_integer() else number}分"
    return text


def _build_comprehensive_summary(section: object) -> Optional[str]:
    if section is None:
        return None
    if isinstance(section, str):
        return section.strip() or None
    if isinstance(section, list):
        items = _normalize_to_list(section)
        return "、".join(items) if items else None
    if not isinstance(section, dict):
        return None

    prepared: dict[str, object] = {}
    for field, aliases in ALIAS_FIELDS.items():
        value = _select_field(section, field, aliases)
        if value is None:
            continue
        if field in {FIELD_FOCUS_SECTORS, FIELD_FOCUS_ITEMS, FIELD_OPPORTUNITIES, FIELD_ACTIONS, FIELD_RISKS}:
            prepared[field] = _normalize_to_list(value)
        else:
            prepared[field] = value

    parts: list[str] = []
    impact = prepared.get(FIELD_MARKET_IMPACT)
    intensity = prepared.get(FIELD_MARKET_INTENSITY)
    intensity_number = _extract_numeric(intensity) if intensity is not None else None

    if impact:
        impact_text = str(impact).strip()
        if intensity_number is not None:
            score = int(intensity_number) if intensity_number.is_integer() else intensity_number
            parts.append(f"{FIELD_MARKET_IMPACT}：{impact_text}（{score}分）")
            prepared.pop(FIELD_MARKET_INTENSITY, None)
        else:
            parts.append(f"{FIELD_MARKET_IMPACT}：{impact_text}")
        prepared.pop(FIELD_MARKET_IMPACT, None)
    elif intensity_number is not None:
        score = int(intensity_number) if intensity_number.is_integer() else intensity_number
        parts.append(f"{FIELD_MARKET_INTENSITY}：{score}分")
        prepared.pop(FIELD_MARKET_INTENSITY, None)

    for field in SUMMARY_ORDER[2:]:
        if field not in prepared:
            continue
        text = _format_value_for_summary(field, prepared[field])
        if text:
            parts.append(f"{field}：{text}")

    return "；".join(parts) if parts else None


def _extract_comprehensive_summary(raw_ai: object) -> Optional[str]:
    payload = _parse_ai_payload(raw_ai)
    if payload is None:
        return None
    section = _locate_comprehensive_section(payload)
    return _build_comprehensive_summary(section)


def _should_override_summary(existing: Optional[str], raw_ai: object) -> bool:
    if existing is None:
        return True
    text = str(existing).strip()
    if not text:
        return True
    if isinstance(raw_ai, str) and text == raw_ai.strip():
        return True
    if text.startswith("{") and text.endswith("}"):
        return True
    if text.startswith("[") and text.endswith("]"):
        return True
    return False


def _normalize_text(value: object) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return text.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def _coerce_datetime(value: object) -> Optional[datetime]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if isinstance(parsed, pd.Timestamp) and not pd.isna(parsed):
        return parsed.to_pydatetime()
    return None


def sync_finance_breakfast(
    *,
    settings_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, Optional[str], Optional[int]], None]] = None,
) -> dict[str, object]:
    """
    Synchronise finance breakfast data into PostgreSQL.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = FinanceBreakfastDAO(settings.postgres)

    if progress_callback:
        progress_callback(0.05, "Fetching finance breakfast feed", 0)

    dataframe = fetch_finance_breakfast()
    fetched_rows = int(len(dataframe.index)) if hasattr(dataframe, "index") else 0

    dataframe = dataframe.copy() if hasattr(dataframe, "copy") else dataframe

    if not dataframe.empty:
        dataframe["title"] = dataframe["title"].apply(_normalize_text)
        dataframe["summary"] = dataframe["summary"].apply(_normalize_text)
        dataframe["url"] = dataframe["url"].apply(lambda val: str(val).strip() if val else None)
        dataframe["published_at"] = pd.to_datetime(dataframe["published_at"], errors="coerce")
        dataframe = dataframe.dropna(subset=["title", "published_at"]).drop_duplicates(subset=["title", "published_at"])
    else:
        if hasattr(dataframe, "dropna"):
            dataframe = dataframe.dropna()  # ensure consistent type even if empty

    latest_published = dao.latest_published_date()
    new_candidate_rows = 0
    affected = 0

    if not dataframe.empty:
        if latest_published is not None:
            dataframe = dataframe[dataframe["published_at"] > latest_published]
        new_candidate_rows = int(len(dataframe.index))

        if new_candidate_rows > 0:
            if progress_callback:
                progress_callback(
                    0.4,
                    f"Upserting {new_candidate_rows} finance breakfast entries (latest existing: {latest_published})",
                    new_candidate_rows,
                )
            affected = dao.upsert(dataframe)
            logger.info(
                "Finance breakfast upsert inserted/updated %s rows (fetched=%s, new_candidates=%s).",
                affected,
                fetched_rows,
                new_candidate_rows,
            )
        else:
            logger.info(
                "No new finance breakfast records detected (latest existing publication: %s)",
                latest_published,
            )
    else:
        logger.info("Finance breakfast feed returned no usable rows (fetched=%s)", fetched_rows)

    new_entries_info: list[dict[str, object]] = []
    if new_candidate_rows > 0:
        for _, row in dataframe.iterrows():
            title = row.get("title")
            published_at = _coerce_datetime(row.get("published_at"))
            if not title or published_at is None:
                continue
            new_entries_info.append(
                {
                    "title": title,
                    "published_at": published_at,
                    "url": row.get("url"),
                }
            )

    entries_for_content: list[dict[str, object]] = []
    source_label = "new finance breakfast entries"
    if new_entries_info:
        logger.info("Finance breakfast: %s new entries need content.", len(new_entries_info))
        entries_for_content = new_entries_info
    else:
        backlog_entries = dao.list_missing_content(limit=200)
        if backlog_entries:
            entries_for_content = backlog_entries
            source_label = "existing entries missing content"
            logger.info(
                "Finance breakfast: fetched %s backlog entries still missing content.",
                len(backlog_entries),
            )
        else:
            logger.info("Finance breakfast: no entries require content refresh.")

    content_entries_map: dict[tuple[str, datetime], str] = {}
    content_updates = 0
    if entries_for_content:
        if progress_callback:
            progress_callback(
                0.65,
                f"Fetching detailed content for {len(entries_for_content)} {source_label}",
                len(entries_for_content),
            )
        content_entries_map, missing_for_content, content_updates = _refresh_content_for_entries(
            dao,
            entries_for_content,
            logger_prefix=source_label,
        )
        if missing_for_content:
            existing_contents = dao.fetch_contents(
                [(entry["title"], entry["published_at"]) for entry in missing_for_content]
            )
            for key, value in existing_contents.items():
                normalized = _normalize_text(value)
                if normalized:
                    content_entries_map[key] = normalized
        if content_updates:
            logger.info("Updated %s finance breakfast articles with detailed content.", content_updates)
        remaining_without_content = [
            entry
            for entry in new_entries_info
            if (entry["title"], entry["published_at"]) not in content_entries_map
        ]
        if remaining_without_content:
            logger.warning(
                "Content unavailable for %s finance breakfast entries: %s",
                len(remaining_without_content),
                [entry["title"] for entry in remaining_without_content],
            )
    else:
        if progress_callback:
            progress_callback(0.65, "No finance breakfast entries required content updates", 0)

    ai_updates = 0
    ai_cutoff = _ai_cutoff_datetime()
    ai_ready_entries: list[dict[str, object]] = []
    for (title, published_at), content in content_entries_map.items():
        if not content or content == EMPTY_CONTENT_SENTINEL:
            continue
        if not _within_ai_window(published_at, ai_cutoff):
            continue
        normalized_published_at = _coerce_datetime(published_at)
        if normalized_published_at is None:
            continue
        ai_ready_entries.append(
            {
                "title": title,
                "published_at": normalized_published_at,
                "content": content,
            }
        )
    if ai_ready_entries:
        logger.warning(
            "Finance breakfast: %s entries ready for AI extraction from current batch.",
            len(ai_ready_entries),
        )
    if not ai_ready_entries:
        backlog_for_ai: list[dict[str, object]] = []
        for entry in dao.list_missing_ai_extract(limit=50):
            content_value = entry.get("content")
            if not content_value or content_value == EMPTY_CONTENT_SENTINEL:
                continue
            normalized_content = str(content_value).strip()
            if not normalized_content or normalized_content == EMPTY_CONTENT_SENTINEL:
                continue
            published_at_value = entry.get("published_at")
            if not _within_ai_window(published_at_value, ai_cutoff):
                continue
            normalized_published_at = _coerce_datetime(published_at_value)
            if normalized_published_at is None:
                continue
            backlog_for_ai.append(
                {
                    "title": entry.get("title"),
                    "published_at": normalized_published_at,
                    "content": normalized_content,
                }
            )
        if backlog_for_ai:
            ai_ready_entries = backlog_for_ai
            logger.warning(
                "Finance breakfast: queued %s backlog entries for AI extraction.",
                len(ai_ready_entries),
            )

    configured = _deepseek_configured(settings.deepseek)
    logger.warning(
        "Finance breakfast: DeepSeek configuration present=%s token_configured=%s",
        bool(settings.deepseek),
        configured,
    )

    if configured:
        logger.warning(
            "Finance breakfast: AI extraction stage starting with %s entries.",
            len(ai_ready_entries),
        )
        if progress_callback:
            progress_callback(
                0.8,
                f"Generating AI analyses for {len(ai_ready_entries)} finance breakfast entries",
                len(ai_ready_entries),
            )
        if ai_ready_entries:
            ai_updates = _refresh_ai_for_entries(
                dao,
                settings.deepseek,
                ai_ready_entries,
                prompt_template=AI_ANALYSIS_PROMPT_TEMPLATE,
                cutoff=ai_cutoff,
            )
            if ai_updates:
                logger.info("Generated AI extracts for %s finance breakfast articles.", ai_updates)
            if ai_updates < len(ai_ready_entries):
                logger.info(
                    "AI extracts generated for %s/%s finance breakfast entries; remaining entries will retry later.",
                    ai_updates,
                    len(ai_ready_entries),
                )
            if ai_updates == 0:
                logger.info("Finance breakfast: AI extraction produced no updates this run.")
        else:
            logger.warning("Finance breakfast: no entries require AI extraction this run.")
    else:
        if progress_callback:
            progress_callback(0.8, "DeepSeek configuration missing; skipping AI extract generation.", 0)
        if ai_ready_entries:
            logger.error(
                "Finance breakfast: DeepSeek credentials not configured; %s entries remain without AI extracts.",
                len(ai_ready_entries),
            )
        else:
            logger.warning("Finance breakfast: DeepSeek configuration missing; nothing to analyse.")

    elapsed = time.perf_counter() - started

    if progress_callback:
        progress_callback(
            1.0,
            "Finance breakfast pipeline completed",
            affected + content_updates + ai_updates,
        )

    return {
        "rows": affected,
        "fetched_rows": fetched_rows,
        "new_candidates": new_candidate_rows,
        "content_updates": content_updates,
        "ai_updates": ai_updates,
        "elapsed_seconds": elapsed,
    }


def _refresh_content_for_entries(
    dao: FinanceBreakfastDAO,
    entries: list[dict[str, object]],
    *,
    logger_prefix: str = "finance breakfast entries",
    failure_limit: int = MAX_CONSECUTIVE_CONTENT_FAILURES,
) -> tuple[dict[tuple[str, datetime], str], list[dict[str, object]], int]:
    """
    Fetch detailed article content for the provided entries and update the database.

    Returns:
        content_map: Mapping of (title, published_at) to normalised article content.
        missing_entries: Entries that still lack content after the fetch attempts.
        updated_count: Number of database rows whose content field was updated.
    """
    if not entries:
        return {}, [], 0

    content_map: dict[tuple[str, datetime], str] = {}
    missing_entries: list[dict[str, object]] = []
    updates: list[tuple[str, datetime, str]] = []
    consecutive_failures = 0
    skipping_due_to_failures = False

    def _persist_content(value: str) -> bool:
        try:
            dao.update_content([(str(title), published_at, value)])
        except Exception as exc:  # pragma: no cover - defensive
            logger.error(
                "Failed to persist finance breakfast content [%s]: title=%s published_at=%s error=%s",
                logger_prefix,
                title,
                published_at,
                exc,
            )
            return False
        else:
            logger.info(
                "Finance breakfast content stored [%s]: title=%s published_at=%s content_len=%s",
                logger_prefix,
                title,
                published_at,
                len(value) if value else 0,
            )
            return True

    for entry in entries:
        title = entry.get("title")
        published_at = entry.get("published_at")
        url = entry.get("url")
        if not title or not published_at or not url:
            logger.warning(
                "Finance breakfast entry missing required fields [%s]: title=%s published_at=%s url=%s",
                logger_prefix,
                title,
                published_at,
                url,
            )
            if title and published_at:
                if _persist_content(EMPTY_CONTENT_SENTINEL):
                    content_map[(str(title), published_at)] = EMPTY_CONTENT_SENTINEL
            consecutive_failures += 1
            if consecutive_failures >= failure_limit:
                skipping_due_to_failures = True
            continue
        logger.info(
            "Finance breakfast content fetch [%s]: title=%s published_at=%s url=%s",
            logger_prefix,
            title,
            published_at,
            url,
        )
        if skipping_due_to_failures:
            logger.warning(
                "Skipping content fetch due to consecutive failures [%s]: title=%s published_at=%s",
                logger_prefix,
                title,
                published_at,
            )
            if _persist_content(EMPTY_CONTENT_SENTINEL):
                content_map[(str(title), published_at)] = EMPTY_CONTENT_SENTINEL
            continue

        attempt = 0
        content: Optional[str] = None
        while attempt < 3 and not content:
            attempt += 1
            try:
                detail = fetch_eastmoney_detail(str(url))
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "Failed to fetch Eastmoney detail (attempt %s) for %s: %s",
                    attempt,
                    url,
                    exc,
                )
                time.sleep(min(2.0 * attempt, 6.0))
                continue

            content = _normalize_text(getattr(detail, "content", None))
            if not content:
                logger.warning(
                    "Finance breakfast content empty after fetch [%s] attempt %s: title=%s published_at=%s",
                    logger_prefix,
                    attempt,
                    title,
                    published_at,
                )
                time.sleep(min(1.5 * attempt, 4.0))
                continue

        if not content:
            consecutive_failures += 1
            logger.warning(
                "Finance breakfast content unavailable after retries [%s]: title=%s published_at=%s (failure streak=%s)",
                logger_prefix,
                title,
                published_at,
                consecutive_failures,
            )
            if _persist_content(EMPTY_CONTENT_SENTINEL):
                content_map[(str(title), published_at)] = EMPTY_CONTENT_SENTINEL
            if consecutive_failures >= failure_limit:
                logger.warning(
                    "Consecutive finance breakfast content failures reached %s; remaining entries will be marked as %s.",
                    failure_limit,
                    EMPTY_CONTENT_SENTINEL,
                )
                skipping_due_to_failures = True
            continue

        consecutive_failures = 0
        updates.append((str(title), published_at, content))
        content_map[(str(title), published_at)] = content
        if _persist_content(content):
            continue
        # Persist failure; keep entry for retry
        missing_entries.append(entry)

    return content_map, missing_entries, len(updates)


def _serialize_section(section: object, fallback: Optional[str]) -> Optional[str]:
    if section is None:
        return fallback
    if isinstance(section, str):
        text = section.strip()
        return text or fallback
    if isinstance(section, (int, float)) and not isinstance(section, bool):
        return str(section)
    try:
        return json.dumps(section, ensure_ascii=False)
    except (TypeError, ValueError):
        return fallback


def _extract_ai_sections(raw_ai: object) -> tuple[Optional[object], Optional[object]]:
    payload = _parse_ai_payload(raw_ai)
    if payload is None:
        return None, None

    summary_section: Optional[object] = None
    detail_section: Optional[object] = None

    if isinstance(payload, dict):
        for key in ("ComprehensiveAssessment", "comprehensiveAssessment", *COMPREHENSIVE_SECTION_KEYS):
            if key in payload and payload[key] is not None:
                summary_section = payload[key]
                break
        if summary_section is None:
            summary_section = _locate_comprehensive_section(payload)

        for key in ("ImpactAnalysis", "impactAnalysis", "impact_items", "Impactanalysis"):
            if key in payload and payload[key] is not None:
                detail_section = payload[key]
                break
        if detail_section is None and isinstance(summary_section, dict) and "ImpactAnalysis" in summary_section:
            detail_section = summary_section["ImpactAnalysis"]

        if detail_section is None:
            for value in payload.values():
                if isinstance(value, dict):
                    candidate = value.get("ImpactAnalysis") or value.get("impactAnalysis")
                    if candidate is not None:
                        detail_section = candidate
                        break
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            candidate = item.get("ImpactAnalysis") or item.get("impactAnalysis")
                            if candidate is not None:
                                detail_section = candidate
                                break
                    if detail_section is not None:
                        break
            if detail_section is None:
                for value in payload.values():
                    if isinstance(value, list) and value:
                        detail_section = value
                        break
    elif isinstance(payload, list):
        detail_section = payload

    return summary_section, detail_section


def _refresh_ai_for_entries(
    dao: FinanceBreakfastDAO,
    deepseek_settings,
    entries: list[dict[str, object]],
    *,
    prompt_template: str,
    cutoff: datetime,
) -> int:
    if not entries:
        return 0

    updates = 0
    for entry in entries:
        title = entry.get("title")
        published_at = entry.get("published_at")
        content = entry.get("content")
        if not title or not published_at or not content:
            logger.warning(
                "Finance breakfast AI skipped entry missing fields: title=%s published_at=%s has_content=%s",
                title,
                published_at,
                bool(content),
            )
            continue

        normalized_published_at = _coerce_datetime(published_at)
        if normalized_published_at is None:
            logger.warning(
                "Finance breakfast AI skipped entry with unparseable published_at: title=%s value=%s",
                title,
                published_at,
            )
            continue
        if normalized_published_at < cutoff:
            logger.warning(
                "Finance breakfast AI skipped entry before cutoff: title=%s published_at=%s cutoff=%s",
                title,
                normalized_published_at,
                cutoff,
            )
            continue

        logger.warning(
            "Finance breakfast AI request starting: title=%s published_at=%s content_len=%s",
            title,
            normalized_published_at,
            len(str(content)),
        )
        call_started = time.perf_counter()
        try:
            analysis_text = generate_finance_analysis(
                str(content),
                settings=deepseek_settings,
                prompt_template=prompt_template,
            )
        except Exception as exc:  # pragma: no cover - external call
            logger.warning("DeepSeek analysis failed for %s: %s", title, exc)
            continue
        call_elapsed = time.perf_counter() - call_started
        logger.warning(
            "Finance breakfast AI response received: title=%s published_at=%s elapsed=%.2fs",
            title,
            normalized_published_at,
            call_elapsed,
        )

        if not analysis_text:
            logger.warning("DeepSeek returned no analysis for finance breakfast entry %s", title)
            continue

        raw_payload = analysis_text if isinstance(analysis_text, str) else json.dumps(analysis_text, ensure_ascii=False)
        summary_section, detail_section = _extract_ai_sections(analysis_text)
        summary_payload = _serialize_section(summary_section, raw_payload)
        detail_payload = _serialize_section(detail_section, raw_payload)

        try:
            dao.update_ai_extract(
                [
                    (
                        str(title),
                        normalized_published_at,
                        raw_payload,
                        summary_payload,
                        detail_payload,
                    )
                ]
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.error(
                "Failed to persist finance breakfast AI extract: title=%s published_at=%s error=%s",
                title,
                published_at,
                exc,
            )
            continue

        updates += 1
        logger.warning(
            "Finance breakfast AI analysis stored: title=%s published_at=%s summary_len=%s detail_len=%s",
            title,
            normalized_published_at,
            len(summary_payload) if summary_payload else 0,
            len(detail_payload) if detail_payload else 0,
        )

    if updates == 0:
        logger.warning("Finance breakfast AI stage generated no analyzable entries.")
        return 0

    logger.warning("Finance breakfast AI stage persisted %s entries.", updates)
    return updates

def list_finance_breakfast(
    *,
    limit: int = 50,
    settings_path: Optional[str] = None,
) -> list[dict[str, object]]:
    settings = load_settings(settings_path)
    dao = FinanceBreakfastDAO(settings.postgres)
    limit = max(1, int(limit))
    entries = dao.list_recent(limit=limit)

    processed: list[dict[str, object]] = []
    for entry in entries:
        record = dict(entry)
        summary_payload = _parse_ai_payload(record.get("ai_extract_summary"))
        if summary_payload is not None:
            record["ai_extract_summary"] = summary_payload
        detail_payload = _parse_ai_payload(record.get("ai_extract_detail"))
        if detail_payload is not None:
            record["ai_extract_detail"] = detail_payload
        legacy_payload = _parse_ai_payload(record.get("ai_extract"))
        summary_text: Optional[str] = None
        summary_source: object = summary_payload if summary_payload is not None else record.get("ai_extract")
        if summary_payload is not None:
            summary_text = _build_comprehensive_summary(summary_payload)
        else:
            summary_text = _extract_comprehensive_summary(record.get("ai_extract"))
        if detail_payload is None and legacy_payload is not None:
            record["ai_extract_detail"] = legacy_payload
        if summary_text and _should_override_summary(record.get("summary"), summary_source):
            record["summary"] = summary_text
        processed.append(record)

    return processed


__all__ = ["AI_ANALYSIS_PROMPT_TEMPLATE", "list_finance_breakfast", "sync_finance_breakfast"]
