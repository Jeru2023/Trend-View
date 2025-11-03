"""
Service layer for synchronising Eastmoney global finance flash headlines.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from zoneinfo import ZoneInfo

from ..api_clients import fetch_global_flash_news, generate_finance_analysis
from ..config.settings import load_settings
from ..dao import GlobalFlashDAO

logger = logging.getLogger(__name__)

GLOBAL_FLASH_COLUMNS = ["url", "title", "summary", "published_at"]

CLASSIFICATION_PROMPT_TEMPLATE = (
    "请阅读以下财经快讯，并判断其对中国A股市场的影响。务必仅以 JSON 形式回复，字段说明如下：\n"
    "impact: true/false，是否可能产生显著影响；\n"
    "confidence: 0-1 之间的小数；\n"
    "reason: 20 字内简要理由；\n"
    "impact_levels: 数组，取值范围 [\"market\", \"industry\", \"sector\", \"theme\", \"stock\"]，表示影响层级，可多选；\n"
    "impact_markets: 若包含 market，请列出受影响的大盘或指数数组；\n"
    "impact_industries: 若包含 industry，请列出行业名称数组；\n"
    "impact_sectors: 若包含 sector，请列出板块名称数组；\n"
    "impact_themes: 若包含 theme，请列出题材概念数组；\n"
    "impact_stocks: 若包含 stock，请列出相关个股代码或简称数组；\n"
    "subject_level: 事件主体层级（仍可返回单个字符串）；\n"
    "impact_scope: 可简要概括整体影响范围；\n"
    "event_type, time_sensitivity, quant_signal: 与此前一致。\n"
    "模板：{\n  \"impact\": true/false,\n  \"confidence\": 0.0-1.0,\n  \"reason\": \"...\",\n  \"impact_levels\": [\"market\",...],\n  \"impact_markets\": [\"上证指数\"],\n  \"impact_industries\": [\"汽车\"],\n  \"impact_sectors\": [\"新能源车\"],\n  \"impact_themes\": [\"智能制造\"],\n  \"impact_stocks\": [\"300750.SZ\"],\n  \"subject_level\": \"国家级\",\n  \"impact_scope\": \"新能源车,汽车\",\n  \"event_type\": \"政策/监管\",\n  \"time_sensitivity\": \"阶段性\",\n  \"quant_signal\": \"机构净买入20亿\"\n}\n"
    "严禁输出额外文字。以下是快讯内容：\n{news_content}"
)
MAX_CLASSIFICATION_REASON_LENGTH = 120


def _prepare_global_flash_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=GLOBAL_FLASH_COLUMNS)

    frame = dataframe.copy()

    def _canonical(name: str) -> str:
        return "".join(ch for ch in name.lower() if ch.isalnum())

    canonical_map: dict[str, str] = {}
    for column in frame.columns:
        key = _canonical(str(column))
        if key and key not in canonical_map:
            canonical_map[key] = column

    rename_map: dict[str, str] = {}
    for target in GLOBAL_FLASH_COLUMNS:
        canonical_key = _canonical(target)
        source_column = canonical_map.get(canonical_key)
        if source_column and source_column != target:
            rename_map[source_column] = target

    if rename_map:
        frame = frame.rename(columns=rename_map)

    for column in GLOBAL_FLASH_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    with pd.option_context("mode.chained_assignment", None):
        frame["title"] = frame["title"].astype(str).str.strip()
        frame["summary"] = frame["summary"].astype(str).str.strip()
        frame["url"] = frame["url"].astype(str).str.strip()
        frame["published_at"] = pd.to_datetime(frame["published_at"], errors="coerce")

    prepared = (
        frame.loc[:, GLOBAL_FLASH_COLUMNS]
        .dropna(subset=["title", "url", "published_at"])
        .drop_duplicates(subset=["url"])
        .sort_values("published_at")
        .reset_index(drop=True)
    )
    return prepared


def _build_classification_payload(entry: Dict[str, object]) -> str:
    title = (entry.get("title") or "").strip()
    summary = (entry.get("summary") or "").strip()
    published_at = entry.get("published_at")
    if isinstance(published_at, pd.Timestamp):
        published_str = published_at.isoformat()
    elif isinstance(published_at, datetime):
        published_str = published_at.isoformat()
    else:
        published_str = str(published_at) if published_at else ""

    parts = [f"标题：{title}" if title else "标题：--"]
    parts.append(f"摘要：{summary if summary else '暂无摘要'}")
    if published_str:
        parts.append(f"发布时间：{published_str}")
    return "\n".join(parts)


def _parse_classification_response(raw_response: Optional[str]) -> Dict[str, object]:
    default_reason = "模型未给出有效判定"
    result = {
        "impact": False,
        "confidence": 0.0,
        "reason": default_reason,
        "raw": raw_response,
        "subject_level": None,
        "impact_scope": None,
        "event_type": None,
        "time_sensitivity": None,
        "quant_signal": None,
        "impact_levels": [],
        "impact_markets": [],
        "impact_industries": [],
        "impact_sectors": [],
        "impact_themes": [],
        "impact_stocks": [],
    }

    if not raw_response:
        return result

    try:
        payload = json.loads(raw_response)
    except json.JSONDecodeError:
        trimmed = raw_response.strip()
        if trimmed:
            result["reason"] = trimmed[:MAX_CLASSIFICATION_REASON_LENGTH]
        return result

    if isinstance(payload, dict):
        impact_value = payload.get("impact")
        if isinstance(impact_value, str):
            impact_value_lower = impact_value.strip().lower()
            if impact_value_lower in {"true", "yes", "1", "y"}:
                result["impact"] = True
            elif impact_value_lower in {"false", "no", "0", "n"}:
                result["impact"] = False
        elif isinstance(impact_value, bool):
            result["impact"] = impact_value

        confidence_value = payload.get("confidence")
        try:
            if confidence_value is not None:
                result["confidence"] = max(0.0, min(float(confidence_value), 1.0))
        except (TypeError, ValueError):
            pass

        reason_value = payload.get("reason")
        if isinstance(reason_value, str) and reason_value.strip():
            result["reason"] = reason_value.strip()[:MAX_CLASSIFICATION_REASON_LENGTH]
        elif isinstance(reason_value, list):
            joined = "、".join(str(item) for item in reason_value if item)
            if joined:
                result["reason"] = joined[:MAX_CLASSIFICATION_REASON_LENGTH]

        def _normalise_text(value: object) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, (list, tuple)):
                flattened = [str(item).strip() for item in value if str(item).strip()]
                return "、".join(flattened) if flattened else None
            text = str(value).strip()
            return text or None

        result["subject_level"] = _normalise_text(payload.get("subject_level"))
        result["impact_scope"] = _normalise_text(payload.get("impact_scope"))
        result["event_type"] = _normalise_text(payload.get("event_type"))
        result["time_sensitivity"] = _normalise_text(payload.get("time_sensitivity"))
        result["quant_signal"] = _normalise_text(payload.get("quant_signal"))
        result["impact_levels"] = _normalise_list(payload.get("impact_levels"))
        result["impact_markets"] = _normalise_list(payload.get("impact_markets"))
        result["impact_industries"] = _normalise_list(payload.get("impact_industries"))
        result["impact_sectors"] = _normalise_list(payload.get("impact_sectors"))
        result["impact_themes"] = _normalise_list(payload.get("impact_themes"))
        result["impact_stocks"] = _normalise_list(payload.get("impact_stocks"))
        _ensure_level_alignment(result)
    else:
        text = str(payload).strip()
        if text:
            result["reason"] = text[:MAX_CLASSIFICATION_REASON_LENGTH]

    return result


def _normalise_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        cleaned = [part.strip() for part in value.split(",") if part.strip()]
        return cleaned
    return []


def _serialize_list(values: List[str]) -> Optional[str]:
    cleaned = [str(item).strip() for item in (values or []) if str(item).strip()]
    if not cleaned:
        return None
    return json.dumps(cleaned, ensure_ascii=False)


def _ensure_level_alignment(parsed: Dict[str, object]) -> None:
    levels = set(parsed.get("impact_levels") or [])
    if parsed.get("impact_markets"):
        levels.add("market")
    if parsed.get("impact_industries"):
        levels.add("industry")
    if parsed.get("impact_sectors"):
        levels.add("sector")
    if parsed.get("impact_themes"):
        levels.add("theme")
    if parsed.get("impact_stocks"):
        levels.add("stock")
    if levels:
        parsed["impact_levels"] = sorted(levels)


def sync_global_flash(*, settings_path: Optional[str] = None) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = GlobalFlashDAO(settings.postgres)

    raw = fetch_global_flash_news()
    prepared = _prepare_global_flash_frame(raw)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        logger.warning("Global flash sync skipped: no data returned.")
        return {"rows": 0, "elapsedSeconds": elapsed}

    affected = dao.upsert(prepared)
    elapsed = time.perf_counter() - started
    return {"rows": int(affected), "elapsedSeconds": elapsed}


def classify_global_flash_batch(
    *, batch_size: int = 10, settings_path: Optional[str] = None
) -> dict[str, object]:
    started = time.perf_counter()
    settings = load_settings(settings_path)
    if not settings.deepseek:
        elapsed = time.perf_counter() - started
        logger.warning("DeepSeek配置缺失，跳过全球快讯分类。")
        return {"rows": 0, "elapsedSeconds": elapsed, "skipped": True}

    dao = GlobalFlashDAO(settings.postgres)
    batch_limit = max(1, min(int(batch_size), 100))
    entries = dao.list_unclassified(limit=batch_limit)
    if not entries:
        elapsed = time.perf_counter() - started
        return {"rows": 0, "elapsedSeconds": elapsed, "requested": 0}

    prepared_rows: List[Dict[str, object]] = []
    original_rows: List[Dict[str, object]] = []
    for entry in entries:
        url_value = entry.get("url")
        if not url_value:
            logger.debug("Skipping global flash classification entry without URL: %s", entry)
            continue
        payload = _build_classification_payload(entry)
        response = generate_finance_analysis(
            payload,
            settings=settings.deepseek,
            prompt_template=CLASSIFICATION_PROMPT_TEMPLATE,
        )
        parsed = _parse_classification_response(response)
        impact_flag = bool(parsed.get("impact"))
        reason_text = parsed.get("reason") or "模型未返回理由"
        confidence_value = parsed.get("confidence")
        if isinstance(confidence_value, (int, float)) and confidence_value > 0:
            reason_text = f"{reason_text} (置信度 {confidence_value:.2f})"
        local_now = datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
        impact_levels = parsed.get("impact_levels", [])
        prepared_rows.append(
            {
                "url": url_value,
                "if_extract": impact_flag,
                "extract_checked_at": local_now,
                "extract_reason": reason_text[:MAX_CLASSIFICATION_REASON_LENGTH],
                "subject_level": parsed.get("subject_level"),
                "impact_scope": parsed.get("impact_scope"),
                "event_type": parsed.get("event_type"),
                "time_sensitivity": parsed.get("time_sensitivity"),
                "quant_signal": parsed.get("quant_signal"),
                "impact_levels": _serialize_list(impact_levels),
                "impact_markets": _serialize_list(parsed.get("impact_markets", [])),
                "impact_industries": _serialize_list(parsed.get("impact_industries", [])),
                "impact_sectors": _serialize_list(parsed.get("impact_sectors", [])),
                "impact_themes": _serialize_list(parsed.get("impact_themes", [])),
                "impact_stocks": _serialize_list(parsed.get("impact_stocks", [])),
            }
        )
        original_rows.append(entry)

    if not prepared_rows:
        elapsed = time.perf_counter() - started
        return {"rows": 0, "elapsedSeconds": elapsed, "requested": len(entries)}

    prepared_dataframe_rows: List[Dict[str, object]] = []
    for original, updates in zip(original_rows, prepared_rows):
        merged = dict(original)
        merged.update(updates)
        prepared_dataframe_rows.append(merged)

    dataframe = pd.DataFrame(prepared_dataframe_rows)
    affected = dao.upsert(dataframe)
    logger.info(
        "Global flash classification processed %s entries (requested %s)",
        affected,
        len(entries),
    )
    elapsed = time.perf_counter() - started
    return {
        "rows": int(affected),
        "elapsedSeconds": elapsed,
        "requested": len(entries),
    }


def list_global_flash(*, limit: int = 200, settings_path: Optional[str] = None) -> List[dict[str, object]]:
    settings = load_settings(settings_path)
    dao = GlobalFlashDAO(settings.postgres)
    limit_value = max(1, min(int(limit), 500))
    return dao.list_recent(limit=limit_value)


__all__ = [
    "sync_global_flash",
    "list_global_flash",
    "classify_global_flash_batch",
    "_prepare_global_flash_frame",
]
