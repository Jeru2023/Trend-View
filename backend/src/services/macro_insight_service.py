"""
Aggregate macroeconomic datasets and generate DeepSeek-driven insights.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from ..dao import MacroInsightDAO
from .macro_cpi_service import list_macro_cpi
from .macro_leverage_service import list_macro_leverage_ratios
from .macro_m2_service import list_macro_m2
from .macro_pbc_rate_service import list_macro_pbc_rate
from .macro_pmi_service import list_macro_pmi
from .macro_ppi_service import list_macro_ppi
from .social_financing_service import list_social_financing_ratios

logger = logging.getLogger(__name__)

SERIES_LIMIT = 11  # latest + 10 historical entries

_LOCAL_TZ = ZoneInfo("Asia/Shanghai")

MACRO_INSIGHT_PROMPT = """
你是一名研究中国宏观经济与A股市场联动的策略分析师。下面提供的 JSON 数据涵盖多个宏观指标，每个指标包含最近一次公布值及过去10个历史值。请综合这些数据进行分析。

数据如下：
{news_content}

请输出符合 JSON 规范的对象，字段要求：
- "market_bias": 取值限定为 ["bullish","neutral","bearish"]，表示对A股的边际影响倾向。
- "confidence": 介于 0 到 1 之间的小数，衡量结论把握度。
- "macro_overview": 用中文给出不少于 120 字的宏观形势综述，结合增长、物价、流动性、政策取向等维度。
- "key_indicators": 数组，包含 3-6 个对象，每个对象需包含
    { "indicator": 指标名称, "latest_value": 最新读数(含单位或环比/同比), "trend_comment": 对比历史的走势点评 }。
- "policy_outlook": 不少于 80 字，描述货币与财政政策可能的取向及时间窗口。
- "risk_warnings": 数组，列出至少 3 条潜在风险或不确定性。
- "watch_points": 数组，列出 3-5 条后续需要重点追踪的经济数据或事件。

请根据给出的数值进行合理推理，如数据缺失需在相关字段说明原因。务必返回单个 JSON 对象，不要包含额外文字。
"""


def _float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_LOCAL_TZ)
        else:
            value = value.astimezone(_LOCAL_TZ)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed.isoformat()
    except ValueError:
        return str(value)


DatasetEntry = Dict[str, Any]
DatasetPayload = Dict[str, Any]


def _sanitize_records(
    records: Iterable[Dict[str, Any]],
    *,
    columns: Sequence[str],
    extra_fields: Optional[Sequence[Tuple[str, str]]] = None,
) -> List[DatasetEntry]:
    """
    Normalise macro records to serialisable dictionaries limited by SERIES_LIMIT.

    columns defines value keys to keep. extra_fields is optional mapping of source->target keys.
    """
    sanitized: List[DatasetEntry] = []
    for record in records:
        entry: DatasetEntry = {
            "period_date": _date_to_iso(record.get("period_date")),
            "period_label": record.get("period_label"),
        }
        for column in columns:
            entry[column] = _float_or_none(record.get(column))
        if extra_fields:
            for source, target in extra_fields:
                entry[target] = record.get(source)
        sanitized.append(entry)
    sanitized.sort(key=lambda item: item.get("period_date") or "", reverse=True)
    return sanitized[:SERIES_LIMIT]


def _collect_macro_datasets(settings_path: Optional[str] = None) -> Tuple[List[DatasetPayload], List[str], Optional[date]]:
    dataset_payloads: List[DatasetPayload] = []
    warnings: List[str] = []
    snapshot_candidates: List[date] = []

    def _record_snapshot(entry_list: Sequence[DatasetEntry]) -> None:
        if not entry_list:
            return
        first = entry_list[0].get("period_date")
        if not first:
            return
        try:
            snapshot_candidates.append(datetime.fromisoformat(str(first)).date())
        except ValueError:
            pass

    # Macro leverage ratios
    leverage_result = list_macro_leverage_ratios(limit=SERIES_LIMIT, settings_path=settings_path)
    leverage_series = _sanitize_records(
        leverage_result.get("items", []),
        columns=[
            "household_ratio",
            "non_financial_corporate_ratio",
            "government_ratio",
            "central_government_ratio",
            "local_government_ratio",
            "real_economy_ratio",
            "financial_assets_ratio",
            "financial_liabilities_ratio",
        ],
    )
    if leverage_series:
        dataset_payloads.append(
            {
                "key": "leverage",
                "titleKey": "macroDatasetLeverage",
                "fields": [
                    {"key": "household_ratio", "labelKey": "macroFieldHousehold", "format": "percent"},
                    {"key": "non_financial_corporate_ratio", "labelKey": "macroFieldCorporate", "format": "percent"},
                    {"key": "government_ratio", "labelKey": "macroFieldGovernment", "format": "percent"},
                    {"key": "real_economy_ratio", "labelKey": "macroFieldRealEconomy", "format": "percent"},
                    {"key": "financial_assets_ratio", "labelKey": "macroFieldFinancialAssets", "format": "percent"},
                    {"key": "financial_liabilities_ratio", "labelKey": "macroFieldFinancialLiabilities", "format": "percent"},
                ],
                "series": leverage_series,
                "latest": leverage_series[0],
                "updatedAt": _date_to_iso(leverage_result.get("lastSyncedAt")),
            }
        )
        _record_snapshot(leverage_series)
    else:
        warnings.append("缺少宏观杠杆率数据")

    # Social financing
    social_result = list_social_financing_ratios(limit=SERIES_LIMIT, settings_path=settings_path)
    social_series = _sanitize_records(
        social_result.get("items", []),
        columns=[
            "total_financing",
            "renminbi_loans",
            "entrusted_and_fx_loans",
            "entrusted_loans",
            "trust_loans",
            "undiscounted_bankers_acceptance",
            "corporate_bonds",
            "domestic_equity_financing",
        ],
    )
    if social_series:
        dataset_payloads.append(
            {
                "key": "social_financing",
                "titleKey": "macroDatasetSocialFinancing",
                "fields": [
                    {"key": "total_financing", "labelKey": "macroFieldTotalFinancing", "format": "number"},
                    {"key": "renminbi_loans", "labelKey": "macroFieldRmbLoans", "format": "number"},
                    {"key": "corporate_bonds", "labelKey": "macroFieldCorporateBonds", "format": "number"},
                    {"key": "domestic_equity_financing", "labelKey": "macroFieldEquityFinancing", "format": "number"},
                ],
                "series": social_series,
                "latest": social_series[0],
                "updatedAt": _date_to_iso(social_result.get("lastSyncedAt")),
            }
        )
        _record_snapshot(social_series)
    else:
        warnings.append("缺少社会融资数据")

    # CPI
    cpi_result = list_macro_cpi(limit=SERIES_LIMIT, settings_path=settings_path)
    cpi_series = _sanitize_records(
        cpi_result.get("items", []),
        columns=["actual_value", "forecast_value", "previous_value"],
    )
    if cpi_series:
        dataset_payloads.append(
            {
                "key": "cpi",
                "titleKey": "macroDatasetCpi",
                "fields": [
                    {"key": "actual_value", "labelKey": "macroFieldActualValue", "format": "percent"},
                    {"key": "forecast_value", "labelKey": "macroFieldForecastValue", "format": "percent"},
                    {"key": "previous_value", "labelKey": "macroFieldPreviousValue", "format": "percent"},
                ],
                "series": cpi_series,
                "latest": cpi_series[0],
                "updatedAt": _date_to_iso(cpi_result.get("lastSyncedAt")),
            }
        )
        _record_snapshot(cpi_series)
    else:
        warnings.append("缺少CPI数据")

    # PPI
    ppi_result = list_macro_ppi(limit=SERIES_LIMIT, settings_path=settings_path)
    ppi_series = _sanitize_records(
        ppi_result.get("items", []),
        columns=["current_index", "yoy_change", "cumulative_index"],
    )
    if ppi_series:
        dataset_payloads.append(
            {
                "key": "ppi",
                "titleKey": "macroDatasetPpi",
                "fields": [
                    {"key": "current_index", "labelKey": "macroFieldCurrentIndex", "format": "number"},
                    {"key": "yoy_change", "labelKey": "macroFieldYoyChange", "format": "percent"},
                    {"key": "cumulative_index", "labelKey": "macroFieldCumulativeIndex", "format": "number"},
                ],
                "series": ppi_series,
                "latest": ppi_series[0],
                "updatedAt": _date_to_iso(ppi_result.get("lastSyncedAt")),
            }
        )
        _record_snapshot(ppi_series)
    else:
        warnings.append("缺少PPI数据")

    # PMI (grouped by series)
    pmi_result = list_macro_pmi(limit=SERIES_LIMIT * 2, settings_path=settings_path)
    pmi_items = pmi_result.get("items", [])
    pmi_grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in pmi_items:
        series_key = str(item.get("series") or "manufacturing")
        pmi_grouped[series_key].append(item)

    if pmi_grouped:
        for series_key, rows in pmi_grouped.items():
            series_rows = _sanitize_records(rows, columns=["actual_value", "forecast_value", "previous_value"])
            if not series_rows:
                continue
            dataset_payloads.append(
                {
                    "key": f"pmi_{series_key}",
                    "titleKey": "macroDatasetPmiManufacturing"
                    if series_key.lower().startswith("manufacturing")
                    else "macroDatasetPmiNonManufacturing",
                    "fields": [
                        {"key": "actual_value", "labelKey": "macroFieldActualValue", "format": "number"},
                        {"key": "forecast_value", "labelKey": "macroFieldForecastValue", "format": "number"},
                        {"key": "previous_value", "labelKey": "macroFieldPreviousValue", "format": "number"},
                    ],
                    "series": series_rows,
                    "latest": series_rows[0],
                    "updatedAt": _date_to_iso(pmi_result.get("lastSyncedAt")),
                }
            )
            _record_snapshot(series_rows)
    else:
        warnings.append("缺少PMI数据")

    # M2
    m2_result = list_macro_m2(limit=SERIES_LIMIT, settings_path=settings_path)
    m2_columns = [
        "m2",
        "m2_yoy",
        "m2_mom",
        "m1",
        "m1_yoy",
        "m1_mom",
        "m0",
        "m0_yoy",
        "m0_mom",
    ]
    m2_series = _sanitize_records(m2_result.get("items", []), columns=m2_columns)
    if m2_series:
        dataset_payloads.append(
            {
                "key": "m2",
                "titleKey": "macroDatasetM2",
                "fields": [
                    {"key": "m2_yoy", "labelKey": "macroFieldM2Yoy", "format": "percent"},
                    {"key": "m2_mom", "labelKey": "macroFieldM2Mom", "format": "percent"},
                    {"key": "m1_yoy", "labelKey": "macroFieldM1Yoy", "format": "percent"},
                    {"key": "m0_yoy", "labelKey": "macroFieldM0Yoy", "format": "percent"},
                    {"key": "m2", "labelKey": "macroFieldM2Value", "format": "number"},
                    {"key": "m1", "labelKey": "macroFieldM1Value", "format": "number"},
                    {"key": "m0", "labelKey": "macroFieldM0Value", "format": "number"},
                ],
                "series": m2_series,
                "latest": m2_series[0],
                "updatedAt": _date_to_iso(m2_result.get("lastSyncedAt")),
            }
        )
        _record_snapshot(m2_series)
    else:
        warnings.append("缺少M2数据")

    # PBC rate decisions
    pbc_result = list_macro_pbc_rate(limit=SERIES_LIMIT, settings_path=settings_path)
    pbc_series = _sanitize_records(
        pbc_result.get("items", []),
        columns=["actual_value", "forecast_value", "previous_value"],
    )
    if pbc_series:
        dataset_payloads.append(
            {
                "key": "pbc_rate",
                "titleKey": "macroDatasetPbcRate",
                "fields": [
                    {"key": "actual_value", "labelKey": "macroFieldActualValue", "format": "number"},
                    {"key": "forecast_value", "labelKey": "macroFieldForecastValue", "format": "number"},
                    {"key": "previous_value", "labelKey": "macroFieldPreviousValue", "format": "number"},
                ],
                "series": pbc_series,
                "latest": pbc_series[0],
                "updatedAt": _date_to_iso(pbc_result.get("lastSyncedAt")),
            }
        )
        _record_snapshot(pbc_series)
    else:
        warnings.append("缺少央行利率数据")

    snapshot_date = max(snapshot_candidates) if snapshot_candidates else None
    dataset_payloads.sort(key=lambda item: item.get("titleKey") or "")
    return dataset_payloads, warnings, snapshot_date


def generate_macro_insight(
    *,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    settings = load_settings(settings_path)
    datasets, warnings, snapshot_date = _collect_macro_datasets(settings_path=settings_path)

    generated_at_local = datetime.now(_LOCAL_TZ)
    snapshot_date = snapshot_date or generated_at_local.date()

    summary_json: Optional[Dict[str, Any]] = None
    raw_response: Optional[str] = None
    model: Optional[str] = None

    if run_llm:
        if settings.deepseek is None:
            logger.info("DeepSeek configuration missing; skipping macro insight reasoning")
        else:
            macro_payload = {
                "generated_at": generated_at_local.isoformat(),
                "snapshot_date": snapshot_date.isoformat(),
                "datasets": datasets,
                "warnings": warnings,
            }
            prompt_payload = json.dumps(macro_payload, ensure_ascii=False, separators=(",", ":"))
            try:
                llm_result = generate_finance_analysis(
                    prompt_payload,
                    settings=settings.deepseek,
                    prompt_template=MACRO_INSIGHT_PROMPT,
                    temperature=0.2,
                )
            except Exception as exc:  # pragma: no cover - defensive for LLM failures
                logger.warning("Macro insight reasoning failed: %s", exc)
                llm_result = None

            if llm_result:
                raw_response = llm_result if isinstance(llm_result, str) else json.dumps(llm_result, ensure_ascii=False)
                try:
                    summary_json = json.loads(raw_response)
                except (TypeError, json.JSONDecodeError):
                    summary_json = None
                model = settings.deepseek.model

    MacroInsightDAO(settings.postgres).upsert_snapshot(
        snapshot_date=snapshot_date,
        generated_at=generated_at_local.replace(tzinfo=None),
        datasets={"items": datasets, "warnings": warnings},
        summary_json=summary_json,
        raw_response=raw_response,
        model=model,
    )

    return {
        "snapshot_date": snapshot_date,
        "generated_at": generated_at_local.isoformat(),
        "datasets": datasets,
        "warnings": warnings,
        "summary": summary_json,
        "raw_response": raw_response,
        "model": model,
    }


def get_latest_macro_insight(*, settings_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    settings = load_settings(settings_path)
    record = MacroInsightDAO(settings.postgres).fetch_latest()
    if not record:
        return None

    def _to_local_iso(dt: Optional[datetime]) -> Optional[str]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_LOCAL_TZ)
        else:
            dt = dt.astimezone(_LOCAL_TZ)
        return dt.isoformat()

    datasets = record.get("datasets") or {}
    if isinstance(datasets, str):
        try:
            datasets = json.loads(datasets)
        except json.JSONDecodeError:
            datasets = {}

    summary_json = record.get("summary_json")
    if isinstance(summary_json, str):
        try:
            summary_json = json.loads(summary_json)
        except json.JSONDecodeError:
            summary_json = None

    payload = {
        "snapshot_date": record.get("snapshot_date"),
        "generated_at": _to_local_iso(record.get("generated_at")),
        "datasets": datasets.get("items", []) if isinstance(datasets, dict) else datasets,
        "warnings": datasets.get("warnings", []) if isinstance(datasets, dict) else [],
        "summary": summary_json,
        "raw_response": record.get("raw_response"),
        "model": record.get("model"),
        "updated_at": _to_local_iso(record.get("updated_at")),
        "created_at": _to_local_iso(record.get("created_at")),
    }
    return payload


__all__ = ["generate_macro_insight", "get_latest_macro_insight"]
