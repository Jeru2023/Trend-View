"""
Aggregate overseas market data points and generate DeepSeek-powered insights.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

from ..api_clients import generate_finance_analysis
from ..config.settings import AppSettings, load_settings
from ..dao import (
    DollarIndexDAO,
    FuturesRealtimeDAO,
    GlobalIndexDAO,
    PeripheralInsightDAO,
    RmbMidpointDAO,
)

logger = logging.getLogger(__name__)

_UTC = timezone.utc

GLOBAL_INDEX_TARGETS: Dict[str, Dict[str, Sequence[str]]] = {
    "dow_jones": {
        "codes": ("DJI", ".DJI", "DJIA"),
        "names": ("道琼斯", "道指", "Dow Jones", "Dow Jones Industrial Average"),
        "display_name": "道琼斯工业指数",
    },
    "nasdaq": {
        "codes": ("IXIC", "NDX"),
        "names": ("纳斯达克", "NASDAQ"),
        "display_name": "纳斯达克综合指数",
    },
    "sp500": {
        "codes": ("INX", "GSPC", "SP500"),
        "names": ("标普500", "标准普尔500"),
        "display_name": "标普500指数",
    },
}

RMB_TARGET_CURRENCIES: Dict[str, str] = {
    "USD": "usd",
    "EUR": "eur",
    "JPY": "jpy",
}

FUTURES_TARGETS: Dict[str, Dict[str, Sequence[str]]] = {
    "brent_crude": {
        "names": ("布伦特原油", "Brent"),
        "display_name": "布伦特原油",
        "unit": "USD/bbl",
    },
    "wti_crude": {
        "names": ("NYMEX原油", "WTI", "NYMEX原油 CL"),
        "display_name": "WTI原油",
        "unit": "USD/bbl",
    },
    "gold": {
        "names": ("COMEX黄金", "伦敦金", "COMEX GOLD"),
        "display_name": "COMEX黄金",
        "unit": "USD/oz",
    },
    "silver": {
        "names": ("COMEX白银", "伦敦银", "COMEX SILVER"),
        "display_name": "COMEX白银",
        "unit": "USD/oz",
    },
}

PERIPHERAL_PROMPT_TEMPLATE = """你的任务是根据下面提供的 JSON 数据分析外围市场（美股主要指数、美元指数、人民币汇率、国际大宗商品）的最新表现，并判断对A股大盘的潜在影响。
数据如下：
{news_content}

请输出一个 JSON 对象，包含以下字段：
- "summary": 以中文写出关键结论，聚焦外部市场对A股情绪和方向的影响。
- "a_share_bias": 从 ["bullish","bearish","neutral"] 中选择一个最符合当前外围环境对A股影响的判断。
- "drivers": 列出 3-5 个影响判断的要点，使用中文短语。
- "risk_level": 用 ["low","medium","high"] 描述外围环境带来的风险程度。
- "confidence": 用 0-100 的数字评估结论把握度。
请确保返回严格的 JSON 格式，不要包含额外文本。
"""


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_UTC)
        return value.astimezone(_UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _match_row(row: Dict[str, Any], codes: Sequence[str], names: Sequence[str]) -> bool:
    code = str(row.get("code") or "").strip().lower()
    name = str(row.get("name") or "").strip().lower()
    return code in {c.lower() for c in codes} or name in {n.lower() for n in names}


def _now_utc() -> datetime:
    return datetime.now(tz=_UTC)


@dataclass
class PeripheralMetrics:
    snapshot_date: date
    generated_at: datetime
    data: Dict[str, Any]
    warnings: List[str]


def _collect_metrics(settings: AppSettings) -> PeripheralMetrics:
    postgres_settings = settings.postgres

    global_index_rows = GlobalIndexDAO(postgres_settings).list_entries(limit=200)["items"]
    dollar_rows = DollarIndexDAO(postgres_settings).list_entries(limit=2)["items"]
    rmb_rows = RmbMidpointDAO(postgres_settings).list_entries(limit=1)["items"]
    futures_rows = FuturesRealtimeDAO(postgres_settings).list_entries(limit=200)["items"]

    warnings: List[str] = []
    global_indices: List[Dict[str, Any]] = []
    for key, spec in GLOBAL_INDEX_TARGETS.items():
        match = next(
            (
                row
                for row in global_index_rows
                if _match_row(row, spec["codes"], spec["names"])
            ),
            None,
        )
        if not match:
            warnings.append(f"缺少{spec['display_name']}数据")
            continue
        as_of = match.get("last_quote_time") or match.get("updated_at")
        global_indices.append(
            {
                "key": key,
                "name": spec["display_name"],
                "code": match.get("code"),
                "last": _to_float(match.get("latest_price")),
                "changeAmount": _to_float(match.get("change_amount")),
                "changePercent": _to_float(match.get("change_percent")),
                "high": _to_float(match.get("high_price")),
                "low": _to_float(match.get("low_price")),
                "asOf": _to_iso(as_of),
            }
        )
        if isinstance(as_of, datetime):
            if _now_utc() - as_of.replace(tzinfo=_UTC if as_of.tzinfo is None else as_of.tzinfo) > timedelta(days=1, hours=6):
                warnings.append(f"{spec['display_name']}数据可能过期（{_to_iso(as_of)}）")

    dollar_summary: Optional[Dict[str, Any]] = None
    if dollar_rows:
        latest = dollar_rows[0]
        prev = dollar_rows[1] if len(dollar_rows) > 1 else None
        close_price = _to_float(latest.get("close_price"))
        prev_close = _to_float(prev.get("close_price")) if prev else None
        change_amount = None
        change_percent = None
        if close_price is not None and prev_close is not None:
            change_amount = close_price - prev_close
            if prev_close:
                change_percent = (change_amount / prev_close) * 100.0
        as_of = latest.get("trade_date")
        dollar_summary = {
            "code": latest.get("code"),
            "name": latest.get("name"),
            "close": close_price,
            "changeAmount": change_amount,
            "changePercent": change_percent,
            "high": _to_float(latest.get("high_price")),
            "low": _to_float(latest.get("low_price")),
            "amplitude": _to_float(latest.get("amplitude")),
            "tradeDate": _to_iso(as_of),
        }
        if isinstance(as_of, date):
            if date.today() - as_of > timedelta(days=1):
                warnings.append("美元指数不是最新交易日数据")
    else:
        warnings.append("缺少美元指数数据")

    rmb_summary: Optional[Dict[str, Any]] = None
    if rmb_rows:
        latest = rmb_rows[0]
        trade_date_value = latest.get("trade_date")
        rates = {}
        for label, column in RMB_TARGET_CURRENCIES.items():
            quote = _to_float(latest.get(column))
            if quote is not None:
                rates[label] = {"quotePer100": quote}
            else:
                warnings.append(f"缺少人民币对{label}汇率")
        rmb_summary = {
            "tradeDate": _to_iso(trade_date_value),
            "rates": rates,
        }
        if isinstance(trade_date_value, date):
            if date.today() - trade_date_value > timedelta(days=1):
                warnings.append("人民币中间价不是最新交易日数据")
    else:
        warnings.append("缺少人民币中间价数据")

    futures_summary: List[Dict[str, Any]] = []
    for key, spec in FUTURES_TARGETS.items():
        match = next(
            (
                row
                for row in futures_rows
                if str(row.get("name") or "").strip().lower() in {n.lower() for n in spec["names"]}
            ),
            None,
        )
        if not match:
            warnings.append(f"缺少{spec['display_name']}报价")
            continue
        quote_time = match.get("quote_time") or match.get("updated_at")
        futures_summary.append(
            {
                "key": key,
                "name": spec["display_name"],
                "code": match.get("code"),
                "last": _to_float(match.get("last_price")),
                "changeAmount": _to_float(match.get("change_amount")),
                "changePercent": _to_float(match.get("change_percent")),
                "unit": spec.get("unit"),
                "quoteTime": _to_iso(quote_time),
            }
        )
        if isinstance(quote_time, datetime):
            if _now_utc() - quote_time.replace(tzinfo=_UTC if quote_time.tzinfo is None else quote_time.tzinfo) > timedelta(hours=12):
                warnings.append(f"{spec['display_name']}报价可能过期（{_to_iso(quote_time)}）")

    all_dates: List[date] = []
    for entry in global_indices:
        as_of_value = entry.get("asOf")
        if isinstance(as_of_value, str):
            try:
                parsed = datetime.fromisoformat(as_of_value.replace("Z", "+00:00"))
                all_dates.append(parsed.date())
            except ValueError:
                pass
    if dollar_summary and isinstance(dollar_summary.get("tradeDate"), str):
        try:
            all_dates.append(datetime.fromisoformat(dollar_summary["tradeDate"]).date())
        except ValueError:
            pass
    if rmb_summary and isinstance(rmb_summary.get("tradeDate"), str):
        try:
            all_dates.append(datetime.fromisoformat(rmb_summary["tradeDate"]).date())
        except ValueError:
            pass

    snapshot_date = max(all_dates) if all_dates else date.today()

    metrics = {
        "generatedAt": _to_iso(_now_utc()),
        "globalIndices": global_indices,
        "dollarIndex": dollar_summary,
        "rmbMidpoint": rmb_summary,
        "commodities": futures_summary,
        "warnings": warnings,
    }

    return PeripheralMetrics(
        snapshot_date=snapshot_date,
        generated_at=_now_utc(),
        data=metrics,
        warnings=warnings,
    )


def generate_peripheral_insight(
    *,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Aggregate latest peripheral market data and optionally request a DeepSeek summary.
    """
    settings = load_settings(settings_path)
    metrics_bundle = _collect_metrics(settings)

    summary: Optional[str] = None
    raw_response: Optional[str] = None
    model: Optional[str] = None

    if run_llm:
        if settings.deepseek is None:
            logger.info("DeepSeek settings not configured; skipping LLM summary")
        else:
            metrics_json = json.dumps(metrics_bundle.data, ensure_ascii=False, separators=(",", ":"))
            llm_output = generate_finance_analysis(
                metrics_json,
                settings=settings.deepseek,
                prompt_template=PERIPHERAL_PROMPT_TEMPLATE,
            )
            if llm_output:
                summary = llm_output
                raw_response = llm_output
                model = settings.deepseek.model
            else:
                logger.warning("DeepSeek did not return content for peripheral insight")

    PeripheralInsightDAO(settings.postgres).upsert_snapshot(
        snapshot_date=metrics_bundle.snapshot_date,
        generated_at=metrics_bundle.generated_at,
        metrics=metrics_bundle.data,
        summary=summary,
        raw_response=raw_response,
        model=model,
    )

    return {
        "snapshot_date": metrics_bundle.snapshot_date,
        "generated_at": metrics_bundle.generated_at,
        "metrics": metrics_bundle.data,
        "summary": summary,
        "raw_response": raw_response,
        "model": model,
        "warnings": metrics_bundle.warnings,
    }


def get_latest_peripheral_insight(settings_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    settings = load_settings(settings_path)
    record = PeripheralInsightDAO(settings.postgres).fetch_latest()
    if not record:
        return None

    return {
        "snapshot_date": record.get("snapshot_date"),
        "generated_at": record.get("generated_at"),
        "metrics": record.get("metrics"),
        "summary": record.get("summary"),
        "raw_response": record.get("raw_response"),
        "model": record.get("model"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
    }


__all__ = ["generate_peripheral_insight", "get_latest_peripheral_insight"]
