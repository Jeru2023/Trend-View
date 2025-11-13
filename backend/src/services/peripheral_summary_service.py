"""
Aggregate overseas market data points and generate DeepSeek-powered insights.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

from ..api_clients import generate_finance_analysis
from ..config.settings import AppSettings, load_settings
from ..dao import DollarIndexDAO, FedStatementDAO, FuturesRealtimeDAO, PeripheralInsightDAO, RmbMidpointDAO
from .global_index_service import list_global_index_history, list_global_indices

logger = logging.getLogger(__name__)

_UTC = timezone.utc
_LOCAL_TZ = ZoneInfo("Asia/Shanghai")
DEEPSEEK_REASONER_MODEL = "deepseek-reasoner"
REASONER_MAX_OUTPUT_TOKENS = 900

GLOBAL_INDEX_TARGETS: Dict[str, Dict[str, Sequence[str]]] = {
    "dow_jones": {
        "codes": ("^DJI", "DJI", "DJIA"),
        "names": ("道琼斯", "道指", "Dow Jones", "Dow Jones Industrial Average"),
        "display_name": "道琼斯工业指数",
    },
    "nasdaq": {
        "codes": ("^IXIC", "IXIC", "NDX"),
        "names": ("纳斯达克", "NASDAQ"),
        "display_name": "纳斯达克综合指数",
    },
    "sp500": {
        "codes": ("^GSPC", "GSPC", "SP500"),
        "names": ("标普500", "标准普尔500"),
        "display_name": "标普500指数",
    },
    "nikkei": {
        "codes": ("^N225", "N225"),
        "names": ("日经225", "Nikkei"),
        "display_name": "日经225指数",
    },
    "hang_seng": {
        "codes": ("^HSI", "HSI"),
        "names": ("恒生指数", "Hang Seng"),
        "display_name": "恒生指数",
    },
    "ftse_a50": {
        "codes": ("XIN9.FGI", "CN50=F", "XIN9", "^XIN9"),
        "names": ("富时中国A50", "FTSE China A50"),
        "display_name": "富时中国A50指数",
    },
    "stoxx50": {
        "codes": ("^STOXX50E", "^SX5E", "SX5E"),
        "names": ("欧洲斯托克50", "Euro Stoxx 50"),
        "display_name": "欧洲斯托克50指数",
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

PERIPHERAL_PROMPT_TEMPLATE = """您的任务是根据提供的JSON数据，分析外围市场（全球主要股指——含美股、日经225、恒生、富时A50、欧洲斯托克50——以及美元指数、人民币中间价、国际大宗商品、美联储声明）的最新表现和短期趋势，并综合判断对A股大盘的潜在影响。数据如下：
{news_content}

推理步骤：
1. **深度趋势分析**：结合最近5-10天历史数据，计算短期趋势（如使用简单移动平均或波动率），识别关键突破/回调位，并评估趋势一致性（如多个指数是否共振）。
2. **详细市场联动机制**：对每个传导路径进行量化分析：
   - 美股情绪传导：对比美股与A股历史相关性，提及具体涨跌幅和成交量变化。
   - 亚洲市场联动：分析日经225、恒生指数和富时A50的日内波动如何映射到A股开盘预期。
   - 欧洲市场影响：评估欧洲斯托克50与美股的联动性，及其对亚洲市场的隔夜引导作用。
   - 汇率渠道：分析美元指数与人民币中间价的互动，计算潜在资金流动压力（如美元强弱对北向资金的影响）。
   - 大宗商品：结合原油和黄金价格变化，讨论对A股板块（如能源、贵金属）的直接成本或通胀效应。
   - 政策沟通：解读美联储声明的政策信号（如监管变化），评估其对全球流动性的隐含影响。
3. **A股特异性推理**：基于数据严格推理，避免猜测，但可引用典型机制（如外资流出历史）。
4. **综合风险评估**：根据波动性（如振幅扩大）、趋势背离概率或极端事件（如政策突变）评估风险，并说明理由。

输出严格的JSON（无额外文本），包含：
- "summary": 中文结论，至少3-5句话，点明关键机制、数据支撑及不确定性。
- "a_share_bias": 从 ["bullish","bearish","neutral"] 选择整体倾向。
- "drivers": 3-5个中文短语，每个短语包含具体数据引用（如“情绪面：道琼斯三日连涨累计+2.5%”）。
- "risk_level": ["low","medium","high"] 之一，需结合量化指标（如波动率）。
- "confidence": 0-100数字，依据数据时效性、多指标共振度及历史回测给出把握。
务必基于数据逐步推理，避免主观臆断。
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


def _to_local_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        base = value if value.tzinfo else value.replace(tzinfo=_LOCAL_TZ)
        return base.astimezone(_LOCAL_TZ).isoformat()
    elif isinstance(value, date):
        base = datetime.combine(value, datetime.min.time(), tzinfo=_LOCAL_TZ)
        return base.isoformat()
    return str(value)


def _match_row(row: Dict[str, Any], codes: Sequence[str], names: Sequence[str]) -> bool:
    code = str(row.get("code") or "").strip().lower()
    name = str(row.get("name") or "").strip().lower()
    return code in {c.lower() for c in codes} or name in {n.lower() for n in names}


def _now_utc() -> datetime:
    return datetime.now(tz=_UTC)


def _now_local() -> datetime:
    return datetime.now(tz=_LOCAL_TZ)


@dataclass
class PeripheralMetrics:
    snapshot_date: date
    generated_at: datetime
    data: Dict[str, Any]
    warnings: List[str]


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_UTC)
        return value.astimezone(_UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _build_prompt_sections(metrics: Dict[str, Any], warnings: List[str]) -> str:
    sections: List[str] = []

    def dump(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=_serialize_value)

    global_section = {
        "latest_timestamp": metrics.get("globalIndicesLatestTimestamp"),
        "latest": metrics.get("globalIndices"),
        "history": metrics.get("globalIndicesHistory"),
    }
    sections.append(f"【全球指数】\n{dump(global_section)}")

    dollar_section = {
        "latest_timestamp": metrics.get("dollarIndexLatestTimestamp"),
        "latest_summary": metrics.get("dollarIndex"),
        "series": metrics.get("dollarIndexSeries"),
    }
    sections.append(f"【美元指数】\n{dump(dollar_section)}")

    rmb_section = {
        "latest_timestamp": metrics.get("rmbMidpointLatestTimestamp"),
        "latest_summary": metrics.get("rmbMidpoint"),
        "series": metrics.get("rmbMidpointSeries"),
    }
    sections.append(f"【人民币中间价】\n{dump(rmb_section)}")

    commodities_section = {
        "latest_timestamp": metrics.get("commoditiesLatestTimestamp"),
        "items": metrics.get("commodities"),
    }
    sections.append(f"【大宗商品】\n{dump(commodities_section)}")

    fed_section = {
        "latest_timestamp": metrics.get("fedStatementsLatestTimestamp"),
        "items": metrics.get("fedStatements"),
    }
    sections.append(f"【美联储声明】\n{dump(fed_section)}")

    quality_section = {
        "generated_at": metrics.get("generatedAt"),
        "warnings": warnings,
    }
    sections.append(f"【数据质量】\n{dump(quality_section)}")

    return "\n\n".join(sections)


def _normalize_metrics(metrics: Dict[str, Any]) -> Dict[str, Any]:
    serialized = json.dumps(metrics, ensure_ascii=False, default=_serialize_value)
    return json.loads(serialized)


def _collect_metrics(settings: AppSettings) -> PeripheralMetrics:
    postgres_settings = settings.postgres

    global_index_payload = list_global_indices(limit=200, settings=settings)
    global_index_rows = global_index_payload.get("items", [])
    dollar_payload = DollarIndexDAO(postgres_settings).list_entries(limit=10)
    dollar_rows = dollar_payload.get("items", [])
    rmb_payload = RmbMidpointDAO(postgres_settings).list_entries(limit=10)
    rmb_rows = rmb_payload.get("items", [])
    futures_rows = FuturesRealtimeDAO(postgres_settings).list_entries(limit=200)["items"]
    fed_rows = FedStatementDAO(postgres_settings).list_entries(limit=3)["items"]

    warnings: List[str] = []
    all_dates: List[date] = []

    global_indices: List[Dict[str, Any]] = []
    global_history: Dict[str, List[Dict[str, Any]]] = {}
    latest_global_dt: Optional[datetime] = None
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
        as_of_iso = _to_iso(as_of)
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
                "asOf": as_of_iso,
            }
        )
        candidate_dt: Optional[datetime] = None
        if isinstance(as_of, datetime):
            candidate_dt = as_of if as_of.tzinfo else as_of.replace(tzinfo=_UTC)
        elif isinstance(as_of, date):
            candidate_dt = datetime.combine(as_of, datetime.min.time(), tzinfo=_UTC)
        if candidate_dt:
            all_dates.append(candidate_dt.date())
            if latest_global_dt is None or candidate_dt > latest_global_dt:
                latest_global_dt = candidate_dt
        if isinstance(as_of, datetime):
            if _now_utc() - candidate_dt > timedelta(days=1, hours=6):
                warnings.append(f"{spec['display_name']}数据可能过期（{as_of_iso}）")
        primary_code = spec["codes"][0]
        try:
            history_payload = list_global_index_history(code=primary_code, limit=10, settings=settings)
            global_history[key] = history_payload.get("items", [])
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to load history for %s: %s", primary_code, exc)
            warnings.append(f"{spec['display_name']}历史缺失")

    global_indices_latest_iso = _to_iso(latest_global_dt)

    dollar_series: List[Dict[str, Any]] = []
    dollar_summary: Optional[Dict[str, Any]] = None
    dollar_latest_iso: Optional[str] = None
    if dollar_rows:
        for row in dollar_rows:
            trade_date_value = row.get("trade_date")
            entry = {
                "tradeDate": _to_iso(trade_date_value),
                "open": _to_float(row.get("open_price")),
                "high": _to_float(row.get("high_price")),
                "low": _to_float(row.get("low_price")),
                "close": _to_float(row.get("close_price")),
                "amplitude": _to_float(row.get("amplitude")),
            }
            dollar_series.append(entry)
        latest_row = dollar_rows[0]
        prev_row = dollar_rows[1] if len(dollar_rows) > 1 else None
        close_price = _to_float(latest_row.get("close_price"))
        prev_close = _to_float(prev_row.get("close_price")) if prev_row else None
        change_amount = None
        change_percent = None
        if close_price is not None and prev_close not in (None, 0):
            change_amount = close_price - prev_close
            change_percent = (change_amount / prev_close) * 100.0
        trade_date_value = latest_row.get("trade_date")
        dollar_latest_iso = _to_iso(trade_date_value)
        dollar_summary = {
            "code": latest_row.get("code"),
            "name": latest_row.get("name"),
            "close": close_price,
            "changeAmount": change_amount,
            "changePercent": change_percent,
            "high": _to_float(latest_row.get("high_price")),
            "low": _to_float(latest_row.get("low_price")),
            "amplitude": _to_float(latest_row.get("amplitude")),
            "tradeDate": dollar_latest_iso,
        }
        if isinstance(trade_date_value, date):
            all_dates.append(trade_date_value)
            if date.today() - trade_date_value > timedelta(days=1):
                warnings.append("美元指数不是最新交易日数据")
    else:
        warnings.append("缺少美元指数数据")

    rmb_series: List[Dict[str, Any]] = []
    rmb_summary: Optional[Dict[str, Any]] = None
    rmb_latest_iso: Optional[str] = None
    if rmb_rows:
        for row in rmb_rows:
            trade_date_value = row.get("trade_date")
            rates: Dict[str, Dict[str, Optional[float]]] = {}
            for label, column in RMB_TARGET_CURRENCIES.items():
                quote = _to_float(row.get(column))
                if quote is not None:
                    rates[label] = {"quotePer100": quote}
            rmb_series.append(
                {
                    "tradeDate": _to_iso(trade_date_value),
                    "rates": rates,
                }
            )
        if rmb_series:
            rmb_summary = rmb_series[0]
            rmb_latest_iso = rmb_summary.get("tradeDate")
            trade_date_value = rmb_rows[0].get("trade_date")
            if isinstance(trade_date_value, date):
                all_dates.append(trade_date_value)
                if date.today() - trade_date_value > timedelta(days=1):
                    warnings.append("人民币中间价不是最新交易日数据")
            missing_labels = [
                label for label in RMB_TARGET_CURRENCIES if label not in (rmb_summary.get("rates") or {})
            ]
            for label in missing_labels:
                warnings.append(f"缺少人民币对{label}汇率")
    else:
        warnings.append("缺少人民币中间价数据")

    futures_summary: List[Dict[str, Any]] = []
    futures_latest_dt: Optional[datetime] = None
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
        quote_time_iso = _to_iso(quote_time)
        futures_summary.append(
            {
                "key": key,
                "name": spec["display_name"],
                "code": match.get("code"),
                "last": _to_float(match.get("last_price")),
                "changeAmount": _to_float(match.get("change_amount")),
                "changePercent": _to_float(match.get("change_percent")),
                "unit": spec.get("unit"),
                "quoteTime": quote_time_iso,
            }
        )
        candidate_dt: Optional[datetime] = None
        if isinstance(quote_time, datetime):
            candidate_dt = quote_time if quote_time.tzinfo else quote_time.replace(tzinfo=_UTC)
        elif isinstance(quote_time, date):
            candidate_dt = datetime.combine(quote_time, datetime.min.time(), tzinfo=_UTC)
        if candidate_dt:
            all_dates.append(candidate_dt.date())
            if futures_latest_dt is None or candidate_dt > futures_latest_dt:
                futures_latest_dt = candidate_dt
            if _now_utc() - candidate_dt > timedelta(hours=12):
                warnings.append(f"{spec['display_name']}报价可能过期（{quote_time_iso}）")

    fed_entries: List[Dict[str, Any]] = []
    fed_latest_date: Optional[date] = None
    for row in fed_rows:
        statement_date = row.get("statement_date")
        fed_entries.append(
            {
                "title": row.get("title"),
                "url": row.get("url"),
                "statementDate": _to_iso(statement_date),
                "updatedAt": _to_iso(row.get("updated_at")),
            }
        )
        if isinstance(statement_date, date):
            all_dates.append(statement_date)
            if fed_latest_date is None or statement_date > fed_latest_date:
                fed_latest_date = statement_date
    if not fed_entries:
        warnings.append("缺少美联储声明数据")

    snapshot_date = max(all_dates) if all_dates else date.today()

    generated_at_local = _now_local()

    metrics = {
        "generatedAt": generated_at_local.isoformat(),
        "globalIndices": global_indices,
        "globalIndicesHistory": global_history,
        "globalIndicesLatestTimestamp": global_indices_latest_iso,
        "dollarIndex": dollar_summary,
        "dollarIndexSeries": dollar_series,
        "dollarIndexLatestTimestamp": dollar_latest_iso,
        "rmbMidpoint": rmb_summary,
        "rmbMidpointSeries": rmb_series,
        "rmbMidpointLatestTimestamp": rmb_latest_iso,
        "commodities": futures_summary,
        "commoditiesLatestTimestamp": _to_iso(futures_latest_dt),
        "fedStatements": fed_entries,
        "fedStatementsLatestTimestamp": _to_iso(fed_latest_date),
        "warnings": warnings,
    }

    return PeripheralMetrics(
        snapshot_date=snapshot_date,
        generated_at=generated_at_local,
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
        deepseek_settings = settings.deepseek
        if deepseek_settings is None:
            logger.info("DeepSeek settings not configured; skipping LLM summary")
        else:
            prompt_payload = _build_prompt_sections(metrics_bundle.data, metrics_bundle.warnings)
            prompt_text = PERIPHERAL_PROMPT_TEMPLATE.replace("{news_content}", prompt_payload)

            def _invoke(response_format: Optional[dict]) -> Any:
                return generate_finance_analysis(
                    prompt_text,
                    settings=deepseek_settings,
                    prompt_template="{news_content}",
                    model_override=DEEPSEEK_REASONER_MODEL,
                    response_format=response_format,
                    temperature=0.2,
                    max_output_tokens=REASONER_MAX_OUTPUT_TOKENS,
                    return_usage=True,
                )

            logger.info("Generating peripheral insight via DeepSeek Reasoner")
            started = time.perf_counter()
            response = _invoke({"type": "json_object"})
            elapsed_ms = int((time.perf_counter() - started) * 1000)

            if not isinstance(response, dict):
                logger.info("Reasoner returned non-JSON payload, retrying without enforced format")
                started = time.perf_counter()
                response = _invoke(None)
                elapsed_ms = int((time.perf_counter() - started) * 1000)

            if not isinstance(response, dict):
                raise RuntimeError("DeepSeek reasoner did not return a valid response")

            content = response.get("content") or ""
            usage = response.get("usage") or {}
            model = response.get("model") or DEEPSEEK_REASONER_MODEL

            if content:
                summary = content
                raw_response = content
                try:
                    json.loads(content)
                except json.JSONDecodeError:
                    logger.warning("Peripheral insight response is not valid JSON; keeping raw content")
            logger.debug(
                "Peripheral insight reasoner usage: prompt=%s completion=%s total=%s elapsed_ms=%s",
                usage.get("prompt_tokens"),
                usage.get("completion_tokens"),
                usage.get("total_tokens"),
                elapsed_ms,
            )

    normalized_metrics = _normalize_metrics(metrics_bundle.data)

    PeripheralInsightDAO(settings.postgres).upsert_snapshot(
        snapshot_date=metrics_bundle.snapshot_date,
        generated_at=metrics_bundle.generated_at.replace(tzinfo=None),
        metrics=normalized_metrics,
        summary=summary,
        raw_response=raw_response,
        model=model,
    )

    return {
        "snapshot_date": metrics_bundle.snapshot_date,
        "generated_at": metrics_bundle.generated_at.isoformat(),
        "metrics": normalized_metrics,
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
        "generated_at": _to_local_iso(record.get("generated_at")),
        "metrics": record.get("metrics"),
        "summary": record.get("summary"),
        "raw_response": record.get("raw_response"),
        "model": record.get("model"),
        "created_at": _to_local_iso(record.get("created_at")),
        "updated_at": _to_local_iso(record.get("updated_at")),
    }


def list_peripheral_insight_history(
    *,
    limit: int = 20,
    settings_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    settings = load_settings(settings_path)
    dao = PeripheralInsightDAO(settings.postgres)
    rows = dao.list_snapshots(limit=limit)

    def _convert(record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "snapshot_date": record.get("snapshot_date"),
            "generated_at": _to_local_iso(record.get("generated_at")),
            "metrics": record.get("metrics"),
            "summary": record.get("summary"),
            "raw_response": record.get("raw_response"),
            "model": record.get("model"),
            "created_at": _to_local_iso(record.get("created_at")),
            "updated_at": _to_local_iso(record.get("updated_at")),
        }

    return [_convert(row) for row in rows]


__all__ = ["generate_peripheral_insight", "get_latest_peripheral_insight", "list_peripheral_insight_history"]
