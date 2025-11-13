"""Service building aggregated market overview payload and reasoning."""

from __future__ import annotations

import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from ..config.settings import load_settings
from ..api_clients import generate_finance_analysis
from ..dao import (
    HSGTFundFlowDAO,
    IndexHistoryDAO,
    MarginAccountDAO,
    MarketActivityDAO,
    MarketFundFlowDAO,
    MarketOverviewInsightDAO,
    NewsMarketInsightDAO,
    PeripheralInsightDAO,
    RealtimeIndexDAO,
)
from .macro_insight_service import get_latest_macro_insight

_INDEX_CODES = [
    "000001.SH",
    "399001.SZ",
    "399006.SZ",
    "588040.SH",
]

_LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def _serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_LOCAL_TZ)
        else:
            value = value.astimezone(_LOCAL_TZ)
        return value.isoformat()
    try:
        parsed = datetime.fromisoformat(str(value).replace(" ", "T"))
    except ValueError:
        return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_LOCAL_TZ)
    else:
        parsed = parsed.astimezone(_LOCAL_TZ)
    return parsed.isoformat()


def _serialise_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _serialize_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _serialise_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_serialise_value(item) for item in value]
    return value


def _fetch_latest_market_insight(settings) -> Optional[Dict[str, Any]]:
    dao = NewsMarketInsightDAO(settings.postgres)
    record = dao.latest_summary()
    if not record:
        return None

    def _ensure_local(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=_LOCAL_TZ)
        else:
            value = value.astimezone(_LOCAL_TZ)
        return value.isoformat()

    record["generated_at"] = _ensure_local(record.get("generated_at"))
    record["window_start"] = _ensure_local(record.get("window_start"))
    record["window_end"] = _ensure_local(record.get("window_end"))
    return record


def format_market_overview_sections(overview: Dict[str, Any]) -> Dict[str, str]:
    def _dump(value: Any) -> str:
        if value is None:
            return "None"
        return json.dumps(
            _serialise_value(value),
            ensure_ascii=False,
            indent=2,
            separators=(",", ": "),
        )

    return {
        "generated_at_section": _dump(overview.get("generatedAt")),
        "realtime_section": _dump(overview.get("realtimeIndices")),
        "index_history_section": _dump(overview.get("indexHistory")),
        "market_fund_flow_section": _dump(overview.get("marketFundFlow")),
        "margin_account_section": _dump(overview.get("marginAccount")),
        "market_insight_section": _dump(overview.get("marketInsight")),
        "macro_insight_section": _dump(overview.get("macroInsight")),
        "peripheral_section": _dump(overview.get("peripheralInsight")),
        "market_activity_section": _dump(overview.get("marketActivity")),
        "news_signals_section": "[]",
    }


def fill_market_overview_prompt(template: str, sections: Dict[str, str]) -> str:
    prompt = template
    for key, value in sections.items():
        prompt = prompt.replace(f"{{{key}}}", value)
    return prompt


def build_market_overview_payload(*, settings_path: Optional[str] = None) -> Dict[str, Any]:
    settings = load_settings(settings_path)

    realtime_dao = RealtimeIndexDAO(settings.postgres)
    history_dao = IndexHistoryDAO(settings.postgres)
    market_fund_flow_dao = MarketFundFlowDAO(settings.postgres)
    hsgt_dao = HSGTFundFlowDAO(settings.postgres)
    margin_dao = MarginAccountDAO(settings.postgres)
    peripheral_dao = PeripheralInsightDAO(settings.postgres)
    activity_dao = MarketActivityDAO(settings.postgres)

    realtime_rows = realtime_dao.list_entries(limit=500)["items"]
    realtime_filtered: List[Dict[str, Any]] = []
    for row in realtime_rows:
        if (row.get("turnover") or 0) <= 5e11:
            continue
        entry = dict(row)
        pct_value = entry.get("change_percent")
        if pct_value is not None:
            try:
                percent = float(pct_value)
            except (TypeError, ValueError):
                percent = None
            if percent is not None:
                entry["change_percent"] = percent / 100.0
            else:
                entry["change_percent"] = None
        realtime_filtered.append(entry)

    index_history: Dict[str, List[Dict[str, Any]]] = {}
    for code in _INDEX_CODES:
        history_rows = history_dao.list_history(index_code=code, limit=10)
        normalised_rows: List[Dict[str, Any]] = []
        for row in history_rows:
            entry = dict(row)
            pct_change = entry.get("pct_change")
            if pct_change is not None:
                try:
                    pct_value = float(pct_change)
                except (TypeError, ValueError):
                    pct_value = None
                if pct_value is not None:
                    entry["pct_change"] = pct_value / 100.0
            for numeric_key in ("open", "close", "high", "low", "volume", "amount", "change_amount", "turnover"):
                value = entry.get(numeric_key)
                if value is None:
                    continue
                try:
                    entry[numeric_key] = float(value)
                except (TypeError, ValueError):
                    pass
            normalised_rows.append(entry)
        index_history[code] = normalised_rows

    market_insight = _fetch_latest_market_insight(settings)
    if market_insight:
        market_insight.pop("referenced_articles", None)
        for key in ("generated_at", "window_start", "window_end"):
            if key in market_insight and market_insight[key] is not None:
                market_insight[key] = _serialize_datetime(market_insight[key])

    macro_insight = get_latest_macro_insight()
    if macro_insight:
        for key in ("generated_at", "updated_at", "created_at"):
            if macro_insight.get(key) is not None:
                macro_insight[key] = _serialize_datetime(macro_insight[key])

    market_fund_flow = market_fund_flow_dao.list_entries(limit=10).get("items", [])
    hsgt_flow = hsgt_dao.list_entries(symbol="北向资金", limit=10).get("items", [])
    margin_stats = margin_dao.list_entries(limit=10).get("items", [])

    peripheral = peripheral_dao.fetch_latest()
    if peripheral:
        metrics = peripheral.get("metrics")
        if isinstance(metrics, str):
            try:
                peripheral["metrics"] = json.loads(metrics)
            except json.JSONDecodeError:
                peripheral["metrics"] = metrics
        for key in ("generated_at", "created_at", "updated_at"):
            if peripheral.get(key) is not None:
                peripheral[key] = _serialize_datetime(peripheral[key])

    activity_rows = activity_dao.list_entries().get("items", [])

    insight = MarketOverviewInsightDAO(settings.postgres).fetch_latest()
    latest_reasoning = None
    if insight:
        latest_reasoning = {
            "summary": insight.get("summary_json"),
            "rawText": insight.get("raw_response"),
            "model": insight.get("model"),
            "generatedAt": _serialize_datetime(insight.get("generated_at")),
        }

    now_local = datetime.now(_LOCAL_TZ)

    payload = {
        "generatedAt": now_local.isoformat(),
        "realtimeIndices": realtime_filtered,
        "indexHistory": index_history,
        "marketInsight": market_insight,
        "macroInsight": macro_insight,
        "marketFundFlow": market_fund_flow,
        "hsgtFundFlow": hsgt_flow,
        "marginAccount": margin_stats,
        "peripheralInsight": peripheral,
        "marketActivity": activity_rows,
        "latestReasoning": latest_reasoning,
    }

    return _serialise_value(payload)


MARKET_OVERVIEW_PROMPT = """
你是一名严谨的A股市场策略首席分析师。你的任务是基于提供的多维度数据，先对每个数据源进行独立深度分析，然后基于所有分析结果进行综合推理，生成一份机构级的市场态势评估报告。

**【输入数据】**
系统会以 JSON 格式一次性提供所有输入数据：

1.  realtimeIndices: {realtime_section}
2.  indexHistory: {index_history_section}
3.  marketFundFlow: {market_fund_flow_section}
4.  marginAccount: {margin_account_section}
5.  marketInsight: {market_insight_section}
6.  macroInsight: {macro_insight_section}
7.  peripheralInsight: {peripheral_section}
8.  marketActivity: {market_activity_section}
9.  newsSignals: {news_signals_section}

**【分析流程 - 两步法】**

**第一步：数据源独立分析**
请对以下每个数据源进行独立的深度分析，每个分析不少于80字，需包含具体数据引用和初步结论：

1.  **指数态势分析**（基于realtimeIndices + indexHistory）
    - 实时强弱：对比各主要指数涨跌幅、量价关系
    - 趋势判断：基于20日历史数据识别趋势方向、关键位
    - 动量评估：近期K线组合和动能变化

2.  **资金流向分析**（基于marketFundFlow + marginAccount）
    - 主力行为：超大单/大单的持续性流向
    - 散户情绪：中小单与主力资金的博弈关系
    - 杠杆态度：融资盘的风险偏好变化

3.  **市场情绪分析**（基于marketActivity + marketInsight）
    - 赚钱效应：涨跌家数比、涨跌停对比
    - 投机热度：连板高度、炸板率等短线情绪指标
    - 情绪周期：当前处于情绪周期的哪个阶段

4.  **宏观环境分析**（基于macroInsight + peripheralInsight）
    - 内部环境：政策面、基本面因素
    - 外部影响：外围市场、汇率、地缘政治等
    - 风险偏好：整体市场的风险承受意愿

**第二步：综合推理与报告生成**
基于以上四个独立分析的结果，进行交叉验证和综合推理，生成完整的JSON格式报告。

{
  "intermediate_analysis": {
    "index_analysis": "第一步中的指数态势分析全文",
    "fund_flow_analysis": "第一步中的资金流向分析全文", 
    "sentiment_analysis": "第一步中的市场情绪分析全文",
    "macro_analysis": "第一步中的宏观环境分析全文"
  },
  "comprehensive_conclusion": {
    "bias": "bullish/neutral/bearish",
    "confidence": 0.00,
    "summary": "【综合综述】基于以上四个分析维度，进行深度交叉验证。重点说明：1) 各维度信号是否协同；2) 是否存在背离及原因解读；3) 当前市场核心矛盾；4) 多空力量对比分析。不少于250字。",
    "key_signals": [
      {
        "title": "信号标题",
        "detail": "具体描述",
        "supporting_analyses": ["支持该信号的分析维度，如index_analysis, fund_flow_analysis"]
      }
    ],
    "position_suggestion": {
      "short_term": "短线策略（3-5天），基于情绪分析和资金流向",
      "medium_term": "中线策略（2-4周），基于指数趋势和宏观环境", 
      "risk_control": "具体风控措施和关键观察点"
    },
    "scenario_analysis": [
      {
        "scenario": "乐观情景（概率XX%）",
        "conditions": "需要满足的条件（如：主力资金连续3日净流入）",
        "target": "对应走势和操作建议"
      },
      {
        "scenario": "基准情景（概率XX%）", 
        "conditions": "当前趋势的延续",
        "target": "对应走势和操作建议"
      },
      {
        "scenario": "悲观情景（概率XX%）",
        "conditions": "风险触发条件（如：外围市场大跌）",
        "target": "对应走势和操作建议"
      }
    ]
  },
  "generated_at": "{generated_at_section}"
}

**【分析要求】**
1. 每个独立分析必须基于具体数据，避免模糊表述
2. 综合推理时要明确指出各维度间的协同或背离关系
3. 概率分配要合理，总和为100%
4. 所有结论必须有明确的数据或分析支撑
5. 引用任何指数点位、涨跌幅、资金流、成交额等数值时，必须直接使用 JSON 中对应字段的原始数据，可适度四舍五入但不得凭空编造。如数据缺失必须明确说明“数据缺失”，不得沿用旧数据或经验值。
6. 若 realtimeIndices、indexHistory、marketFundFlow、marginAccount、marketActivity、macroInsight、peripheralInsight 中任一字段非空，视为“数据已提供”，你必须引用其中的真实数值进行分析，禁止出现“数据缺失”“暂无数据”等默认文案。只有在对应字段确实为空数组或所有值皆为 null 时，才能写明具体哪个字段“数据缺失”。
7. 最终 JSON 中的 summary、key_signals、position_suggestion、scenario_analysis 也必须复用上述真实数据，不能输出模板化、与数据脱节的内容。

现在请开始按照两步法进行分析。
"""


def generate_market_overview_reasoning(
    *,
    run_llm: bool = True,
    settings_path: Optional[str] = None,
) -> Dict[str, Any]:
    overview = build_market_overview_payload(settings_path=settings_path)
    settings = load_settings(settings_path)
    generated_at_local = datetime.now(_LOCAL_TZ)
    generated_at_db = generated_at_local.replace(tzinfo=None)

    summary_text: Optional[str] = None
    model_name: Optional[str] = None
    if run_llm:
        if settings.deepseek is None:
            run_llm = False
        else:
            model_name = "deepseek-reasoner"
            sections = format_market_overview_sections(overview)
            prompt = fill_market_overview_prompt(MARKET_OVERVIEW_PROMPT, sections)
            result = generate_finance_analysis(
                prompt,
                settings=settings.deepseek,
                prompt_template=MARKET_OVERVIEW_PROMPT,
                model_override="deepseek-reasoner",
                temperature=0.2,
                max_output_tokens=4096,
            )
            if isinstance(result, dict):
                summary_text = result.get("content")
                model_name = result.get("model") or model_name
            elif isinstance(result, str):
                summary_text = result

    default_summary = {
        "bias": "neutral",
        "confidence": 0,
        "summary": "暂无推理结果，待模型输出后再评估。",
        "key_signals": [],
        "position_suggestion": "暂无模型建议，请等待任务完成。",
        "risks": [],
    }

    summary_dict: Dict[str, Any]
    raw_payload: str
    if summary_text:
        raw_payload = summary_text
        try:
            parsed = json.loads(summary_text)
        except (TypeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, dict):
            summary_dict = parsed
        else:
            summary_dict = {**default_summary, "summary": str(summary_text)}
    else:
        summary_dict = default_summary
        raw_payload = json.dumps(default_summary, ensure_ascii=False)

    summary_dict.setdefault("bias", "neutral")
    summary_dict.setdefault("confidence", 0)
    summary_dict.setdefault("key_signals", [])
    summary_dict.setdefault("position_suggestion", "")
    summary_dict.setdefault("risks", [])

    serialised_summary = _serialise_value(summary_dict)

    MarketOverviewInsightDAO(settings.postgres).insert_snapshot(
        generated_at=generated_at_db,
        summary_json=serialised_summary,
        raw_response=raw_payload,
        model=model_name,
    )

    return {
        "overview": overview,
        "summary": serialised_summary,
        "rawText": raw_payload,
        "model": model_name,
        "generatedAt": generated_at_local.isoformat(),
    }


__all__ = [
    "build_market_overview_payload",
    "generate_market_overview_reasoning",
    "format_market_overview_sections",
    "fill_market_overview_prompt",
    "MARKET_OVERVIEW_PROMPT",
]
