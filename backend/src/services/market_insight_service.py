"""Services for generating aggregated market insight summaries."""

from __future__ import annotations

import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo
from psycopg2 import sql
from dataclasses import dataclass

from ..api_clients import generate_finance_analysis
from ..config.settings import load_settings
from ..dao import MarketInsightDAO, NewsArticleDAO, NewsInsightDAO
from .market_overview_service import build_market_overview_payload

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_ARTICLE_LIMIT = 40
DEEPSEEK_REASONER_MODEL = "deepseek-reasoner"
MAX_OUTPUT_TOKENS = 32000
MAX_CANDIDATE_LIMIT = 150
_CANDIDATE_MULTIPLIER = 3
MAX_STAGE_HEADLINES = 12
STAGE_TIMEOUT_SECONDS = 180
STAGE_STATUS_RUNNING = "running"
STAGE_STATUS_SUCCESS = "success"
STAGE_STATUS_FAILED = "failed"
STAGE_CACHE_MAX_AGE_SECONDS = 3600

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

_INDEX_FACT_CODES: Tuple[Tuple[str, str], ...] = (
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
)


@dataclass(frozen=True)
class StageDefinition:
    key: str
    title: str
    data_keys: Tuple[str, ...]
    prompt_template: str


STAGE_INDEX_PROMPT = """
你是一名A股指数策略分析师。以下JSON包含实时指数(realtimeIndices)与重点指数近20日历史(indexHistory)：
{stage_payload}

请仅依据上述数据输出结构化JSON，格式如下（严禁添加额外字段）：
{{
  "stage": "{stage_key}",
  "title": "{stage_title}",
  "analysis": "不少于150字，必须引用至少3个具体数据点（含点位/涨跌幅/成交额等），需要覆盖实时强弱、趋势判断、动量评估，并给出初步结论。",
  "highlights": ["要点1","要点2"],
  "bias": "bullish/neutral/bearish",
  "confidence": 0.0,
  "key_metrics": [
    {{"label": "成交额", "value": "5426亿元", "insight": "量能较前一交易日缩12%，显示观望"}},
    {{"label": "支撑阻力", "value": "3980/4050", "insight": "若跌破3980需警惕加速下行"}}
  ]
}}

规则：
1. 所有数值必须直接引用JSON中的原始数据，可四舍五入但不可凭空编造。
2. 若某字段确实为空，请明确写明“realtimeIndices 数据缺失”或“indexHistory 数据缺失”，而不是笼统的“数据缺失”。
3. bias 仅允许 bull/neutral/bearish 三种，confidence 0-1 之间的数字。
"""

STAGE_FUND_PROMPT = """
你是一名A股资金流向分析师。以下JSON包含 marketFundFlow 与 marginAccount 的最新记录：
{stage_payload}

输出结构化JSON，格式如下：
{{
  "stage": "{stage_key}",
  "title": "{stage_title}",
  "analysis": "不少于150字，必须引用主力/超大单/大单/中小单及融资数据，给出主力行为、散户情绪、杠杆态度结论。",
  "highlights": ["要点1","要点2"],
  "bias": "bullish/neutral/bearish",
  "confidence": 0.0,
  "key_metrics": [
    {{"label": "主力净流出", "value": "-671.9亿元", "insight": "连续5日流出，机构减仓"}},
    {{"label": "融资余额", "value": "24871亿元", "insight": "较前日+40亿元，杠杆偏稳"}}
  ]
}}

要求同样引用真实数据，若某字段为空需指明“marketFundFlow 数据缺失”等具体项。
"""

STAGE_SENTIMENT_PROMPT = """
你是一名A股市场情绪分析师。以下JSON包含 marketActivity 指标与上一期 marketInsight 摘要：
{stage_payload}

输出结构化JSON：
{{
  "stage": "{stage_key}",
  "title": "{stage_title}",
  "analysis": "不少于150字，综合赚钱效应、投机热度、情绪周期判断，需引用涨跌家数、涨跌停、活跃度等具体指标；若引用 marketInsight 摘要，请注明其中的关键结论。",
  "highlights": ["要点1","要点2"],
  "bias": "bullish/neutral/bearish",
  "confidence": 0.0,
  "key_metrics": [
    {{"label": "涨跌家数", "value": "2765 / 2220", "insight": "赚钱效应偏弱"}},
    {{"label": "涨停数量", "value": "61", "insight": "连板高度下降，投机热度降温"}}
  ]
}}

务必引用真实数值；若上述两类数据全部缺失，需逐项说明缺失来源。
"""

STAGE_MACRO_PROMPT = """
你是一名A股宏观策略分析师。以下JSON包含 macroInsight 与 peripheralInsight：
{stage_payload}

输出JSON：
{{
  "stage": "{stage_key}",
  "title": "{stage_title}",
  "analysis": "不少于150字，需涵盖内部政策/基本面、外部市场/汇率/地缘政治、整体风险偏好，并引用至少3个具体指标（如PMI、CPI、LPR、美元指数、外围指数涨跌等）。",
  "highlights": ["要点1","要点2"],
  "bias": "bullish/neutral/bearish",
  "confidence": 0.0,
  "key_metrics": [
    {{"label": "PMI制造业", "value": "49.4", "insight": "低于荣枯线，制造业收缩"}},
    {{"label": "美元指数", "value": "99.48 (-0.14%)", "insight": "美元回落利于风险偏好"}}
  ]
}}

若某类数据缺失请明确指出是哪一个字段缺失，禁止使用笼统模板。
"""

STAGE_NEWS_PROMPT = """
你是一名A股财经新闻研究员。以下JSON包含最近24小时筛选出的高权重宏观/大盘类新闻（marketHeadlines）：
{stage_payload}

请仅基于这些新闻输出结构化JSON：
{{
  "stage": "{stage_key}",
  "title": "{stage_title}",
  "analysis": "不少于180字，必须引用至少3条不同新闻中的关键信息（时间、来源、事件内容、影响对象或数值），并说明它们对大盘的合力影响或分歧。",
  "highlights": ["要点1","要点2","要点3"],
  "bias": "bullish/neutral/bearish",
  "confidence": 0.0,
  "key_metrics": [
    {{"label": "政策/事件", "value": "事件简称 + 时间", "insight": "对指数/行业的潜在影响"}},
    {{"label": "影响范围", "value": "受影响市场/板块", "insight": "与市场当前情绪的联动"}}
  ]
}}

要求：
1. 必须指明每个要点来自哪条新闻（引用标题或来源+时间），不得凭空杜撰。
2. 若新闻相互矛盾，需要解释矛盾点及更可信的方向。
3. 若筛选结果少于3条新闻，需逐条说明缺失原因（如“仅1条新闻符合条件”），但仍需输出结构化JSON。
"""

FINAL_SUMMARY_PROMPT = """
你是一名A股首席策略分析师。以下是五个维度（指数、资金、情绪、宏观、财经新闻）的阶段性分析结果（JSON 数组）：
{stage_results}

请基于这些结论（不得忽视其中的数字与判断）输出最终的策略推理，JSON格式如下：
{{
  "comprehensive_conclusion": {{
    "bias": "bullish/neutral/bearish",
    "confidence": 0.0,
    "summary": "不少于250字，明确五个维度之间的协同/背离、核心矛盾、多空力量对比，引用阶段结论中的关键数据（含新闻维度）。",
    "key_signals": [
      {{"title": "信号标题", "detail": "具体描述，包含数据", "supporting_analyses": ["index_analysis","fund_flow_analysis"]}}
    ],
    "position_suggestion": {{
      "short_term": "短线策略（3-5天）",
      "medium_term": "中线策略（2-4周）",
      "risk_control": "风控要点"
    }},
    "scenario_analysis": [
      {{"scenario": "乐观情景（概率XX%）", "conditions": "触发条件（引用数据）", "target": "对应策略"}},
      {{"scenario": "基准情景（概率XX%）", "conditions": "...", "target": "..."}},
      {{"scenario": "悲观情景（概率XX%）", "conditions": "...", "target": "..."}}
    ]
  }}
}}

要求：
1. 必须引用前述阶段分析中的关键结论和数值，禁止重新编造数据。
2. 概率合计需为100%。
3. 若某阶段分析明确指出数据缺失，才可在最终总结中引用“XXX数据缺失”描述，否则禁止出现模板化“数据不足”。
"""

STAGE_DEFINITIONS: Tuple[StageDefinition, ...] = (
    StageDefinition(
        key="index_analysis",
        title="指数态势分析",
        data_keys=("realtimeIndices", "indexHistory"),
        prompt_template=STAGE_INDEX_PROMPT,
    ),
    StageDefinition(
        key="fund_flow_analysis",
        title="资金流向分析",
        data_keys=("marketFundFlow", "marginAccount"),
        prompt_template=STAGE_FUND_PROMPT,
    ),
    StageDefinition(
        key="sentiment_analysis",
        title="市场情绪分析",
        data_keys=("marketActivity", "marketInsight"),
        prompt_template=STAGE_SENTIMENT_PROMPT,
    ),
    StageDefinition(
        key="macro_analysis",
        title="宏观环境分析",
        data_keys=("macroInsight", "peripheralInsight"),
        prompt_template=STAGE_MACRO_PROMPT,
    ),
    StageDefinition(
        key="news_analysis",
        title="财经新闻解读",
        data_keys=("marketHeadlines",),
        prompt_template=STAGE_NEWS_PROMPT,
    ),
)
STAGE_TITLE_MAP = {stage.key: stage.title for stage in STAGE_DEFINITIONS}
STAGE_ORDER = tuple(stage.key for stage in STAGE_DEFINITIONS)
STAGE_COLUMN_NAMES = {
    "index_analysis": "index_stage",
    "fund_flow_analysis": "fund_stage",
    "sentiment_analysis": "sentiment_stage",
    "macro_analysis": "macro_stage",
    "news_analysis": "news_stage",
}


def _extract_stage_payload(stage: StageDefinition, overview: Dict[str, object]) -> Dict[str, object]:
    return {key: overview.get(key) for key in stage.data_keys}


def _format_stage_prompt(stage: StageDefinition, payload: Dict[str, object]) -> str:
    return stage.prompt_template.format(
        stage_key=stage.key,
        stage_title=stage.title,
        stage_payload=json.dumps(payload, ensure_ascii=False, indent=2, separators=(",", ": ")),
    )


def _execute_stage_reasoning(
    stage: StageDefinition,
    stage_payload: Dict[str, object],
    overview_payload: Dict[str, object],
    *,
    settings,
) -> Tuple[Dict[str, object], str, Dict[str, int]]:
    stage_prompt = _format_stage_prompt(stage, stage_payload)
    parsed, raw_content, usage_map = _invoke_reasoner(
        stage_prompt,
        settings=settings,
        response_format={"type": "json_object"},
        temperature=0.15,
    )
    analysis_text = parsed.get("analysis") or ""
    if _looks_like_placeholder(analysis_text):
        if stage.key == "index_analysis":
            fallback = _format_index_fact_text(overview_payload)
            if fallback:
                analysis_text = fallback
        elif stage.key == "fund_flow_analysis":
            fallback = _format_fund_flow_fact_text(overview_payload)
            if fallback:
                analysis_text = fallback
    stage_result = {
        "stage": stage.key,
        "title": stage.title,
        "analysis": analysis_text,
        "highlights": parsed.get("highlights") or [],
        "bias": (parsed.get("bias") or "neutral").lower(),
        "confidence": parsed.get("confidence"),
        "key_metrics": parsed.get("key_metrics") or [],
    }
    return stage_result, raw_content, usage_map


def _format_final_prompt(stage_results: List[Dict[str, object]]) -> str:
    summary_payload = [
        {
            "stage": item.get("stage"),
            "title": item.get("title"),
            "bias": item.get("bias"),
            "confidence": item.get("confidence"),
            "highlights": item.get("highlights"),
            "key_metrics": item.get("key_metrics"),
            "analysis": item.get("analysis"),
        }
        for item in stage_results
    ]
    return FINAL_SUMMARY_PROMPT.format(
        stage_results=json.dumps(summary_payload, ensure_ascii=False, indent=2, separators=(",", ": "))
    )


def _invoke_reasoner(
    prompt: str,
    *,
    settings,
    response_format: Optional[Dict[str, str]] = None,
    temperature: float = 0.2,
) -> tuple[Dict[str, object], str, Dict[str, int]]:
    response = generate_finance_analysis(
        prompt,
        settings=settings,
        prompt_template=prompt,
        model_override=DEEPSEEK_REASONER_MODEL,
        response_format=response_format or {"type": "json_object"},
        max_output_tokens=MAX_OUTPUT_TOKENS,
        temperature=temperature,
        return_usage=True,
    )
    if not response or not isinstance(response, dict):
        raise RuntimeError("DeepSeek reasoner did not return a valid response")
    content = response.get("content") or ""
    usage = response.get("usage") or {}
    attempts = [content.strip()]
    if attempts[0] and not attempts[0].startswith("{"):
        candidate = "{\n" + attempts[0]
        if not attempts[0].rstrip().endswith("}"):
            candidate += "\n}"
        attempts.append(candidate)
    parsed: Optional[Dict[str, object]] = None
    for attempt in attempts:
        try:
            parsed_data = json.loads(attempt)
            if isinstance(parsed_data, dict):
                parsed = parsed_data
                break
        except json.JSONDecodeError:
            continue
    if parsed is None:
        raise RuntimeError("DeepSeek reasoner response is not valid JSON")
    return parsed, content, {
        "prompt_tokens": int(usage.get("prompt_tokens") or 0),
        "completion_tokens": int(usage.get("completion_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
    }


def _local_now() -> datetime:
    return datetime.now(LOCAL_TZ).replace(tzinfo=None)


def _coerce_float(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _looks_like_placeholder(text: Optional[str]) -> bool:
    if text is None:
        return True
    value = str(text).strip()
    if not value:
        return True
    markers = ["数据缺失", "数据不足", "数据不可用", "无数据", "未提供数据", "无法", "缺乏数据"]
    for marker in markers:
        if marker in value:
            return True
    return False


def _format_index_fact_text(overview_payload: Dict[str, object]) -> Optional[str]:
    history_map = overview_payload.get("indexHistory") or {}
    if not isinstance(history_map, dict):
        return None

    fragments: List[str] = []
    for code, label in _INDEX_FACT_CODES:
        rows = history_map.get(code) or []
        if not isinstance(rows, list) or not rows:
            continue
        latest = rows[0]
        close_value = _coerce_float(latest.get("close"))
        pct_change = _coerce_float(latest.get("pct_change"))
        trade_date = latest.get("trade_date")
        if close_value is None:
            continue
        text = f"{label}收于{close_value:.2f}点"
        if pct_change is not None:
            text += f"（{pct_change * 100:+.2f}%）"
        if trade_date:
            text += f"，数据日期{trade_date}"
        if len(rows) > 1:
            comparison = rows[-1]
            ref_close = _coerce_float(comparison.get("close"))
            if ref_close:
                cumulative = (close_value - ref_close) / ref_close * 100.0
                text += f"，近{len(rows)}个样本累计{cumulative:+.2f}%"
        fragments.append(text)

    if not fragments:
        return None
    return "；".join(fragments) + "。"


def _format_fund_flow_fact_text(overview_payload: Dict[str, object]) -> Optional[str]:
    flows = overview_payload.get("marketFundFlow") or []
    if not isinstance(flows, list) or not flows:
        return None
    latest = flows[0]
    trade_date = latest.get("trade_date")

    def _flow_sentence(label: str, key: str) -> Optional[str]:
        amount = _coerce_float(latest.get(key))
        if amount is None:
            return None
        direction = "净流入" if amount >= 0 else "净流出"
        return f"{label}{direction}{abs(amount) / 1e8:.1f}亿元"

    parts: List[str] = []
    for key, label in (
        ("main_net_inflow_amount", "主力资金"),
        ("huge_order_net_inflow_amount", "超大单"),
        ("large_order_net_inflow_amount", "大单"),
        ("medium_order_net_inflow_amount", "中单"),
        ("small_order_net_inflow_amount", "小单"),
    ):
        sentence = _flow_sentence(label, key)
        if sentence:
            parts.append(sentence)

    margin_section: Optional[str] = None
    margin_rows = overview_payload.get("marginAccount") or []
    if isinstance(margin_rows, list) and margin_rows:
        latest_margin = margin_rows[0]
        financing_balance = _coerce_float(latest_margin.get("financing_balance"))
        second_entry = margin_rows[1] if len(margin_rows) > 1 else None
        previous_balance = _coerce_float(second_entry.get("financing_balance")) if isinstance(second_entry, dict) else None
        financing_purchase = _coerce_float(latest_margin.get("financing_purchase_amount"))
        margin_bits: List[str] = []
        if financing_balance is not None:
            margin_text = f"融资余额约{financing_balance:,.0f}亿元"
            if previous_balance is not None:
                delta = financing_balance - previous_balance
                margin_text += f"（较前日{delta:+.1f}亿元）"
            margin_bits.append(margin_text)
        if financing_purchase is not None:
            margin_bits.append(f"当日融资买入额{financing_purchase:.1f}亿元")
        if margin_bits:
            margin_section = "两融数据：" + "，".join(margin_bits)

    intro = f"{trade_date}两市资金流：" if trade_date else "两市资金流："
    sentences = [intro + ("；".join(parts) if parts else "数据缺失")]
    if margin_section:
        sentences.append(margin_section)
    return " ".join(sentences)




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


def _normalise_article_timestamp(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=LOCAL_TZ)
        else:
            value = value.astimezone(LOCAL_TZ)
        return value.isoformat()
    try:
        parsed = datetime.fromisoformat(str(value).replace(" ", "T"))
    except ValueError:
        return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TZ)
    else:
        parsed = parsed.astimezone(LOCAL_TZ)
    return parsed.isoformat()


def _ensure_str_list(value: object) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if value not in (None, ""):
        return [str(value).strip()]
    return []


def _build_stage_headlines_payload(
    articles: List[Dict[str, object]],
    *,
    limit: int = MAX_STAGE_HEADLINES,
) -> List[Dict[str, object]]:
    payload: List[Dict[str, object]] = []
    for article in articles[:limit]:
        entry = {
            "article_id": article.get("article_id"),
            "source": article.get("source"),
            "title": article.get("title"),
            "summary": article.get("summary"),
            "impact_summary": article.get("impact_summary"),
            "impact_analysis": article.get("impact_analysis"),
            "impact_confidence": article.get("impact_confidence"),
            "macro_score": _coerce_float(article.get("macro_score")),
            "severity_score": _coerce_float(article.get("severity_score")),
            "impact_severity": article.get("impact_severity"),
            "event_type": article.get("event_type"),
            "subject_level": article.get("subject_level"),
            "macro_tags": _ensure_str_list(article.get("macro_tags")),
            "impact_markets": _ensure_str_list(article.get("impact_markets")),
            "impact_industries": _ensure_str_list(article.get("impact_industries")),
            "impact_themes": _ensure_str_list(article.get("impact_themes")),
            "impact_stocks": _ensure_str_list(article.get("impact_stocks")),
            "published_at": _normalise_article_timestamp(article.get("published_at")),
            "url": article.get("url"),
        }
        payload.append(entry)
    return payload


def _describe_stage_payload(payload: Dict[str, object]) -> str:
    if not payload:
        return "empty payload"
    summary: List[str] = []
    for key, value in payload.items():
        if isinstance(value, list):
            summary.append(f"{key}[{len(value)}]")
        elif isinstance(value, dict):
            summary.append(f"{key}{{{len(value)}}}")
        elif value is None:
            summary.append(f"{key}=None")
        else:
            summary.append(f"{key}({type(value).__name__})")
    return ", ".join(summary)

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
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> Dict[str, object]:
    def _notify(progress: float, message: str) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(max(0.0, min(progress, 1.0)), message)
        except Exception:  # pragma: no cover - defensive
            logger.debug("market insight progress callback failed", exc_info=True)

    settings = load_settings(settings_path)
    summary_dao = MarketInsightDAO(settings.postgres)

    articles: List[Dict[str, object]] = []
    article_error: Optional[BaseException] = None
    headlines_done = threading.Event()

    def _collect_headlines() -> None:
        start_ts = time.perf_counter()
        logger.info(
            "Collecting market-impact headlines (lookback=%sh, limit=%s)",
            lookback_hours,
            limit,
        )
        nonlocal articles, article_error
        try:
            articles = collect_recent_market_headlines(
                lookback_hours=lookback_hours,
                limit=limit,
                settings_path=settings_path,
            )
            logger.info(
                "Collected %s qualifying headlines in %.2fs",
                len(articles),
                time.perf_counter() - start_ts,
            )
        except BaseException as exc:  # pragma: no cover - defensive
            logger.exception("Collecting market-impact headlines failed")
            article_error = exc
        finally:
            headlines_done.set()

    collector_thread = threading.Thread(
        target=_collect_headlines,
        name="market_insight_headlines",
        daemon=True,
    )
    collector_thread.start()

    heartbeat_seconds = 0
    _notify(0.08, "开始筛选大盘相关新闻")
    heartbeat_progress_start = 0.08
    heartbeat_progress_end = 0.18
    while not headlines_done.wait(timeout=5):
        heartbeat_seconds += 5
        progress_delta = min(heartbeat_seconds / 60.0, 1.0)
        current_progress = heartbeat_progress_start + (heartbeat_progress_end - heartbeat_progress_start - 0.01) * progress_delta
        _notify(
            current_progress,
            f"正在筛选大盘相关新闻（耗时约 {heartbeat_seconds}s）",
        )

    collector_thread.join()
    if article_error:
        raise article_error

    _notify(heartbeat_progress_end, f"已筛选 {len(articles)} 条大盘相关新闻")

    window_end = _local_now()
    window_start = window_end - timedelta(hours=max(lookback_hours, 1))

    overview_payload = build_market_overview_payload(settings_path=settings_path)
    overview_payload["marketHeadlines"] = _build_stage_headlines_payload(articles)
    _notify(0.18, "完成市场概览与参考数据整理")

    deepseek_settings = settings.deepseek
    if deepseek_settings is None:
        raise RuntimeError("DeepSeek configuration is required for market insight generation")

    logger.info(
        "Generating staged market insight summary: headlines=%s lookback=%sh model=%s",
        len(articles),
        lookback_hours,
        DEEPSEEK_REASONER_MODEL,
    )

    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    stage_total = len(STAGE_DEFINITIONS)
    stage_weight = 0.55 / stage_total if stage_total else 0.0
    stage_base_progress = 0.2
    stage_success_results: List[Dict[str, object]] = []
    stage_raw_responses: Dict[str, str] = {}
    stage_records: Dict[str, Dict[str, object]] = {}
    stage_cached_results: Dict[str, Dict[str, object]] = {}
    reused_pending_summary = False
    summary_id: Optional[str] = None
    referenced_articles_payload = [
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
            "impact_severity": item.get("impact_severity"),
            "severity_score": item.get("severity_score"),
            "macro_score": item.get("macro_score"),
            "event_type": item.get("event_type"),
            "subject_level": item.get("subject_level"),
            "macro_tags": item.get("macro_tags"),
        }
        for item in articles
    ]

    pending_summary = summary_dao.latest_pending_summary()
    if pending_summary and not pending_summary.get("comprehensive_stage"):
        pending_generated_at = pending_summary.get("generated_at")
        if isinstance(pending_generated_at, datetime):
            age_seconds = abs((window_end - pending_generated_at).total_seconds())
            if age_seconds <= STAGE_CACHE_MAX_AGE_SECONDS:
                summary_id = pending_summary.get("summary_id")
                reused_pending_summary = True
                for stage in STAGE_DEFINITIONS:
                    column_name = STAGE_COLUMN_NAMES.get(stage.key)
                    cached_record = pending_summary.get(column_name) if column_name else None
                    if isinstance(cached_record, dict):
                        stage_records[stage.key] = dict(cached_record)
                        if (
                            cached_record.get("status") == STAGE_STATUS_SUCCESS
                            and cached_record.get("result")
                        ):
                            stage_cached_results[stage.key] = cached_record["result"]
                            usage_map = cached_record.get("usage")
                            if isinstance(usage_map, dict):
                                total_prompt_tokens += usage_map.get("prompt_tokens", 0)
                                total_completion_tokens += usage_map.get("completion_tokens", 0)
                                total_tokens += usage_map.get("total_tokens", 0)
    if summary_id is None:
        summary_id = uuid.uuid4().hex
        summary_dao.insert_summary(
            {
                "summary_id": summary_id,
                "generated_at": window_end,
                "window_start": window_start,
                "window_end": window_end,
                "headline_count": len(articles),
                "summary_json": None,
                "raw_response": None,
                "referenced_articles": json.dumps(referenced_articles_payload, ensure_ascii=False),
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "elapsed_ms": None,
                "model_used": DEEPSEEK_REASONER_MODEL,
            }
        )
    else:
        logger.info("Reusing pending summary %s with cached stage results", summary_id)
        summary_dao.update_columns(
            summary_id,
            generated_at=window_end,
            window_start=window_start,
            window_end=window_end,
            headline_count=len(articles),
        )
        existing_articles = pending_summary.get("referenced_articles") if pending_summary else None
        if isinstance(existing_articles, list):
            referenced_articles_payload = existing_articles

    def _accumulate_usage(usage_map: Dict[str, int]) -> None:
        nonlocal total_prompt_tokens, total_completion_tokens, total_tokens
        total_prompt_tokens += usage_map.get("prompt_tokens", 0)
        total_completion_tokens += usage_map.get("completion_tokens", 0)
        total_tokens += usage_map.get("total_tokens", 0)

    started = time.perf_counter()
    for index, stage in enumerate(STAGE_DEFINITIONS):
        stage_progress = stage_base_progress + stage_weight * index
        cached_result = stage_cached_results.get(stage.key)
        if cached_result:
            stage_success_results.append(cached_result)
            _notify(
                min(stage_progress + stage_weight * 0.85, 0.88),
                f"[{index + 1}/{stage_total}] 「{stage.title}」沿用现有推理结果",
            )
            logger.info("Stage '%s' reused cached result for summary %s", stage.key, summary_id)
            continue
        _notify(stage_progress, f"[{index + 1}/{stage_total}] 正在推理「{stage.title}」")
        stage_payload = _extract_stage_payload(stage, overview_payload)
        stage_started = time.perf_counter()
        logger.info(
            "Stage '%s' reasoning started with payload: %s",
            stage.key,
            _describe_stage_payload(stage_payload),
        )
        stage_record = {
            "stage": stage.key,
            "title": stage.title,
            "status": STAGE_STATUS_RUNNING,
            "started_at": _local_now().isoformat(),
        }
        stage_records[stage.key] = stage_record
        summary_dao.update_stage(summary_id, stage.key, stage_record)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    _execute_stage_reasoning,
                    stage,
                    stage_payload,
                    overview_payload,
                    settings=deepseek_settings,
                )
                stage_result, raw_content, usage_map = future.result(timeout=STAGE_TIMEOUT_SECONDS)
        except FuturesTimeoutError:
            error_message = f"{stage.title} 推理超时（>{STAGE_TIMEOUT_SECONDS}s）"
            logger.warning("Stage '%s' timed out after %ss", stage.key, STAGE_TIMEOUT_SECONDS)
            stage_record.update(
                {
                    "status": STAGE_STATUS_FAILED,
                    "finished_at": _local_now().isoformat(),
                    "error": error_message,
                }
            )
            summary_dao.update_stage(summary_id, stage.key, stage_record)
            _notify(stage_progress, error_message)
            continue
        except Exception as exc:
            logger.exception("Stage '%s' reasoning failed", stage.key)
            stage_record.update(
                {
                    "status": STAGE_STATUS_FAILED,
                    "finished_at": _local_now().isoformat(),
                    "error": str(exc),
                }
            )
            summary_dao.update_stage(summary_id, stage.key, stage_record)
            _notify(stage_progress, f"{stage.title} 失败：{exc}")
            continue

        _accumulate_usage(usage_map)
        stage_success_results.append(stage_result)
        stage_raw_responses[stage.key] = raw_content
        stage_record.update(
            {
                "status": STAGE_STATUS_SUCCESS,
                "finished_at": _local_now().isoformat(),
                "result": stage_result,
                "usage": usage_map,
            }
        )
        summary_dao.update_stage(summary_id, stage.key, stage_record)
        _notify(
            min(stage_progress + stage_weight * 0.85, 0.88),
            f"[{index + 1}/{stage_total}] 「{stage.title}」推理完成",
        )
        logger.info(
            "Stage '%s' reasoning finished in %.2fs (bias=%s, confidence=%s, prompt_tokens=%s, completion_tokens=%s)",
            stage.key,
            time.perf_counter() - stage_started,
            stage_result["bias"],
            stage_result["confidence"],
            usage_map.get("prompt_tokens"),
            usage_map.get("completion_tokens"),
        )

    if not stage_success_results:
        raise RuntimeError("All reasoning stages failed; unable to generate market insight summary")

    final_prompt = _format_final_prompt(stage_success_results)
    _notify(0.9, "整合阶段结论，生成综合推理")
    final_started = time.perf_counter()
    logger.info(
        "Comprehensive reasoning started with %s stage results",
        len(stage_success_results),
    )
    _notify(0.96, "写入推理结果")
    try:
        final_data, final_raw, final_usage = _invoke_reasoner(
            final_prompt,
            settings=deepseek_settings,
            response_format={"type": "json_object"},
            temperature=0.2,
        )
    except Exception as exc:
        summary_dao.update_comprehensive(
            summary_id,
            {
                "status": STAGE_STATUS_FAILED,
                "finished_at": _local_now().isoformat(),
                "error": str(exc),
            },
        )
        logger.exception("Comprehensive reasoning failed")
        raise
    logger.info(
        "Comprehensive reasoning finished in %.2fs (prompt_tokens=%s, completion_tokens=%s)",
        time.perf_counter() - final_started,
        final_usage.get("prompt_tokens"),
        final_usage.get("completion_tokens"),
    )
    _accumulate_usage(final_usage)

    elapsed_ms = int((time.perf_counter() - started) * 1000)

    comprehensive = final_data.get("comprehensive_conclusion")
    if not isinstance(comprehensive, dict):
        raise RuntimeError("Final strategy reasoning did not return comprehensive_conclusion")
    comprehensive["generated_at"] = window_end.isoformat()

    intermediate_analysis = {stage["stage"]: stage.get("analysis") for stage in stage_success_results}
    summary_json: Dict[str, object] = {
        "generated_at": window_end.isoformat(),
        "stage_results": stage_success_results,
        "intermediate_analysis": intermediate_analysis,
        "comprehensive_conclusion": comprehensive,
    }

    summary_dao.update_comprehensive(
        summary_id,
        {
            "status": "success",
            "finished_at": _local_now().isoformat(),
            "result": comprehensive,
        },
    )
    summary_dao.update_columns(
        summary_id,
        summary_json=json.dumps(summary_json, ensure_ascii=False),
        raw_response=json.dumps({"stages": stage_raw_responses, "final": final_raw}, ensure_ascii=False),
        prompt_tokens=total_prompt_tokens,
        completion_tokens=total_completion_tokens,
        total_tokens=total_tokens,
        elapsed_ms=elapsed_ms,
        model_used="deepseek-reasoner (multi-stage)",
    )

    logger.info(
        "Market insight summary stored (id=%s, stages=%s, headlines=%s, elapsed=%.2fs, total_tokens=%s)",
        summary_id,
        len(stage_success_results),
        len(articles),
        elapsed_ms / 1000.0,
        total_tokens,
    )

    return {
        "summary_id": summary_id,
        "generated_at": window_end,
        "window_start": window_start,
        "window_end": window_end,
        "headline_count": len(articles),
        "summary_json": summary_json,
        "referenced_articles": referenced_articles_payload,
        "stage_records": stage_records,
        "prompt_tokens": total_prompt_tokens,
        "completion_tokens": total_completion_tokens,
        "total_tokens": total_tokens,
        "elapsed_ms": elapsed_ms,
        "model_used": "deepseek-reasoner (multi-stage)",
    }


def get_latest_market_insight(*, settings_path: Optional[str] = None) -> Optional[Dict[str, object]]:
    settings = load_settings(settings_path)
    summary_dao = MarketInsightDAO(settings.postgres)
    record = summary_dao.latest_summary()
    if not record:
        return None

    def _ensure_local_iso(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=LOCAL_TZ)
        else:
            value = value.astimezone(LOCAL_TZ)
        return value.isoformat()

    record["generated_at"] = _ensure_local_iso(record.get("generated_at"))
    record["window_start"] = _ensure_local_iso(record.get("window_start"))
    record["window_end"] = _ensure_local_iso(record.get("window_end"))
    return record


def list_market_insights(*, limit: int = 10, settings_path: Optional[str] = None) -> List[Dict[str, object]]:
    settings = load_settings(settings_path)
    summary_dao = MarketInsightDAO(settings.postgres)
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
