"""Service for syncing and listing research reports from Sina Finance."""

from __future__ import annotations

import logging
import json
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd

from ..api_clients import fetch_sina_reports, fetch_sina_report_detail, generate_finance_analysis
from ..config.settings import load_settings
from ..dao import ResearchReportDAO, ResearchReportSummaryDAO

logger = logging.getLogger(__name__)

SNIPPET_CHAR_LIMIT = 1600
MAX_DISTILLATION_REPORTS = 10
DISTILLATION_LOOKBACK_MONTHS = 3
REPORT_SYNC_LOOKBACK_DAYS = 90

DISTILLATION_PROMPT_TEMPLATE = """
# 角色
你是一位专业的金融分析师，擅长从券商研报中提取核心信息并进行结构化整理。

# 任务
请仔细阅读输入的个股研报内容，提取关键信息并输出结构化的JSON格式数据。

# 输出要求
输出必须是严格的JSON格式，包含以下字段：

{
  "report_metadata": {
    "stock_name": "股票名称",
    "issuer": "券商/发布机构",
    "report_date": "研报发布日期(YYYY-MM-DD)",
    "report_title": "研报标题",
    "report_type": "研报类型(首次覆盖/跟踪报告/深度报告等)",
    "rating": "投资评级(买入/增持/中性/减持/卖出)",
    "target_price": "目标价格(数字)"
  },
  "core_analysis": {
    "investment_thesis": "核心投资逻辑(3-5个要点)",
    "key_drivers": ["业绩驱动因素1", "业绩驱动因素2", "业绩驱动因素3"],
    "risk_factors": ["主要风险因素1", "主要风险因素2", "主要风险因素3"],
    "catalysts": ["近期催化剂1", "近期催化剂2"]
  },
  "financial_data": {
    "revenue_forecast": {
      "current_year": "当年营收预测(亿元)",
      "next_year": "次年营收预测(亿元)",
      "growth_rate": "增长率"
    },
    "profit_forecast": {
      "current_year": "当年净利润预测(亿元)",
      "next_year": "次年净利润预测(亿元)",
      "growth_rate": "增长率"
    },
    "valuation_metrics": {
      "pe_ratio": "市盈率",
      "pb_ratio": "市净率",
      "ps_ratio": "市销率"
    }
  },
  "business_analysis": {
    "main_business": "主营业务描述",
    "competitive_advantage": "核心竞争力",
    "industry_position": "行业地位",
    "growth_prospects": "成长前景"
  },
  "key_highlights": ["核心亮点1", "核心亮点2", "核心亮点3", "核心亮点4", "核心亮点5"]
}

# 处理规则
1. 只提取研报中明确提到的信息，不要自行推断
2. 如果某些信息在研报中未提及，对应字段设为null
3. 投资逻辑和要点要用简洁的完整句子表达
4. 数字信息要确保准确性
5. 保持客观中立，不添加个人观点

# 输入
以下为研报内容：
{news_content}
""".strip()

SUMMARY_PROMPT_TEMPLATE = """
# 角色
你是一位资深金融研究主管，需要综合分析多份个股研报，形成统一的投资观点。

# 任务
请基于输入的10份最新个股研报（已结构化的JSON数据），在充分吸收各家机构关于产品矩阵、市场份额、估值区间、财务预测、竞争优势与风险提示的关键信息后，给出统一的投资结论。

# 输入数据
{input_reports}

# 输出要求
务必输出严格的 JSON，字段齐全且与示例完全一致。若资料缺失，用 null 或空数组代替，不要删减字段：

{
  "consensus_analysis": {
    "rating_consensus": {
      "most_common_rating": "最集中的评级",
      "rating_distribution": {
        "买入": 数量,
        "增持": 数量,
        "中性": 数量,
        "减持": 数量,
        "卖出": 数量
      },
      "rating_trend": "评级变化趋势(改善/恶化/稳定)"
    },
    "price_consensus": {
      "avg_target_price": "平均目标价",
      "highest_target": "最高目标价",
      "lowest_target": "最低目标价",
      "upside_potential": "平均上涨空间百分比",
      "price_range": "目标价区间"
    }
  },
  "product_market_analysis": {
    "product_portfolio": {
      "core_products": ["核心产品1", "核心产品2", "核心产品3"],
      "product_lifecycle": "产品生命周期阶段(导入/成长/成熟/衰退)",
      "product_pipeline": "产品管线丰富程度(丰富/一般/薄弱)"
    },
    "market_analysis": {
      "target_markets": ["主要目标市场1", "目标市场2"],
      "market_share": "市场份额及趋势",
      "market_growth": "目标市场增长率",
      "customer_segments": "主要客户群体"
    },
    "competitive_landscape": {
      "competitive_position": "竞争地位(领导者/挑战者/追随者)",
      "key_competitors": ["主要竞争对手1", "竞争对手2"],
      "barriers_to_entry": "进入壁垒程度(高/中/低)"
    }
  },
  "opportunity_analysis": {
    "growth_opportunities": [
      {
        "opportunity": "增长机会描述",
        "potential_impact": "潜在影响(高/中/低)",
        "timeframe": "时间框架(短期/中期/长期)"
      }
    ],
    "market_expansion": {
      "geographic_opportunities": "地域扩张机会",
      "product_expansion": "产品线扩展机会",
      "new_applications": "新应用领域机会"
    },
    "industry_tailwinds": ["行业顺风因素1", "因素2", "因素3"],
    "addressable_market": "可触达市场空间(TAM)及增长预期"
  },
  "valuation_analysis": {
    "relative_valuation": {
      "pe_percentile": "市盈率历史分位数",
      "pb_percentile": "市净率历史分位数",
      "industry_comparison": "行业相对估值(高估/合理/低估)"
    },
    "absolute_valuation": {
      "dcf_range": "DCF估值区间",
      "nav_valuation": "净资产价值评估",
      "sum_of_parts": "分部加总估值"
    },
    "valuation_attractiveness": {
      "current_level": "当前估值水平(极度低估/低估/合理/高估/极度高估)",
      "historical_comparison": "与历史估值比较",
      "margin_of_safety": "安全边际程度(高/中/低)"
    }
  },
  "fundamental_synthesis": {
    "growth_consensus": {
      "revenue_growth_forecast": "营收增长预期共识",
      "profit_growth_forecast": "利润增长预期共识",
      "growth_drivers": ["共同看好的增长驱动因素1", "增长驱动因素2", "增长驱动因素3"]
    },
    "competitive_position": {
      "core_advantages": ["公认的核心竞争力1", "核心竞争力2", "核心竞争力3"],
      "industry_trends": "行业趋势共识",
      "market_position": "行业地位评价"
    }
  },
  "risk_assessment": {
    "common_risks": ["共同关注的风险因素1", "风险因素2", "风险因素3", "风险因素4", "风险因素5"],
    "risk_severity": "整体风险程度(高/中/低)",
    "risk_trend": "风险变化趋势(上升/下降/平稳)"
  },
  "investment_conclusion": {
    "overall_rating": "综合投资评级(强烈推荐/推荐/中性/谨慎/回避)",
    "investment_thesis": "综合投资逻辑(3-5个核心要点)",
    "time_horizon": "投资期限建议(短期/中期/长期)",
    "key_catalysts": ["重要催化剂1(按重要性排序)", "催化剂2", "催化剂3"],
    "position_suggestion": "仓位建议(重仓/标配/低配/观望)",
    "entry_timing": "建议入场时机(立即买入/回调买入/观望等待)"
  },
  "divergence_analysis": {
    "major_disagreements": ["主要分歧点1", "分歧点2", "分歧点3"],
    "bull_case": "乐观情景下的投资逻辑",
    "bear_case": "悲观情景下的投资逻辑",
    "base_case": "基准情景下的投资逻辑"
  },
  "data_quality": {
    "report_coverage": "研报覆盖时间范围",
    "analyst_consistency": "分析师观点一致性(高/中/低)",
    "confidence_level": "结论置信度(高/中/低)"
  }
}

# 分析规则
1. 产品与市场分析：评估产品组合、客户/市场结构、竞争格局。
2. 机会分析：识别增长/扩张机会及行业顺风因素。
3. 估值分析：结合相对和绝对估值，明确当前估值吸引力与安全边际。
4. 共识识别：引用研报中的具体数据（例如目标价、增长率、销量等）说明共识。
5. 分歧识别：指出关键假设差异及对结论影响。
6. 趋势判断：阐述评级、目标价、基本面指标的变化方向。
7. 风险评估：评估概率和影响程度，并结合行业/宏观背景。
8. 置信度评估：说明结论的可靠性和未知因素。

# 推理要求
- 仅依据研报数据与结论，不主观臆断；
- 强调共识与变化趋势，识别乐观/悲观偏差；
- 给出可执行的投资建议并标注不确定性范围；
- 严格输出 JSON，不要附加自然语言。

请开始综合分析并输出 JSON 结果。
""".strip()


def _parse_distillation_payload(raw: Optional[object]) -> Optional[dict]:
    if not raw:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Failed to decode stored distillation payload: %s", raw[:120])
            return None
    return None


def _normalize_ts_code(value: str) -> str:
    token = (value or "").strip().upper()
    if not token:
        raise ValueError("ts_code is required")
    token = token.replace("-", "")
    if "." in token:
        left, right = token.split(".", 1)
        token = f"{right}{left}"
    if token[:2] in {"SH", "SZ", "BJ"} and token[2:].isdigit():
        return f"{token[:2]}{token[2:].zfill(6)}"
    digits = "".join(ch for ch in token if ch.isdigit())
    if len(digits) < 6:
        raise ValueError("Invalid ts_code format")
    digits = digits[-6:]
    first = digits[0]
    if digits.startswith(("43", "83", "87")) or first in {"4", "8"}:
        prefix = "BJ"
    elif first in {"6", "9", "5"}:
        prefix = "SH"
    else:
        prefix = "SZ"
    return f"{prefix}{digits}"


def _derive_symbol(symbol: Optional[str], fallback_ts: str) -> str:
    token = (symbol or "").strip().lower()
    if token.startswith(("sh", "sz", "bj")) and len(token) >= 8:
        prefix = token[:2]
        digits = "".join(ch for ch in token[2:] if ch.isdigit()).zfill(6)
        return prefix + digits
    prefix = fallback_ts[:2].lower()
    digits = fallback_ts[2:]
    return prefix + digits


def sync_research_reports(
    *,
    ts_code: str,
    symbol: Optional[str] = None,
    lookback_years: int = 1,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = ResearchReportDAO(settings.postgres)

    normalized_ts = _normalize_ts_code(ts_code or symbol or "")
    query_symbol = _derive_symbol(symbol, normalized_ts)

    list_items = fetch_sina_reports(query_symbol)
    if not list_items:
        return {"rows": 0, "reports": []}

    requested_days = REPORT_SYNC_LOOKBACK_DAYS
    if lookback_years > 0:
        requested_days = max(1, int(lookback_years * 365))
    window_days = min(requested_days, REPORT_SYNC_LOOKBACK_DAYS)
    cutoff_date = (datetime.utcnow() - timedelta(days=window_days)).date()
    latest_publish_date = dao.latest_publish_date(normalized_ts)
    if isinstance(latest_publish_date, datetime):
        latest_publish_date = latest_publish_date.date()

    existing_ids = set(dao.existing_report_ids(normalized_ts))
    new_rows: List[dict] = []
    for item in list_items:
        detail_url = item.get("detail_url")
        if not detail_url:
            continue
        report_id = detail_url.rsplit("/rptid/", 1)[-1].split("/", 1)[0]
        if report_id in existing_ids:
            continue
        publish_date_str = item.get("publish_date")
        publish_date = None
        if publish_date_str:
            try:
                publish_date = datetime.strptime(publish_date_str, "%Y-%m-%d").date()
            except ValueError:
                publish_date = None
        if publish_date is None:
            continue
        if publish_date < cutoff_date:
            break
        if latest_publish_date and publish_date < latest_publish_date:
            break
        try:
            detail = fetch_sina_report_detail(detail_url)
        except Exception as exc:  # pragma: no cover - external dependency
            logger.warning("Failed to fetch report detail %s: %s", detail_url, exc)
            continue
        new_rows.append(
            {
                "ts_code": normalized_ts,
                "symbol": query_symbol,
                "report_id": report_id,
                "title": detail.get("title") or item.get("title") or "",
                "report_type": item.get("report_type"),
                "publish_date": publish_date,
                "org": detail.get("org") or item.get("org"),
                "analysts": detail.get("analysts") or item.get("analysts"),
                "detail_url": detail_url,
                "content_html": detail.get("content_html"),
                "content_text": detail.get("content_text"),
            }
        )

    if not new_rows:
        return {"rows": 0, "reports": []}

    frame = pd.DataFrame(new_rows)
    inserted = dao.upsert(frame)
    return {"rows": inserted, "reports": new_rows[:5]}


def list_research_reports(
    *,
    ts_code: str,
    limit: int = 20,
    offset: int = 0,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = ResearchReportDAO(settings.postgres)
    normalized = _normalize_ts_code(ts_code)
    return dao.list_reports(ts_code=normalized, limit=limit, offset=offset)


def analyze_research_reports(
    *,
    ts_code: str,
    months: int = 3,
    max_reports: int = MAX_DISTILLATION_REPORTS,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    if not settings.deepseek:
        raise RuntimeError("DeepSeek settings missing; cannot run research analysis")
    normalized = _normalize_ts_code(ts_code)
    cutoff_months = max(1, min(months, DISTILLATION_LOOKBACK_MONTHS))
    reports_limit = max(1, min(max_reports, MAX_DISTILLATION_REPORTS))
    start_date = datetime.utcnow() - timedelta(days=cutoff_months * 30)
    dao = ResearchReportDAO(settings.postgres)
    summary_dao = ResearchReportSummaryDAO(settings.postgres)
    rows = dao.fetch_reports_for_distillation(
        ts_code=normalized,
        start_date=start_date,
        limit=reports_limit,
    )
    if not rows:
        return {"total": 0, "processed": 0, "items": []}

    processed_items: List[dict] = []
    processed_count = 0
    distillation_payloads: List[dict] = []
    for row in rows:
        existing = _parse_distillation_payload(row.get("distillation"))
        content = (row.get("content_text") or "").strip()
        if not existing and not content:
            continue
        snippet = content[:SNIPPET_CHAR_LIMIT] if content else ""
        publish_date = row.get("publish_date")
        news_input_parts = [
            row.get("title") or "",
            row.get("org") or "",
            publish_date.isoformat() if publish_date else "",
            snippet,
        ]
        news_input = "\n".join(part for part in news_input_parts if part).strip()
        if not existing and not news_input:
            continue
        parsed = existing
        ran_llm = False
        model_used = row.get("distillation_model")
        if not parsed:
            llm_result = generate_finance_analysis(
                news_input,
                settings=settings.deepseek,
                prompt_template=DISTILLATION_PROMPT_TEMPLATE,
                max_output_tokens=1600,
                return_usage=True,
            )
            if not llm_result:
                logger.warning("Skipping distillation for report %s: empty response", row.get("report_id"))
                continue
            content_text = llm_result.get("content") if isinstance(llm_result, dict) else None
            model_used = llm_result.get("model") if isinstance(llm_result, dict) else None
            if not content_text:
                logger.warning("Distillation result missing content for report %s", row.get("report_id"))
                continue
            try:
                parsed = json.loads(content_text)
            except json.JSONDecodeError:
                logger.warning("Distillation returned non-JSON content for %s: %s", row.get("report_id"), content_text)
                continue
            dao.save_distillation(
                report_id=row.get("report_id"),
                payload=parsed,
                model=model_used,
            )
            ran_llm = True
        if not parsed:
            continue
        metadata = parsed.get("report_metadata") if isinstance(parsed, dict) else {}
        distillation_payloads.append(parsed)
        processed_items.append(
            {
                "reportId": row.get("report_id"),
                "title": row.get("title"),
                "publishDate": publish_date.isoformat() if publish_date else None,
                "org": row.get("org"),
                "reportType": metadata.get("report_type") or row.get("report_type"),
                "rating": metadata.get("rating"),
                "targetPrice": metadata.get("target_price"),
                "detailUrl": row.get("detail_url"),
                "model": model_used,
                "distillation": parsed,
            }
        )
        if ran_llm:
            processed_count += 1

    summary_payload = None
    if distillation_payloads:
        summary_payload = _run_research_summary(distillation_payloads, settings)
        if summary_payload:
            stored_summary = dict(summary_payload)
            meta_model = stored_summary.pop("_model", None)
            stored_summary.pop("_raw", None)
            summary_dao.upsert_summary(ts_code=normalized, payload=stored_summary, model=meta_model)
            stored_summary["_model"] = meta_model
            stored_summary["_raw"] = summary_payload.get("_raw")
            summary_payload = stored_summary

    return {
        "total": len(rows),
        "processed": processed_count,
        "items": processed_items,
        "summary": summary_payload,
    }


def _run_research_summary(distillations: List[dict], settings) -> Optional[dict]:
    structured = [item for item in distillations if isinstance(item, dict)]
    if not structured:
        return None
    serialized = json.dumps(structured, ensure_ascii=False)
    prompt_template = SUMMARY_PROMPT_TEMPLATE.replace("{input_reports}", "{news_content}")
    result = generate_finance_analysis(
        serialized,
        settings=settings.deepseek,
        prompt_template=prompt_template,
        max_output_tokens=1800,
        return_usage=True,
    )
    if not result:
        return None
    content = result.get("content") if isinstance(result, dict) else None
    if not content:
        return None
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        logger.warning("Failed to parse summary JSON: %s", content)
        return None
    if isinstance(result, dict) and "model" in result:
        parsed["_model"] = result.get("model")
    parsed["_raw"] = content
    return parsed


def list_research_report_distillation(
    *,
    ts_code: str,
    months: int = 3,
    limit: int = MAX_DISTILLATION_REPORTS,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    normalized = _normalize_ts_code(ts_code)
    cutoff_months = max(1, min(months, DISTILLATION_LOOKBACK_MONTHS))
    reports_limit = max(1, min(limit, MAX_DISTILLATION_REPORTS))
    start_date = datetime.utcnow() - timedelta(days=cutoff_months * 30)
    dao = ResearchReportDAO(settings.postgres)
    rows = dao.list_distilled_reports(
        ts_code=normalized,
        start_date=start_date,
        limit=reports_limit,
    )
    summary_dao = ResearchReportSummaryDAO(settings.postgres)
    summary_payload = summary_dao.get_summary(normalized)
    items: List[dict] = []
    for row in rows:
        parsed = _parse_distillation_payload(row.get("distillation"))
        metadata = parsed.get("report_metadata") if isinstance(parsed, dict) else {}
        publish_date = row.get("publish_date")
        items.append(
            {
                "reportId": row.get("report_id"),
                "title": row.get("title"),
                "publishDate": publish_date.isoformat() if publish_date else None,
                "org": row.get("org"),
                "reportType": metadata.get("report_type") or row.get("report_type"),
                "rating": metadata.get("rating"),
                "targetPrice": metadata.get("target_price"),
                "detailUrl": row.get("detail_url"),
                "model": row.get("distillation_model"),
                "distillation": parsed,
            }
        )
    return {"total": len(rows), "processed": 0, "items": items, "summary": summary_payload}


__all__ = [
    "sync_research_reports",
    "list_research_reports",
    "analyze_research_reports",
    "list_research_report_distillation",
]
