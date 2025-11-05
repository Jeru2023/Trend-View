"""Aggregate industry and concept fund flow ranks into weighted hotlists."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from zoneinfo import ZoneInfo

from ..config.settings import load_settings
from ..dao import ConceptFundFlowDAO, IndustryFundFlowDAO

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

DEFAULT_SYMBOL_WEIGHTS: Dict[str, float] = {
    "即时": 1.0,
    "3日排行": 0.7,
    "5日排行": 0.5,
    "10日排行": 0.35,
    "20日排行": 0.25,
}
DEFAULT_SYMBOL_ORDER: Tuple[str, ...] = tuple(DEFAULT_SYMBOL_WEIGHTS.keys())

INDUSTRY_TOP_LIMIT = 5
CONCEPT_TOP_LIMIT = 10
CANDIDATE_LIMIT = 40


@dataclass
class StageRecord:
    symbol: str
    weight: float
    rank: Optional[int]
    net_amount: Optional[float]
    inflow: Optional[float]
    outflow: Optional[float]
    price_change_percent: Optional[float]
    stage_change_percent: Optional[float]
    index_value: Optional[float]
    current_price: Optional[float]
    company_count: Optional[int]
    leading_stock: Optional[str]
    leading_stock_change_percent: Optional[float]
    updated_at: Optional[str]


@dataclass
class HotlistEntry:
    name: str
    score: float = 0.0
    best_rank: Optional[int] = None
    best_symbol: Optional[str] = None
    stages: List[StageRecord] = field(default_factory=list)
    total_net_amount: float = 0.0
    total_inflow: float = 0.0
    total_outflow: float = 0.0


def _to_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=LOCAL_TZ)
    else:
        value = value.astimezone(LOCAL_TZ)
    return value.isoformat()


def _compute_stage_score(rank: Optional[int], max_rank: int, weight: float) -> float:
    if not rank or rank <= 0 or max_rank <= 0:
        return 0.0
    normalized = (max_rank + 1 - rank) / max_rank
    return max(0.0, normalized) * max(weight, 0.0)


def _collect_hotlist(
    *,
    dao_factory,
    name_field: str,
    index_field: str,
    symbols: Sequence[str],
    weights: Dict[str, float],
    top_limit: int,
) -> List[HotlistEntry]:
    aggregator: Dict[str, HotlistEntry] = {}

    for symbol in symbols:
        weight = weights.get(symbol, 0.0)
        if weight <= 0:
            continue

        dao = dao_factory()
        entries = dao.list_entries(symbol=symbol, limit=CANDIDATE_LIMIT, offset=0).get("items", [])

        ranked_entries = [item for item in entries if item.get("rank")]
        if not ranked_entries:
            continue

        max_rank = max(item.get("rank") or 0 for item in ranked_entries)
        if max_rank <= 0:
            max_rank = len(ranked_entries)

        for item in ranked_entries:
            name = item.get(name_field)
            if not name:
                continue
            entry = aggregator.setdefault(name, HotlistEntry(name=name))

            rank = item.get("rank")
            score_delta = _compute_stage_score(rank, max_rank, weight)
            entry.score += score_delta

            if rank and (entry.best_rank is None or rank < entry.best_rank):
                entry.best_rank = rank
                entry.best_symbol = symbol

            net_amount = item.get("net_amount") or 0.0
            if net_amount:
                entry.total_net_amount += float(net_amount)

            inflow = item.get("inflow") or 0.0
            if inflow:
                entry.total_inflow += float(inflow)

            outflow = item.get("outflow") or 0.0
            if outflow:
                entry.total_outflow += float(outflow)

            stage = StageRecord(
                symbol=symbol,
                weight=weight,
                rank=rank,
                net_amount=item.get("net_amount"),
                inflow=item.get("inflow"),
                outflow=item.get("outflow"),
                price_change_percent=item.get("price_change_percent"),
                stage_change_percent=item.get("stage_change_percent"),
                index_value=item.get(index_field),
                current_price=item.get("current_price"),
                company_count=item.get("company_count"),
                leading_stock=item.get("leading_stock"),
                leading_stock_change_percent=item.get("leading_stock_change_percent"),
                updated_at=_to_iso(item.get("updated_at")),
            )
            entry.stages.append(stage)

    ordered: List[HotlistEntry] = sorted(
        aggregator.values(),
        key=lambda item: (item.score, item.total_net_amount),
        reverse=True,
    )

    for entry in ordered:
        entry.stages.sort(key=lambda record: (-record.weight, record.symbol))

    return ordered[:top_limit]


def build_sector_fund_flow_snapshot(
    *,
    symbols: Optional[Sequence[str]] = None,
    symbol_weights: Optional[Dict[str, float]] = None,
    industry_limit: int = INDUSTRY_TOP_LIMIT,
    concept_limit: int = CONCEPT_TOP_LIMIT,
    settings_path: Optional[str] = None,
) -> Dict[str, object]:
    symbols = tuple(symbols) if symbols else DEFAULT_SYMBOL_ORDER
    weights = dict(DEFAULT_SYMBOL_WEIGHTS)
    if symbol_weights:
        weights.update({str(key): float(value) for key, value in symbol_weights.items()})

    settings = load_settings(settings_path)

    def _industry_dao():
        return IndustryFundFlowDAO(settings.postgres)

    def _concept_dao():
        return ConceptFundFlowDAO(settings.postgres)

    industries = _collect_hotlist(
        dao_factory=_industry_dao,
        name_field="industry",
        index_field="industry_index",
        symbols=symbols,
        weights=weights,
        top_limit=max(1, industry_limit),
    )
    concepts = _collect_hotlist(
        dao_factory=_concept_dao,
        name_field="concept",
        index_field="concept_index",
        symbols=symbols,
        weights=weights,
        top_limit=max(1, concept_limit),
    )

    generated_at = _to_iso(datetime.now(LOCAL_TZ))

    def _serialize(entry: HotlistEntry) -> Dict[str, object]:
        return {
            "name": entry.name,
            "score": round(entry.score, 6),
            "bestRank": entry.best_rank,
            "bestSymbol": entry.best_symbol,
            "totalNetAmount": round(entry.total_net_amount, 2),
            "totalInflow": round(entry.total_inflow, 2),
            "totalOutflow": round(entry.total_outflow, 2),
            "stages": [
                {
                    "symbol": stage.symbol,
                    "weight": stage.weight,
                    "rank": stage.rank,
                    "netAmount": stage.net_amount,
                    "inflow": stage.inflow,
                    "outflow": stage.outflow,
                    "priceChangePercent": stage.price_change_percent,
                    "stageChangePercent": stage.stage_change_percent,
                    "indexValue": stage.index_value,
                    "currentPrice": stage.current_price,
                    "companyCount": stage.company_count,
                    "leadingStock": stage.leading_stock,
                    "leadingStockChangePercent": stage.leading_stock_change_percent,
                    "updatedAt": stage.updated_at,
                }
                for stage in entry.stages
            ],
        }

    return {
        "generatedAt": generated_at,
        "symbols": [{"symbol": symbol, "weight": weights.get(symbol, 0.0)} for symbol in symbols],
        "industries": [_serialize(entry) for entry in industries],
        "concepts": [_serialize(entry) for entry in concepts],
    }


__all__ = ["build_sector_fund_flow_snapshot"]
