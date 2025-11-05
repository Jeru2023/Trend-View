from __future__ import annotations

import sys
import types
from datetime import datetime

import pytest

sys.modules.setdefault("akshare", types.ModuleType("akshare"))

from backend.src.services import sector_fund_flow_service as service


class DummySettings:
    def __init__(self) -> None:
        self.postgres = object()


@pytest.fixture(autouse=True)
def patch_settings(monkeypatch):
    monkeypatch.setattr(service, "load_settings", lambda _path=None: DummySettings())


def test_build_sector_fund_flow_snapshot(monkeypatch):
    base_time = datetime(2025, 1, 1, 9, 30)

    industry_data = {
        "即时": [
            {
                "industry": "光伏",
                "rank": 1,
                "net_amount": 8.4e8,
                "inflow": 9.1e8,
                "outflow": 7e7,
                "price_change_percent": 2.6,
                "stage_change_percent": 4.2,
                "industry_index": 3250.4,
                "current_price": 3150.2,
                "company_count": 35,
                "leading_stock": "隆基股份",
                "leading_stock_change_percent": 3.1,
                "updated_at": base_time,
            }
        ],
        "3日排行": [
            {
                "industry": "光伏",
                "rank": 2,
                "net_amount": 5.2e8,
                "inflow": 6.1e8,
                "outflow": 9e7,
                "price_change_percent": 1.4,
                "stage_change_percent": 3.9,
                "industry_index": 3220.1,
                "current_price": 3120.8,
                "company_count": 35,
                "leading_stock": "阳光电源",
                "leading_stock_change_percent": 2.5,
                "updated_at": base_time,
            }
        ],
    }

    concept_data = {
        "即时": [
            {
                "concept": "AI算力",
                "rank": 3,
                "net_amount": 4.8e8,
                "inflow": 5.5e8,
                "outflow": 7e7,
                "price_change_percent": 3.6,
                "stage_change_percent": 6.3,
                "concept_index": 4100.6,
                "current_price": 4055.2,
                "company_count": 58,
                "leading_stock": "浪潮信息",
                "leading_stock_change_percent": 5.2,
                "updated_at": base_time,
            }
        ]
    }

    class IndustryStub:
        def __init__(self, _config) -> None:
            pass

        def list_entries(self, *, symbol: str, limit: int, offset: int):
            return {"items": industry_data.get(symbol, [])}

    class ConceptStub:
        def __init__(self, _config) -> None:
            pass

        def list_entries(self, *, symbol: str, limit: int, offset: int):
            return {"items": concept_data.get(symbol, [])}

    monkeypatch.setattr(service, "IndustryFundFlowDAO", IndustryStub)
    monkeypatch.setattr(service, "ConceptFundFlowDAO", ConceptStub)

    snapshot = service.build_sector_fund_flow_snapshot(symbols=("即时", "3日排行"))

    assert snapshot["symbols"][0]["symbol"] == "即时"
    assert snapshot["symbols"][1]["symbol"] == "3日排行"

    industries = snapshot["industries"]
    assert len(industries) == 1
    industry_entry = industries[0]
    assert industry_entry["name"] == "光伏"
    # Score should aggregate weight contributions with rank adjustment.
    assert industry_entry["score"] > 1.3
    assert industry_entry["bestRank"] == 1
    assert industry_entry["bestSymbol"] == "即时"
    assert pytest.approx(industry_entry["totalNetAmount"], rel=1e-5) == round((8.4e8 + 5.2e8), 2)
    assert pytest.approx(industry_entry["totalInflow"], rel=1e-5) == round((9.1e8 + 6.1e8), 2)
    assert pytest.approx(industry_entry["totalOutflow"], rel=1e-5) == round((7e7 + 9e7), 2)
    assert len(industry_entry["stages"]) == 2
    first_stage = industry_entry["stages"][0]
    assert first_stage["symbol"] == "即时"
    assert first_stage["rank"] == 1
    assert first_stage["netAmount"] == 8.4e8
    assert first_stage["leadingStock"] == "隆基股份"
    assert first_stage["inflow"] == 9.1e8
    assert first_stage["outflow"] == 7e7

    concepts = snapshot["concepts"]
    assert len(concepts) == 1
    concept_entry = concepts[0]
    assert concept_entry["name"] == "AI算力"
    assert concept_entry["score"] > 0.0
    assert len(concept_entry["stages"]) == 1
    assert concept_entry["stages"][0]["symbol"] == "即时"
    assert pytest.approx(concept_entry["totalInflow"], rel=1e-5) == round(5.5e8, 2)
    assert pytest.approx(concept_entry["totalOutflow"], rel=1e-5) == round(7e7, 2)
