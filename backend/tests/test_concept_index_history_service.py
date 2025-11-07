from __future__ import annotations

import sys
import types
from datetime import date

import pandas as pd
import pytest

sys.modules.setdefault("akshare", types.ModuleType("akshare"))

import backend.src.app as app_module
from backend.src.services import concept_index_history_service as service


class DummySettings:
    def __init__(self) -> None:
        self.postgres = object()


@pytest.fixture(autouse=True)
def patch_settings(monkeypatch):
    monkeypatch.setattr(service, "load_settings", lambda _path=None: DummySettings())


def test_sync_concept_index_history(monkeypatch):
    recorded_frames = []

    class RecordingDAO:
        def __init__(self, _config, table_name=None) -> None:
            pass

        def upsert(self, frame):
            recorded_frames.append(frame.copy())
            return len(frame)

    monkeypatch.setattr(service, "ConceptIndexHistoryDAO", RecordingDAO)

    def fake_concept_name_list():
        return pd.DataFrame({"name": ["AI算力"]})

    service._CONCEPT_NAME_LOOKUP = None
    service._NORMALIZED_CONCEPT_MAP = None

    def fake_ths_fetch(symbol, start_date, end_date):
        assert symbol == "AI算力"
        assert start_date == "20250101"
        assert end_date == "20250131"
        return pd.DataFrame(
            {
                "日期": ["2025-01-02", "2025-01-03"],
                "开盘价": [10.5, 10.8],
                "最高价": [11.0, 11.2],
                "最低价": [10.3, 10.6],
                "收盘价": [10.9, 11.1],
                "涨跌幅": [3.2, 1.8],
                "涨跌额": [0.3, 0.2],
                "成交量": [1.2e5, 1.5e5],
                "成交额": [3.1e7, 3.4e7],
            }
        )

    ak_stub = types.SimpleNamespace(
        stock_board_concept_index_ths=fake_ths_fetch,
        stock_board_concept_name_ths=fake_concept_name_list,
    )
    monkeypatch.setattr(service, "ak", ak_stub)

    result = service.sync_concept_index_history(
        ["AI算力"],
        start_date="20250101",
        end_date="20250131",
    )

    assert result["totalRows"] == 2
    assert result["concepts"][0]["concept"] == "AI算力"
    assert recorded_frames, "Expected DAO upsert to be called"
    stored = recorded_frames[0]
    assert "concept_name" in stored.columns
    assert stored.iloc[0]["concept_name"] == "AI算力"
    assert stored.iloc[0]["trade_date"].isoformat() == "2025-01-02"


def test_concept_index_history_endpoint(monkeypatch):
    sample_rows = [
        {
            "ts_code": "THS-AI算力",
            "concept_name": "AI算力",
            "trade_date": date(2025, 1, 2),
            "open": 10.5,
            "high": 11.0,
            "low": 10.3,
            "close": 10.9,
            "pre_close": 10.6,
            "change": 0.3,
            "pct_chg": 2.83,
            "vol": 120000.0,
            "amount": 3.1e7,
        }
    ]

    def fake_list_concept_index_history(**kwargs):
        assert kwargs["concept_name"] == "AI算力"
        assert kwargs["limit"] == 30
        return {"total": len(sample_rows), "items": sample_rows}

    monkeypatch.setattr(app_module, "list_concept_index_history", fake_list_concept_index_history)

    response = app_module.get_concept_index_history_api(concept="AI算力", limit=30)

    assert response.concept == "AI算力"
    assert response.total == 1
    assert response.rows[0].trade_date.isoformat() == "2025-01-02"
    assert response.rows[0].close == 10.9


def test_concept_index_history_endpoint_auto_refresh(monkeypatch):
    sample_rows = [
        {
            "ts_code": "THS-AI算力",
            "concept_name": "AI算力",
            "trade_date": date(2025, 2, 1),
            "open": 11.0,
            "high": 11.5,
            "low": 10.8,
            "close": 11.3,
            "pre_close": 10.9,
            "change": 0.4,
            "pct_chg": 3.67,
            "vol": 150000.0,
            "amount": 4.2e7,
        }
    ]

    list_calls = {"count": 0}

    def fake_list_concept_index_history(**kwargs):
        list_calls["count"] += 1
        if list_calls["count"] == 1:
            return {"total": 0, "items": []}
        return {"total": len(sample_rows), "items": sample_rows}

    captured_sync = {}

    def fake_sync_concept_index_history(concepts, start_date=None, end_date=None):
        captured_sync["concepts"] = concepts
        captured_sync["start_date"] = start_date
        captured_sync["end_date"] = end_date
        return {"totalRows": len(sample_rows), "concepts": [{"concept": concepts[0], "rows": len(sample_rows)}]}

    monkeypatch.setattr(app_module, "list_concept_index_history", fake_list_concept_index_history)
    monkeypatch.setattr(app_module, "sync_concept_index_history", fake_sync_concept_index_history)

    response = app_module.get_concept_index_history_api(concept="AI算力", limit=60)

    assert list_calls["count"] >= 2
    assert captured_sync["concepts"] == ["AI算力"]
    assert captured_sync["start_date"] is not None
    assert captured_sync["end_date"] is not None
    assert response.total == 1
    assert response.rows[0].close == 11.3


def test_resolve_concept_symbol_alias(monkeypatch):
    def fake_concept_name_list():
        return pd.DataFrame({"name": ["东数西算(算力)", "光伏概念"]})

    service._CONCEPT_NAME_LOOKUP = None
    service._NORMALIZED_CONCEPT_MAP = None
    ak_stub = types.SimpleNamespace(stock_board_concept_name_ths=fake_concept_name_list)
    monkeypatch.setattr(service, "ak", ak_stub)

    result = service._resolve_concept_symbol("AI算力")
    assert result == "东数西算(算力)"
