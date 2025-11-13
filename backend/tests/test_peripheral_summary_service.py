import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.src.services import peripheral_summary_service  # noqa: E402


class DummyInsightDAO:
    def __init__(self, *args, **kwargs):
        self.last_payload = None

    def upsert_snapshot(self, **kwargs):
        self.last_payload = kwargs

    def fetch_latest(self):
        return None

    def stats(self):  # pragma: no cover - not used in unit test
        return {"count": 0, "updated_at": None}


@pytest.fixture
def sample_settings():
    return SimpleNamespace(
        tushare=SimpleNamespace(token="dummy"),
        deepseek=None,
        postgres=SimpleNamespace(),
    )


@pytest.fixture
def patch_data_access(monkeypatch, sample_settings):
    now = datetime.now() - timedelta(minutes=5)

    snapshot_items = []
    for idx, spec in enumerate(peripheral_summary_service.GLOBAL_INDEX_TARGETS.values(), start=1):
        direction = 1 if idx % 2 else -1
        snapshot_items.append(
            {
                "code": spec["codes"][0],
                "name": spec["display_name"],
                "seq": idx,
                "latest_price": 1000.0 + idx * 100,
                "change_amount": direction * 10.0,
                "change_percent": direction * 0.5,
                "high_price": 1010.0 + idx * 100,
                "low_price": 990.0 + idx * 100,
                "last_quote_time": now - timedelta(minutes=idx),
            }
        )

    class DummyDollarDAO:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_entries(self, limit=10):
            return {
                "items": [
                    {
                        "trade_date": date.today(),
                        "code": "UDI",
                        "name": "美元指数",
                        "close_price": 105.5,
                        "high_price": 106.0,
                        "low_price": 104.8,
                        "amplitude": 0.9,
                    },
                    {
                        "trade_date": date.today() - timedelta(days=1),
                        "code": "UDI",
                        "name": "美元指数",
                        "close_price": 104.8,
                    },
                ]
            }

    class DummyRmbDAO:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_entries(self, limit=10):
            return {
                "items": [
                    {
                        "trade_date": date.today(),
                        "usd": 715.32,
                        "eur": 768.12,
                        "jpy": 4.85,
                    }
                ]
            }

    class DummyFuturesDAO:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_entries(self, limit=200):
            return {
                "items": [
                    {
                        "name": "布伦特原油",
                        "code": "OIL",
                        "last_price": 82.5,
                        "change_amount": 1.2,
                        "change_percent": 1.48,
                        "quote_time": now,
                    },
                    {
                        "name": "NYMEX原油",
                        "code": "CL",
                        "last_price": 78.4,
                        "change_amount": 0.8,
                        "change_percent": 1.03,
                        "quote_time": now,
                    },
                    {
                        "name": "COMEX黄金",
                        "code": "GC",
                        "last_price": 2150.7,
                        "change_amount": -5.2,
                        "change_percent": -0.24,
                        "quote_time": now,
                    },
                    {
                        "name": "COMEX白银",
                        "code": "SI",
                        "last_price": 26.8,
                        "change_amount": 0.2,
                        "change_percent": 0.75,
                        "quote_time": now,
                    },
                ]
            }

    class DummyFedDAO:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_entries(self, limit=3):
            return {
                "items": [
                    {
                        "title": "FOMC statement",
                        "url": "https://fed.test/stmt",
                        "statement_date": date.today(),
                        "content": "Policy unchanged",
                        "raw_text": "Policy unchanged",
                        "updated_at": now,
                    }
                ]
            }

    dummy_dao = DummyInsightDAO()

    monkeypatch.setattr(peripheral_summary_service, "load_settings", lambda *_args, **_kwargs: sample_settings)
    monkeypatch.setattr(
        peripheral_summary_service,
        "list_global_indices",
        lambda limit=200, offset=0, settings=None: {
            "total": len(snapshot_items),
            "items": snapshot_items[:limit],
            "lastSyncedAt": now,
        },
    )
    monkeypatch.setattr(peripheral_summary_service, "DollarIndexDAO", DummyDollarDAO)
    monkeypatch.setattr(peripheral_summary_service, "RmbMidpointDAO", DummyRmbDAO)
    monkeypatch.setattr(peripheral_summary_service, "FuturesRealtimeDAO", DummyFuturesDAO)
    monkeypatch.setattr(peripheral_summary_service, "FedStatementDAO", DummyFedDAO)
    monkeypatch.setattr(peripheral_summary_service, "PeripheralInsightDAO", lambda *_args, **_kwargs: dummy_dao)
    monkeypatch.setattr(
        peripheral_summary_service,
        "list_global_index_history",
        lambda code, limit=10, settings=None: {
            "code": code,
            "items": [{"trade_date": date.today(), "close_price": 100.0}],
        },
    )

    return dummy_dao


def test_generate_peripheral_insight_without_llm(patch_data_access):
    result = peripheral_summary_service.generate_peripheral_insight(run_llm=False)

    assert result["summary"] is None
    metrics = result["metrics"]
    assert metrics["globalIndices"]
    assert patch_data_access.last_payload is not None
    payload = patch_data_access.last_payload
    assert payload["summary"] is None
    assert metrics["dollarIndex"]["close"] == 105.5
    assert metrics["dollarIndexSeries"]
    assert metrics["rmbMidpoint"]["rates"]["USD"]
    assert metrics["rmbMidpointSeries"]
    assert metrics["fedStatements"]
    assert metrics["globalIndicesLatestTimestamp"]
    assert len(metrics["globalIndices"]) == len(peripheral_summary_service.GLOBAL_INDEX_TARGETS)
    assert metrics["globalIndicesHistory"]
    assert not metrics["warnings"]
