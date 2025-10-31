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

    class DummyGlobalDAO:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_entries(self, limit=200):  # noqa: D401
            return {
                "items": [
                    {
                        "code": "DJI",
                        "name": "道琼斯",
                        "latest_price": 39000.5,
                        "change_amount": 120.3,
                        "change_percent": 0.31,
                        "high_price": 39100.0,
                        "low_price": 38800.0,
                        "last_quote_time": now,
                    },
                    {
                        "code": "IXIC",
                        "name": "纳斯达克",
                        "latest_price": 15000.2,
                        "change_amount": -80.0,
                        "change_percent": -0.53,
                        "high_price": 15120.0,
                        "low_price": 14950.0,
                        "last_quote_time": now,
                    },
                    {
                        "code": "INX",
                        "name": "标普500",
                        "latest_price": 5200.1,
                        "change_amount": 15.2,
                        "change_percent": 0.29,
                        "high_price": 5220.0,
                        "low_price": 5180.0,
                        "last_quote_time": now,
                    },
                ]
            }

    class DummyDollarDAO:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_entries(self, limit=2):
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

        def list_entries(self, limit=1):
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

    dummy_dao = DummyInsightDAO()

    monkeypatch.setattr(peripheral_summary_service, "load_settings", lambda *_args, **_kwargs: sample_settings)
    monkeypatch.setattr(peripheral_summary_service, "GlobalIndexDAO", DummyGlobalDAO)
    monkeypatch.setattr(peripheral_summary_service, "DollarIndexDAO", DummyDollarDAO)
    monkeypatch.setattr(peripheral_summary_service, "RmbMidpointDAO", DummyRmbDAO)
    monkeypatch.setattr(peripheral_summary_service, "FuturesRealtimeDAO", DummyFuturesDAO)
    monkeypatch.setattr(peripheral_summary_service, "PeripheralInsightDAO", lambda *_args, **_kwargs: dummy_dao)

    return dummy_dao


def test_generate_peripheral_insight_without_llm(patch_data_access):
    result = peripheral_summary_service.generate_peripheral_insight(run_llm=False)

    assert result["summary"] is None
    assert result["metrics"]["globalIndices"]
    assert patch_data_access.last_payload is not None
    payload = patch_data_access.last_payload
    assert payload["summary"] is None
    assert payload["metrics"]["dollarIndex"]["close"] == 105.5
    assert payload["metrics"]["rmbMidpoint"]["rates"]["USD"]
