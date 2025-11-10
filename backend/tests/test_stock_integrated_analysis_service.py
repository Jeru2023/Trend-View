from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.src.services import stock_integrated_analysis_service as service


class DummyNewsDAO:
    def __init__(self, _config) -> None:
        self.called_with: dict | None = None

    def list_since(self, stock_code: str, *, since: datetime | None, limit: int) -> list[dict]:
        self.called_with = {"code": stock_code, "since": since, "limit": limit}
        return [
            {
                "id": 1,
                "title": "焦点资讯",
                "content": "摘要",
                "source": "Source",
                "url": "https://example.com",
                "published_at": datetime(2024, 1, 15, 9, 30),
            }
        ]


class DummyIntegratedDAO:
    def __init__(self, _config) -> None:
        self.insert_payload: dict | None = None

    def fetch_latest(self, _code: str):
        return None

    def insert_snapshot(self, **payload) -> int:  # noqa: ANN003
        self.insert_payload = payload
        return 42


def _stub_detail() -> dict:
    history = []
    base_date = datetime(2024, 1, 1)
    for index in range(12):
        day = base_date + timedelta(days=index)
        history.append(
            {
                "time": day.strftime("%Y-%m-%d"),
                "open": 10 + index * 0.5,
                "high": 10.5 + index * 0.5,
                "low": 9.8 + index * 0.5,
                "close": 10.2 + index * 0.5,
                "volume": 10000 + index * 100,
            }
        )
    return {
        "profile": {"code": "600519.SH", "name": "Stock"},
        "tradingData": {"lastPrice": 10.2},
        "tradingStats": {"pctChange1W": 5},
        "financialData": {"netIncome": 123},
        "financialStats": {"netIncomeYoyLatest": 10},
        "businessProfile": {"desc": "业务"},
        "businessComposition": {"items": []},
        "dailyTradeHistory": history,
    }


def _stub_settings(deepseek: object | None = None, coze: object | None = None):
    return SimpleNamespace(postgres=SimpleNamespace(), deepseek=deepseek, coze=coze)


@pytest.fixture(autouse=True)
def _common_stubs(monkeypatch):
    monkeypatch.setattr(service, "get_stock_detail", lambda *args, **kwargs: _stub_detail())
    monkeypatch.setattr(service, "list_individual_fund_flow", lambda **_: {"items": [{"symbol": "即时"}]})
    monkeypatch.setattr(
        service, "list_big_deal_fund_flow", lambda **_: {"items": [{"trade_price": 11.1, "trade_volume": 1000}]}
    )
    monkeypatch.setattr(
        service,
        "get_latest_stock_volume_price_reasoning",
        lambda *args, **kwargs: {"summary": {"wyckoffPhase": "吸筹"}},
    )
    dummy_news = DummyNewsDAO(None)
    monkeypatch.setattr(service, "StockNewsDAO", lambda _config: dummy_news)
    return dummy_news


def test_build_stock_integrated_context(monkeypatch):
    monkeypatch.setattr(service, "load_settings", lambda *args, **kwargs: _stub_settings())
    context = service.build_stock_integrated_context("600519.SH", news_days=5, trade_days=10)
    assert context["code"] == "600519.SH"
    assert len(context["dailyTrades"]) == 10
    assert context["news"][0]["title"] == "焦点资讯"
    assert context["individualFundFlow"][0]["symbol"] == "即时"
    assert context["bigDeals"][0]["trade_price"] == 11.1
    assert context["volumeReasoning"]["wyckoffPhase"] == "吸筹"


def test_generate_stock_integrated_analysis(monkeypatch):
    dummy_settings = _stub_settings(deepseek=SimpleNamespace(token="x", base_url="https://api", model="deepseek"))
    monkeypatch.setattr(service, "load_settings", lambda *args, **kwargs: dummy_settings)
    dummy_dao = DummyIntegratedDAO(None)
    monkeypatch.setattr(service, "StockIntegratedAnalysisDAO", lambda _config: dummy_dao)
    monkeypatch.setattr(service, "StockNewsDAO", lambda _config: DummyNewsDAO(None))
    monkeypatch.setattr(service, "generate_finance_analysis", lambda *args, **kwargs: '{"overview":"ok","keyFindings":["A"]}')

    record = service.generate_stock_integrated_analysis("600519.SH", news_days=6, trade_days=7)

    assert record["id"] == 42
    assert record["summary"]["overview"] == "ok"
    assert dummy_dao.insert_payload is not None
    assert dummy_dao.insert_payload["news_days"] == 6
    assert dummy_dao.insert_payload["trade_days"] == 7


def test_generate_stock_integrated_analysis_with_coze(monkeypatch):
    dummy_settings = _stub_settings(coze=SimpleNamespace(token="coze-token", bot_id="bot"))
    monkeypatch.setattr(service, "load_settings", lambda *args, **kwargs: dummy_settings)
    dummy_dao = DummyIntegratedDAO(None)
    monkeypatch.setattr(service, "StockIntegratedAnalysisDAO", lambda _config: dummy_dao)
    monkeypatch.setattr(service, "StockNewsDAO", lambda _config: DummyNewsDAO(None))

    captured_query: dict | None = {}

    def _fake_run(query: str, *, settings):
        captured_query["payload"] = query
        return {"content": '{"overview":"coze","keyFindings":["B"]}', "model": "coze-agent"}

    monkeypatch.setattr(service, "run_coze_agent", _fake_run)
    monkeypatch.setattr(service, "generate_finance_analysis", lambda *args, **kwargs: None)

    record = service.generate_stock_integrated_analysis("600519.SH")

    assert record["summary"]["overview"] == "coze"
    assert captured_query["payload"].startswith("{\"code\":\"600519.SH\"")
