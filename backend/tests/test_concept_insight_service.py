"""Unit tests for concept insight helpers."""

from datetime import date, timedelta

import pytest

from backend.src.services import concept_insight_service as service


def _build_history_rows(days: int, *, latest_close: float = 120.0) -> list[dict]:
    rows: list[dict] = []
    current_close = latest_close
    for idx in range(days):
        rows.append(
            {
                "trade_date": date(2025, 1, 31) - timedelta(days=idx),
                "close": current_close,
                "pct_chg": 0.5,
                "vol": 100 + idx,
                "ts_code": "399006.SZ",
            }
        )
        current_close -= 1.5  # ensure monotonic change for deterministic % calc
    return rows


def test_compute_index_metrics_handles_history_span():
    rows = _build_history_rows(25, latest_close=150.0)
    metrics = service._compute_index_metrics(rows)  # pylint: disable=protected-access

    assert metrics["latestClose"] == 150.0
    # Latest pct_chg should be passed through directly
    assert metrics["change1d"] == pytest.approx(0.5)
    # With deterministic closing path, 5-day change should be positive
    assert metrics["change5d"] > 0
    assert metrics["change20d"] > metrics["change5d"]
    assert metrics["avgVolume5d"] == pytest.approx(102.0)


def test_build_concept_entries_merges_flow_index_and_news(monkeypatch):
    hotlist = [
        {
            "name": "绿色电力",
            "score": 2.8,
            "bestRank": 1,
            "bestSymbol": "即时",
            "totalNetAmount": 3.5,
            "totalInflow": 5.0,
            "totalOutflow": 1.5,
            "stages": [
                {
                    "symbol": "即时",
                    "weight": 1.0,
                    "rank": 1,
                    "netAmount": 1.2,
                    "inflow": 2.1,
                    "outflow": 0.9,
                    "priceChangePercent": 1.1,
                    "stageChangePercent": 3.4,
                    "leadingStock": "龙头股",
                    "leadingStockChangePercent": 4.5,
                }
            ],
        }
    ]

    class StubIndexDAO:
        def __init__(self, mapping):
            self.mapping = mapping

        def list_entries(self, concept_name=None, **_kwargs):
            return {"items": self.mapping.get(concept_name, [])}

    history_rows = _build_history_rows(10, latest_close=60.0)
    index_dao = StubIndexDAO({"绿色电力": history_rows})

    monkeypatch.setattr(
        service,
        "_fetch_concept_news",
        lambda *_args, **_kwargs: [
            {"title": "绿色电力扩容", "published_at": "2025-01-31T10:00:00+08:00"}
        ],
        raising=False,
    )

    entries = service._build_concept_entries(  # pylint: disable=protected-access
        hotlist,
        lookback_hours=48,
        index_history_dao=index_dao,
        news_article_dao=None,
        news_insight_dao=None,
    )

    assert len(entries) == 1
    entry = entries[0]
    assert entry["name"] == "绿色电力"
    assert entry["fundFlow"]["bestRank"] == 1
    assert entry["fundFlow"]["stages"][0]["symbol"] == "即时"
    assert entry["indexMetrics"]["latestClose"] == pytest.approx(60.0)
    assert entry["tsCode"] == "399006.SZ"
    assert entry["news"][0]["title"] == "绿色电力扩容"


def test_parse_iso_datetime_accepts_strings_without_microseconds():
    value = "2025-02-12T09:30:00+08:00"
    parsed = service._parse_iso_datetime(value)  # pylint: disable=protected-access
    assert parsed.tzinfo is not None
    assert parsed.hour == 9
