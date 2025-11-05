"""Unit tests for industry insight helpers."""

from datetime import datetime, timedelta

import pytest

from backend.src.services import industry_insight_service as service


def test_calculate_stage_metrics_assigns_expected_fields():
    now = datetime(2025, 2, 10, 10, 0, 0)
    stages = [
        {
            "symbol": "即时",
            "priceChangePercent": 1.2,
            "stageChangePercent": None,
            "indexValue": 3150.6,
        },
        {
            "symbol": "3日排行",
            "priceChangePercent": None,
            "stageChangePercent": 2.5,
        },
        {
            "symbol": "5日排行",
            "stageChangePercent": 4.8,
        },
        {
            "symbol": "10日排行",
            "stageChangePercent": 7.1,
        },
        {
            "symbol": "20日排行",
            "stageChangePercent": 12.4,
        },
        {
            "symbol": "其他",
            "stageChangePercent": 99.0,
        },
    ]

    metrics = service._calculate_stage_metrics(stages)  # pylint: disable=protected-access

    assert metrics["latestIndex"] == pytest.approx(3150.6)
    assert metrics["change1d"] == pytest.approx(1.2)
    assert metrics["change3d"] == pytest.approx(2.5)
    assert metrics["change5d"] == pytest.approx(4.8)
    assert metrics["change10d"] == pytest.approx(7.1)
    assert metrics["change20d"] == pytest.approx(12.4)


def test_build_industry_entries_merges_flow_and_news(monkeypatch):
    generated = datetime(2025, 2, 12, 9, 30)
    industries = [
        {
            "name": "高端制造",
            "score": 3.2,
            "bestRank": 1,
            "bestSymbol": "即时",
            "totalNetAmount": 8.6,
            "totalInflow": 12.3,
            "totalOutflow": 3.7,
            "stages": [
                {
                    "symbol": "即时",
                    "weight": 1.0,
                    "rank": 1,
                    "netAmount": 4.5,
                    "inflow": 6.0,
                    "outflow": 1.5,
                    "priceChangePercent": 1.1,
                    "stageChangePercent": 3.6,
                    "indexValue": 4105.2,
                    "leadingStock": "龙头股份",
                    "leadingStockChangePercent": 5.2,
                    "updatedAt": (generated - timedelta(minutes=5)).isoformat(),
                }
            ],
        }
    ]

    monkeypatch.setattr(
        service,
        "_fetch_industry_news",
        lambda *_args, **_kwargs: [
            {
                "title": "高端制造迎政策扶持",
                "source": "TestWire",
                "published_at": generated.isoformat(),
            }
        ],
        raising=False,
    )

    entries = service._build_industry_entries(  # pylint: disable=protected-access
        industries,
        lookback_hours=48,
        news_article_dao=None,
        news_insight_dao=None,
    )

    assert len(entries) == 1
    entry = entries[0]
    assert entry["name"] == "高端制造"
    assert entry["fundFlow"]["score"] == pytest.approx(3.2)
    assert entry["fundFlow"]["bestSymbol"] == "即时"
    assert entry["stageMetrics"]["change1d"] == pytest.approx(1.1)
    assert entry["news"][0]["title"] == "高端制造迎政策扶持"


def test_parse_iso_datetime_accepts_simple_strings():
    value = "2025-11-05T09:04:00+08:00"
    parsed = service._parse_iso_datetime(value)  # pylint: disable=protected-access
    assert parsed.tzinfo is not None
    assert parsed.minute == 4
