import pandas as pd
from unittest.mock import patch

from backend.src.services.finance_breakfast_service import (
    FINANCE_BREAKFAST_COLUMNS,
    _prepare_finance_breakfast_frame,
    _frame_to_pipeline_records,
    _enrich_with_content,
)


def test_prepare_finance_breakfast_frame_normalizes_values():
    raw = pd.DataFrame(
        [
            {"title": " 早间资讯 ", "summary": " 简要 ", "url": " https://example.com/a ", "published_at": "2024-01-01 07:30"},
            {"title": "早间资讯", "summary": "重复", "url": "https://example.com/a", "published_at": "2024-01-01 07:30"},
            {"title": None, "summary": "无标题", "url": "https://example.com/b", "published_at": "2024-01-01 08:00"},
            {"title": "午间资讯", "summary": "", "url": "https://example.com/c", "published_at": "invalid"},
        ]
    )

    prepared = _prepare_finance_breakfast_frame(raw)

    assert prepared.columns.tolist() == FINANCE_BREAKFAST_COLUMNS
    assert len(prepared) == 1
    row = prepared.iloc[0]
    assert row["title"] == "早间资讯"
    assert row["summary"] == "简要"
    assert row["url"] == "https://example.com/a"
    assert str(row["published_at"]).startswith("2024-01-01")


def test_frame_to_pipeline_records_creates_payloads():
    frame = pd.DataFrame(
        [
            {"title": "资讯一", "summary": "摘要", "url": "https://example.com/a", "published_at": "2024-01-01 07:30"},
            {"title": "资讯二", "summary": "摘要2", "url": "https://example.com/b", "published_at": "2024-01-01 08:30"},
        ]
    )

    records = _frame_to_pipeline_records(frame)

    assert len(records) == 2
    for record in records:
        payload = record["payload"]
        assert payload["title"] in {"资讯一", "资讯二"}
        assert payload["published_at"].startswith("2024-01-01")
        assert payload["content_type"] == "morning_brief"


def test_enrich_with_content_populates_payload():
    records = [
        {
            "article_id": "abc",
            "payload": {
                "source_item_id": "https://example.com/a",
                "title": "资讯",
                "summary": "摘要",
                "published_at": "2024-01-01T07:30:00",
                "url": "https://example.com/a",
                "language": "zh-CN",
                "content_type": "morning_brief",
            },
        }
    ]

    class DummyDetail:
        content = "详细内容"

    with patch("backend.src.services.finance_breakfast_service.fetch_eastmoney_detail", return_value=DummyDetail()):
        _enrich_with_content(records)

    assert records[0]["payload"].get("content") == "详细内容"

