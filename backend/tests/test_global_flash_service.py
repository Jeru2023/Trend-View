import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from backend.src.services.global_flash_service import (
    GLOBAL_FLASH_COLUMNS,
    _prepare_global_flash_frame,
    _frame_to_pipeline_records,
)


def test_prepare_global_flash_frame_handles_casing_and_filters_rows():
    raw = pd.DataFrame(
        [
            ["Headline A", " Summary A ", "https://example.com/a", "2024-01-01 08:00"],
            ["Headline B", "Summary B", "https://example.com/b", "2024-01-01 07:00"],
            [None, "Summary C", "https://example.com/c", "2024-01-01 09:00"],
            ["Headline D", "Summary D", "https://example.com/d", "invalid"],
        ],
        columns=["Title", "Summary", "URL", "PublishedAt"],
    )

    prepared = _prepare_global_flash_frame(raw)

    assert prepared.columns.tolist() == GLOBAL_FLASH_COLUMNS
    assert len(prepared) == 2
    assert prepared.iloc[0]["title"] == "Headline B"
    assert prepared.iloc[0]["summary"] == "Summary B"
    assert prepared.iloc[0]["url"] == "https://example.com/b"
    assert prepared.iloc[1]["title"] == "Headline A"
    assert prepared.iloc[1]["summary"] == "Summary A"
    assert prepared.iloc[1]["url"] == "https://example.com/a"
    assert is_datetime64_any_dtype(prepared["published_at"])
    assert prepared["published_at"].iloc[0].isoformat().startswith("2024-01-01T07:00:00")
    assert prepared["published_at"].iloc[1].isoformat().startswith("2024-01-01T08:00:00")


def test_prepare_global_flash_frame_returns_empty_for_none_input():
    prepared = _prepare_global_flash_frame(None)
    assert prepared.columns.tolist() == GLOBAL_FLASH_COLUMNS
    assert prepared.empty


def test_frame_to_pipeline_records_produces_unique_ids():
    frame = pd.DataFrame(
        [
            {"title": "Headline A", "summary": "Summary A", "url": "https://example.com/a", "published_at": "2024-01-01T08:00:00"},
            {"title": "Headline B", "summary": "Summary B", "url": "https://example.com/b", "published_at": "2024-01-01T09:00:00"},
        ]
    )
    records = _frame_to_pipeline_records(frame)
    assert len(records) == 2
    article_ids = {record["article_id"] for record in records}
    assert len(article_ids) == 2
    for record in records:
        payload = record["payload"]
        assert payload["title"] in {"Headline A", "Headline B"}
        assert payload["url"].startswith("https://example.com/")
        assert payload["published_at"].startswith("2024-01-01T")


def test_frame_to_pipeline_records_skips_invalid_entries():
    frame = pd.DataFrame(
        [
            {"title": None, "summary": "Summary A", "url": "https://example.com/a", "published_at": "2024-01-01T08:00:00"},
            {"title": "Headline B", "summary": "Summary B", "url": None, "published_at": "2024-01-01T09:00:00"},
        ]
    )
    records = _frame_to_pipeline_records(frame)
    assert records == []
