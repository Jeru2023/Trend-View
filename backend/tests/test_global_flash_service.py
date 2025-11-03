import pandas as pd
import pytest
from pandas.api.types import is_datetime64_any_dtype

from backend.src.services.global_flash_service import (
    GLOBAL_FLASH_COLUMNS,
    _prepare_global_flash_frame,
    _parse_classification_response,
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


def test_parse_classification_response_returns_structured_payload():
    payload = (
        '{"impact": true, "confidence": 0.72, "reason": "央行政策利好", '
        '"subject_level": "国家级", "impact_scope": ["上证指数","银行板块"], '
        '"event_type": "政策/监管", "time_sensitivity": "短期", "quant_signal": "央行降准预期", '
        '"impact_levels": ["market","sector","stock"], '
        '"impact_markets": ["上证指数"], '
        '"impact_sectors": ["银行"], '
        '"impact_stocks": ["601398.SH","工商银行"], '
        '"impact_themes": ["金融科技"], '
        '"impact_industries": ["非银金融"]}'
    )
    parsed = _parse_classification_response(payload)
    assert parsed["impact"] is True
    assert parsed["confidence"] == pytest.approx(0.72)
    assert parsed["reason"] == "央行政策利好"
    assert parsed["subject_level"] == "国家级"
    assert parsed["impact_scope"] == "上证指数、银行板块"
    assert parsed["event_type"] == "政策/监管"
    assert parsed["time_sensitivity"] == "短期"
    assert parsed["quant_signal"] == "央行降准预期"
    assert parsed["impact_levels"] == ["market", "sector", "stock"]
    assert parsed["impact_markets"] == ["上证指数"]
    assert parsed["impact_sectors"] == ["银行"]
    assert parsed["impact_stocks"] == ["601398.SH", "工商银行"]
    assert parsed["impact_themes"] == ["金融科技"]
    assert parsed["impact_industries"] == ["非银金融"]


def test_parse_classification_response_handles_invalid_json():
    parsed = _parse_classification_response("not-json")
    assert parsed["impact"] is False
    assert "not-json"[:20] in parsed["reason"]
    assert parsed["impact_levels"] == []
