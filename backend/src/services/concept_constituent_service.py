"""Service helpers for retrieving Tonghuashun concept constituents."""

from __future__ import annotations

import logging
import re
import time
from io import StringIO
from typing import Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
import requests
from akshare.datasets import get_ths_js
from py_mini_racer import MiniRacer

from ..config.settings import load_settings
from ..dao import ConceptConstituentDAO, ConceptDirectoryDAO

logger = logging.getLogger(__name__)

_CONCEPT_CODE_CACHE: Optional[Dict[str, str]] = None
_THS_JS_SOURCE: Optional[str] = None

PAGE_INFO_PATTERN = re.compile(r'class="page_info">\s*(\d+)\s*/\s*(\d+)\s*<')

COLUMN_MAPPING = {
    "序号": "rank",
    "代码": "symbol",
    "名称": "name",
    "现价": "lastPrice",
    "涨跌幅(%)": "changePercent",
    "涨跌": "changeAmount",
    "涨速(%)": "speedPercent",
    "换手(%)": "turnoverRate",
    "量比": "volumeRatio",
    "振幅(%)": "amplitudePercent",
    "成交额": "turnoverAmount",
    "流通股": "floatShares",
    "流通市值": "floatMarketCap",
    "市盈率": "pe",
}

PERCENT_COLUMNS = {
    "changePercent",
    "speedPercent",
    "turnoverRate",
    "amplitudePercent",
}

AMOUNT_COLUMNS = {"turnoverAmount", "floatShares", "floatMarketCap"}


class ConceptNotFoundError(ValueError):
    """Raised when a concept name cannot be mapped to a THS code."""


def _fetch_remote_concept_codes() -> Dict[str, str]:
    try:
        frame = ak.stock_board_concept_name_ths()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to load THS concept dictionary: %s", exc)
        return {}

    mapping: Dict[str, str] = {}
    if not frame.empty:
        values = frame[["name", "code"]].dropna()
        for _, row in values.iterrows():
            name = str(row["name"]).strip()
            code = str(row["code"]).strip()
            if not name or not code:
                continue
            mapping[name] = code
    return mapping


def _load_concept_codes(*, settings_path: Optional[str] = None, refresh: bool = False) -> Dict[str, str]:
    global _CONCEPT_CODE_CACHE
    if _CONCEPT_CODE_CACHE is not None and not refresh:
        return _CONCEPT_CODE_CACHE
    settings = load_settings(settings_path)
    directory_dao = ConceptDirectoryDAO(settings.postgres)
    rows = directory_dao.list_entries()
    if rows and not refresh:
        mapping = {row["concept_name"]: row["concept_code"] for row in rows}
        _CONCEPT_CODE_CACHE = mapping
        return mapping
    mapping = _fetch_remote_concept_codes()
    directory_dao.replace_all(mapping)
    _CONCEPT_CODE_CACHE = mapping
    return mapping


def _resolve_concept_code(concept: str, *, settings_path: Optional[str] = None) -> Tuple[str, str]:
    lookup = _load_concept_codes(settings_path=settings_path)
    target = concept or ""
    normalized = target.strip()
    if not normalized:
        raise ConceptNotFoundError("Concept name cannot be empty.")
    if normalized in lookup:
        return normalized, lookup[normalized]
    for name, code in lookup.items():
        if normalized.lower() == name.lower():
            return name, code
    raise ConceptNotFoundError(f"Concept '{concept}' is not present in THS dictionary.")


def _load_ths_js() -> str:
    global _THS_JS_SOURCE
    if _THS_JS_SOURCE is None:
        with open(get_ths_js("ths.js"), encoding="utf-8") as handle:
            _THS_JS_SOURCE = handle.read()
    return _THS_JS_SOURCE


def _issue_hexin_token() -> str:
    runner = MiniRacer()
    runner.eval(_load_ths_js())
    return runner.call("v")


def _prepare_session(concept_code: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/116.0 Safari/537.36",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }
    )
    _refresh_session_token(session, concept_code, detail_prefetch=True)
    return session


def _refresh_session_token(session: requests.Session, concept_code: str, *, detail_prefetch: bool = False) -> None:
    token = _issue_hexin_token()
    session.cookies.set("v", token, domain=".10jqka.com.cn")
    referer = f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/"
    session.headers.update(
        {
            "hexin-v": token,
            "Referer": referer,
            "Origin": "https://q.10jqka.com.cn",
            "Accept": "text/html, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    if detail_prefetch:
        try:
            response = session.get(referer, timeout=10)
            response.raise_for_status()
            latest = session.cookies.get("v") or token
            session.headers["hexin-v"] = latest
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("Prefetch detail page failed (token still usable): %s", exc)


def _build_ajax_url(concept_code: str, page: int) -> str:
    prefix = f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/ajax/1/"
    if page <= 1:
        return prefix
    return f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/ajax/1/page/{page}/"


def _fetch_page_html(
    session: requests.Session,
    concept_code: str,
    page: int,
    *,
    max_attempts: int = 5,
    timeout: int = 10,
) -> str:
    url = _build_ajax_url(concept_code, page)
    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        try:
            response = session.get(url, timeout=timeout)
        except Exception as exc:  # pragma: no cover - network failures
            logger.warning("Concept constituent request failed for page %s: %s", page, exc)
            _refresh_session_token(session, concept_code, detail_prefetch=True)
            continue
        if response.status_code == 200 and "<table" in response.text:
            response.encoding = "gbk"
            return response.text
        logger.debug(
            "THS ajax response blocked (status=%s len=%s attempt=%s)", response.status_code, len(response.text), attempts
        )
        _refresh_session_token(session, concept_code, detail_prefetch=True)
        time.sleep(0.25)
    raise RuntimeError(f"Failed to load THS constituent page {page} after {max_attempts} attempts.")


def _extract_total_pages(html: str) -> int:
    match = PAGE_INFO_PATTERN.search(html)
    if match:
        try:
            return max(int(match.group(2)), 1)
        except ValueError:
            return 1
    return 1


def _parse_number(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text == "--":
        return None
    multiplier = 1.0
    if text.endswith("亿"):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 1e4
        text = text[:-1]
    text = text.replace(",", "")
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _parse_percent(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text == "--":
        return None
    if text.endswith("%"):
        text = text[:-1]
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _prepare_dataframe(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html))
    if not tables:
        raise ValueError("No table found in THS response.")
    frame = tables[0].rename(columns=COLUMN_MAPPING)
    for english in COLUMN_MAPPING.values():
        if english not in frame.columns:
            frame[english] = None
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype("Int64")
    frame["symbol"] = (
        frame["symbol"]
        .astype(str)
        .str.strip()
        .map(lambda code: code.zfill(6) if code.isdigit() and len(code) < 6 else code)
    )
    frame["name"] = frame["name"].astype(str).str.strip()
    for column in PERCENT_COLUMNS:
        frame[column] = frame[column].map(_parse_percent)
    for column in AMOUNT_COLUMNS:
        frame[column] = frame[column].map(_parse_number)
    frame["lastPrice"] = frame["lastPrice"].map(_parse_number)
    frame["changeAmount"] = frame["changeAmount"].map(_parse_number)
    frame["volumeRatio"] = frame["volumeRatio"].map(_parse_number)
    frame["pe"] = frame["pe"].map(_parse_number)
    frame = frame.dropna(subset=["symbol", "name"], how="any")
    ordered = list(COLUMN_MAPPING.values())
    return frame.loc[:, ordered]


def _frame_to_records(frame: pd.DataFrame) -> List[dict]:
    records: List[dict] = []
    for row in frame.to_dict(orient="records"):
        records.append({key: row.get(key) for key in COLUMN_MAPPING.values()})
    return records


def list_concept_constituents(
    concept: str,
    *,
    max_pages: Optional[int] = None,
    page_delay: float = 5.0,
    settings_path: Optional[str] = None,
    refresh: bool = False,
) -> dict:
    """Fetch or load cached Tonghuashun concept constituents."""

    resolved_name, concept_code = _resolve_concept_code(concept, settings_path=settings_path)
    settings = load_settings(settings_path)
    dao = ConceptConstituentDAO(settings.postgres)

    if not refresh:
        cached = dao.list_entries(resolved_name)
        return {
            "concept": resolved_name,
            "conceptCode": concept_code,
            "totalPages": 0,
            "pagesFetched": 0,
            "blocked": False,
            "items": cached,
        }

    session = _prepare_session(concept_code)
    first_html = _fetch_page_html(session, concept_code, 1)
    total_pages = _extract_total_pages(first_html)
    limit = total_pages if max_pages is None else max(1, min(max_pages, total_pages))
    logger.info("Refreshing concept constituents for %s (max pages=%s)", resolved_name, limit)

    frames: List[pd.DataFrame] = []
    frames.append(_prepare_dataframe(first_html))

    pages_fetched = 1
    blocked = False

    page = 2
    while page <= total_pages and pages_fetched < limit:
        try:
            html = _fetch_page_html(session, concept_code, page)
        except Exception as exc:  # pragma: no cover - network volatility
            logger.warning("Stopped fetching THS constituents at page %s due to: %s", page, exc)
            blocked = True
            break
        frames.append(_prepare_dataframe(html))
        pages_fetched += 1
        page += 1
        if page_delay > 0:
            time.sleep(page_delay)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMN_MAPPING.values())
    combined = combined.sort_values(by="rank", na_position="last").reset_index(drop=True)

    limit_reached = pages_fetched >= limit
    blocked = blocked or (not limit_reached and pages_fetched < total_pages)

    records = _frame_to_records(combined)
    dao.replace_entries(resolved_name, concept_code, records)

    logger.info(
        "Concept constituents refreshed: %s (pages=%s/%s, blocked=%s, rows=%s)",
        resolved_name,
        pages_fetched,
        total_pages,
        blocked,
        len(records),
    )

    return {
        "concept": resolved_name,
        "conceptCode": concept_code,
        "totalPages": total_pages,
        "pagesFetched": pages_fetched,
        "blocked": blocked,
        "items": records,
    }


def list_concept_directory(*, settings_path: Optional[str] = None, refresh: bool = False) -> Dict[str, str]:
    """Return a copy of the THS concept name -> code mapping."""
    return _load_concept_codes(settings_path=settings_path, refresh=refresh).copy()


def search_concept_directory(
    query: Optional[str] = None,
    limit: int = 20,
    *,
    settings_path: Optional[str] = None,
    refresh: bool = False,
) -> List[Dict[str, str]]:
    """
    Return concept directory entries filtered by fuzzy name match.

    Args:
        query: Optional case-insensitive substring to filter names.
        limit: Maximum number of matches to return.
    """
    mapping = list_concept_directory(settings_path=settings_path, refresh=refresh)
    limit = max(1, min(int(limit), 200))
    if not query:
        items = list(mapping.items())[:limit]
    else:
        keyword = query.strip().lower()
        items = [
            (name, code)
            for name, code in mapping.items()
            if keyword in name.lower()
        ][:limit]
    return [{"name": name, "code": code} for name, code in items]


def sync_concept_directory(*, settings_path: Optional[str] = None) -> Dict[str, object]:
    """Refresh the concept directory cache and return summary stats."""
    mapping = list_concept_directory(settings_path=settings_path, refresh=True)
    return {"rows": len(mapping)}


def resolve_concept_label(concept: str, *, settings_path: Optional[str] = None) -> Dict[str, str]:
    """Resolve the provided label to the canonical THS concept name/code."""
    name, code = _resolve_concept_code(concept, settings_path=settings_path)
    if not name or not code:
        raise ConceptNotFoundError(f"Concept '{concept}' not found.")
    return {"name": name, "code": code}


__all__ = [
    "list_concept_constituents",
    "ConceptNotFoundError",
    "list_concept_directory",
    "search_concept_directory",
    "sync_concept_directory",
    "resolve_concept_label",
]
