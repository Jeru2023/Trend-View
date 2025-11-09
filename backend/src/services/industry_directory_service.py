"Providers for Eastmoney industry directory lookups."

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import akshare as ak

from ..config.settings import load_settings
from ..dao import IndustryDirectoryDAO

logger = logging.getLogger(__name__)

_INDUSTRY_CACHE: Optional[Dict[str, str]] = None


def _fetch_remote_industry_codes() -> Dict[str, str]:
    try:
        frame = ak.stock_board_industry_name_em()
    except Exception as exc:  # pragma: no cover - external dependency
        logger.error("Failed to load Eastmoney industry directory: %s", exc)
        return {}

    mapping: Dict[str, str] = {}
    if frame is not None and not frame.empty:
        columns = frame.columns
        name_col = next((col for col in columns if "板块名称" in col or "名称" in col), None)
        code_col = next((col for col in columns if "板块代码" in col or "代码" in col), None)
        if name_col and code_col:
            for _, row in frame[[name_col, code_col]].dropna().iterrows():
                name = str(row[name_col]).strip()
                code = str(row[code_col]).strip()
                if not name or not code:
                    continue
                mapping[name] = code
    return mapping


def _load_industry_codes(*, settings_path: Optional[str] = None, refresh: bool = False) -> Dict[str, str]:
    global _INDUSTRY_CACHE
    if _INDUSTRY_CACHE is not None and not refresh:
        return _INDUSTRY_CACHE
    settings = load_settings(settings_path)
    directory_dao = IndustryDirectoryDAO(settings.postgres)
    rows = directory_dao.list_entries()
    if rows and not refresh:
        mapping = {row["industry_name"]: row["industry_code"] for row in rows}
        _INDUSTRY_CACHE = mapping
        return mapping
    mapping = _fetch_remote_industry_codes()
    if mapping:
        directory_dao.replace_all(mapping)
    _INDUSTRY_CACHE = mapping
    return mapping


def list_industry_directory(*, settings_path: Optional[str] = None) -> List[Tuple[str, str]]:
    lookup = _load_industry_codes(settings_path=settings_path)
    return sorted(lookup.items(), key=lambda item: item[0])


def search_industry_directory(
    query: Optional[str] = None,
    *,
    limit: int = 20,
    settings_path: Optional[str] = None,
) -> List[Dict[str, str]]:
    mapping = _load_industry_codes(settings_path=settings_path)
    items: List[Tuple[str, str]]
    if query:
        needle = query.strip().lower()
        items = [(name, code) for name, code in mapping.items() if needle in name.lower()]
    else:
        items = list(mapping.items())
    items = sorted(items, key=lambda item: item[0])[: max(1, limit)]
    return [{"name": name, "code": code} for name, code in items]


def resolve_industry_label(industry: str, *, settings_path: Optional[str] = None) -> Dict[str, str]:
    mapping = _load_industry_codes(settings_path=settings_path)
    target = (industry or "").strip()
    if not target:
        raise ValueError("Industry name cannot be empty.")
    if target in mapping:
        return {"name": target, "code": mapping[target]}
    for name, code in mapping.items():
        if target.lower() == name.lower():
            return {"name": name, "code": code}
    raise ValueError(f"Industry '{industry}' is not available in the Eastmoney directory.")


__all__ = [
    "list_industry_directory",
    "search_industry_directory",
    "resolve_industry_label",
]
