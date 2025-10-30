"""
Shared AkShare helper utilities for report period and symbol handling.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

_REPORT_PERIOD_MONTH_DAY: tuple[tuple[int, int], ...] = (
    (3, 31),
    (6, 30),
    (9, 30),
    (12, 31),
)


def _coerce_date(value: object) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def latest_report_period(reference: date) -> date:
    """
    Return the most recent quarter-end date strictly before ``reference``.
    """
    candidates = [
        date(reference.year, month, day)
        for month, day in _REPORT_PERIOD_MONTH_DAY
        if date(reference.year, month, day) < reference
    ]
    if candidates:
        return max(candidates)
    previous_year = reference.year - 1
    return date(previous_year, 12, 31)


def resolve_report_period(value: Optional[object]) -> date:
    """
    Resolve an optional report period, defaulting to the latest completed quarter.
    """
    parsed = _coerce_date(value)
    if parsed:
        return parsed
    today = datetime.utcnow().date()
    return latest_report_period(today)


def normalize_symbol(value: object) -> Optional[str]:
    """
    Normalise raw symbol text to a zero-padded 6-digit form where applicable.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return text.zfill(6)
    return text


def symbol_to_ts_code(symbol: Optional[str]) -> Optional[str]:
    """
    Derive a Tushare-style ``ts_code`` from a six-digit symbol when possible.
    """
    normalized = normalize_symbol(symbol)
    if normalized is None:
        return None

    digits = normalized
    prefix = digits[0]

    if digits.startswith(("43", "83", "87")) or prefix in {"4", "8"}:
        suffix = "BJ"
    elif prefix in {"6", "9", "5"}:
        suffix = "SH"
    elif prefix in {"0", "2", "3"}:
        suffix = "SZ"
    else:
        return None

    return f"{digits}.{suffix}"


__all__ = ["latest_report_period", "resolve_report_period", "normalize_symbol", "symbol_to_ts_code"]
