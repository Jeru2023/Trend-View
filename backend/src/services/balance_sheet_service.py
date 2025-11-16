"""Sync and query stock balance sheet statements."""

from __future__ import annotations

import logging
import time
from typing import Iterable, List, Optional

import pandas as pd
import tushare as ts

from ..api_clients import BALANCE_SHEET_FIELDS, fetch_stock_basic, get_balance_sheets
from ..config.settings import load_settings
from ..dao import BalanceSheetDAO, StockBasicDAO

logger = logging.getLogger(__name__)

INITIAL_PERIOD_COUNT = 32
RATE_LIMIT_PER_MINUTE = 180
MAX_FETCH_RETRIES = 3

DATE_COLUMNS = ("ann_date", "end_date")


def _resolve_token(settings, token: Optional[str]) -> str:
    resolved = token or settings.tushare.token
    if not resolved:
        raise RuntimeError("Tushare token is required to sync balance sheet statements.")
    return resolved


def _ensure_codes(stock_dao: StockBasicDAO, token: str, codes: Optional[Iterable[str]]) -> List[str]:
    normalized = []
    seen = set()
    if codes:
        for code in codes:
            if code and code not in seen:
                seen.add(code)
                normalized.append(code)
    if normalized:
        return normalized

    fallback = stock_dao.list_codes()
    if fallback:
        return fallback

    try:
        frame = fetch_stock_basic(token)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to fetch stock basics for balance sheet sync: %s", exc)
        return []
    if frame is None or frame.empty:
        return []
    stock_dao.upsert(frame)
    return [code for code in frame["ts_code"].dropna().unique().tolist() if code]


class _RateLimiter:
    def __init__(self, per_minute: int) -> None:
        self._min_interval = 60.0 / max(1, per_minute)
        self._last_call: Optional[float] = None

    def wait(self) -> None:
        now = time.perf_counter()
        if self._last_call is not None:
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
                now = time.perf_counter()
        self._last_call = now


def _prepare_balance_sheet_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    prepared = frame.loc[:, list(BALANCE_SHEET_FIELDS)].copy()
    for column in DATE_COLUMNS:
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(prepared[column], errors="coerce").dt.date
    numeric_columns = [col for col in BALANCE_SHEET_FIELDS if col not in ("ts_code", "ann_date", "end_date")]
    for column in numeric_columns:
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    prepared = prepared.drop_duplicates(subset=["ts_code", "ann_date", "end_date"], keep="last")
    return prepared.reset_index(drop=True)


def sync_balance_sheets(
    *,
    token: Optional[str] = None,
    codes: Optional[Iterable[str]] = None,
    limit: int = INITIAL_PERIOD_COUNT,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    resolved_token = _resolve_token(settings, token)
    stock_dao = StockBasicDAO(settings.postgres)
    dao = BalanceSheetDAO(settings.postgres)
    pro_client = ts.pro_api(resolved_token)

    rate_limiter = _RateLimiter(RATE_LIMIT_PER_MINUTE)
    target_codes = _ensure_codes(stock_dao, resolved_token, codes)

    if not target_codes:
        return {"rows": 0, "codes": [], "codeCount": 0, "elapsedSeconds": 0.0}

    affected_rows = 0
    processed_codes: list[str] = []
    started = time.perf_counter()

    for idx, code in enumerate(target_codes, start=1):
        rate_limiter.wait()
        frame = pd.DataFrame(columns=list(BALANCE_SHEET_FIELDS))
        for attempt in range(1, MAX_FETCH_RETRIES + 1):
            try:
                frame = get_balance_sheets(pro_client, ts_code=code, limit=limit)
                break
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Balance sheet fetch failed for %s (attempt %s/%s): %s", code, attempt, MAX_FETCH_RETRIES, exc)
                time.sleep(min(8, attempt))
        if frame.empty:
            continue
        prepared = _prepare_balance_sheet_frame(frame)
        if prepared.empty:
            continue
        affected_rows += dao.upsert(prepared)
        processed_codes.append(code)

    elapsed = time.perf_counter() - started
    return {
        "rows": affected_rows,
        "codes": processed_codes[:10],
        "codeCount": len(processed_codes),
        "elapsedSeconds": elapsed,
    }


def list_balance_sheets(
    *,
    limit: int = 50,
    offset: int = 0,
    keyword: Optional[str] = None,
    ts_code: Optional[str] = None,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    settings = load_settings(settings_path)
    dao = BalanceSheetDAO(settings.postgres)
    result = dao.list_entries(limit=limit, offset=offset, keyword=keyword, ts_code=ts_code)
    items = []
    for entry in result.get("items", []):
        items.append(
            {
                "tsCode": entry.get("ts_code"),
                "name": entry.get("name"),
                "industry": entry.get("industry"),
                "market": entry.get("market"),
                "annDate": entry.get("ann_date"),
                "endDate": entry.get("end_date"),
                "moneyCap": entry.get("money_cap"),
                "accountsReceiv": entry.get("accounts_receiv"),
                "inventories": entry.get("inventories"),
                "fixAssets": entry.get("fix_assets"),
                "totalCurAssets": entry.get("total_cur_assets"),
                "totalNca": entry.get("total_nca"),
                "totalAssets": entry.get("total_assets"),
                "stBorr": entry.get("st_borr"),
                "ltBorr": entry.get("lt_borr"),
                "acctPayable": entry.get("acct_payable"),
                "totalCurLiab": entry.get("total_cur_liab"),
                "totalNcl": entry.get("total_ncl"),
                "totalLiab": entry.get("total_liab"),
                "totalShare": entry.get("total_share"),
                "capRese": entry.get("cap_rese"),
                "surplusRese": entry.get("surplus_rese"),
                "undistrPorfit": entry.get("undistr_porfit"),
                "totalHldrEqyExcMinInt": entry.get("total_hldr_eqy_exc_min_int"),
                "totalLiabHldrEqy": entry.get("total_liab_hldr_eqy"),
                "updatedAt": entry.get("updated_at"),
            }
        )
    return {
        "total": int(result.get("total", 0)),
        "items": items,
    }


__all__ = ["sync_balance_sheets", "list_balance_sheets"]
