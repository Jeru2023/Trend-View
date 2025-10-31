"""
Service for syncing AkShare profit forecast (盈利预测) data and exposing list APIs.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd

from ..api_clients import fetch_profit_forecast_em
from ..config.settings import load_settings
from ..dao import ProfitForecastDAO
from ..dao.profit_forecast_dao import PROFIT_FORECAST_FIELDS
from ._akshare_utils import normalize_symbol, symbol_to_ts_code

logger = logging.getLogger(__name__)

_NUMERIC_COLUMNS = (
    "report_count",
    "rating_buy",
    "rating_add",
    "rating_neutral",
    "rating_reduce",
    "rating_sell",
    "forecast_year",
    "forecast_eps",
    "row_number",
)


def _prepare_profit_forecast_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame(columns=list(PROFIT_FORECAST_FIELDS))

    frame = dataframe.copy()

    frame["symbol"] = frame["symbol"].apply(normalize_symbol)
    frame = frame.dropna(subset=["symbol"])

    for column in _NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["report_count"] = pd.to_numeric(frame.get("report_count"), errors="coerce").fillna(0).astype("Int64")
    frame["forecast_year"] = pd.to_numeric(frame.get("forecast_year"), errors="coerce").astype("Int64")
    frame["forecast_eps"] = frame.get("forecast_eps").astype(float)
    frame["stock_name"] = frame.get("stock_name").fillna("").astype(str).str.strip()

    frame["ts_code"] = frame["symbol"].apply(symbol_to_ts_code)

    ordered = frame.loc[:, list(PROFIT_FORECAST_FIELDS)].copy()
    ordered = ordered.dropna(subset=["forecast_year"])
    ordered = ordered.drop_duplicates(subset=["symbol", "forecast_year"])
    return ordered.reset_index(drop=True)


def sync_profit_forecast(
    symbol: Optional[str] = None,
    *,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    """
    Synchronise profit forecast data from AkShare into PostgreSQL.
    """
    started = time.perf_counter()
    settings = load_settings(settings_path)
    dao = ProfitForecastDAO(settings.postgres)

    raw_frame = fetch_profit_forecast_em(symbol)
    if raw_frame.empty:
        elapsed = time.perf_counter() - started
        return {
            "rows": 0,
            "codes": [],
            "codeCount": 0,
            "elapsedSeconds": elapsed,
            "years": [],
        }

    prepared = _prepare_profit_forecast_frame(raw_frame)
    if prepared.empty:
        elapsed = time.perf_counter() - started
        return {
            "rows": 0,
            "codes": [],
            "codeCount": 0,
            "elapsedSeconds": elapsed,
            "years": [],
        }

    symbols = prepared["symbol"].dropna().unique().tolist()
    years = sorted({int(year) for year in prepared["forecast_year"].dropna().unique().tolist() if year})

    if symbol is None or not str(symbol).strip():
        logger.info("Clearing existing profit forecast table before full refresh.")
        dao.clear_table()

    with dao.connect() as conn:
        dao.ensure_table(conn)
        if symbol and symbol.strip():
            dao.delete_symbols(symbols, conn=conn)
        affected = dao.upsert(prepared, conn=conn)
        conn.commit()

    elapsed = time.perf_counter() - started
    codes = prepared["ts_code"].dropna().unique().tolist() or symbols

    return {
        "rows": int(affected),
        "codes": codes[:10],
        "codeCount": len(codes),
        "elapsedSeconds": elapsed,
        "years": years,
    }


def list_profit_forecast(
    *,
    limit: int = 100,
    offset: int = 0,
    keyword: Optional[str] = None,
    industry: Optional[str] = None,
    forecast_year: Optional[int] = None,
    settings_path: Optional[str] = None,
) -> dict[str, object]:
    """
    Return paginated profit forecast rows with aggregated EPS projections.
    """
    settings = load_settings(settings_path)
    dao = ProfitForecastDAO(settings.postgres)
    result = dao.list_entries(
        limit=limit,
        offset=offset,
        keyword=keyword,
        industry=industry,
        forecast_year=forecast_year,
    )

    items = []
    industries = set()
    years = set(dao.list_years())

    for entry in result["items"]:
        industry_value = entry.get("industry")
        if industry_value:
            industries.add(industry_value)

        forecasts = [
            {
                "year": int(info.get("year")),
                "eps": float(info.get("eps")) if info.get("eps") is not None else None,
            }
            for info in entry.get("forecasts", [])
            if info and info.get("year") is not None
        ]
        for forecast in forecasts:
            years.add(forecast["year"])

        items.append(
            {
                "symbol": entry.get("symbol"),
                "code": entry.get("ts_code") or entry.get("symbol"),
                "tsCode": entry.get("ts_code"),
                "name": entry.get("name"),
                "industry": industry_value,
                "market": entry.get("market"),
                "reportCount": entry.get("report_count"),
                "ratings": {
                    "buy": entry.get("rating_buy"),
                    "add": entry.get("rating_add"),
                    "neutral": entry.get("rating_neutral"),
                    "reduce": entry.get("rating_reduce"),
                    "sell": entry.get("rating_sell"),
                },
                "forecasts": forecasts,
                "updatedAt": entry.get("updated_at"),
            }
        )

    return {
        "total": int(result.get("total", 0)),
        "items": items,
        "industries": sorted(industries),
        "years": sorted(years),
    }


__all__ = ["sync_profit_forecast", "list_profit_forecast", "_prepare_profit_forecast_frame"]
