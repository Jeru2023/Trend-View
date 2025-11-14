"""
Service layer responsible for fetching and persisting daily trade data.
"""

from __future__ import annotations

from collections import defaultdict
import logging
import time
from datetime import datetime, timedelta
from typing import Callable, Iterable, List

import pandas as pd

import tushare as ts

from ..api_clients import get_daily_trade
from ..config.runtime_config import load_runtime_config
from ..config.settings import AppSettings, load_settings
from ..dao import DailyTradeDAO, StockBasicDAO

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y%m%d"


def _resolve_token(token: str | None, settings: AppSettings) -> str:
    resolved = token or settings.tushare.token
    if not resolved:
        raise RuntimeError(
            "Tushare token is required. Update the configuration file or pass it explicitly."
        )
    return resolved


def _prepare_date_range(
    start_date: str | None,
    end_date: str | None,
    window_days: int,
) -> tuple[str, str]:
    today = datetime.now()
    end = (
        datetime.strptime(end_date, DATE_FORMAT)
        if end_date
        else today
    )
    start = (
        datetime.strptime(start_date, DATE_FORMAT)
        if start_date
        else end - timedelta(days=window_days)
    )
    return start.strftime(DATE_FORMAT), end.strftime(DATE_FORMAT)


def sync_daily_trade(
    token: str | None = None,
    *,
    batch_size: int = 20,
    window_days: int | None = None,
    progress_callback: Callable[[float, str | None, int | None], None] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    codes: Iterable[str] | None = None,
    settings_path: str | None = None,
    batch_pause_seconds: float = 0.6,
) -> dict[str, float | int]:
    """
    Fetch daily trade data from Tushare and upsert it into PostgreSQL.

    Returns summary statistics including total rows inserted and elapsed seconds.
    """
    overall_start = time.perf_counter()
    settings = load_settings(settings_path)
    runtime_config = load_runtime_config()
    window_days = window_days if window_days is not None else runtime_config.daily_trade_window_days
    resolved_token = _resolve_token(token, settings)

    if progress_callback:
        progress_callback(0.0, "Preparing daily trade sync", None)


    stock_basic_dao = StockBasicDAO(settings.postgres)
    if codes is None:
        code_list = stock_basic_dao.list_codes(list_statuses=("L",))
    else:
        code_list = list(dict.fromkeys(codes))

    if not code_list:
        logger.warning("No stock codes available to process.")
        return {"rows": 0, "elapsed_seconds": 0.0}

    start_str, end_str = _prepare_date_range(start_date, end_date, window_days)

    daily_dao = DailyTradeDAO(settings.postgres)

    total_codes = len(code_list)
    logger.info("Total codes considered for download: %s", total_codes)

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    start_dt = datetime.strptime(start_str, DATE_FORMAT).date()
    end_dt = datetime.strptime(end_str, DATE_FORMAT).date()

    latest_dates: dict[str, datetime | None] = {}
    if not start_date:
        latest_dates = daily_dao.latest_trade_dates_for_codes(code_list)

    code_groups: dict[str, List[str]] = defaultdict(list)
    codes_to_sync = 0
    for code in code_list:
        if start_date:
            fetch_start_dt = start_dt
        else:
            last_trade = latest_dates.get(code)
            if last_trade is None:
                fetch_start_dt = start_dt
            else:
                normalized = last_trade.date() if isinstance(last_trade, datetime) else last_trade
                fetch_start_dt = normalized + timedelta(days=1)

        if fetch_start_dt > end_dt:
            continue

        fetch_start_str = fetch_start_dt.strftime(DATE_FORMAT)
        code_groups[fetch_start_str].append(code)
        codes_to_sync += 1

    if codes_to_sync == 0:
        elapsed = time.perf_counter() - overall_start
        message = "Daily trade data already up to date; nothing to fetch."
        logger.info(message)
        if progress_callback:
            progress_callback(1.0, message, 0)
        return {"rows": 0, "elapsed_seconds": elapsed}

    skipped = total_codes - codes_to_sync
    if skipped > 0:
        logger.info("Skipping %s codes that are already up to date.", skipped)

    batch_plan: List[tuple[str, List[str]]] = []
    for fetch_start in sorted(code_groups.keys()):
        codes_for_start = code_groups[fetch_start]
        for idx in range(0, len(codes_for_start), batch_size):
            batch_plan.append((fetch_start, codes_for_start[idx : idx + batch_size]))

    total_batches = len(batch_plan)
    logger.info(
        "Processing %s batches (batch size %s) covering %s codes.",
        total_batches,
        batch_size,
        codes_to_sync,
    )

    pro_client = ts.pro_api(resolved_token)

    frames: List[pd.DataFrame] = []
    fetched_rows = 0

    for batch_index, (batch_start, batch_codes) in enumerate(batch_plan, start=1):
        logger.info(
            "Processing batch %s/%s (%s codes) for %s→%s",
            batch_index,
            total_batches,
            len(batch_codes),
            batch_start,
            end_str,
        )

        try:
            dataframe = get_daily_trade(
                pro=pro_client,
                code_list=batch_codes,
                start_date=batch_start,
                end_date=end_str,
            )
        except Exception as exc:  # pragma: no cover - network errors
            logger.error("Error processing batch %s: %s", batch_index, exc)
            continue

        if dataframe.empty:
            logger.warning("No data returned for batch %s", batch_index)
        else:
            dataframe = dataframe.drop_duplicates(subset=["ts_code", "trade_date"])
            frames.append(dataframe)
            fetched_rows += len(dataframe.index)
            logger.info("Fetched %s rows for batch %s", len(dataframe.index), batch_index)

        if batch_index < total_batches and batch_pause_seconds > 0:
            time.sleep(batch_pause_seconds)

        if progress_callback:
            progress_callback(
                batch_index / total_batches,
                f"Processed batch {batch_index}/{total_batches}",
                fetched_rows,
            )

    elapsed = time.perf_counter() - overall_start

    if not frames:
        message = "No daily trade data retrieved."
        logger.warning(message)
        if progress_callback:
            progress_callback(1.0, message, 0)
        return {"rows": 0, "elapsed_seconds": elapsed}

    combined = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["ts_code", "trade_date"])
        .sort_values(["ts_code", "trade_date"])
    )

    inserted = daily_dao.upsert(combined)
    logger.info("Upsert completed, affected rows: %s", inserted)

    if progress_callback:
        progress_callback(1.0, "Daily trade sync completed", inserted)

    return {"rows": inserted, "elapsed_seconds": elapsed}


__all__ = [
    "sync_daily_trade",
]






