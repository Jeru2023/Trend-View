"""
Service layer responsible for fetching and persisting daily trade data.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timedelta
from typing import Callable, Iterable, Sequence

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
    deleted_rows = daily_dao.delete_date_range(start_str, end_str)
    if deleted_rows:
        logger.info(
            "Removed %s existing rows in date range %s-%s",
            deleted_rows,
            start_str,
            end_str,
        )

    total_codes = len(code_list)
    logger.info("Total codes to download: %s", total_codes)

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    num_batches = math.ceil(total_codes / batch_size)
    logger.info("Processing in %s batches, %s codes per batch", num_batches, batch_size)

    pro_client = ts.pro_api(resolved_token)

    total_rows = 0

    for batch_index in range(num_batches):
        start_idx = batch_index * batch_size
        end_idx = min((batch_index + 1) * batch_size, total_codes)
        batch_codes = code_list[start_idx:end_idx]

        logger.info(
            "Processing batch %s/%s: codes %s to %s",
            batch_index + 1,
            num_batches,
            start_idx + 1,
            end_idx,
        )

        try:
            dataframe = get_daily_trade(
                pro=pro_client,
                code_list=batch_codes,
                start_date=start_str,
                end_date=end_str,
            )
        except Exception as exc:  # pragma: no cover - network errors
            logger.error("Error processing batch %s: %s", batch_index + 1, exc)
            continue

        if dataframe.empty:
            logger.warning("No data returned for batch %s", batch_index + 1)
        else:
            inserted = daily_dao.upsert(dataframe)
            logger.info(
                "Successfully upserted %s rows for batch %s", inserted, batch_index + 1
            )
            total_rows += inserted

        if batch_index < num_batches - 1 and batch_pause_seconds > 0:
            time.sleep(batch_pause_seconds)

        if progress_callback:
            progress_callback((batch_index + 1) / num_batches, f"Processed batch {batch_index + 1}/{num_batches}", total_rows)

    elapsed = time.perf_counter() - overall_start
    logger.info("All batches processed successfully in %.2f seconds.", elapsed)

    if progress_callback:
        progress_callback(1.0, "Daily trade sync completed", total_rows)

    return {"rows": total_rows, "elapsed_seconds": elapsed}


__all__ = [
    "sync_daily_trade",
]







