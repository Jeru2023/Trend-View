"""
Helpers for interacting with AkShare endpoints.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
from contextlib import suppress
from queue import Empty
from typing import Final, Optional, Tuple

import pandas as pd

import akshare as ak

logger = logging.getLogger(__name__)

FINANCE_BREAKFAST_COLUMNS: Final[dict[str, str]] = {
    "标题": "title",
    "摘要": "summary",
    "发布时间": "published_at",
    "链接": "url",
}

_FINANCE_BREAKFAST_TIMEOUT_SECONDS: Final[float] = 12.0


def _empty_finance_breakfast_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the expected schema."""
    return pd.DataFrame(columns=FINANCE_BREAKFAST_COLUMNS.values())


def _finance_breakfast_worker(queue: mp.Queue) -> None:
    """
    Fetch the finance breakfast feed and send either the DataFrame or an error back.

    Executed in a separate process so that a hung network request cannot block the
    main application thread indefinitely.
    """
    try:
        dataframe = ak.stock_info_cjzc_em()
    except Exception as exc:  # pragma: no cover - external dependency
        with suppress(Exception):
            queue.put(("error", repr(exc)))
        return

    try:
        queue.put(("data", dataframe))
    except Exception as exc:  # pragma: no cover - defensive
        with suppress(Exception):
            queue.put(("error", repr(exc)))


def _run_with_timeout(timeout: float) -> Tuple[str, Optional[pd.DataFrame], Optional[str]]:
    """
    Run the AkShare fetch in a child process with a timeout.

    Returns a tuple (status, dataframe, error_message).
    """
    ctx = mp.get_context("spawn")
    queue: mp.Queue = ctx.Queue(maxsize=1)
    process = ctx.Process(target=_finance_breakfast_worker, args=(queue,))
    process.daemon = True
    process.start()

    try:
        process.join(timeout)
        if process.is_alive():
            process.terminate()
            process.join()
            return "timeout", None, f"Timed out after {timeout:.1f}s"

        status: str = "error"
        dataframe: Optional[pd.DataFrame] = None
        error_message: Optional[str] = None

        try:
            status, payload = queue.get(timeout=1.0)
        except Empty:
            status, payload = "error", None
        except Exception as exc:  # pragma: no cover - defensive
            status, payload = "error", repr(exc)

        if status == "data":
            dataframe = payload
            status = "ok"
        else:
            error_message = str(payload) if payload is not None else "Unknown AkShare error"

        return status, dataframe, error_message
    finally:
        queue.close()
        queue.join_thread()
        with suppress(Exception):
            process.close()


def fetch_finance_breakfast(timeout: float = _FINANCE_BREAKFAST_TIMEOUT_SECONDS) -> pd.DataFrame:
    """
    Fetch finance breakfast summaries from AkShare.
    """
    status, dataframe, error_message = _run_with_timeout(timeout)

    if status == "timeout":
        logger.error(
            "AkShare finance breakfast request exceeded %.1f seconds; skipping update.",
            timeout,
        )
        return _empty_finance_breakfast_frame()

    if status != "ok" or dataframe is None:
        logger.error(
            "Failed to fetch finance breakfast data from AkShare: %s",
            error_message,
        )
        return _empty_finance_breakfast_frame()

    if dataframe is None or dataframe.empty:
        logger.warning("AkShare returned no finance breakfast data.")
        return _empty_finance_breakfast_frame()

    renamed = dataframe.rename(columns=FINANCE_BREAKFAST_COLUMNS)
    for column in FINANCE_BREAKFAST_COLUMNS.values():
        if column not in renamed.columns:
            renamed[column] = None

    subset = renamed.loc[:, list(FINANCE_BREAKFAST_COLUMNS.values())]
    subset["published_at"] = pd.to_datetime(subset["published_at"], errors="coerce")
    return subset


__all__ = ["FINANCE_BREAKFAST_COLUMNS", "fetch_finance_breakfast"]
