"""
Shared in-memory state for sync jobs and progress tracking.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


@dataclass
class JobProgress:
    status: str = "idle"  # idle, running, success, failed
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    progress: float = 0.0
    message: Optional[str] = None
    total_rows: Optional[int] = None
    last_duration: Optional[float] = None
    last_market: Optional[str] = None
    error: Optional[str] = None


logger = logging.getLogger(__name__)

STATE_FILE = Path(__file__).resolve().parents[1] / "config" / "control_state.json"


class SyncMonitor:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state_file = STATE_FILE
        self._jobs: Dict[str, JobProgress] = {
            "stock_basic": JobProgress(),
            "daily_trade": JobProgress(),
            "daily_trade_metrics": JobProgress(),
            "daily_indicator": JobProgress(),
            "income_statement": JobProgress(),
            "financial_indicator": JobProgress(),
            "finance_breakfast": JobProgress(),
            "global_flash": JobProgress(),
            "global_flash_classification": JobProgress(),
            "trade_calendar": JobProgress(),
            "fundamental_metrics": JobProgress(),
            "performance_express": JobProgress(),
            "performance_forecast": JobProgress(),
            "profit_forecast": JobProgress(),
            "global_index": JobProgress(),
            "dollar_index": JobProgress(),
            "rmb_midpoint": JobProgress(),
            "futures_realtime": JobProgress(),
            "fed_statements": JobProgress(),
            "peripheral_aggregate": JobProgress(),
            "peripheral_insight": JobProgress(),
            "industry_fund_flow": JobProgress(),
            "concept_fund_flow": JobProgress(),
            "individual_fund_flow": JobProgress(),
            "big_deal_fund_flow": JobProgress(),
            "stock_main_business": JobProgress(),
            "stock_main_composition": JobProgress(),
            "leverage_ratio": JobProgress(),
            "social_financing": JobProgress(),
            "cpi_monthly": JobProgress(),
            "pmi_monthly": JobProgress(),
            "m2_monthly": JobProgress(),
            "ppi_monthly": JobProgress(),
            "pbc_rate": JobProgress(),
        }
        self._hydrate_from_disk()
        if not self._state_file.exists():
            with self._lock:
                self._persist_locked()

    def _get(self, job: str) -> JobProgress:
        if job not in self._jobs:
            self._jobs[job] = JobProgress()
        return self._jobs[job]

    def _hydrate_from_disk(self) -> None:
        try:
            raw = self._state_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except OSError as exc:
            logger.warning("Failed to read persisted control state: %s", exc)
            return

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("Invalid JSON in control state file: %s", exc)
            return

        if not isinstance(data, dict):
            return

        for name, payload in data.items():
            if name == "market_cap":
                key = "daily_indicator"
            elif name == "financial_report":
                key = "income_statement"
            else:
                key = name
            state = self._jobs.get(key)
            if not state or not isinstance(payload, dict):
                continue
            duration = payload.get("last_duration")
            if isinstance(duration, (int, float)):
                state.last_duration = float(duration)
            elif duration is None:
                state.last_duration = None

    def _persist_locked(self) -> None:
        snapshot = {}
        for name, state in self._jobs.items():
            snapshot[name] = {"last_duration": state.last_duration}

        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_file = self._state_file.with_suffix(".tmp")
            tmp_file.write_text(
                json.dumps(snapshot, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp_file.replace(self._state_file)
        except OSError as exc:
            logger.warning("Failed to persist control state: %s", exc)

    def start(self, job: str, *, message: Optional[str] = None) -> None:
        with self._lock:
            state = self._get(job)
            state.status = "running"
            state.started_at = datetime.utcnow()
            state.finished_at = None
            state.progress = 0.0
            state.message = message
            state.error = None
            state.total_rows = None
            state.last_duration = None
            state.last_market = None

    def update(
        self,
        job: str,
        *,
        progress: Optional[float] = None,
        message: Optional[str] = None,
        total_rows: Optional[int] = None,
        last_duration: Optional[float] = None,
        last_market: Optional[str] = None,
    ) -> None:
        with self._lock:
            state = self._get(job)
            should_persist = False
            if progress is not None:
                state.progress = max(0.0, min(1.0, progress))
            if message is not None:
                state.message = message
            if total_rows is not None:
                state.total_rows = total_rows
            if last_duration is not None:
                state.last_duration = last_duration
                should_persist = True
            if last_market is not None:
                state.last_market = last_market
            if should_persist:
                self._persist_locked()

    def finish(
        self,
        job: str,
        *,
        success: bool,
        total_rows: Optional[int] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
        finished_at: Optional[datetime] = None,
        last_duration: Optional[float] = None,
    ) -> None:
        with self._lock:
            state = self._get(job)
            state.status = "success" if success else "failed"
            completed_at = finished_at or datetime.utcnow()
            state.finished_at = completed_at
            if state.started_at is None:
                state.started_at = completed_at
            state.progress = 1.0 if success else state.progress
            if total_rows is not None:
                state.total_rows = total_rows
            if message is not None:
                state.message = message
            state.error = error
            if last_duration is not None:
                state.last_duration = last_duration
            elif state.started_at and state.finished_at:
                state.last_duration = (state.finished_at - state.started_at).total_seconds()
            self._persist_locked()

    def snapshot(self) -> Dict[str, Dict[str, Optional[str]]]:
        with self._lock:
            result: Dict[str, Dict[str, Optional[str]]] = {}
            for name, state in self._jobs.items():
                result[name] = {
                    "status": state.status,
                    "startedAt": state.started_at.isoformat() if state.started_at else None,
                    "finishedAt": state.finished_at.isoformat() if state.finished_at else None,
                    "progress": state.progress,
                    "message": state.message,
                    "totalRows": state.total_rows,
                    "lastDuration": state.last_duration,
                    "lastMarket": state.last_market,
                    "error": state.error,
                }
            return result


monitor = SyncMonitor()

__all__ = ["monitor", "SyncMonitor"]
