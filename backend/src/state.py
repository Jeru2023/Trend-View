"""
Shared in-memory state for sync jobs and progress tracking.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
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


class SyncMonitor:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: Dict[str, JobProgress] = {
            "stock_basic": JobProgress(),
            "daily_trade": JobProgress(),
        }

    def _get(self, job: str) -> JobProgress:
        if job not in self._jobs:
            self._jobs[job] = JobProgress()
        return self._jobs[job]

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
            if progress is not None:
                state.progress = max(0.0, min(1.0, progress))
            if message is not None:
                state.message = message
            if total_rows is not None:
                state.total_rows = total_rows
            if last_duration is not None:
                state.last_duration = last_duration
            if last_market is not None:
                state.last_market = last_market

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





