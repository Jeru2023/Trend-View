"""
Runtime configuration helpers for control panel-driven settings.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

CONFIG_FILE = Path(__file__).resolve().parents[2] / "config" / "control_config.json"
_LOCK = threading.Lock()


@dataclass
class RuntimeConfig:
    include_st: bool = False
    include_delisted: bool = False
    daily_trade_window_days: int = 420
    peripheral_aggregate_time: str = "06:00"
    global_flash_frequency_minutes: int = 180

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuntimeConfig":
        time_value = str(data.get("peripheral_aggregate_time", "06:00"))
        if not _validate_time_string(time_value):
            time_value = "06:00"
        frequency_value = int(data.get("global_flash_frequency_minutes", 180))
        if frequency_value <= 0:
            frequency_value = 180

        return cls(
            include_st=bool(data.get("include_st", False)),
            include_delisted=bool(data.get("include_delisted", False)),
            daily_trade_window_days=int(data.get("daily_trade_window_days", 420)),
            peripheral_aggregate_time=time_value,
            global_flash_frequency_minutes=frequency_value,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "include_st": self.include_st,
            "include_delisted": self.include_delisted,
            "daily_trade_window_days": self.daily_trade_window_days,
            "peripheral_aggregate_time": self.peripheral_aggregate_time,
            "global_flash_frequency_minutes": self.global_flash_frequency_minutes,
        }


def _validate_time_string(value: str) -> bool:
    if not isinstance(value, str):
        return False
    parts = value.split(":")
    if len(parts) != 2:
        return False
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return False
    return 0 <= hour <= 23 and 0 <= minute <= 59


def load_runtime_config() -> RuntimeConfig:
    with _LOCK:
        if CONFIG_FILE.exists():
            with CONFIG_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return RuntimeConfig.from_dict(data)
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        config = RuntimeConfig()
        save_runtime_config(config)
        return config


def save_runtime_config(config: RuntimeConfig) -> None:
    with _LOCK:
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with CONFIG_FILE.open("w", encoding="utf-8") as f:
            json.dump(config.to_dict(), f, ensure_ascii=False, indent=2)


__all__ = ["RuntimeConfig", "load_runtime_config", "save_runtime_config"]
