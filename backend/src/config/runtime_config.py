"""
Runtime configuration helpers for control panel-driven settings.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

CONFIG_FILE = Path(__file__).resolve().parents[2] / "config" / "control_config.json"
_LOCK = threading.Lock()


@dataclass
class VolumeSurgeConfig:
    min_volume_ratio: float = 3.0
    breakout_threshold_percent: float = 3.0
    daily_change_threshold_percent: float = 7.0
    max_range_percent: float = 25.0

    @classmethod
    def from_dict(cls, data: Dict[str, Any] | None) -> "VolumeSurgeConfig":
        data = data or {}
        return cls(
            min_volume_ratio=_sanitize_float(data.get("min_volume_ratio"), default=3.0, minimum=0.5),
            breakout_threshold_percent=_sanitize_float(data.get("breakout_threshold_percent"), default=3.0, minimum=0.0),
            daily_change_threshold_percent=_sanitize_float(data.get("daily_change_threshold_percent"), default=7.0, minimum=0.0),
            max_range_percent=_sanitize_float(data.get("max_range_percent"), default=25.0, minimum=1.0),
        )

    def to_dict(self) -> Dict[str, float]:
        return {
            "min_volume_ratio": float(self.min_volume_ratio),
            "breakout_threshold_percent": float(self.breakout_threshold_percent),
            "daily_change_threshold_percent": float(self.daily_change_threshold_percent),
            "max_range_percent": float(self.max_range_percent),
        }


@dataclass
class RuntimeConfig:
    include_st: bool = False
    include_delisted: bool = False
    daily_trade_window_days: int = 420
    peripheral_aggregate_time: str = "06:00"
    global_flash_frequency_minutes: int = 180
    concept_alias_map: Dict[str, List[str]] = field(default_factory=dict)
    volume_surge_config: VolumeSurgeConfig = field(default_factory=VolumeSurgeConfig)

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
            concept_alias_map=normalize_concept_alias_map(data.get("concept_alias_map")),
            volume_surge_config=VolumeSurgeConfig.from_dict(
                data.get("volume_surge_config") or data.get("volume_surge")
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "include_st": self.include_st,
            "include_delisted": self.include_delisted,
            "daily_trade_window_days": self.daily_trade_window_days,
            "peripheral_aggregate_time": self.peripheral_aggregate_time,
            "global_flash_frequency_minutes": self.global_flash_frequency_minutes,
            "concept_alias_map": self.concept_alias_map,
            "volume_surge_config": self.volume_surge_config.to_dict(),
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


def normalize_concept_alias_map(raw_value: Any) -> Dict[str, List[str]]:
    """Convert arbitrary payload into a normalized concept alias mapping."""
    if not isinstance(raw_value, dict):
        return {}
    result: Dict[str, List[str]] = {}
    for key, value in raw_value.items():
        concept = str(key).strip()
        if not concept:
            continue
        if isinstance(value, str):
            candidates = value.split()
        elif isinstance(value, (list, tuple, set)):
            candidates = [str(item) for item in value]
        else:
            continue
        cleaned: List[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            alias = str(candidate).strip()
            if not alias:
                continue
            lower = alias.lower()
            if lower in seen:
                continue
            seen.add(lower)
            cleaned.append(alias)
        if cleaned:
            result[concept] = cleaned
    return result


def _sanitize_float(value: Any, *, default: float, minimum: float) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = default
    if numeric < minimum:
        numeric = minimum
    return numeric


__all__ = [
    "RuntimeConfig",
    "VolumeSurgeConfig",
    "load_runtime_config",
    "save_runtime_config",
    "normalize_concept_alias_map",
]
