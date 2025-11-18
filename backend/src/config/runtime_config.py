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
class ObservationStrategyConfig:
    lookback_days: int = 60
    min_history: int = 45
    breakout_buffer_percent: float = 0.5
    max_range_percent: float = 15.0
    volume_ratio_threshold: float = 2.0
    volume_average_window: int = 20
    max_weekly_gain_percent: float = 3.0
    require_big_deal_inflow: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any] | None) -> "ObservationStrategyConfig":
        data = data or {}
        return cls(
            lookback_days=_sanitize_int(data.get("lookback_days") or data.get("lookbackDays"), default=60, minimum=20),
            min_history=_sanitize_int(data.get("min_history") or data.get("minHistory"), default=45, minimum=10),
            breakout_buffer_percent=_sanitize_float(
                data.get("breakout_buffer_percent") or data.get("breakoutBufferPercent"),
                default=0.5,
                minimum=0.0,
            ),
            max_range_percent=_sanitize_float(
                data.get("max_range_percent") or data.get("maxRangePercent"),
                default=15.0,
                minimum=1.0,
            ),
            volume_ratio_threshold=_sanitize_float(
                data.get("volume_ratio_threshold") or data.get("volumeRatioThreshold"),
                default=2.0,
                minimum=0.5,
            ),
            volume_average_window=_sanitize_int(
                data.get("volume_average_window") or data.get("volumeAverageWindow"),
                default=20,
                minimum=5,
            ),
            max_weekly_gain_percent=_sanitize_float(
                data.get("max_weekly_gain_percent") or data.get("maxWeeklyGainPercent"),
                default=3.0,
                minimum=0.0,
            ),
            require_big_deal_inflow=_sanitize_bool(
                data.get("require_big_deal_inflow") if "require_big_deal_inflow" in data else data.get("requireBigDealInflow"),
                default=False,
            ),
        )

    def to_dict(self) -> Dict[str, float]:
        return {
            "lookback_days": int(self.lookback_days),
            "min_history": int(self.min_history),
            "breakout_buffer_percent": float(self.breakout_buffer_percent),
            "max_range_percent": float(self.max_range_percent),
            "volume_ratio_threshold": float(self.volume_ratio_threshold),
            "volume_average_window": int(self.volume_average_window),
            "max_weekly_gain_percent": float(self.max_weekly_gain_percent),
            "require_big_deal_inflow": bool(self.require_big_deal_inflow),
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
    observation_strategy_config: ObservationStrategyConfig = field(default_factory=ObservationStrategyConfig)

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
            observation_strategy_config=ObservationStrategyConfig.from_dict(
                data.get("observation_pool") or data.get("observation_strategy_config")
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
            "observation_pool": self.observation_strategy_config.to_dict(),
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
        concept_aliases: List[str] = []
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
        # Automatically add stripped alias for concepts ending with “概念”
        if concept.endswith("概念"):
            stripped = concept[:-2].strip()
            if stripped:
                concept_aliases.append(stripped)
        concept_aliases.extend(cleaned)
        # Deduplicate with case-insensitive tracking
        deduped: List[str] = []
        seen_lower: set[str] = set()
        for alias in concept_aliases:
            lower = alias.lower()
            if lower in seen_lower:
                continue
            seen_lower.add(lower)
            deduped.append(alias)
        if deduped:
            result[concept] = deduped
    return result


def _sanitize_float(value: Any, *, default: float, minimum: float) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = default
    if numeric < minimum:
        numeric = minimum
    return numeric


def _sanitize_int(value: Any, *, default: int, minimum: int) -> int:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        numeric = default
    if numeric < minimum:
        numeric = minimum
    return numeric


def _sanitize_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return default


__all__ = [
    "RuntimeConfig",
    "VolumeSurgeConfig",
    "ObservationStrategyConfig",
    "load_runtime_config",
    "save_runtime_config",
    "normalize_concept_alias_map",
]
