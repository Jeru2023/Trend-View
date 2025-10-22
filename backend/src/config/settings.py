"""
Configuration loader for the Trend View backend.

Reads secrets and connection information from a JSON configuration file so the
rest of the application does not need to depend on hard-coded credentials.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


CONFIG_PATH_ENV_VAR = "TREND_VIEW_CONFIG_PATH"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.local.json"


@dataclass(frozen=True)
class TushareSettings:
    token: str


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str
    stock_table: str
    daily_indicator_table: str
    income_statement_table: str
    financial_indicator_table: str


@dataclass(frozen=True)
class AppSettings:
    tushare: TushareSettings
    postgres: PostgresSettings


def _resolve_config_path(explicit_path: Optional[str]) -> Path:
    """Return the path to the configuration file."""
    env_override = os.getenv(CONFIG_PATH_ENV_VAR)
    candidate = explicit_path or env_override
    if candidate:
        return Path(candidate).expanduser().resolve()
    return DEFAULT_CONFIG_PATH


def _load_raw_config(path: Path) -> Dict[str, Any]:
    """Load raw JSON content from the configuration file."""
    try:
        with path.open("r", encoding="utf-8") as config_file:
            return json.load(config_file)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Configuration file not found at {path}. "
            "Create it from the provided example file."
        ) from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON content in configuration file: {path}") from exc


def load_settings(path: Optional[str] = None) -> AppSettings:
    """
    Load application settings from disk.

    Args:
        path: Optional override for configuration file location. When omitted the
            loader checks the ``TREND_VIEW_CONFIG_PATH`` environment variable and
            falls back to ``config/settings.local.json``.

    Returns:
        Fully-populated ``AppSettings`` dataclass instance.
    """
    resolved_path = _resolve_config_path(path)
    raw_config = _load_raw_config(resolved_path)

    try:
        tushare_config = raw_config["tushare"]
    except KeyError as exc:
        raise KeyError("Missing 'tushare' section in configuration file") from exc

    try:
        postgres_config = raw_config["postgres"]
    except KeyError as exc:
        raise KeyError("Missing 'postgres' section in configuration file") from exc

    try:
        tushare_settings = TushareSettings(token=str(tushare_config["token"]))
    except KeyError as exc:
        raise KeyError("Missing 'tushare.token' in configuration file") from exc

    try:
        postgres_settings = PostgresSettings(
            host=str(postgres_config.get("host", "localhost")),
            port=int(postgres_config.get("port", 5432)),
            database=str(postgres_config["database"]),
            user=str(postgres_config["user"]),
            password=str(postgres_config["password"]),
            schema=str(postgres_config.get("schema", "public")),
            stock_table=str(postgres_config.get("stock_table", "stock_basic")),
            daily_indicator_table=str(
                postgres_config.get(
                    "daily_indicator_table",
                    postgres_config.get("market_cap_table", "daily_indicator"),
                )
            ),
            income_statement_table=str(
                postgres_config.get("income_statement_table", "income_statements")
            ),
            financial_indicator_table=str(
                postgres_config.get("financial_indicator_table", "financial_indicators")
            ),
        )
    except KeyError as exc:
        raise KeyError(f"Missing postgres configuration value: {exc}") from exc

    return AppSettings(
        tushare=tushare_settings,
        postgres=postgres_settings,
    )


__all__ = [
    "AppSettings",
    "PostgresSettings",
    "TushareSettings",
    "load_settings",
]

