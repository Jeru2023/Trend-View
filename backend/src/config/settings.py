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
DEFAULT_APPLICATION_NAME = "trend_view_backend"


@dataclass(frozen=True)
class TushareSettings:
    token: str


@dataclass(frozen=True)
class DeepseekSettings:
    token: str
    base_url: str = "https://api.deepseek.com"
    model: str = "deepseek-chat"
    request_timeout_seconds: float = 90.0
    max_retries: int = 2


@dataclass(frozen=True)
class CozeSettings:
    token: str
    bot_id: str
    user_id: str = "trend-view"
    base_url: str = "https://api.coze.com"
    conversation_id: Optional[str] = None
    request_timeout_seconds: float = 90.0


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
    daily_trade_metrics_table: str
    income_statement_table: str
    financial_indicator_table: str
    finance_breakfast_table: str
    global_flash_table: str
    trade_calendar_table: str
    news_articles_table: str
    news_insights_table: str
    news_market_insight_table: str
    news_sector_insight_table: str
    index_history_table: str
    fundamental_metrics_table: str
    favorites_table: str
    stock_main_business_table: str
    stock_main_composition_table: str
    performance_express_table: str
    performance_forecast_table: str
    profit_forecast_table: str
    global_index_history_table: str
    realtime_index_table: str
    industry_fund_flow_table: str
    concept_fund_flow_table: str
    concept_index_history_table: str
    concept_insight_table: str
    concept_watchlist_table: str
    industry_insight_table: str
    individual_fund_flow_table: str
    big_deal_fund_flow_table: str
    stock_integrated_analysis_table: str
    intraday_volume_profile_daily_table: str
    intraday_volume_profile_avg_table: str
    stock_notes_table: str
    investment_journal_table: str
    indicator_screening_table: str
    hsgt_fund_flow_table: str
    peripheral_insight_table: str
    leverage_ratio_table: str
    social_financing_table: str
    cpi_table: str
    pmi_table: str
    m2_table: str
    ppi_table: str
    lpr_table: str
    shibor_table: str
    connect_timeout: int = 3
    application_name: str = DEFAULT_APPLICATION_NAME
    statement_timeout_ms: Optional[int] = None
    idle_in_transaction_session_timeout_ms: Optional[int] = None


@dataclass(frozen=True)
class AppSettings:
    tushare: TushareSettings
    deepseek: Optional[DeepseekSettings]
    coze: Optional[CozeSettings]
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
        with path.open("r", encoding="utf-8-sig") as config_file:
            return json.load(config_file)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Configuration file not found at {path}. "
            "Create it from the provided example file."
        ) from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON content in configuration file: {path}") from exc


def _optional_int(value: Any) -> Optional[int]:
    """Convert optional JSON number fields to integers."""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return int(value)


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

    deepseek_config: Optional[dict[str, Any]] = raw_config.get("deepseek")
    deepseek_settings: Optional[DeepseekSettings] = None
    if deepseek_config:
        try:
            deepseek_settings = DeepseekSettings(
                token=str(deepseek_config["token"]),
                base_url=str(deepseek_config.get("base_url", "https://api.deepseek.com")),
                model=str(deepseek_config.get("model", "deepseek-chat")),
                request_timeout_seconds=float(deepseek_config.get("request_timeout_seconds", 90.0)),
                max_retries=int(deepseek_config.get("max_retries", 2)),
            )
        except KeyError as exc:
            raise KeyError("Missing 'deepseek.token' in configuration file") from exc

    coze_config: Optional[dict[str, Any]] = raw_config.get("coze")
    coze_settings: Optional[CozeSettings] = None
    if coze_config:
        try:
            coze_settings = CozeSettings(
                token=str(coze_config["token"]),
                bot_id=str(coze_config["bot_id"]),
                user_id=str(coze_config.get("user_id", "trend-view")),
                base_url=str(coze_config.get("base_url", "https://api.coze.com")),
                conversation_id=coze_config.get("conversation_id"),
                request_timeout_seconds=float(coze_config.get("request_timeout_seconds", 90.0)),
            )
        except KeyError as exc:
            raise KeyError("Missing 'coze.token' or 'coze.bot_id' in configuration file") from exc

    try:
        application_name = postgres_config.get("application_name")
        statement_timeout_value = postgres_config.get("statement_timeout_ms")
        idle_timeout_value = postgres_config.get("idle_in_transaction_session_timeout_ms")

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
            daily_trade_metrics_table=str(
                postgres_config.get("daily_trade_metrics_table", "daily_trade_metrics")
            ),
            income_statement_table=str(
                postgres_config.get("income_statement_table", "income_statements")
            ),
            financial_indicator_table=str(
                postgres_config.get("financial_indicator_table", "financial_indicators")
            ),
            finance_breakfast_table=str(
                postgres_config.get("finance_breakfast_table", "finance_breakfast")
            ),
            global_flash_table=str(
                postgres_config.get("global_flash_table", "global_flash")
            ),
            trade_calendar_table=str(
                postgres_config.get("trade_calendar_table", "trade_calendar")
            ),
            news_articles_table=str(
                postgres_config.get("news_articles_table", "news_articles")
            ),
            news_insights_table=str(
                postgres_config.get("news_insights_table", "news_insights")
            ),
            news_market_insight_table=str(
                postgres_config.get("news_market_insight_table", "news_market_insights")
            ),
            news_sector_insight_table=str(
                postgres_config.get("news_sector_insight_table", "news_sector_insights")
            ),
            index_history_table=str(
                postgres_config.get("index_history_table", "index_history")
            ),
            fundamental_metrics_table=str(
                postgres_config.get("fundamental_metrics_table", "fundamental_metrics")
            ),
            favorites_table=str(
                postgres_config.get("favorites_table", "stock_favorites")
            ),
            stock_main_business_table=str(
                postgres_config.get("stock_main_business_table", "stock_main_business")
            ),
            stock_main_composition_table=str(
                postgres_config.get("stock_main_composition_table", "stock_main_composition")
            ),
            performance_express_table=str(
                postgres_config.get("performance_express_table", "performance_express")
            ),
            performance_forecast_table=str(
                postgres_config.get("performance_forecast_table", "performance_forecast")
            ),
            profit_forecast_table=str(
                postgres_config.get("profit_forecast_table", "profit_forecast")
            ),
            global_index_history_table=str(
                postgres_config.get("global_index_history_table", "global_index_history")
            ),
            realtime_index_table=str(
                postgres_config.get("realtime_index_table", "realtime_indices")
            ),
            industry_fund_flow_table=str(
                postgres_config.get("industry_fund_flow_table", "industry_fund_flow")
            ),
            concept_fund_flow_table=str(
                postgres_config.get("concept_fund_flow_table", "concept_fund_flow")
            ),
            concept_index_history_table=str(
                postgres_config.get("concept_index_history_table", "concept_index_history")
            ),
            concept_insight_table=str(
                postgres_config.get("concept_insight_table", "concept_insights")
            ),
            concept_watchlist_table=str(
                postgres_config.get("concept_watchlist_table", "concept_watchlist")
            ),
            industry_insight_table=str(
                postgres_config.get("industry_insight_table", "industry_insights")
            ),
            individual_fund_flow_table=str(
                postgres_config.get("individual_fund_flow_table", "individual_fund_flow")
            ),
            big_deal_fund_flow_table=str(
                postgres_config.get("big_deal_fund_flow_table", "big_deal_fund_flow")
            ),
            stock_integrated_analysis_table=str(
                postgres_config.get("stock_integrated_analysis_table", "stock_integrated_analysis")
            ),
            intraday_volume_profile_daily_table=str(
                postgres_config.get("intraday_volume_profile_daily_table", "intraday_volume_profile_daily")
            ),
            intraday_volume_profile_avg_table=str(
                postgres_config.get("intraday_volume_profile_avg_table", "intraday_volume_profile_avg")
            ),
            stock_notes_table=str(
                postgres_config.get("stock_notes_table", "stock_notes")
            ),
            investment_journal_table=str(
                postgres_config.get("investment_journal_table", "investment_journal")
            ),
            indicator_screening_table=str(
                postgres_config.get("indicator_screening_table", "indicator_screening")
            ),
            hsgt_fund_flow_table=str(
                postgres_config.get("hsgt_fund_flow_table", "hsgt_fund_flow")
            ),
            peripheral_insight_table=str(
                postgres_config.get("peripheral_insight_table", "peripheral_insights")
            ),
            leverage_ratio_table=str(
                postgres_config.get("leverage_ratio_table", "macro_leverage_ratio")
            ),
            social_financing_table=str(
                postgres_config.get("social_financing_table", "macro_social_financing")
            ),
            cpi_table=str(
                postgres_config.get("cpi_table", "macro_cpi_monthly")
            ),
            pmi_table=str(
                postgres_config.get("pmi_table", "macro_pmi")
            ),
            m2_table=str(
                postgres_config.get("m2_table", "macro_m2")
            ),
            ppi_table=str(
                postgres_config.get("ppi_table", "macro_ppi")
            ),
            lpr_table=str(
                postgres_config.get("lpr_table")
                or postgres_config.get("pbc_rate_table")
                or "macro_pbc_rate"
            ),
            shibor_table=str(
                postgres_config.get("shibor_table", "macro_shibor")
            ),
            connect_timeout=int(postgres_config.get("connect_timeout", 3)),
            application_name=str(application_name).strip() if isinstance(application_name, str) and application_name.strip() else DEFAULT_APPLICATION_NAME,
            statement_timeout_ms=_optional_int(statement_timeout_value),
            idle_in_transaction_session_timeout_ms=_optional_int(idle_timeout_value),
        )
    except KeyError as exc:
        raise KeyError(f"Missing postgres configuration value: {exc}") from exc

    return AppSettings(
        tushare=tushare_settings,
        deepseek=deepseek_settings,
        coze=coze_settings,
        postgres=postgres_settings,
    )


__all__ = [
    "AppSettings",
    "PostgresSettings",
    "TushareSettings",
    "DeepseekSettings",
    "CozeSettings",
    "load_settings",
]
