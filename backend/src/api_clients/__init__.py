"""API client package exports."""

from .tushare_api import (
    DAILY_INDICATOR_FIELDS,
    DAILY_TRADE_FIELDS,
    DATE_COLUMNS,
    INCOME_STATEMENT_FIELDS,
    FINANCIAL_INDICATOR_FIELDS,
    STOCK_BASIC_FIELDS,
    fetch_stock_basic,
    get_daily_indicator,
    get_daily_trade,
    get_income_statements,
    get_financial_indicators,
)
from .akshare_api import (
    FINANCE_BREAKFAST_COLUMNS,
    PERFORMANCE_EXPRESS_COLUMN_MAP,
    PERFORMANCE_FORECAST_COLUMN_MAP,
    fetch_finance_breakfast,
    fetch_performance_express_em,
    fetch_performance_forecast_em,
)
from .eastmoney_news import EastmoneyNewsDetail, fetch_eastmoney_detail
from .deepseek_api import generate_finance_analysis

__all__ = [
    "DAILY_INDICATOR_FIELDS",
    "DAILY_TRADE_FIELDS",
    "DATE_COLUMNS",
    "INCOME_STATEMENT_FIELDS",
    "FINANCIAL_INDICATOR_FIELDS",
    "STOCK_BASIC_FIELDS",
    "fetch_stock_basic",
    "get_daily_indicator",
    "get_daily_trade",
    "get_income_statements",
    "get_financial_indicators",
    "PERFORMANCE_EXPRESS_COLUMN_MAP",
    "PERFORMANCE_FORECAST_COLUMN_MAP",
    "FINANCE_BREAKFAST_COLUMNS",
    "fetch_finance_breakfast",
    "fetch_performance_express_em",
    "fetch_performance_forecast_em",
    "EastmoneyNewsDetail",
    "fetch_eastmoney_detail",
    "generate_finance_analysis",
]
