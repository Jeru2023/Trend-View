"""Database access objects."""

from .base import PostgresDAOBase
from .daily_trade_dao import DailyTradeDAO
from .stock_basic_dao import StockBasicDAO

__all__ = [
    "DailyTradeDAO",
    "PostgresDAOBase",
    "StockBasicDAO",
]
