"""Database access objects."""

from .base import PostgresDAOBase
from .stock_basic_dao import StockBasicDAO

__all__ = [
    "PostgresDAOBase",
    "StockBasicDAO",
]
