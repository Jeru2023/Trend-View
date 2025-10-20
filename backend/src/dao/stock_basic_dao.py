"""
Data access object for the stock_basic table.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd

from ..api_clients.tushare_api import DATE_COLUMNS, STOCK_BASIC_FIELDS
from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "schema.sql"


class StockBasicDAO(PostgresDAOBase):
    """Handles persistence of stock basic data."""

    _conflict_keys: Sequence[str] = ("ts_code",)

    def __init__(self, config: PostgresSettings) -> None:
        super().__init__(config=config)
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        """Ensure the destination table exists."""
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self.config.stock_table,
        )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        """Synchronise the provided DataFrame into the stock_basic table."""
        with self.connect() as conn:
            self.ensure_table(conn)
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self.config.stock_table,
                dataframe=dataframe,
                columns=STOCK_BASIC_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=DATE_COLUMNS,
            )
        return affected


__all__ = [
    "StockBasicDAO",
]
