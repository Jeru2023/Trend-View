"""
Data access object for financial indicator data from Tushare ``fina_indicator``.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "financial_indicator_schema.sql"

FINANCIAL_INDICATOR_FIELDS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "end_date",
    "eps",
    "gross_margin",
    "current_ratio",
    "quick_ratio",
    "invturn_days",
    "arturn_days",
    "inv_turn",
    "ar_turn",
    "netprofit_margin",
    "grossprofit_margin",
    "profit_to_gr",
    "saleexp_to_gr",
    "adminexp_of_gr",
    "finaexp_of_gr",
    "roe",
    "q_eps",
    "q_netprofit_margin",
    "q_gsprofit_margin",
    "q_roe",
    "basic_eps_yoy",
    "op_yoy",
    "ebt_yoy",
    "netprofit_yoy",
    "q_sales_yoy",
    "q_sales_qoq",
    "q_op_yoy",
    "q_op_qoq",
    "q_profit_yoy",
    "q_profit_qoq",
)


class FinancialIndicatorDAO(PostgresDAOBase):
    """Handles persistence for financial indicator data."""

    _conflict_keys: Sequence[str] = ("ts_code", "end_date")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "financial_indicator_table", "financial_indicators")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=FINANCIAL_INDICATOR_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=("ann_date", "end_date"),
            )
        return affected

    def stats(self) -> dict[str, Optional[datetime]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, last_updated = cur.fetchone()
        return {"count": count or 0, "updated_at": last_updated}

    def latest_period_end(self) -> Optional[date]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT MAX(end_date) FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                latest = cur.fetchone()[0]
        return latest


__all__ = [
    "FINANCIAL_INDICATOR_FIELDS",
    "FinancialIndicatorDAO",
]
