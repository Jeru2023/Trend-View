"""
Data access object for income statements fetched via Tushare ``pro.income``.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "income_statement_schema.sql"

INCOME_STATEMENT_FIELDS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "report_type",
    "comp_type",
    "basic_eps",
    "diluted_eps",
    "oper_exp",
    "total_revenue",
    "revenue",
    "operate_profit",
    "total_profit",
    "n_income",
    "ebitda",
)


class IncomeStatementDAO(PostgresDAOBase):
    """Handles persistence for income statement data."""

    _conflict_keys: Sequence[str] = ("ts_code", "end_date")

    def __init__(
        self,
        config: PostgresSettings,
        table_name: str | None = None,
    ) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "income_statement_table", "income_statements")
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
                columns=INCOME_STATEMENT_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=("ann_date", "f_ann_date", "end_date"),
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

    def fetch_latest_statements(self, codes: Sequence[str]) -> Dict[str, dict]:
        if not codes:
            return {}

        query = sql.SQL(
            """
            SELECT DISTINCT ON (ts_code)
                   ts_code,
                   ann_date,
                   end_date,
                   basic_eps,
                   revenue,
                   operate_profit,
                   n_income
            FROM {schema}.{table}
            WHERE ts_code = ANY(%s)
            ORDER BY ts_code, ann_date DESC, end_date DESC
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (list(codes),))
                rows = cur.fetchall()

        statements: Dict[str, dict] = {}
        for (
            ts_code,
            ann_date,
            end_date,
            basic_eps,
            revenue,
            operate_profit,
            n_income,
        ) in rows:
            statements[ts_code] = {
                "ann_date": ann_date,
                "end_date": end_date,
                "basic_eps": basic_eps,
                "revenue": revenue,
                "operate_profit": operate_profit,
                "n_income": n_income,
            }
        return statements


__all__ = [
    "INCOME_STATEMENT_FIELDS",
    "IncomeStatementDAO",
]
