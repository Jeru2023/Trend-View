"""DAO for A-share index historical prices."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "index_history_schema.sql"

INDEX_HISTORY_FIELDS: tuple[str, ...] = (
    "index_code",
    "index_name",
    "trade_date",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "amount",
    "amplitude",
    "pct_change",
    "change_amount",
    "turnover",
)

DATE_COLUMNS: tuple[str, ...] = ("trade_date",)


class IndexHistoryDAO(PostgresDAOBase):
    """Persistence helper for index historical prices."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "index_history_table", "index_history")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            table_trade_date_idx=f"{self._table_name}_trade_date_idx",
        )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            available_columns = [column for column in INDEX_HISTORY_FIELDS if column in dataframe.columns]
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe.loc[:, available_columns],
                columns=available_columns,
                conflict_keys=("index_code", "trade_date"),
                date_columns=DATE_COLUMNS,
            )
        return affected

    def latest_trade_date(self, index_code: str) -> Optional[date]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT MAX(trade_date) FROM {schema}.{table} WHERE index_code = %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (index_code,),
                )
                row = cur.fetchone()
        if not row:
            return None
        latest = row[0]
        return latest if isinstance(latest, date) else None

    def list_history(
        self,
        *,
        index_code: str,
        limit: int = 500,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, object]]:
        conditions: List[sql.Composed] = [sql.SQL("index_code = %s")]
        params: List[object] = [index_code]
        if start_date is not None:
            conditions.append(sql.SQL("trade_date >= %s"))
            params.append(start_date)
        if end_date is not None:
            conditions.append(sql.SQL("trade_date <= %s"))
            params.append(end_date)

        where_clause = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions) if conditions else sql.SQL("")

        limit_value = max(1, min(int(limit), 2000))

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT index_code, index_name, trade_date, open, close, high, low, volume, amount, "
                        "amplitude, pct_change, change_amount, turnover "
                        "FROM {schema}.{table}"
                        "{where_clause} "
                        "ORDER BY trade_date DESC "
                        "LIMIT %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                        where_clause=where_clause,
                    ),
                    (*params, limit_value),
                )
                rows = cur.fetchall()

        columns = [
            "index_code",
            "index_name",
            "trade_date",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "amount",
            "amplitude",
            "pct_change",
            "change_amount",
            "turnover",
        ]
        return [{column: value for column, value in zip(columns, row)} for row in rows]

    def stats(self, index_code: Optional[str] = None) -> Dict[str, object]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                if index_code:
                    cur.execute(
                        sql.SQL(
                            "SELECT COUNT(*) AS total, MAX(trade_date) AS latest "
                            "FROM {schema}.{table} WHERE index_code = %s"
                        ).format(
                            schema=sql.Identifier(self.config.schema),
                            table=sql.Identifier(self._table_name),
                        ),
                        (index_code,),
                    )
                else:
                    cur.execute(
                        sql.SQL(
                            "SELECT COUNT(*) AS total, MAX(trade_date) AS latest FROM {schema}.{table}"
                        ).format(
                            schema=sql.Identifier(self.config.schema),
                            table=sql.Identifier(self._table_name),
                        )
                    )
                total, latest = cur.fetchone()
        return {
            "count": int(total or 0),
            "latest": latest,
        }


__all__ = ["IndexHistoryDAO"]
