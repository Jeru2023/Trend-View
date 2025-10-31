"""
DAO for historical global index quotes (e.g. Dollar Index).
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "dollar_index_schema.sql"

DOLLAR_INDEX_FIELDS: Sequence[str] = (
    "trade_date",
    "code",
    "name",
    "open_price",
    "close_price",
    "high_price",
    "low_price",
    "amplitude",
)


class DollarIndexDAO(PostgresDAOBase):
    """Persistence helper for AkShare global index historical data."""

    _conflict_keys: Sequence[str] = ("code", "trade_date")

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "dollar_index_table", "dollar_index_history")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (trade_date DESC)").format(
                    index=sql.Identifier(f"{self._table_name}_trade_date_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (updated_at DESC)").format(
                    index=sql.Identifier(f"{self._table_name}_updated_at_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert(self, dataframe: pd.DataFrame, *, conn: Optional[PGConnection] = None) -> int:
        if dataframe.empty:
            return 0

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=dataframe,
                    columns=DOLLAR_INDEX_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("trade_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=DOLLAR_INDEX_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("trade_date",),
        )

    def list_entries(
        self,
        *,
        limit: int = 200,
        offset: int = 0,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, object]:
        filters: List[sql.SQL] = []
        params: List[object] = []

        if start_date:
            filters.append(sql.SQL("trade_date >= %s"))
            params.append(start_date)
        if end_date:
            filters.append(sql.SQL("trade_date <= %s"))
            params.append(end_date)

        where_clause = sql.SQL("")
        if filters:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(filters)

        query = sql.SQL(
            """
            SELECT trade_date,
                   code,
                   name,
                   open_price,
                   close_price,
                   high_price,
                   low_price,
                   amplitude,
                   updated_at
            FROM {schema}.{table}
            {where_clause}
            ORDER BY trade_date DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        count_query = sql.SQL(
            """
            SELECT COUNT(*), MAX(updated_at)
            FROM {schema}.{table}
            {where_clause}
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, params)
                count_row = cur.fetchone()
                total = count_row[0] if count_row else 0
                last_updated = count_row[1] if count_row else None

                cur.execute(query, [*params, limit, offset])
                rows = cur.fetchall()

        items: List[Dict[str, object]] = []
        for (
            trade_date_value,
            code,
            name,
            open_price,
            close_price,
            high_price,
            low_price,
            amplitude,
            updated_at,
        ) in rows:
            items.append(
                {
                    "trade_date": trade_date_value,
                    "code": code,
                    "name": name,
                    "open_price": open_price,
                    "close_price": close_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "amplitude": amplitude,
                    "updated_at": updated_at,
                }
            )

        return {"total": total or 0, "items": items, "updated_at": last_updated}

    def stats(self) -> Dict[str, Optional[datetime]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, last_updated = cur.fetchone()
        return {"count": count or 0, "updated_at": last_updated}
