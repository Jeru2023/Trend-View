"""
Data access object for the stock_basic table.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..api_clients.tushare_api import DATE_COLUMNS, STOCK_BASIC_FIELDS
from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "stock_basic_schema.sql"


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

    def clear_table(self) -> int:
        """Remove all rows from the stock_basic table."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DELETE FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self.config.stock_table),
                    )
                )
                return cur.rowcount or 0

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

    def stats(self) -> dict[str, Optional[datetime]]:
        """Return total row count and latest updated timestamp."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self.config.stock_table),
                    )
                )
                count, last_updated = cur.fetchone()
        return {
            "count": count or 0,
            "updated_at": last_updated,
        }

    def list_codes(self, list_statuses: Sequence[str] | None = ("L",)) -> List[str]:
        """
        Return a list of ``ts_code`` identifiers, optionally filtered by list status.
        """
        query = sql.SQL(
            "SELECT ts_code FROM {schema}.{table}"
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self.config.stock_table),
        )
        params: List[str] = []
        if list_statuses:
            placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in list_statuses)
            query += sql.SQL(" WHERE list_status IN ({statuses})").format(
                statuses=placeholders
            )
            params = list(list_statuses)
        query += sql.SQL(" ORDER BY ts_code")

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        return [row[0] for row in rows]

    def query_fundamentals(
        self,
        *,
        keyword: str | None = None,
        market: str | None = None,
        exchange: str | None = None,
        include_st: bool = True,
        include_delisted: bool = True,
        limit: int = 50,
        offset: int = 0,
        codes: Sequence[str] | None = None,
        filters: dict[str, object] | None = None,
    ) -> dict[str, object]:
        """
        Retrieve stock fundamentals with optional filtering and pagination.
        """
        select_base = sql.SQL(
            """
            SELECT ts_code, name, industry, market, exchange, list_status
            FROM {schema}.{table}
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self.config.stock_table),
        )
        count_base = sql.SQL(
            """
            SELECT COUNT(*)
            FROM {schema}.{table}
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self.config.stock_table),
        )

        conditions: list[sql.SQL] = []
        params: list[object] = []

        if keyword:
            like_value = f"%{keyword}%"
            conditions.append(
                sql.SQL(
                    "(ts_code ILIKE %s OR name ILIKE %s OR industry ILIKE %s OR "
                    "symbol ILIKE %s)"
                )
            )
            params.extend([like_value, like_value, like_value, like_value])

        if market and market.lower() != "all":
            conditions.append(sql.SQL("market = %s"))
            params.append(market)

        if exchange and exchange.lower() != "all":
            conditions.append(sql.SQL("exchange = %s"))
            params.append(exchange)

        if not include_delisted:
            exclude_statuses = ["D", "P"]
            placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in exclude_statuses)
            conditions.append(
                sql.SQL("list_status NOT IN ({statuses})").format(statuses=placeholders)
            )
            params.extend(exclude_statuses)

        if not include_st:
            for prefix in ("ST", "*ST"):
                conditions.append(sql.SQL("name NOT ILIKE %s"))
                params.append(f"{prefix}%")

        if codes:
            conditions.append(sql.SQL("ts_code = ANY(%s)"))
            params.append(list(codes))

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions)

        order_clause = sql.SQL(" ORDER BY ts_code")
        limit_clause: Optional[sql.SQL] = None
        query_params = params.copy()
        count_params = params.copy()
        if limit is not None and limit > 0:
            limit_clause = sql.SQL(" LIMIT %s OFFSET %s")
            query_params.extend([limit, offset])

        query = select_base + where_clause + order_clause
        if limit_clause is not None:
            query += limit_clause

        count_query = count_base + where_clause
        count_params = params.copy()

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, count_params)
                total = cur.fetchone()[0]
                cur.execute(query, query_params)
                rows = cur.fetchall()

        items = [
            {
                "code": row[0],
                "name": row[1],
                "industry": row[2],
                "market": row[3],
                "exchange": row[4],
                "status": row[5],
            }
            for row in rows
        ]
        return {"total": total, "items": items}


__all__ = [
    "StockBasicDAO",
]

