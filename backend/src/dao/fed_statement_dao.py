"""
DAO for persisted Federal Reserve press statements.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "fed_statement_schema.sql"

FED_STATEMENT_FIELDS: Sequence[str] = (
    "url",
    "title",
    "statement_date",
    "content",
    "raw_text",
    "position",
)


class FedStatementDAO(PostgresDAOBase):
    """Persistence helper for Federal Reserve press statements."""

    _conflict_keys: Sequence[str] = ("url",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "fed_statement_table", "fed_statements")
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
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (statement_date DESC, id DESC)"
                ).format(
                    index=sql.Identifier(f"{self._table_name}_statement_date_idx"),
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
                    columns=FED_STATEMENT_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("statement_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=FED_STATEMENT_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("statement_date",),
        )

    def list_entries(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, object]:
        query = sql.SQL(
            """
            SELECT id,
                   url,
                   title,
                   statement_date,
                   content,
                   raw_text,
                   position,
                   updated_at
            FROM {schema}.{table}
            ORDER BY
                COALESCE(statement_date, DATE '1900-01-01') DESC,
                COALESCE(position, 999999),
                id DESC
            LIMIT %s OFFSET %s
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        count_query = sql.SQL(
            "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}"
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query)
                total_row = cur.fetchone()
                total = total_row[0] if total_row else 0
                last_updated = total_row[1] if total_row else None

                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

        items: List[dict[str, object]] = []
        for row in rows:
            (
                pk,
                url,
                title,
                statement_date,
                content,
                raw_text,
                position,
                updated_at,
            ) = row
            items.append(
                {
                    "id": pk,
                    "url": url,
                    "title": title,
                    "statement_date": statement_date,
                    "content": content,
                    "raw_text": raw_text,
                    "position": position,
                    "updated_at": updated_at,
                }
            )

        return {"total": total or 0, "items": items, "updated_at": last_updated}

    def stats(self) -> Dict[str, Optional[datetime]]:
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

    def prune(self, max_records: int) -> int:
        if max_records <= 0:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        DELETE FROM {schema}.{table}
                        WHERE id NOT IN (
                            SELECT id
                            FROM {schema}.{table}
                            ORDER BY
                                COALESCE(statement_date, DATE '1900-01-01') DESC,
                                COALESCE(position, 999999),
                                id DESC
                            LIMIT %s
                        )
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (max_records,),
                )
                return cur.rowcount or 0


__all__ = ["FedStatementDAO"]
