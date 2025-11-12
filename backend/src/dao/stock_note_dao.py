"""DAO for storing per-stock notes."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "stock_notes_schema.sql"


class StockNoteDAO(PostgresDAOBase):
    """Persistence helper for user-authored stock notes."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "stock_notes_table", "stock_notes")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            stock_code_idx=f"{self._table_name}_stock_code_idx",
        )

    def insert_note(
        self,
        stock_code: str,
        content: str,
        *,
        conn: Optional[PGConnection] = None,
    ) -> Dict[str, Any]:
        def _insert(connection: PGConnection) -> Dict[str, Any]:
            self.ensure_table(connection)
            with connection.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.{table} (stock_code, content, created_at, updated_at)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        RETURNING id, stock_code, content, created_at, updated_at
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code, content),
                )
                row = cur.fetchone()
            return {
                "id": row[0],
                "stock_code": row[1],
                "content": row[2],
                "created_at": row[3],
                "updated_at": row[4],
            }

        if conn is None:
            with self.connect() as owned_conn:
                return _insert(owned_conn)

        return _insert(conn)

    def list_notes(
        self,
        stock_code: str,
        *,
        limit: int,
        offset: int,
    ) -> Dict[str, Any]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {schema}.{table} WHERE stock_code = %s").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code,),
                )
                total_row = cur.fetchone()
                total = int(total_row[0] or 0) if total_row else 0

                cur.execute(
                    sql.SQL(
                        """
                        SELECT id, stock_code, content, created_at, updated_at
                        FROM {schema}.{table}
                        WHERE stock_code = %s
                        ORDER BY created_at DESC, id DESC
                        LIMIT %s OFFSET %s
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code, limit, offset),
                )
                rows = cur.fetchall()

        items = [
            {
                "id": row[0],
                "stock_code": row[1],
                "content": row[2],
                "created_at": row[3],
                "updated_at": row[4],
            }
            for row in rows
        ]
        return {"total": total, "items": items}

    def list_recent_notes(
        self,
        start_date: date,
        end_date: date,
        *,
        limit: int,
    ) -> Dict[str, Any]:
        sanitized_limit = max(1, min(limit, 500))
        query = sql.SQL(
            """
            SELECT id, stock_code, content, created_at, updated_at
            FROM {schema}.{table}
            WHERE created_at::date BETWEEN %s AND %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (start_date, end_date, sanitized_limit))
                rows = cur.fetchall()

        items = [
            {
                "id": row[0],
                "stock_code": row[1],
                "content": row[2],
                "created_at": row[3],
                "updated_at": row[4],
            }
            for row in rows
        ]
        return {"total": len(items), "items": items}


__all__ = ["StockNoteDAO"]
