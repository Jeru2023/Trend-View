"""
Data access object for managing the stock favorites list.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "favorite_stocks_schema.sql"


class FavoriteStockDAO(PostgresDAOBase):
    """Persist and query favorite stock codes."""

    def __init__(self, config: PostgresSettings) -> None:
        super().__init__(config=config)
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def _qualified_table(self) -> sql.Composed:
        return sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self.config.favorites_table),
        )

    def ensure_table(self, conn) -> None:
        """Create the favorites table when missing and align latest schema."""
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self.config.favorites_table,
        )
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS group_name TEXT"
                ).format(table=self._qualified_table())
            )

    def list_entries(self, group: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return favorite records ordered by most recent activity."""
        with self.connect() as conn:
            self.ensure_table(conn)
            query = [
                sql.SQL(
                    "SELECT ts_code, group_name, created_at, updated_at FROM {table}"
                ).format(table=self._qualified_table())
            ]
            params: List[Any] = []
            if group is not None:
                if group == "":
                    query.append(sql.SQL(" WHERE group_name IS NULL"))
                else:
                    query.append(sql.SQL(" WHERE group_name = %s"))
                    params.append(group)
            query.append(sql.SQL(" ORDER BY updated_at DESC"))
            with conn.cursor() as cur:
                cur.execute(sql.Composed(query), params)
                rows = cur.fetchall()
        return [
            {
                "code": row[0],
                "group": row[1],
                "created_at": row[2],
                "updated_at": row[3],
            }
            for row in rows
        ]

    def list_codes(self, group: Optional[str] = None) -> List[str]:
        """Return all favorite stock codes ordered by most recently updated."""
        return [entry["code"] for entry in self.list_entries(group=group)]

    def get_entry(self, code: str) -> Optional[Dict[str, Any]]:
        """Return the favorite entry for the given code, if any."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT ts_code, group_name, created_at, updated_at FROM {table} WHERE ts_code = %s"
                    ).format(table=self._qualified_table()),
                    (code,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "code": row[0],
            "group": row[1],
            "created_at": row[2],
            "updated_at": row[3],
        }

    def is_favorite(self, code: str) -> bool:
        """Check whether the provided stock code is marked as favorite."""
        return self.get_entry(code) is not None

    def add(self, code: str, group: Optional[str] = None) -> Dict[str, Any]:
        """Add or refresh a favorite stock code."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {table} (ts_code, group_name, created_at, updated_at)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (ts_code) DO UPDATE
                        SET group_name = EXCLUDED.group_name,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING ts_code, group_name, created_at, updated_at
                        """
                    ).format(table=self._qualified_table()),
                    (code, group),
                )
                row = cur.fetchone()
        return {
            "code": row[0],
            "group": row[1],
            "created_at": row[2],
            "updated_at": row[3],
        }

    def remove(self, code: str) -> Optional[Dict[str, Any]]:
        """Remove a favorite stock code and return the removed entry, if any."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        DELETE FROM {table}
                        WHERE ts_code = %s
                        RETURNING ts_code, group_name, created_at, updated_at
                        """
                    ).format(table=self._qualified_table()),
                    (code,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "code": row[0],
            "group": row[1],
            "created_at": row[2],
            "updated_at": row[3],
        }

    def count(self) -> int:
        """Return the total number of favorite stocks."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {table}").format(
                        table=self._qualified_table()
                    )
                )
                result = cur.fetchone()
        return int(result[0] or 0)

    def list_groups(self) -> List[Dict[str, Any]]:
        """Return distinct favorite groups with their stock counts."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT group_name, COUNT(*) AS total
                        FROM {table}
                        GROUP BY group_name
                        ORDER BY
                            CASE WHEN group_name IS NULL THEN 0 ELSE 1 END,
                            group_name
                        """
                    ).format(table=self._qualified_table())
                )
                rows = cur.fetchall()
        return [
            {
                "name": row[0],
                "total": int(row[1] or 0),
            }
            for row in rows
        ]


__all__ = ["FavoriteStockDAO"]
